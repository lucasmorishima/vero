# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Validação de Endereços e Dados Cadastrais — V4
# MAGIC
# MAGIC **Diferença vs V3:** Section 4 não chama a BrasilAPI — lê um arquivo dump (CSV / Parquet / Delta)
# MAGIC da Receita Federal, faz JOIN com a base de clientes e insere os CNPJs correspondentes em
# MAGIC `tab_dados_receita` via MERGE incremental.
# MAGIC
# MAGIC **Arquitetura V4:**
# MAGIC ```
# MAGIC spark.sql() → cache() → JOIN tab_dados_receita + tab_dados_cep → CASE WHEN columns → SELECT por tabela → write
# MAGIC tab_dados_receita ← leitura de dump (CSV/Parquet/Delta) + JOIN base clientes + MERGE incremental
# MAGIC ```
# MAGIC
# MAGIC **Saída (tabelas Delta):**
# MAGIC - `hive_metastore.accenture.validacao_enderecos`
# MAGIC - `hive_metastore.accenture.validacao_dados_cadastrais`
# MAGIC - `hive_metastore.accenture.validacao_status_fatura`
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Imports e configuração
# COMMAND ----------

from __future__ import annotations

import concurrent.futures
import re
import time
import unicodedata
from datetime import datetime
from typing import Any

import requests
from delta.tables import DeltaTable
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)


print("V4 — Processamento nativo Spark (sem .toPandas()) | Receita via dump de arquivo")

spark = SparkSession.getActiveSession()

try:
    TOKEN = dbutils.secrets.get(scope="vero", key="correios_token")
except Exception:
    TOKEN = ""

CATALOG        = "hive_metastore.accenture"
TABELA_RECEITA = f"{CATALOG}.tab_dados_receita"
TABELA_CEP     = f"{CATALOG}.tab_dados_cep"

# Limite de registros para processar. None = sem limite (produção).
LIMIT_REGISTROS = None  # ex: 1000 para teste

# ---------------------------------------------------------------------------
# Configuração do dump da Receita Federal
# ---------------------------------------------------------------------------

# Caminho do arquivo dump da Receita Federal
# Suporta: CSV (sep=";"), Parquet, ou Delta
# Exemplos:
#   "dbfs:/FileStore/receita/dump_cnpj.csv"
#   "dbfs:/FileStore/receita/dump_cnpj.parquet"
#   "hive_metastore.accenture.dump_receita_federal"
ARQUIVO_DUMP_RECEITA = "dbfs:/FileStore/receita/dump_cnpj.csv"
FORMATO_DUMP         = "csv"   # "csv", "parquet", "delta"
SEP_CSV              = ";"     # separador quando FORMATO_DUMP = "csv"

# Mapeamento: coluna_no_dump -> campo_em_tab_dados_receita
# Ajuste conforme o schema do arquivo recebido.
# Campos não mapeados ficam NULL na tabela.
MAPA_COLUNAS = {
    "cnpj":                      "CNPJ",                    # 14 dígitos sem máscara
    "razao_social":               "RAZAO_SOCIAL",
    "nome_fantasia":              "NOME_FANTASIA",
    "situacao_cadastral":         "SITUACAO_CADASTRAL",
    "data_situacao_cadastral":    "DATA_SITUACAO_CADASTRAL",
    "motivo_situacao_cadastral":  "MOTIVO_SITUACAO_CADASTRAL",
    "natureza_juridica":          "NATUREZA_JURIDICA",
    "data_inicio_atividade":      "DATA_INICIO_ATIVIDADE",
    "cnae_principal_codigo":      "CNAE_FISCAL",
    "cnae_principal_descricao":   "CNAE_FISCAL_DESCRICAO",
    "porte":                      "PORTE",
    "capital_social":             "CAPITAL_SOCIAL",
    "opcao_simples":              "OPCAO_SIMPLES",
    "opcao_mei":                  "OPCAO_MEI",
    "email":                      "EMAIL",
    "telefone":                   "TELEFONE",
    "receita_cep":                "CEP",
    "receita_logradouro":         "LOGRADOURO",
    "receita_numero":             "NUMERO",
    "receita_complemento":        "COMPLEMENTO",
    "receita_bairro":             "BAIRRO",
    "receita_municipio":          "MUNICIPIO",
    "receita_uf":                 "UF",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DDL — cria tab_dados_receita e tab_dados_cep se não existirem

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABELA_RECEITA} (
        cnpj                      STRING,
        razao_social              STRING,
        nome_fantasia             STRING,
        situacao_cadastral        STRING,
        data_situacao_cadastral   STRING,
        motivo_situacao_cadastral STRING,
        natureza_juridica         STRING,
        data_inicio_atividade     STRING,
        cnae_principal_codigo     BIGINT,
        cnae_principal_descricao  STRING,
        porte                     STRING,
        capital_social            DOUBLE,
        opcao_simples             BOOLEAN,
        opcao_mei                 BOOLEAN,
        email                     STRING,
        telefone                  STRING,
        receita_cep               STRING,
        receita_logradouro        STRING,
        receita_numero            STRING,
        receita_complemento       STRING,
        receita_bairro            STRING,
        receita_municipio         STRING,
        receita_uf                STRING,
        data_consulta             STRING,
        status_consulta           STRING
    )
    USING DELTA
    COMMENT 'Cache de dados cadastrais da Receita Federal por CNPJ.'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABELA_CEP} (
        cep             STRING,
        logradouro      STRING,
        bairro          STRING,
        cidade          STRING,
        uf              STRING,
        complemento     STRING,
        fonte           STRING,
        data_consulta   STRING,
        status_consulta STRING
    )
    USING DELTA
    COMMENT 'Cache de CEPs consultados via ViaCEP/Correios.'
""")

print(f"Tabelas prontas: '{TABELA_RECEITA}' | '{TABELA_CEP}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query — carrega base de clientes (Spark nativo, sem .toPandas())

# COMMAND ----------

_QUERY = """
SELECT
    ROW_NUMBER() OVER (ORDER BY bc.codigocliente) + 1 AS FATURA,
    COALESCE(ca.CONTRATO, ca.ID_CLIENTE)               AS ID_CLIENTE_CONTRATO,
    ca.segmento                                        AS SEGMENTO,

    -- Endereço de instalação
    bc.cidade   AS cidade_instalacao,
    bc.bairro   AS bairro_instalacao,
    bc.cep      AS cep_instalacao,
    bc.uf       AS uf_instalacao,

    -- Endereço legal (cobrança / cadastro)
    bc.cidade   AS cidade_legal,
    bc.bairro   AS bairro_legal,
    bc.cep      AS cep_legal,
    bc.uf       AS uf_legal,

    ca.CPF_CNPJ                              AS CPF_CNPJ,
    ca.NOME_CLIENTE                          AS NOME_CLIENTE,
    ''                                       AS INSCRICAO_ESTADUAL,
    bc.nome_produto                          AS PRODUTO,
    ''                                       AS TIPO_SERVICO,
    ''                                       AS DESCRICAO_SERVICO,
    ''                                       AS TIPO_IMPOSTO,
    ''                                       AS PROMOCAO,
    ''                                       AS GRUPO_LOCALIDADE,
    date_format(current_date(), 'yyyy_MM')   AS ID_LOTE,
    ca.sistema_origem                        AS CRM
FROM hive_metastore.accenture.base_clientes_centralizada bc
LEFT JOIN hive_metastore.accenture.tb_dispersao_competencia_analitica ca
    ON (
        (bc.crm = 'NG' AND ca.ID_CLIENTE = bc.codigocliente)
        OR
        (bc.crm <> 'NG' AND ca.CONTRATO = bc.idcontrato)
    )
WHERE bc.crm = 'NG'
  AND ca.CPF_CNPJ IS NOT NULL
"""

if LIMIT_REGISTROS:
    _QUERY += f"\nLIMIT {LIMIT_REGISTROS}"

sdf = spark.sql(_QUERY)
# MEMORY_AND_DISK: usa memória quando disponível e faz spill para disco
# sem OOM — seguro para qualquer tamanho de cluster
_total = sdf.count()

# Diagnóstico de tipos de documento
_doc_counts = (
    sdf
    .select(F.regexp_replace(F.col("CPF_CNPJ"), r"\D", "").alias("doc"))
    .groupBy(
        F.when(F.length("doc") == 14, F.lit("CNPJ"))
         .when(F.length("doc") == 11, F.lit("CPF"))
         .otherwise(F.lit("INVALIDO"))
         .alias("tipo")
    )
    .count()
    .collect()
)
_cnts = {r["tipo"]: r["count"] for r in _doc_counts}
print(
    f"{_total} registros carregados — "
    f"CPF: {_cnts.get('CPF', 0)} | "
    f"CNPJ: {_cnts.get('CNPJ', 0)} | "
    f"INVALIDO: {_cnts.get('INVALIDO', 0)}"
)
if _cnts.get("CNPJ", 0) == 0:
    print("  AVISO: nenhum CNPJ no lote — DADOS_CADASTRAIS não será gerado.")

display(sdf.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Popula tab_dados_receita — leitura de arquivo dump da Receita Federal

# COMMAND ----------

_SCHEMA_RECEITA = StructType([
    StructField("cnpj",                      StringType(),  True),
    StructField("razao_social",              StringType(),  True),
    StructField("nome_fantasia",             StringType(),  True),
    StructField("situacao_cadastral",        StringType(),  True),
    StructField("data_situacao_cadastral",   StringType(),  True),
    StructField("motivo_situacao_cadastral", StringType(),  True),
    StructField("natureza_juridica",         StringType(),  True),
    StructField("data_inicio_atividade",     StringType(),  True),
    StructField("cnae_principal_codigo",     LongType(),    True),
    StructField("cnae_principal_descricao",  StringType(),  True),
    StructField("porte",                     StringType(),  True),
    StructField("capital_social",            DoubleType(),  True),
    StructField("opcao_simples",             BooleanType(), True),
    StructField("opcao_mei",                 BooleanType(), True),
    StructField("email",                     StringType(),  True),
    StructField("telefone",                  StringType(),  True),
    StructField("receita_cep",               StringType(),  True),
    StructField("receita_logradouro",        StringType(),  True),
    StructField("receita_numero",            StringType(),  True),
    StructField("receita_complemento",       StringType(),  True),
    StructField("receita_bairro",            StringType(),  True),
    StructField("receita_municipio",         StringType(),  True),
    StructField("receita_uf",                StringType(),  True),
    StructField("data_consulta",             StringType(),  True),
    StructField("status_consulta",           StringType(),  True),
])

_VAZIOS_RECEITA = {"", "nan", "NaN", "None", "none", "null", "NULL", "NaT", "na", "NA"}


def _cnpj_valido(cnpj: str) -> bool:
    """Valida CNPJ pelo algoritmo dos dígitos verificadores."""
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    for pesos, pos in (([5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], 12),
                       ([6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], 13)):
        soma = sum(int(cnpj[i]) * pesos[i] for i in range(pos))
        resto = soma % 11
        dv = 0 if resto < 2 else 11 - resto
        if dv != int(cnpj[pos]):
            return False
    return True


# ---------------------------------------------------------------------------
# Tradução: alias do MAPA_COLUNAS → nome real da coluna em tab_dados_receita
# Necessário porque MAPA_COLUNAS usa rótulos legíveis (ex: "CEP", "CNAE_FISCAL")
# enquanto o schema da tabela usa nomes técnicos (ex: "receita_cep", "cnae_principal_codigo").
# ---------------------------------------------------------------------------
_TARGET_TO_SCHEMA = {
    "CNPJ":                     "cnpj",
    "RAZAO_SOCIAL":              "razao_social",
    "NOME_FANTASIA":             "nome_fantasia",
    "SITUACAO_CADASTRAL":        "situacao_cadastral",
    "DATA_SITUACAO_CADASTRAL":   "data_situacao_cadastral",
    "MOTIVO_SITUACAO_CADASTRAL": "motivo_situacao_cadastral",
    "NATUREZA_JURIDICA":         "natureza_juridica",
    "DATA_INICIO_ATIVIDADE":     "data_inicio_atividade",
    "CNAE_FISCAL":               "cnae_principal_codigo",
    "CNAE_FISCAL_DESCRICAO":     "cnae_principal_descricao",
    "PORTE":                     "porte",
    "CAPITAL_SOCIAL":            "capital_social",
    "OPCAO_SIMPLES":             "opcao_simples",
    "OPCAO_MEI":                 "opcao_mei",
    "EMAIL":                     "email",
    "TELEFONE":                  "telefone",
    "CEP":                       "receita_cep",
    "LOGRADOURO":                "receita_logradouro",
    "NUMERO":                    "receita_numero",
    "COMPLEMENTO":               "receita_complemento",
    "BAIRRO":                    "receita_bairro",
    "MUNICIPIO":                 "receita_municipio",
    "UF":                        "receita_uf",
}

# Mapeamento invertido e resolvido: dump_col -> schema_field_name
_DUMP_COL_TO_SCHEMA: dict[str, str] = {}
for _dump_col, _target_alias in MAPA_COLUNAS.items():
    _schema_field = _TARGET_TO_SCHEMA.get(_target_alias.upper())
    if _schema_field:
        _DUMP_COL_TO_SCHEMA[_dump_col] = _schema_field

# ---------------------------------------------------------------------------
# 1. Carrega o arquivo dump
# ---------------------------------------------------------------------------
if FORMATO_DUMP == "csv":
    df_dump = (
        spark.read
        .option("header", "true")
        .option("sep", SEP_CSV)
        .option("encoding", "latin1")
        .csv(ARQUIVO_DUMP_RECEITA)
    )
elif FORMATO_DUMP == "parquet":
    df_dump = spark.read.parquet(ARQUIVO_DUMP_RECEITA)
elif FORMATO_DUMP == "delta":
    df_dump = spark.read.table(ARQUIVO_DUMP_RECEITA)
else:
    raise ValueError(
        f"FORMATO_DUMP inválido: '{FORMATO_DUMP}'. Use 'csv', 'parquet' ou 'delta'."
    )

_dump_cols_set = set(df_dump.columns)
_n_dump_total = df_dump.count()
print(
    f"Dump carregado: {_n_dump_total:,} registros | "
    f"Formato: {FORMATO_DUMP} | Arquivo: {ARQUIVO_DUMP_RECEITA}"
)

# ---------------------------------------------------------------------------
# 2. Identifica coluna CNPJ no dump e normaliza (remove máscara, zpad 14)
# ---------------------------------------------------------------------------
_cnpj_dump_col = None
for _dc, _schema_f in _DUMP_COL_TO_SCHEMA.items():
    if _schema_f == "cnpj" and _dc in _dump_cols_set:
        _cnpj_dump_col = _dc
        break

if _cnpj_dump_col is None:
    raise ValueError(
        "Coluna CNPJ não encontrada no dump. "
        "Verifique a chave mapeada para 'CNPJ' em MAPA_COLUNAS e o schema do arquivo."
    )

df_dump = df_dump.withColumn(
    "_cnpj_norm",
    F.lpad(F.regexp_replace(F.col(_cnpj_dump_col), r"\D", ""), 14, "0"),
)

# ---------------------------------------------------------------------------
# 3. Extrai CNPJs distintos da base de clientes (sdf)
# ---------------------------------------------------------------------------
_cnpjs_base_sdf = (
    sdf
    .select(
        F.lpad(
            F.regexp_replace(F.col("CPF_CNPJ"), r"\D", ""), 14, "0"
        ).alias("cnpj_base")
    )
    .filter(F.length(F.col("cnpj_base")) == 14)
    .distinct()
)

# ---------------------------------------------------------------------------
# 4. Filtra dump: apenas CNPJs que existem na base de clientes (semi-join)
# ---------------------------------------------------------------------------
df_dump_filtrado = (
    df_dump
    .join(_cnpjs_base_sdf, on=F.col("_cnpj_norm") == F.col("cnpj_base"), how="inner")
    .drop("cnpj_base")
)

_n_match = df_dump_filtrado.count()
print(f"CNPJs do dump presentes na base de clientes: {_n_match:,}")

# ---------------------------------------------------------------------------
# 5. Incremental: exclui CNPJs já gravados com status_consulta = 'ok'
# ---------------------------------------------------------------------------
_ja_ok_sdf = spark.sql(
    f"SELECT cnpj AS cnpj_ok FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'"
)

df_novos = (
    df_dump_filtrado
    .join(_ja_ok_sdf, on=F.col("_cnpj_norm") == F.col("cnpj_ok"), how="left_anti")
)
_n_a_merge = df_novos.count()
_n_ja_ok   = _n_match - _n_a_merge
print(
    f"Já em tab_dados_receita (ok): {_n_ja_ok:,} | "
    f"A processar via MERGE: {_n_a_merge:,}"
)

# ---------------------------------------------------------------------------
# 6. Constrói select com renomeação + cast para cada campo do schema
# ---------------------------------------------------------------------------
_BOOL_TRUE_VALS  = {"S", "SIM", "1", "TRUE",  "T", "VERDADEIRO"}
_BOOL_FALSE_VALS = {"N", "NAO", "NÃO", "0", "FALSE", "F", "FALSO"}

_select_exprs = []
for _field in _SCHEMA_RECEITA.fields:
    _fname = _field.name
    if _fname in ("data_consulta", "status_consulta"):
        continue  # adicionados depois

    _dump_col = next(
        (dc for dc, sf in _DUMP_COL_TO_SCHEMA.items() if sf == _fname), None
    )

    if _fname == "cnpj":
        # Usa o CNPJ já normalizado (sem máscara, 14 dígitos, zpad)
        _select_exprs.append(F.col("_cnpj_norm").alias("cnpj"))
        continue

    if _dump_col and _dump_col in _dump_cols_set:
        # Normaliza strings vazias/nulas para NULL
        _raw = F.when(
            F.trim(F.col(_dump_col)).isin(list(_VAZIOS_RECEITA))
            | F.col(_dump_col).isNull()
            | (F.trim(F.col(_dump_col)) == ""),
            F.lit(None).cast(StringType()),
        ).otherwise(F.trim(F.col(_dump_col)))

        if _fname == "cnae_principal_codigo":
            _expr = F.regexp_replace(_raw, r"\D", "").cast(LongType())
        elif _fname == "capital_social":
            _expr = _raw.cast(DoubleType())
        elif _fname in ("opcao_simples", "opcao_mei"):
            _expr = (
                F.when(F.upper(_raw).isin(list(_BOOL_TRUE_VALS)),  F.lit(True))
                 .when(F.upper(_raw).isin(list(_BOOL_FALSE_VALS)), F.lit(False))
                 .otherwise(F.lit(None).cast(BooleanType()))
            )
        else:
            _expr = _raw.cast(StringType())
    else:
        # Coluna ausente no dump — preenche com NULL do tipo correto
        _expr = F.lit(None).cast(_field.dataType)

    _select_exprs.append(_expr.alias(_fname))

# Adiciona colunas geradas
df_para_merge = (
    df_novos
    .select(_select_exprs)
    .withColumn(
        "data_consulta",
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    .withColumn("status_consulta", F.lit("ok"))
)

# ---------------------------------------------------------------------------
# 7. MERGE incremental em tab_dados_receita via DeltaTable
# ---------------------------------------------------------------------------
if _n_a_merge > 0:
    (
        DeltaTable.forName(spark, TABELA_RECEITA)
        .alias("t")
        .merge(df_para_merge.alias("n"), "t.cnpj = n.cnpj")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"MERGE concluído: {_n_a_merge:,} registros inseridos/atualizados.")
else:
    print("Todos os CNPJs do lote já estão na tabela. Nenhum MERGE necessário.")

# ---------------------------------------------------------------------------
# 8. Sumário
# ---------------------------------------------------------------------------
print(
    f"\nResumo — Receita Federal (dump):"
    f"\n  Registros no dump:             {_n_dump_total:,}"
    f"\n  Correspondentes na base:       {_n_match:,}"
    f"\n  Já gravados com status ok:     {_n_ja_ok:,}"
    f"\n  Enviados ao MERGE:             {_n_a_merge:,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Popula tab_dados_cep com CEPs ainda não carregados

# COMMAND ----------

_SCHEMA_CEP = StructType([
    StructField("cep",             StringType(), True),
    StructField("logradouro",      StringType(), True),
    StructField("bairro",          StringType(), True),
    StructField("cidade",          StringType(), True),
    StructField("uf",              StringType(), True),
    StructField("complemento",     StringType(), True),
    StructField("fonte",           StringType(), True),
    StructField("data_consulta",   StringType(), True),
    StructField("status_consulta", StringType(), True),
])


def _buscar_cep_api(cep: str) -> dict:
    """Consulta ViaCEP (fallback Correios) e retorna dict normalizado para Delta."""
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        if TOKEN:
            try:
                r = requests.get(
                    f"https://api.correios.com.br/cep/v2/{cep}",
                    headers={"Authorization": f"Bearer {TOKEN}"},
                    timeout=10,
                )
                r.raise_for_status()
                d = r.json()
                return {"cep": cep, "logradouro": d.get("logradouro", ""),
                        "bairro": d.get("bairro", ""), "cidade": d.get("localidade", ""),
                        "uf": d.get("uf", ""), "complemento": d.get("complemento", ""),
                        "fonte": "Correios API", "data_consulta": data_consulta,
                        "status_consulta": "ok"}
            except Exception:
                pass  # fallback para ViaCEP

        r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
        r.raise_for_status()
        d = r.json()
        if d.get("erro"):
            return {"cep": cep, "data_consulta": data_consulta, "status_consulta": "nao_encontrado"}
        return {"cep": cep, "logradouro": d.get("logradouro", ""),
                "bairro": d.get("bairro", ""), "cidade": d.get("localidade", ""),
                "uf": d.get("uf", ""), "complemento": d.get("complemento", ""),
                "fonte": "ViaCEP", "data_consulta": data_consulta,
                "status_consulta": "ok"}
    except requests.Timeout:
        return {"cep": cep, "data_consulta": data_consulta, "status_consulta": "timeout"}
    except requests.RequestException as exc:
        return {"cep": cep, "data_consulta": data_consulta,
                "status_consulta": f"erro_rede: {str(exc)[:120]}"}


# --- Extrai CEPs distintos usando Spark (sem .toPandas()) ---
_cep_inst_sdf = (
    sdf
    .select(F.lpad(F.regexp_replace(F.col("cep_instalacao"), r"\D", ""), 8, "0").alias("cep"))
    .filter((F.length("cep") == 8) & (F.col("cep") != "00000000"))
)
_cep_legal_sdf = (
    sdf
    .select(F.lpad(F.regexp_replace(F.col("cep_legal"), r"\D", ""), 8, "0").alias("cep"))
    .filter((F.length("cep") == 8) & (F.col("cep") != "00000000"))
)
_cep_receita_sdf = spark.sql(f"""
    SELECT DISTINCT receita_cep AS cep
    FROM {TABELA_RECEITA}
    WHERE status_consulta = 'ok'
      AND receita_cep IS NOT NULL
      AND LENGTH(receita_cep) = 8
      AND receita_cep != '00000000'
""")

ceps_lote: set[str] = {
    r["cep"]
    for r in _cep_inst_sdf.union(_cep_legal_sdf).union(_cep_receita_sdf).distinct().collect()
}

ja_ok_cep: set[str] = {
    r["cep"]
    for r in spark.sql(f"SELECT cep FROM {TABELA_CEP} WHERE status_consulta = 'ok'").collect()
}
ceps_buscar = sorted(ceps_lote - ja_ok_cep)
print(
    f"CEPs no lote: {len(ceps_lote)} | Já na tabela (ok): {len(ja_ok_cep)} | "
    f"A buscar na API: {len(ceps_buscar)}"
)

if ceps_buscar:
    t0_cep = time.time()
    novos_cep: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures_cep = {executor.submit(_buscar_cep_api, cep): cep for cep in ceps_buscar}
        for i, future in enumerate(concurrent.futures.as_completed(futures_cep), 1):
            cep = futures_cep[future]
            try:
                rec = future.result()
                novos_cep.append(rec)
                if rec.get("status_consulta") != "ok":
                    print(f"  [ERRO CEP] {cep} — {rec.get('status_consulta')}")
            except Exception as exc:
                novos_cep.append({"cep": cep, "status_consulta": f"excecao: {exc}",
                                  "data_consulta": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
            if i % 100 == 0 or i == len(ceps_buscar):
                print(f"  {i}/{len(ceps_buscar)} CEPs consultados ({time.time() - t0_cep:.0f}s)")

    def _limpar_rec_cep(r: dict) -> dict:
        limpo = {}
        for campo in [f.name for f in _SCHEMA_CEP.fields]:
            v = r.get(campo)
            limpo[campo] = None if (isinstance(v, str) and v in _VAZIOS_RECEITA) else v
        return limpo

    df_ceps = spark.createDataFrame(
        [_limpar_rec_cep(r) for r in novos_cep], schema=_SCHEMA_CEP
    )
    (
        DeltaTable.forName(spark, TABELA_CEP)
        .alias("t")
        .merge(df_ceps.alias("n"), "t.cep = n.cep")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    ok_cep = sum(1 for r in novos_cep if r.get("status_consulta") == "ok")
    print(
        f"MERGE CEP concluído: {ok_cep} gravados OK | "
        f"{len(novos_cep) - ok_cep} com erro | {time.time() - t0_cep:.1f}s"
    )
else:
    print("Todos os CEPs do lote já estão na tabela.")

print(
    f"Cache CNPJ disponível: {spark.sql(f'SELECT COUNT(*) AS n FROM {TABELA_RECEITA} WHERE status_consulta = \"ok\"').collect()[0]['n']} registros | "
    f"Cache CEP disponível: {spark.sql(f'SELECT COUNT(*) AS n FROM {TABELA_CEP} WHERE status_consulta = \"ok\"').collect()[0]['n']} registros."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. UDFs + helpers

# COMMAND ----------

import unicodedata as _ud


def _normalizar_py(texto: str) -> str:
    if not texto:
        return ""
    s = str(texto).upper().strip()
    s = _ud.normalize("NFD", s)
    s = "".join(c for c in s if _ud.category(c) != "Mn")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# Pandas UDF vetorizado: processa a coluna inteira como pd.Series
# ~10–50x mais rápido que UDF row-by-row para operações de string
@pandas_udf(StringType())
def norm_udf(series: pd.Series) -> pd.Series:
    return series.fillna("").apply(_normalizar_py)

# UDF para validação de dígito verificador de CNPJ (utilitário; não usado no caminho principal)
cnpj_valido_udf = F.udf(_cnpj_valido, BooleanType())

# ---------------------------------------------------------------------------
# _to_spark: mantido por compatibilidade e uso nas seções de populate
# O caminho principal (enriquecimento + output) usa Spark nativo.
# ---------------------------------------------------------------------------
_VAZIOS = {"", "nan", "NaN", "None", "none", "null", "NULL", "NaT", "na", "NA"}


def _to_spark(linhas: list[dict], tipos: dict | None = None):
    """Converte lista de dicts em Spark DataFrame. Mantido para compatibilidade."""
    import pandas as pd
    df_pd = pd.DataFrame(linhas)
    tipos = tipos or {}
    for col in df_pd.columns:
        spark_type = tipos.get(col)
        if isinstance(spark_type, LongType):
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce") \
                           .apply(lambda x: int(x) if pd.notna(x) else None)
        elif isinstance(spark_type, DoubleType):
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce") \
                           .apply(lambda x: float(x) if pd.notna(x) else None)
        elif isinstance(spark_type, BooleanType):
            df_pd[col] = df_pd[col].apply(
                lambda x: None if (x is None or str(x).lower() in _VAZIOS)
                else str(x).lower() not in ("false", "0")
            )
        else:
            df_pd[col] = df_pd[col].astype(str).apply(
                lambda x: None if x in _VAZIOS else x
            )
    schema = StructType([
        StructField(c, tipos.get(c, StringType()), True)
        for c in df_pd.columns
    ])
    return spark.createDataFrame(df_pd, schema=schema)


print("UDFs e helpers carregados: norm_udf | cnpj_valido_udf | _to_spark")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Enrich — computed columns, JOINs, normalized columns, colunas de validação

# COMMAND ----------

# ---------------------------------------------------------------------------
# 7a. Tabelas de lookup com prefixo nos nomes das colunas para evitar ambiguidade
# ---------------------------------------------------------------------------

_receita_raw = spark.sql(f"SELECT * FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'")
receita_lkp  = _receita_raw.select(
    [F.col(c).alias(f"r_{c}") for c in _receita_raw.columns]
)

_cep_raw     = spark.sql(f"SELECT * FROM {TABELA_CEP} WHERE status_consulta = 'ok'")
cep_inst_lkp = _cep_raw.select([F.col(c).alias(f"ci_{c}") for c in _cep_raw.columns])
cep_legal_lkp= _cep_raw.select([F.col(c).alias(f"cl_{c}") for c in _cep_raw.columns])

# ---------------------------------------------------------------------------
# 7b. Colunas computadas pré-JOIN
# ---------------------------------------------------------------------------

sdf_enriched = (
    sdf
    # Documento normalizado e tipo
    .withColumn("_doc_norm",
        F.regexp_replace(F.col("CPF_CNPJ"), r"\D", ""))
    .withColumn("_doc_tipo",
        F.when(F.length("_doc_norm") == 14, F.lit("CNPJ"))
         .when(F.length("_doc_norm") == 11, F.lit("CPF"))
         .otherwise(F.lit("INVALIDO")))
    # CEPs normalizados (apenas dígitos, zero-padded para 8 chars)
    .withColumn("_cep_inst_norm",
        F.lpad(F.regexp_replace(F.col("cep_instalacao"), r"\D", ""), 8, "0"))
    .withColumn("_cep_legal_norm",
        F.lpad(F.regexp_replace(F.col("cep_legal"), r"\D", ""), 8, "0"))
    # Cidade sem sufixo de UF (ex: "SÃO PAULO - SP" → "SÃO PAULO")
    .withColumn("_cidade_inst_clean",
        F.trim(F.regexp_replace(F.col("cidade_instalacao"), r"\s*-\s*\w+$", "")))
    .withColumn("_cidade_legal_clean",
        F.trim(F.regexp_replace(F.col("cidade_legal"), r"\s*-\s*\w+$", "")))
    # Flag de CEP genérico (sede de município, ex: "01000000", "12300000")
    .withColumn("_cep_inst_generico",  F.col("_cep_inst_norm").endswith("000"))
    .withColumn("_cep_legal_generico", F.col("_cep_legal_norm").endswith("000"))
)

# ---------------------------------------------------------------------------
# 7c. JOINs com as tabelas de cache
# ---------------------------------------------------------------------------

# Receita: somente para CNPJs
sdf_enriched = sdf_enriched.join(
    receita_lkp,
    on=(F.col("_doc_norm") == F.col("r_cnpj")) & (F.col("_doc_tipo") == "CNPJ"),
    how="left",
)

# CEP de instalação
sdf_enriched = sdf_enriched.join(
    cep_inst_lkp,
    on=F.col("_cep_inst_norm") == F.col("ci_cep"),
    how="left",
)

# CEP legal
sdf_enriched = sdf_enriched.join(
    cep_legal_lkp,
    on=F.col("_cep_legal_norm") == F.col("cl_cep"),
    how="left",
)

# ---------------------------------------------------------------------------
# 7d. Colunas normalizadas pré-computadas (1 chamada UDF por campo)
#     Usa coalesce(..., '') para garantir string não-nula mesmo quando a UDF
#     é omitida pelo Spark em entradas NULL.
# ---------------------------------------------------------------------------

sdf_enriched = (
    sdf_enriched
    # Campos da base de dados (instalação)
    .withColumn("_norm_cidade_inst",
        F.coalesce(norm_udf(F.col("_cidade_inst_clean")),  F.lit("")))
    .withColumn("_norm_uf_inst",
        F.coalesce(norm_udf(F.col("uf_instalacao")),       F.lit("")))
    # Campos da base de dados (legal)
    .withColumn("_norm_cidade_legal",
        F.coalesce(norm_udf(F.col("_cidade_legal_clean")), F.lit("")))
    .withColumn("_norm_uf_legal",
        F.coalesce(norm_udf(F.col("uf_legal")),            F.lit("")))
    # Campos do join CEP instalação
    .withColumn("_norm_ci_cidade",
        F.coalesce(norm_udf(F.col("ci_cidade")),           F.lit("")))
    .withColumn("_norm_ci_uf",
        F.coalesce(norm_udf(F.col("ci_uf")),               F.lit("")))
    # Campos do join CEP legal
    .withColumn("_norm_cl_cidade",
        F.coalesce(norm_udf(F.col("cl_cidade")),           F.lit("")))
    .withColumn("_norm_cl_uf",
        F.coalesce(norm_udf(F.col("cl_uf")),               F.lit("")))
    # Campos do join Receita
    .withColumn("_norm_razao_rec",
        F.coalesce(norm_udf(F.col("r_razao_social")),      F.lit("")))
    .withColumn("_norm_nome_base",
        F.coalesce(norm_udf(F.col("NOME_CLIENTE")),        F.lit("")))
    .withColumn("_norm_mun_rec",
        F.coalesce(norm_udf(F.col("r_receita_municipio")), F.lit("")))
    .withColumn("_norm_uf_rec",
        F.coalesce(norm_udf(F.col("r_receita_uf")),        F.lit("")))
)

# ---------------------------------------------------------------------------
# 7e. Observação e status — ENDERECO_INSTALACAO
#     Lógica: CEP ausente → genérico → divergência cidade/UF
# ---------------------------------------------------------------------------

sdf_enriched = sdf_enriched.withColumn(
    "_obs_end_inst",
    F.when(
        F.col("ci_cep").isNull(),
        F.concat(F.lit("[CEP] CEP não encontrado no cache: "), F.col("_cep_inst_norm"))
    ).when(
        F.col("_cep_inst_generico"),
        # CEP genérico: registra nota e resultado do município
        F.concat(
            F.lit("[CEP] CEP genérico: "),
            F.substring(F.col("_cep_inst_norm"), 1, 5),
            F.lit("-"),
            F.substring(F.col("_cep_inst_norm"), 6, 3),
            F.lit(" (sede do município) | "),
            F.when(
                (F.col("_norm_cidade_inst") != "") &
                (F.col("_norm_ci_cidade")   != "") &
                (F.col("_norm_cidade_inst") != F.col("_norm_ci_cidade")),
                F.concat(
                    F.lit("Município divergente: base '"),
                    F.col("_cidade_inst_clean"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("ci_cidade"), F.lit("")),
                    F.lit("'")
                )
            ).otherwise(
                F.concat(
                    F.lit("Município confirmado: "),
                    F.coalesce(F.col("ci_cidade"), F.lit("")),
                    F.lit("/"),
                    F.coalesce(F.col("ci_uf"), F.lit(""))
                )
            )
        )
    ).otherwise(
        # CEP encontrado e não-genérico: verifica divergência de cidade e UF
        F.concat_ws(
            " | ",
            F.when(
                (F.col("_norm_cidade_inst") != "") &
                (F.col("_norm_ci_cidade")   != "") &
                (F.col("_norm_cidade_inst") != F.col("_norm_ci_cidade")),
                F.concat(
                    F.lit("Cidade divergente: base '"),
                    F.col("_cidade_inst_clean"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("ci_cidade"), F.lit("")),
                    F.lit("'")
                )
            ),
            F.when(
                (F.col("_norm_uf_inst") != "") &
                (F.col("_norm_ci_uf")   != "") &
                (F.col("_norm_uf_inst") != F.col("_norm_ci_uf")),
                F.concat(
                    F.lit("UF divergente: base '"),
                    F.col("uf_instalacao"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("ci_uf"), F.lit("")),
                    F.lit("'")
                )
            ),
        )
    )
)

sdf_enriched = sdf_enriched.withColumn(
    "_sv_end_inst",
    F.when(
        F.col("_obs_end_inst").isNull() | (F.col("_obs_end_inst") == ""),
        F.lit("Confere")
    ).otherwise(F.lit("Divergente"))
)

# ---------------------------------------------------------------------------
# 7f. Observação e status — ENDERECO_LEGAL
#     Parte CEP (igual ao instalação mas usando cl_* e cep_legal_*)
#     Parte Receita (somente CNPJ): compara CEP, cidade e UF com os dados da RF
# ---------------------------------------------------------------------------

# Passo intermediário: obs baseada apenas no CEP legal
sdf_enriched = sdf_enriched.withColumn(
    "_obs_cep_legal",
    F.when(
        F.col("cl_cep").isNull(),
        F.concat(F.lit("[CEP] CEP não encontrado no cache: "), F.col("_cep_legal_norm"))
    ).when(
        F.col("_cep_legal_generico"),
        F.concat(
            F.lit("[CEP] CEP genérico: "),
            F.substring(F.col("_cep_legal_norm"), 1, 5),
            F.lit("-"),
            F.substring(F.col("_cep_legal_norm"), 6, 3),
            F.lit(" (sede do município) | "),
            F.when(
                (F.col("_norm_cidade_legal") != "") &
                (F.col("_norm_cl_cidade")    != "") &
                (F.col("_norm_cidade_legal") != F.col("_norm_cl_cidade")),
                F.concat(
                    F.lit("Município divergente: base '"),
                    F.col("_cidade_legal_clean"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("cl_cidade"), F.lit("")),
                    F.lit("'")
                )
            ).otherwise(
                F.concat(
                    F.lit("Município confirmado: "),
                    F.coalesce(F.col("cl_cidade"), F.lit("")),
                    F.lit("/"),
                    F.coalesce(F.col("cl_uf"), F.lit(""))
                )
            )
        )
    ).otherwise(
        F.concat_ws(
            " | ",
            F.when(
                (F.col("_norm_cidade_legal") != "") &
                (F.col("_norm_cl_cidade")    != "") &
                (F.col("_norm_cidade_legal") != F.col("_norm_cl_cidade")),
                F.concat(
                    F.lit("Cidade divergente: base '"),
                    F.col("_cidade_legal_clean"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("cl_cidade"), F.lit("")),
                    F.lit("'")
                )
            ),
            F.when(
                (F.col("_norm_uf_legal") != "") &
                (F.col("_norm_cl_uf")    != "") &
                (F.col("_norm_uf_legal") != F.col("_norm_cl_uf")),
                F.concat(
                    F.lit("UF divergente: base '"),
                    F.col("uf_legal"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("cl_uf"), F.lit("")),
                    F.lit("'")
                )
            ),
        )
    )
)

# Obs final para ENDERECO_LEGAL:
# - CNPJ: combina checagem de CEP + checagem da Receita Federal
# - CPF:  apenas checagem de CEP (idem ENDERECO_INSTALACAO mas com endereço legal)
sdf_enriched = sdf_enriched.withColumn(
    "_obs_end_legal",
    F.when(
        F.col("_doc_tipo") == "CNPJ",
        F.concat_ws(
            " | ",
            # Parte CEP (reusa _obs_cep_legal, ignorando null/vazio)
            F.when(
                F.col("_obs_cep_legal").isNotNull() & (F.col("_obs_cep_legal") != ""),
                F.col("_obs_cep_legal")
            ),
            # Receita: CNPJ ausente no cache
            F.when(
                F.col("r_cnpj").isNull(),
                F.lit("[CACHE] CNPJ não encontrado em tab_dados_receita")
            ),
            # Receita: CEP diferente do registrado na RF
            F.when(
                F.col("r_cnpj").isNotNull() &
                (F.coalesce(F.col("_cep_legal_norm"), F.lit(""))  != "") &
                (F.coalesce(F.col("r_receita_cep"),   F.lit(""))  != "") &
                (F.col("_cep_legal_norm") != F.col("r_receita_cep")),
                F.concat(
                    F.lit("CEP divergente: base '"),
                    F.col("_cep_legal_norm"),
                    F.lit("' x Receita '"),
                    F.col("r_receita_cep"),
                    F.lit("'")
                )
            ),
            # Receita: cidade diferente da RF
            F.when(
                F.col("r_cnpj").isNotNull() &
                (F.col("_norm_cidade_legal") != "") &
                (F.col("_norm_mun_rec")      != "") &
                (F.col("_norm_cidade_legal") != F.col("_norm_mun_rec")),
                F.concat(
                    F.lit("Cidade divergente: base '"),
                    F.col("_cidade_legal_clean"),
                    F.lit("' x Receita '"),
                    F.coalesce(F.col("r_receita_municipio"), F.lit("")),
                    F.lit("'")
                )
            ),
            # Receita: UF diferente da RF
            F.when(
                F.col("r_cnpj").isNotNull() &
                (F.col("_norm_uf_legal") != "") &
                (F.col("_norm_uf_rec")   != "") &
                (F.col("_norm_uf_legal") != F.col("_norm_uf_rec")),
                F.concat(
                    F.lit("UF divergente: base '"),
                    F.col("uf_legal"),
                    F.lit("' x Receita '"),
                    F.coalesce(F.col("r_receita_uf"), F.lit("")),
                    F.lit("'")
                )
            ),
        )
    ).otherwise(
        # CPF: somente verificação de CEP
        F.col("_obs_cep_legal")
    )
)

sdf_enriched = sdf_enriched.withColumn(
    "_sv_end_legal",
    F.when(
        F.col("_obs_end_legal").isNull() | (F.col("_obs_end_legal") == ""),
        F.lit("Confere")
    ).otherwise(F.lit("Divergente"))
)

# ---------------------------------------------------------------------------
# 7g. Observação e status — DADOS_CADASTRAIS (CNPJ only)
# ---------------------------------------------------------------------------

sdf_enriched = sdf_enriched.withColumn(
    "_obs_cad",
    F.concat_ws(
        " | ",
        # CNPJ ausente no cache
        F.when(
            F.col("r_cnpj").isNull(),
            F.lit("[CACHE] CNPJ não encontrado em tab_dados_receita. Execute a célula de populate.")
        ),
        # Situação cadastral diferente de ATIVA
        F.when(
            F.col("r_cnpj").isNotNull() &
            F.col("r_situacao_cadastral").isNotNull() &
            (F.upper(F.col("r_situacao_cadastral")) != "ATIVA"),
            F.concat(F.lit("Situação: "), F.col("r_situacao_cadastral"))
        ),
        # Razão social divergente
        F.when(
            F.col("r_cnpj").isNotNull() &
            (F.col("_norm_razao_rec")  != "") &
            (F.col("_norm_nome_base")  != "") &
            (F.col("_norm_razao_rec")  != F.col("_norm_nome_base")),
            F.concat(
                F.lit("Razão social divergente: base '"),
                F.substring(F.coalesce(F.col("NOME_CLIENTE"),    F.lit("")), 1, 40),
                F.lit("' x Receita '"),
                F.substring(F.coalesce(F.col("r_razao_social"), F.lit("")), 1, 40),
                F.lit("'")
            )
        ),
    )
)

sdf_enriched = sdf_enriched.withColumn(
    "_sv_cad",
    F.when(F.col("r_cnpj").isNull() & (F.col("_doc_tipo") == "CNPJ"), F.lit("nao_carregado"))
     .when(F.col("_obs_cad").isNull() | (F.col("_obs_cad") == ""),    F.lit("Confere"))
     .otherwise(F.lit("Divergente"))
)

# Persiste sdf_enriched com spill para disco — libera sdf base logo depois
# para não ocupar memória desnecessária durante as seções de output
_n_enriched = sdf_enriched.count()
print(f"sdf_enriched: {_n_enriched} linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8a. validacao_enderecos — UNION de ENDERECO_INSTALACAO + ENDERECO_LEGAL + DADOS_CADASTRAIS + INVALIDOS

# COMMAND ----------

_null = F.lit(None).cast(StringType())

# Colunas comuns de dimensão (presentes em todos os membros do UNION)
_DIMS = [
    F.col("PRODUTO").alias("Produto"),
    F.col("TIPO_SERVICO").alias("Tipo_Servico"),
    F.col("DESCRICAO_SERVICO").alias("Desc_Servico"),
    F.col("TIPO_IMPOSTO").alias("Tipo_Imposto"),
    F.col("PROMOCAO").alias("Promocao"),
    F.col("GRUPO_LOCALIDADE").alias("Grupo_Localidade"),
    F.col("ID_LOTE").alias("ID_Lote"),
    F.col("CRM"),
]

# ------------------------------------------------------------------
# 1) ENDERECO_INSTALACAO — todos os tipos de doc exceto INVALIDO
# ------------------------------------------------------------------
sdf_end_inst = sdf_enriched.filter(F.col("_doc_tipo") != "INVALIDO").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("ENDERECO_INSTALACAO").alias("REGRA"),
    F.col("SEGMENTO"),
    F.col("_doc_norm").alias("Documento"),
    F.col("_doc_tipo").alias("Tipo"),
    # CEP de instalação
    F.col("_cep_inst_norm").alias("CEP_Instalacao"),
    _null.alias("CEP_Legal"),
    F.col("ci_logradouro").alias("Logradouro_CEP"),
    F.col("ci_bairro").alias("Bairro_CEP"),
    F.col("ci_cidade").alias("Cidade_CEP"),
    F.col("ci_uf").alias("UF_CEP"),
    F.when(
        F.col("ci_cep").isNotNull() &
        (F.col("_norm_cidade_inst") == F.col("_norm_ci_cidade")),
        F.lit("Sim")
    ).otherwise(F.lit("Não")).alias("Cidade_Confere"),
    F.when(
        F.col("ci_cep").isNotNull() &
        (F.col("_norm_uf_inst") == F.col("_norm_ci_uf")),
        F.lit("Sim")
    ).otherwise(F.lit("Não")).alias("UF_Confere"),
    F.col("ci_fonte").alias("Fonte_CEP"),
    # Campos de Receita (null para instalação)
    _null.alias("CEP_Receita"),
    _null.alias("Cidade_Receita"),
    _null.alias("UF_Receita"),
    _null.alias("Razao_Social"),
    _null.alias("Situacao_Cadastral"),
    # Validação
    F.col("_sv_end_inst").alias("Status_Validacao"),
    F.col("_obs_end_inst").alias("Observacao"),
    *_DIMS,
)

# ------------------------------------------------------------------
# 2) ENDERECO_LEGAL — somente CNPJ
# ------------------------------------------------------------------
sdf_end_legal = sdf_enriched.filter(F.col("_doc_tipo") == "CNPJ").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("ENDERECO_LEGAL").alias("REGRA"),
    F.col("SEGMENTO"),
    F.col("_doc_norm").alias("Documento"),
    F.col("_doc_tipo").alias("Tipo"),
    # CEP legal
    _null.alias("CEP_Instalacao"),
    F.col("_cep_legal_norm").alias("CEP_Legal"),
    F.col("cl_logradouro").alias("Logradouro_CEP"),
    F.col("cl_bairro").alias("Bairro_CEP"),
    F.col("cl_cidade").alias("Cidade_CEP"),
    F.col("cl_uf").alias("UF_CEP"),
    F.when(
        F.col("cl_cep").isNotNull() &
        (F.col("_norm_cidade_legal") == F.col("_norm_cl_cidade")),
        F.lit("Sim")
    ).otherwise(F.lit("Não")).alias("Cidade_Confere"),
    F.when(
        F.col("cl_cep").isNotNull() &
        (F.col("_norm_uf_legal") == F.col("_norm_cl_uf")),
        F.lit("Sim")
    ).otherwise(F.lit("Não")).alias("UF_Confere"),
    F.col("cl_fonte").alias("Fonte_CEP"),
    # Campos de Receita (para CNPJ)
    F.col("r_receita_cep").alias("CEP_Receita"),
    F.col("r_receita_municipio").alias("Cidade_Receita"),
    F.col("r_receita_uf").alias("UF_Receita"),
    F.col("r_razao_social").alias("Razao_Social"),
    F.col("r_situacao_cadastral").alias("Situacao_Cadastral"),
    # Validação
    F.col("_sv_end_legal").alias("Status_Validacao"),
    F.col("_obs_end_legal").alias("Observacao"),
    *_DIMS,
)

# ------------------------------------------------------------------
# 3) DADOS_CADASTRAIS — somente CNPJ (linha resumida na tabela de endereços)
# ------------------------------------------------------------------
sdf_dados_cad_val = sdf_enriched.filter(F.col("_doc_tipo") == "CNPJ").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("DADOS_CADASTRAIS").alias("REGRA"),
    F.col("SEGMENTO"),
    F.col("_doc_norm").alias("Documento"),
    F.lit("CNPJ").alias("Tipo"),
    # Sem CEP
    _null.alias("CEP_Instalacao"),
    _null.alias("CEP_Legal"),
    _null.alias("Logradouro_CEP"),
    _null.alias("Bairro_CEP"),
    _null.alias("Cidade_CEP"),
    _null.alias("UF_CEP"),
    _null.alias("Cidade_Confere"),
    _null.alias("UF_Confere"),
    _null.alias("Fonte_CEP"),
    _null.alias("CEP_Receita"),
    _null.alias("Cidade_Receita"),
    _null.alias("UF_Receita"),
    F.col("r_razao_social").alias("Razao_Social"),
    F.col("r_situacao_cadastral").alias("Situacao_Cadastral"),
    # Validação
    F.col("_sv_cad").alias("Status_Validacao"),
    F.col("_obs_cad").alias("Observacao"),
    *_DIMS,
)

# ------------------------------------------------------------------
# 4) INVALIDOS
# ------------------------------------------------------------------
sdf_invalidos = sdf_enriched.filter(F.col("_doc_tipo") == "INVALIDO").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("INVALIDO").alias("REGRA"),
    F.col("SEGMENTO"),
    F.col("CPF_CNPJ").alias("Documento"),
    F.lit("INVALIDO").alias("Tipo"),
    _null.alias("CEP_Instalacao"),
    _null.alias("CEP_Legal"),
    _null.alias("Logradouro_CEP"),
    _null.alias("Bairro_CEP"),
    _null.alias("Cidade_CEP"),
    _null.alias("UF_CEP"),
    _null.alias("Cidade_Confere"),
    _null.alias("UF_Confere"),
    _null.alias("Fonte_CEP"),
    _null.alias("CEP_Receita"),
    _null.alias("Cidade_Receita"),
    _null.alias("UF_Receita"),
    _null.alias("Razao_Social"),
    _null.alias("Situacao_Cadastral"),
    F.lit("Documento inválido").alias("Status_Validacao"),
    _null.alias("Observacao"),
    *_DIMS,
)

# UNION final
validacao_enderecos = (
    sdf_end_inst
    .union(sdf_end_legal)
    .union(sdf_dados_cad_val)
    .union(sdf_invalidos)
)

# Contagens por regra para diagnóstico
_counts_end = (
    validacao_enderecos
    .groupBy("REGRA")
    .count()
    .collect()
)
for r in _counts_end:
    print(f"  validacao_enderecos → {r['REGRA']}: {r['count']} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8b. validacao_dados_cadastrais — detalhamento completo da Receita Federal

# COMMAND ----------

validacao_dados_cadastrais = sdf_enriched.filter(F.col("_doc_tipo") == "CNPJ").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("DADOS_CADASTRAIS").alias("REGRA"),
    F.col("_doc_norm").alias("CNPJ"),
    F.col("r_razao_social").alias("Razao_Social"),
    F.col("r_nome_fantasia").alias("Nome_Fantasia"),
    F.col("r_situacao_cadastral").alias("Situacao_Cadastral"),
    F.col("r_data_situacao_cadastral").alias("Data_Situacao_Cadastral"),
    F.col("r_motivo_situacao_cadastral").alias("Motivo_Situacao"),
    F.col("r_natureza_juridica").alias("Natureza_Juridica"),
    F.col("r_data_inicio_atividade").alias("Data_Inicio_Atividade"),
    F.regexp_replace(F.col("r_cnae_principal_codigo").cast(StringType()), r"\D", "").cast(LongType()).alias("CNAE_Principal_Codigo"),
    F.col("r_cnae_principal_descricao").alias("CNAE_Principal_Descricao"),
    F.col("r_porte").alias("Porte"),
    F.col("r_capital_social").cast(DoubleType()).alias("Capital_Social"),
    F.col("r_opcao_simples").cast(BooleanType()).alias("Simples_Nacional"),
    F.col("r_opcao_mei").cast(BooleanType()).alias("MEI"),
    F.col("r_email").alias("Email"),
    F.col("r_telefone").alias("Telefone"),
    F.col("r_receita_cep").alias("CEP_Receita"),
    F.col("r_receita_logradouro").alias("Logradouro_Receita"),
    F.col("r_receita_numero").alias("Numero_Receita"),
    F.col("r_receita_complemento").alias("Complemento_Receita"),
    F.col("r_receita_bairro").alias("Bairro_Receita"),
    F.col("r_receita_municipio").alias("Municipio_Receita"),
    F.col("r_receita_uf").alias("UF_Receita"),
    F.col("r_data_consulta").alias("Data_Consulta_Receita"),
    # Dimensões comuns
    F.col("SEGMENTO"),
    F.col("PRODUTO").alias("Produto"),
    F.col("TIPO_SERVICO").alias("Tipo_Servico"),
    F.col("DESCRICAO_SERVICO").alias("Desc_Servico"),
    F.col("TIPO_IMPOSTO").alias("Tipo_Imposto"),
    F.col("PROMOCAO").alias("Promocao"),
    F.col("GRUPO_LOCALIDADE").alias("Grupo_Localidade"),
    F.col("ID_LOTE").alias("ID_Lote"),
    F.col("CRM"),
)

_n_cad = validacao_dados_cadastrais.count()
print(f"  validacao_dados_cadastrais: {_n_cad} linhas (CNPJs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8c. validacao_status_fatura — relatório consolidado de status por regra

# COMMAND ----------

# Helper: expressa STATUS e SUBSTATUS a partir de uma coluna de sv (string)
def _status_cols(sv_col: str):
    """Retorna (status_expr, substatus_expr) baseado na coluna de sv."""
    status = (
        F.when(F.col(sv_col) == "Confere",        F.lit("CORRETO"))
         .when(F.col(sv_col) == "nao_carregado",  F.lit("PENDENTE"))
         .otherwise(F.lit("INCORRETO"))
    )
    substatus = (
        F.when(F.col(sv_col) == "Confere",        F.lit("OK"))
         .when(F.col(sv_col) == "nao_carregado",  F.lit("CACHE_VAZIO"))
         .otherwise(F.lit("ERRO"))
    )
    return status, substatus


_st_inst,  _ss_inst  = _status_cols("_sv_end_inst")
_st_legal, _ss_legal = _status_cols("_sv_end_legal")
_st_cad,   _ss_cad   = _status_cols("_sv_cad")

# Colunas de contexto para diagnóstico
_billing_inst = F.concat_ws(
    " | ",
    F.concat(F.lit("NOME: "),   F.coalesce(F.col("NOME_CLIENTE"),    F.lit(""))),
    F.concat(F.lit("DOC: "),    F.col("_doc_norm")),
    F.concat(F.lit("CEP: "),    F.col("_cep_inst_norm")),
    F.concat(F.lit("CIDADE: "), F.coalesce(F.col("cidade_instalacao"), F.lit(""))),
    F.concat(F.lit("UF: "),     F.coalesce(F.col("uf_instalacao"),     F.lit(""))),
)
_billing_legal = F.concat_ws(
    " | ",
    F.concat(F.lit("NOME: "),   F.coalesce(F.col("NOME_CLIENTE"),  F.lit(""))),
    F.concat(F.lit("DOC: "),    F.col("_doc_norm")),
    F.concat(F.lit("CEP: "),    F.col("_cep_legal_norm")),
    F.concat(F.lit("CIDADE: "), F.coalesce(F.col("cidade_legal"),  F.lit(""))),
    F.concat(F.lit("UF: "),     F.coalesce(F.col("uf_legal"),       F.lit(""))),
)
_billing_cad = F.concat_ws(
    " | ",
    F.concat(F.lit("NOME: "), F.coalesce(F.col("NOME_CLIENTE"), F.lit(""))),
    F.concat(F.lit("DOC: "),  F.col("_doc_norm")),
)

_tv_inst = F.concat_ws(
    " | ",
    F.concat(F.lit("CEP: "),        F.col("_cep_inst_norm")),
    F.concat(F.lit("LOGRADOURO: "), F.coalesce(F.col("ci_logradouro"), F.lit(""))),
    F.concat(F.lit("CIDADE: "),     F.coalesce(F.col("ci_cidade"),     F.lit(""))),
    F.concat(F.lit("UF: "),         F.coalesce(F.col("ci_uf"),         F.lit(""))),
)
_tv_legal = F.concat_ws(
    " | ",
    F.concat(F.lit("CEP: "),        F.col("_cep_legal_norm")),
    F.concat(F.lit("LOGRADOURO: "), F.coalesce(F.col("cl_logradouro"), F.lit(""))),
    F.concat(F.lit("CIDADE: "),     F.coalesce(F.col("cl_cidade"),     F.lit(""))),
    F.concat(F.lit("UF: "),         F.coalesce(F.col("cl_uf"),         F.lit(""))),
)
_contrato_legal = F.when(
    F.col("r_cnpj").isNotNull(),
    F.concat_ws(
        " | ",
        F.concat(F.lit("RAZAO: "),   F.coalesce(F.col("r_razao_social"),      F.lit(""))),
        F.concat(F.lit("CEP: "),     F.coalesce(F.col("r_receita_cep"),        F.lit(""))),
        F.concat(F.lit("CIDADE: "),  F.coalesce(F.col("r_receita_municipio"),  F.lit(""))),
        F.concat(F.lit("/"),         F.coalesce(F.col("r_receita_uf"),         F.lit(""))),
    )
)

# Status dims (comuns a todos os membros do UNION)
_STATUS_DIMS = [
    F.col("PRODUTO").alias("Produto"),
    F.col("TIPO_SERVICO").alias("Tipo_Servico"),
    F.col("DESCRICAO_SERVICO").alias("Desc_Servico"),
    F.col("TIPO_IMPOSTO").alias("Tipo_Imposto"),
    F.col("PROMOCAO").alias("Promocao"),
    F.col("GRUPO_LOCALIDADE").alias("Grupo_Localidade"),
    F.col("ID_LOTE").alias("ID_Lote"),
    F.col("CRM"),
]

# ENDERECO_INSTALACAO
_sf_inst = sdf_enriched.filter(F.col("_doc_tipo") != "INVALIDO").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("ENDERECO_INSTALACAO").alias("REGRA"),
    F.col("SEGMENTO"),
    _st_inst.alias("STATUS"),
    _ss_inst.alias("SUBSTATUS"),
    F.col("_obs_end_inst").alias("OBSERVACAO"),
    _billing_inst.alias("DADOS_BILLING"),
    _null.alias("DADOS_CONTRATO"),
    _tv_inst.alias("DADOS_TABELA_VERDADE"),
    *_STATUS_DIMS,
)

# ENDERECO_LEGAL (CNPJ only)
_sf_legal = sdf_enriched.filter(F.col("_doc_tipo") == "CNPJ").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("ENDERECO_LEGAL").alias("REGRA"),
    F.col("SEGMENTO"),
    _st_legal.alias("STATUS"),
    _ss_legal.alias("SUBSTATUS"),
    F.col("_obs_end_legal").alias("OBSERVACAO"),
    _billing_legal.alias("DADOS_BILLING"),
    _contrato_legal.alias("DADOS_CONTRATO"),
    _tv_legal.alias("DADOS_TABELA_VERDADE"),
    *_STATUS_DIMS,
)

# DADOS_CADASTRAIS (CNPJ only)
_sf_cad = sdf_enriched.filter(F.col("_doc_tipo") == "CNPJ").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("DADOS_CADASTRAIS").alias("REGRA"),
    F.col("SEGMENTO"),
    _st_cad.alias("STATUS"),
    _ss_cad.alias("SUBSTATUS"),
    F.col("_obs_cad").alias("OBSERVACAO"),
    _billing_cad.alias("DADOS_BILLING"),
    F.when(
        F.col("r_cnpj").isNotNull(),
        F.concat_ws(
            " | ",
            F.concat(F.lit("RAZAO: "),    F.coalesce(F.col("r_razao_social"),     F.lit(""))),
            F.concat(F.lit("SITUACAO: "), F.coalesce(F.col("r_situacao_cadastral"), F.lit(""))),
            F.concat(F.lit("CEP: "),      F.coalesce(F.col("r_receita_cep"),       F.lit(""))),
            F.concat(F.lit("CIDADE: "),   F.coalesce(F.col("r_receita_municipio"), F.lit(""))),
            F.concat(F.lit("/"),          F.coalesce(F.col("r_receita_uf"),         F.lit(""))),
        )
    ).alias("DADOS_CONTRATO"),
    _null.alias("DADOS_TABELA_VERDADE"),
    *_STATUS_DIMS,
)

# INVALIDOS
_sf_inv = sdf_enriched.filter(F.col("_doc_tipo") == "INVALIDO").select(
    F.col("FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("INVALIDO").alias("REGRA"),
    F.col("SEGMENTO"),
    F.lit("INCORRETO").alias("STATUS"),
    F.lit("ERRO").alias("SUBSTATUS"),
    F.lit("[DOC] Documento inválido").alias("OBSERVACAO"),
    F.concat(F.lit("DOC: "), F.coalesce(F.col("CPF_CNPJ"), F.lit(""))).alias("DADOS_BILLING"),
    _null.alias("DADOS_CONTRATO"),
    _null.alias("DADOS_TABELA_VERDADE"),
    *_STATUS_DIMS,
)

validacao_status_fatura = (
    _sf_inst
    .union(_sf_legal)
    .union(_sf_cad)
    .union(_sf_inv)
)

_n_status = validacao_status_fatura.count()
print(f"  validacao_status_fatura: {_n_status} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Grava resultados nas tabelas Delta

# COMMAND ----------

# Garante que colunas nullable não bloqueiem o append
for _tbl, _col in [
    ("validacao_status_fatura",    "CRM"),
    ("validacao_enderecos",        "CRM"),
    ("validacao_dados_cadastrais", "CRM"),
]:
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.{_tbl} ALTER COLUMN {_col} DROP NOT NULL")
    except Exception:
        pass

# Grava as 3 tabelas de output
(
    validacao_enderecos
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.validacao_enderecos")
)

(
    validacao_dados_cadastrais
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.validacao_dados_cadastrais")
)

(
    validacao_status_fatura
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.validacao_status_fatura")
)

# Contagens finais (sdf_enriched ainda está em cache — leitura rápida)
n_end_inst  = sdf_end_inst.count()
n_end_legal = sdf_end_legal.count()
n_cad       = sdf_dados_cad_val.count()


print(
    f"\nGravação concluída."
    f"\n  DADOS_CADASTRAIS={n_cad}"
    f" | ENDERECO_LEGAL={n_end_legal}"
    f" | ENDERECO_INSTALACAO={n_end_inst}"
    f"\n  → {CATALOG}.validacao_enderecos"
    f"\n  → {CATALOG}.validacao_dados_cadastrais"
    f"\n  → {CATALOG}.validacao_status_fatura"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verificação rápida dos resultados

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT STATUS, SUBSTATUS, COUNT(*) AS QTD
        FROM {CATALOG}.validacao_status_fatura
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
)
