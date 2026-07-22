# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação de Endereços e Dados Cadastrais — V2
# MAGIC
# MAGIC **Diferença vs V1:** dados da Receita Federal são persistidos em
# MAGIC `hive_metastore.accenture.tab_dados_receita` na primeira vez que um CNPJ é encontrado.
# MAGIC Nas execuções seguintes o CNPJ já está na tabela e nenhuma chamada à API é feita.
# MAGIC
# MAGIC **Fluxo por execução:**
# MAGIC 1. Cria `tab_dados_receita` se não existir (DDL automático)
# MAGIC 2. Detecta CNPJs do lote que ainda não estão na tabela
# MAGIC 3. Busca somente esses na BrasilAPI e faz MERGE na tabela
# MAGIC 4. Carrega cache em memória e valida offline (sem chamadas à Receita)
# MAGIC
# MAGIC **Fonte:** `hive_metastore.accenture.base_clientes_centralizada` + `tb_dispersao_competencia_analitica`
# MAGIC
# MAGIC **Saída (tabelas Delta):**
# MAGIC - `hive_metastore.accenture.validacao_enderecos`
# MAGIC - `hive_metastore.accenture.validacao_dados_cadastrais`
# MAGIC - `hive_metastore.accenture.validacao_status_fatura`
# MAGIC
# MAGIC **Processamento:** `ThreadPoolExecutor(max_workers=20)` · apenas ViaCEP em runtime · sem chamadas à BrasilAPI durante o batimento

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e configuração

# COMMAND ----------

from __future__ import annotations

import concurrent.futures
import re
import threading
import time
import unicodedata
from datetime import datetime
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

spark = SparkSession.getActiveSession()

try:
    TOKEN = dbutils.secrets.get(scope="vero", key="correios_token")
except Exception:
    TOKEN = ""

CATALOG        = "hive_metastore.accenture"
TABELA_RECEITA = f"{CATALOG}.tab_dados_receita"
TABELA_CEP     = f"{CATALOG}.tab_dados_cep"

# Limite de registros para processar. None = sem limite (processa tudo).
LIMIT_REGISTROS = None  # ex: 1000 para teste, None para produção

# BrasilAPI: 1 chamada simultânea para evitar 429
_brasilapi_sem = threading.Semaphore(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DDL — cria tab_dados_receita se não existir

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
    COMMENT 'Cache de dados cadastrais da Receita Federal por CNPJ. Populada automaticamente pelo notebook de validação.'
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
    COMMENT 'Cache de CEPs consultados via ViaCEP/Correios. Populada automaticamente pelo notebook de validação.'
""")

print(f"Tabelas prontas: '{TABELA_RECEITA}' | '{TABELA_CEP}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query — carrega base de clientes

# COMMAND ----------

_QUERY = """
SELECT
    ROW_NUMBER() OVER (ORDER BY bc.codigocliente) + 1          AS FATURA,
    COALESCE(ca.CONTRATO, ca.ID_CLIENTE)                       AS ID_CLIENTE_CONTRATO,
    'DADOS CADASTRAIS'                                         AS REGRA,
    ca.segmento                                                AS SEGMENTO,
    bc.cidade                                                  AS cidade,
    bc.bairro                                                  AS bairro,
    bc.cep                                                     AS cep,
    bc.uf                                                      AS uf,
    ca.CPF_CNPJ                                                AS CPF_CNPJ,
    ca.NOME_CLIENTE                                            AS NOME_CLIENTE,
    ''                                                         AS INSCRICAO_ESTADUAL,
    bc.nome_produto                                            AS PRODUTO,
    ''                                                         AS TIPO_SERVICO,
    ''                                                         AS DESCRICAO_SERVICO,
    ''                                                         AS TIPO_IMPOSTO,
    ''                                                         AS PROMOCAO,
    ''                                                         AS GRUPO_LOCALIDADE,
    date_format(current_date(), 'yyyy_MM')                     AS ID_LOTE,
    ca.sistema_origem                                          AS CRM
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
    _QUERY += f"LIMIT {LIMIT_REGISTROS}"

df = spark.sql(_QUERY).toPandas().astype(str)
print(f"{len(df)} registros carregados.")
display(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Popula tab_dados_receita com CNPJs ainda não carregados

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


def _consultar_receita_api(cnpj: str) -> dict:
    """Consulta BrasilAPI e retorna dict normalizado para gravar no Delta."""
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(cnpj)}"
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "VeroValidacaoEnderecos/2.0"})
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        with _brasilapi_sem:
            resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return {"cnpj": cnpj, "data_consulta": data_consulta,
                    "status_consulta": f"erro_http_{resp.status_code}"}
        p = resp.json()
        cep_raw = (p.get("cep") or "").replace("-", "").replace(".", "").strip()
        return {
            "cnpj":                      cnpj,
            "razao_social":              p.get("razao_social"),
            "nome_fantasia":             p.get("nome_fantasia"),
            "situacao_cadastral":        p.get("descricao_situacao_cadastral"),
            "data_situacao_cadastral":   p.get("data_situacao_cadastral"),
            "motivo_situacao_cadastral": p.get("descricao_motivo_situacao_cadastral"),
            "natureza_juridica":         p.get("descricao_natureza_juridica"),
            "data_inicio_atividade":     p.get("data_inicio_atividade"),
            "cnae_principal_codigo":     p.get("cnae_fiscal"),
            "cnae_principal_descricao":  p.get("cnae_fiscal_descricao"),
            "porte":                     p.get("descricao_porte"),
            "capital_social":            p.get("capital_social"),
            "opcao_simples":             p.get("opcao_pelo_simples"),
            "opcao_mei":                 p.get("opcao_pelo_mei"),
            "email":                     p.get("email"),
            "telefone":                  p.get("ddd_telefone_1"),
            "receita_cep":               cep_raw.zfill(8) if cep_raw else None,
            "receita_logradouro":        p.get("logradouro"),
            "receita_numero":            p.get("numero"),
            "receita_complemento":       p.get("complemento"),
            "receita_bairro":            p.get("bairro"),
            "receita_municipio":         p.get("municipio"),
            "receita_uf":                p.get("uf"),
            "data_consulta":             data_consulta,
            "status_consulta":           "ok",
        }
    except requests.Timeout:
        return {"cnpj": cnpj, "data_consulta": data_consulta, "status_consulta": "timeout"}
    except requests.RequestException as exc:
        return {"cnpj": cnpj, "data_consulta": data_consulta,
                "status_consulta": f"erro_rede: {str(exc)[:120]}"}
    finally:
        session.close()


def _limpar_rec_receita(r: dict) -> dict:
    limpo: dict = {}
    for campo in [f.name for f in _SCHEMA_RECEITA.fields]:
        v = r.get(campo)
        if isinstance(v, str) and v in _VAZIOS_RECEITA:
            v = None
        if campo == "cnae_principal_codigo" and v is not None:
            try:
                v = int(v)
            except (TypeError, ValueError):
                v = None
        elif campo == "capital_social" and v is not None:
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
        elif campo in ("opcao_simples", "opcao_mei"):
            if isinstance(v, bool):
                pass
            elif v is None or str(v).lower() in _VAZIOS_RECEITA:
                v = None
            elif str(v).lower() in ("true", "1", "sim"):
                v = True
            else:
                v = False
        limpo[campo] = v
    return limpo


# --- Detecta CNPJs do lote ainda não gravados com status ok ---
col_doc_raw = next((c for c in df.columns if c.upper() in ("CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ")), None)
cnpjs_lote: set[str] = set()
if col_doc_raw:
    for v in df[col_doc_raw]:
        d = re.sub(r"\D", "", str(v).strip())
        if len(d) == 14:
            cnpjs_lote.add(d)

ja_ok: set[str] = {
    r["cnpj"]
    for r in spark.sql(
        f"SELECT cnpj FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'"
    ).collect()
}

cnpjs_buscar = sorted(cnpjs_lote - ja_ok)
print(f"CNPJs no lote: {len(cnpjs_lote)} | Já na tabela (ok): {len(ja_ok)} | A buscar na API: {len(cnpjs_buscar)}")

# --- Busca somente os CNPJs novos / com erro anterior ---
if cnpjs_buscar:
    t0 = time.time()
    novos: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_consultar_receita_api, cnpj): cnpj for cnpj in cnpjs_buscar}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            cnpj = futures[future]
            try:
                rec = future.result()
                novos.append(rec)
                status = rec.get("status_consulta", "?")
                if status != "ok":
                    print(f"  [ERRO] {cnpj} — {status}")
            except Exception as exc:
                novos.append({"cnpj": cnpj, "status_consulta": f"excecao: {exc}",
                              "data_consulta": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
            if i % 50 == 0 or i == len(cnpjs_buscar):
                print(f"  {i}/{len(cnpjs_buscar)} CNPJs consultados ({time.time()-t0:.0f}s)")

    # MERGE na tabela Delta (upsert por CNPJ)
    df_novos = spark.createDataFrame(
        [_limpar_rec_receita(r) for r in novos], schema=_SCHEMA_RECEITA
    )
    (
        DeltaTable.forName(spark, TABELA_RECEITA)
        .alias("t")
        .merge(df_novos.alias("n"), "t.cnpj = n.cnpj")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    ok_novos  = sum(1 for r in novos if r.get("status_consulta") == "ok")
    err_novos = len(novos) - ok_novos
    print(f"MERGE concluído: {ok_novos} gravados OK | {err_novos} com erro | {time.time()-t0:.1f}s")
else:
    print("Todos os CNPJs do lote já estão na tabela. Nenhuma chamada à API necessária.")

# --- Carrega cache de CNPJ em memória ---
_cache_receita: dict[str, dict] = {
    r["cnpj"]: r.asDict()
    for r in spark.sql(f"SELECT * FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'").collect()
}
print(f"Cache CNPJ carregado: {len(_cache_receita)} registros.")

# ---------------------------------------------------------------------------
# CEP — mesma lógica: detecta CEPs novos, busca no ViaCEP, MERGE, carrega cache
# ---------------------------------------------------------------------------

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


# Coleta todos os CEPs necessários: da base de clientes + dos endereços da Receita
_col_cep_raw = next((c for c in df.columns if c.upper() == "CEP"), None)
ceps_lote: set[str] = set()
if _col_cep_raw:
    for v in df[_col_cep_raw]:
        cep = re.sub(r"\D", "", str(v)).zfill(8)
        if len(cep) == 8 and cep not in ("00000000",):
            ceps_lote.add(cep)

for rec in _cache_receita.values():
    cep = (rec.get("receita_cep") or "").replace("-", "").strip().zfill(8)
    if len(cep) == 8 and cep not in ("00000000",):
        ceps_lote.add(cep)

ja_ok_cep: set[str] = {
    r["cep"]
    for r in spark.sql(f"SELECT cep FROM {TABELA_CEP} WHERE status_consulta = 'ok'").collect()
}
ceps_buscar = sorted(ceps_lote - ja_ok_cep)
print(f"CEPs no lote: {len(ceps_lote)} | Já na tabela (ok): {len(ja_ok_cep)} | A buscar na API: {len(ceps_buscar)}")

if ceps_buscar:
    t0_cep = time.time()
    novos_cep: list[dict] = []
    # ViaCEP não tem rate-limit agressivo — pode usar mais workers
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
                print(f"  {i}/{len(ceps_buscar)} CEPs consultados ({time.time()-t0_cep:.0f}s)")

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
    ok_cep  = sum(1 for r in novos_cep if r.get("status_consulta") == "ok")
    print(f"MERGE CEP concluído: {ok_cep} gravados OK | {len(novos_cep)-ok_cep} com erro | {time.time()-t0_cep:.1f}s")
else:
    print("Todos os CEPs do lote já estão na tabela.")

# Carrega cache de CEP em memória
_cep_cache: dict[str, dict] = {
    r["cep"]: r.asDict()
    for r in spark.sql(f"SELECT * FROM {TABELA_CEP} WHERE status_consulta = 'ok'").collect()
}
print(f"Cache CEP carregado: {len(_cep_cache)} registros disponíveis para o batimento.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Funções auxiliares — CEP, normalização, thread-safety

# COMMAND ----------

def _consultar_cep_cached(cep: str) -> dict:
    """Lê do cache em memória (_cep_cache) populado antes do batimento.
    Lança ValueError se o CEP não estiver na tabela tab_dados_cep."""
    row = _cep_cache.get(cep)
    if row is None:
        raise ValueError(f"CEP {cep} não encontrado no cache (tab_dados_cep).")
    return {
        "CEP":        row.get("cep", cep),
        "Logradouro": row.get("logradouro") or "",
        "Bairro":     row.get("bairro") or "",
        "Cidade":     row.get("cidade") or "",
        "UF":         row.get("uf") or "",
        "Complemento":row.get("complemento") or "",
        "Fonte_CEP":  row.get("fonte") or "tab_dados_cep",
        "Status_CEP": "OK",
    }


def _limpar_doc(doc: str) -> str:
    return re.sub(r"\D", "", str(doc).strip())


def _cep_generico(cep: str) -> bool:
    return len(cep) == 8 and cep.endswith("000")


def tipo_documento(doc: str) -> str:
    d = _limpar_doc(doc)
    if len(d) == 11:
        return "CPF"
    if len(d) == 14:
        return "CNPJ"
    return "INVALIDO"


_TIPOS_LOGRADOURO = re.compile(
    r"^(AVENIDA|AVENUE|AV|RUA|RODOVIA|ROD|ESTRADA|EST|ALAMEDA|AL|"
    r"TRAVESSA|TR|TV|PRACA|PCA|PC|LARGO|LGO|LADEIRA|VIELA|BECO|"
    r"SETOR|QUADRA|QD|CONJUNTO|CJ|LOTE|LT|VILA|VL|PARQUE|LINHA|"
    r"CORREDOR|GALERIA|RAMAL|TRECHO|TREVO|VIA|VIADUTO|ACESSO)\b\s*",
    re.IGNORECASE,
)


def _normalizar(texto: str | None) -> str:
    if not texto:
        return ""
    s = str(texto).upper().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalizar_logradouro(texto: str | None) -> str:
    return _TIPOS_LOGRADOURO.sub("", _normalizar(texto)).strip()


def comparar_enderecos(receita: dict, correios: dict) -> str:
    if correios.get("Status_CEP") != "OK":
        return "CEP não encontrado nos Correios"
    uf_ok  = _normalizar(receita.get("receita_uf"))       == _normalizar(correios.get("UF"))
    mun_ok = _normalizar(receita.get("receita_municipio")) == _normalizar(correios.get("Cidade"))
    log_r  = _normalizar_logradouro(receita.get("receita_logradouro"))
    log_c  = _normalizar_logradouro(correios.get("Logradouro"))
    log_ok = (not log_r or not log_c) or (log_r == log_c)
    return "Confere" if (uf_ok and mun_ok and log_ok) else "Divergente"


def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    upper = {c.strip().upper(): c for c in df.columns}
    for nome in candidatos:
        if nome.upper() in upper:
            return upper[nome.upper()]
    return None


print("Funções carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Worker — processa uma linha (sem chamada à Receita em runtime)

# COMMAND ----------

def _processar_linha(args: tuple) -> tuple:
    """
    Processa uma única linha do DataFrame.
    Para CNPJs: lê dados da Receita do _cache_receita (dict em memória).
    Para CPFs: consulta apenas ViaCEP.
    """
    i, row_dict, col_map, total = args

    col_doc     = col_map["doc"]
    col_cep     = col_map["cep"]
    col_fatura  = col_map["fatura"]
    col_id      = col_map["id"]
    col_regra   = col_map["regra"]
    col_cidade  = col_map["cidade"]
    col_bairro  = col_map["bairro"]
    col_uf      = col_map["uf"]
    col_nome    = col_map["nome"]
    col_ie      = col_map["ie"]
    col_produto = col_map["produto"]
    col_tpsvc   = col_map["tpsvc"]
    col_dssvc   = col_map["dssvc"]
    col_imposto = col_map["imposto"]
    col_promo   = col_map["promo"]
    col_grupo   = col_map["grupo"]
    col_lote    = col_map["lote"]
    col_seg     = col_map["seg"]
    col_crm     = col_map["crm"]

    def _val(col):
        v = str(row_dict[col]).strip() if col and col in row_dict else ""
        return "" if v in ("nan", "None") else v

    doc_raw = str(row_dict[col_doc]).strip()
    cep_raw = str(row_dict[col_cep]).replace("-", "").replace(".", "").strip().zfill(8)
    tipo    = tipo_documento(doc_raw)
    doc_num = _limpar_doc(doc_raw)

    fatura      = _val(col_fatura)
    id_cli      = _val(col_id)
    regra       = _val(col_regra)
    cidade_base = _val(col_cidade)
    bairro_base = _val(col_bairro)
    uf_base     = _val(col_uf)
    nome_base   = _val(col_nome)
    ie_base     = _val(col_ie)
    produto     = _val(col_produto)
    tipo_svc    = _val(col_tpsvc)
    desc_svc    = _val(col_dssvc)
    imposto     = _val(col_imposto)
    promo       = _val(col_promo)
    grupo       = _val(col_grupo)
    lote        = _val(col_lote)
    segmento    = _val(col_seg)
    crm         = _val(col_crm)

    cidade_base_limpa = re.sub(r"\s*-\s*\w+$", "", cidade_base).strip()

    prefixo = {
        "Fatura":               fatura,
        "ID_Cliente":           id_cli,
        "Regra":                regra,
        "Segmento":             segmento,
        "Cidade_Base":          cidade_base,
        "Bairro_Base":          bairro_base,
        "UF_Base":              uf_base,
        "Nome_Cliente_Base":    nome_base,
        "IE_Base":              ie_base,
        "Produto":              produto,
        "Tipo_Servico":         tipo_svc,
        "Descricao_Servico":    desc_svc,
        "Tipo_Imposto":         imposto,
        "Promocao":             promo,
        "Grupo_Localidade":     grupo,
        "ID_Lote":              lote,
        "CRM":                  crm,
    }

    header = f"  [{int(i)+1:4d}/{total}] {doc_raw[:18]:<20} ({tipo}) "

    # ------------------------------------------------------------------
    # CEP genérico + CPF
    # ------------------------------------------------------------------
    if _cep_generico(cep_raw) and tipo == "CPF":
        try:
            end_gen    = _consultar_cep_cached(cep_raw)
            cidade_gen = _normalizar(end_gen.get("Cidade", ""))
            loc_str    = f"{end_gen.get('Cidade','')}/{end_gen.get('UF','')}"
        except Exception:
            cidade_gen = ""
            loc_str    = "não identificada"

        cidade_ok_gen = bool(cidade_gen) and _normalizar(cidade_base_limpa) == cidade_gen
        nota_gen = f"[CEP] CEP genérico: {cep_raw[:5]}-{cep_raw[5:]} (representa sede do município)"
        if cidade_ok_gen:
            obs_gen        = f"{nota_gen} | Município confirmado: {loc_str}"
            status_gen     = "CORRETO"
            substatus_gen  = "ALERTA"
            status_val_gen = "CEP genérico - município confirmado"
        else:
            obs_gen        = f"{nota_gen} | Município divergente: base '{cidade_base_limpa}' x CEP '{loc_str}'"
            status_gen     = "INCORRETO"
            substatus_gen  = "ERRO"
            status_val_gen = "CEP genérico - município divergente"

        linha_val = {
            **prefixo,
            "Documento":        doc_num,
            "Tipo":             "CPF",
            "CEP_Informado":    cep_raw,
            "Status_Validacao": status_val_gen,
            "Observacao":       obs_gen,
        }
        linha_rel = {
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": status_gen, "SUBSTATUS": substatus_gen, "OBSERVACAO": obs_gen,
            "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
        }
        return (i, linha_val, None, linha_rel, header + f"— CEP GENÉRICO | {status_gen} | {loc_str}")

    # ------------------------------------------------------------------
    # Documento inválido
    # ------------------------------------------------------------------
    if tipo == "INVALIDO":
        linha_val = {
            **prefixo,
            "Documento":        doc_raw,
            "Tipo":             "INVALIDO",
            "CEP_Informado":    cep_raw,
            "Status_Validacao": "Documento inválido",
        }
        linha_rel = {
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "INCORRETO", "SUBSTATUS": "ERRO", "OBSERVACAO": "[DOC] Documento inválido",
            "DADOS_BILLING": f"DOC: {doc_raw} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
        }
        return (i, linha_val, None, linha_rel, header + "— DOCUMENTO INVÁLIDO")

    # ------------------------------------------------------------------
    # CPF: valida apenas via ViaCEP
    # ------------------------------------------------------------------
    if tipo == "CPF":
        try:
            end = _consultar_cep_cached(cep_raw)
            cidade_cor = _normalizar(end.get("Cidade", ""))
            uf_cor     = _normalizar(end.get("UF", ""))
            cidade_ok  = not cidade_base_limpa or not cidade_cor or _normalizar(cidade_base_limpa) == cidade_cor
            uf_ok      = not uf_base or not uf_cor or _normalizar(uf_base) == uf_cor

            divs_cpf = []
            if not cidade_ok:
                divs_cpf.append(f"Cidade divergente: base '{cidade_base_limpa}' x Correios '{end.get('Cidade','')}'")
            if not uf_ok:
                divs_cpf.append(f"UF divergente: base '{uf_base}' x Correios '{end.get('UF','')}'")

            obs_cpf        = " | ".join(divs_cpf)
            status_cpf     = "INCORRETO" if divs_cpf else "CORRETO"
            substatus_cpf  = "ERRO" if divs_cpf else "OK"
            status_val_cpf = "Divergente" if divs_cpf else "Confere"

            linha_val = {
                **prefixo,
                "Documento":        doc_num,
                "Tipo":             "CPF",
                "CEP_Informado":    cep_raw,
                "Logradouro":       end.get("Logradouro", ""),
                "Bairro":           end.get("Bairro", ""),
                "Cidade":           end.get("Cidade", ""),
                "UF":               end.get("UF", ""),
                "Complemento":      end.get("Complemento", ""),
                "Cidade_Confere":   "Sim" if cidade_ok else "Não",
                "UF_Confere":       "Sim" if uf_ok else "Não",
                "Fonte_CEP":        end.get("Fonte_CEP", ""),
                "Status_Validacao": status_val_cpf,
                "Observacao":       obs_cpf,
            }
            linha_rel = {
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": status_cpf, "SUBSTATUS": substatus_cpf, "OBSERVACAO": obs_cpf,
                "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
                "DADOS_CONTRATO": None,
                "DADOS_TABELA_VERDADE": f"CEP: {cep_raw} | LOGRADOURO: {end.get('Logradouro','')} | BAIRRO: {end.get('Bairro','')} | CIDADE: {end.get('Cidade','')} | UF: {end.get('UF','')}",
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
            }
            return (i, linha_val, None, linha_rel, header + f"— {status_val_cpf} | {end.get('Logradouro','')[:30]}, {end.get('Cidade','')}/{end.get('UF','')}")

        except Exception as exc:
            linha_val = {
                **prefixo,
                "Documento":        doc_num,
                "Tipo":             "CPF",
                "CEP_Informado":    cep_raw,
                "Status_Validacao": f"Erro CEP: {exc}",
            }
            linha_rel = {
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
                "OBSERVACAO": f"[CEP] CEP não encontrado: {cep_raw}",
                "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
                "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
            }
            return (i, linha_val, None, linha_rel, header + f"— ERRO CEP: {exc}")

    # ------------------------------------------------------------------
    # CNPJ: lê do cache em memória (sem chamada de API em runtime)
    # ------------------------------------------------------------------
    receita = _cache_receita.get(doc_num)

    if receita is None:
        # CNPJ não foi carregado pelo notebook de carga — avisar operador
        obs_nc = "[CACHE] CNPJ não encontrado em tb_dados_receita. Execute carga_dados_receita_notebook."
        linha_val = {
            **prefixo,
            "Documento":        doc_num,
            "Tipo":             "CNPJ",
            "CEP_Informado":    cep_raw,
            "Status_Validacao": "nao_carregado_na_cache",
            "Observacao":       obs_nc,
        }
        linha_rel = {
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "PENDENTE", "SUBSTATUS": "CACHE_VAZIO", "OBSERVACAO": obs_nc,
            "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
        }
        return (i, linha_val, None, linha_rel, header + "— NÃO CARREGADO NO CACHE")

    # Valida CEP da Receita via ViaCEP
    cep_receita = (receita.get("receita_cep") or "").replace("-", "").strip().zfill(8)
    try:
        end_correios = _consultar_cep_cached(cep_receita) if cep_receita and cep_receita != "00000000" else {"Status_CEP": "CEP vazio"}
    except Exception as exc:
        end_correios = {"Status_CEP": str(exc)}

    status_end = comparar_enderecos(receita, end_correios)

    cidade_rec = _normalizar(receita.get("receita_municipio", ""))
    uf_rec     = _normalizar(receita.get("receita_uf", ""))
    cidade_ok  = not cidade_base_limpa or not cidade_rec or _normalizar(cidade_base_limpa) == cidade_rec
    uf_ok      = not uf_base or not uf_rec or _normalizar(uf_base) == uf_rec
    cep_ok     = cep_raw == cep_receita
    mun_base   = _normalizar(cidade_base_limpa)
    mun_rec    = _normalizar(receita.get("receita_municipio", ""))

    divergencias = []
    if _cep_generico(cep_raw):
        nota_gen_b = f"[CEP] CEP da base é genérico: {cep_raw[:5]}-{cep_raw[5:]}"
        if mun_rec and mun_rec == mun_base:
            divergencias.append(f"{nota_gen_b} | Município confirmado: {receita.get('receita_municipio','')}")
        else:
            divergencias.append(f"{nota_gen_b} | Município divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'")
    if _cep_generico(cep_receita):
        nota_gen_r = f"[CEP] CEP da Receita é genérico: {cep_receita[:5]}-{cep_receita[5:]}"
        if mun_rec and mun_rec == mun_base:
            divergencias.append(f"{nota_gen_r} | Município confirmado: {receita.get('receita_municipio','')}")
        else:
            divergencias.append(f"{nota_gen_r} | Município divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'")
    if not cep_ok:
        divergencias.append(f"CEP divergente: base '{cep_raw}' x Receita '{cep_receita}'")
    if not cidade_ok:
        divergencias.append(f"Cidade divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'")
    if not uf_ok:
        divergencias.append(f"UF divergente: base '{uf_base}' x Receita '{receita.get('receita_uf','')}'")
    if status_end == "Divergente":
        divergencias.append("Endereço Receita x Correios divergente")
    elif status_end == "CEP não encontrado nos Correios":
        divergencias.append("CEP da Receita não encontrado nos Correios")

    status_validacao = "Divergente" if divergencias else "Confere"
    observacao       = " | ".join(divergencias)

    linha_val = {
        **prefixo,
        "Documento":                 doc_num,
        "Tipo":                      "CNPJ",
        "CEP_Informado":             cep_raw,
        "CEP_Receita":               cep_receita,
        "CEP_Confere_com_Informado": "Sim" if cep_ok else "Não",
        "Cidade_Confere":            "Sim" if cidade_ok else "Não",
        "UF_Confere":                "Sim" if uf_ok else "Não",
        "Logradouro_Receita":        receita.get("receita_logradouro", ""),
        "Numero_Receita":            receita.get("receita_numero", ""),
        "Complemento_Receita":       receita.get("receita_complemento", ""),
        "Bairro_Receita":            receita.get("receita_bairro", ""),
        "Cidade_Receita":            receita.get("receita_municipio", ""),
        "UF_Receita":                receita.get("receita_uf", ""),
        "Logradouro_Correios":       end_correios.get("Logradouro", ""),
        "Bairro_Correios":           end_correios.get("Bairro", ""),
        "Cidade_Correios":           end_correios.get("Cidade", ""),
        "UF_Correios":               end_correios.get("UF", ""),
        "Fonte_CEP":                 end_correios.get("Fonte_CEP", ""),
        "Status_Validacao":          status_validacao,
        "Observacao":                observacao,
        "Razao_Social":              receita.get("razao_social", ""),
        "Situacao_Cadastral":        receita.get("situacao_cadastral", ""),
        "Data_Consulta_Receita":     receita.get("data_consulta", ""),
    }

    _end_rec = (
        f"{receita.get('receita_logradouro','')}, {receita.get('receita_numero','')}"
        f" - {receita.get('receita_bairro','')} - {receita.get('receita_municipio','')}/{receita.get('receita_uf','')}"
    ).strip(", -/")

    linha_rel = {
        "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
        "STATUS": "INCORRETO" if divergencias else "CORRETO",
        "SUBSTATUS": "ERRO" if divergencias else "OK",
        "OBSERVACAO": observacao,
        "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
        "DADOS_CONTRATO": f"RAZAO: {receita.get('razao_social','')} | SITUACAO: {receita.get('situacao_cadastral','')} | CEP: {cep_receita} | END: {_end_rec}",
        "DADOS_TABELA_VERDADE": None,
        "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
        "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
        "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
    }

    linha_cad = {
        **prefixo,
        "CNPJ":                     doc_num,
        "Razao_Social":             receita.get("razao_social"),
        "Nome_Fantasia":            receita.get("nome_fantasia"),
        "Situacao_Cadastral":       receita.get("situacao_cadastral"),
        "Data_Situacao_Cadastral":  receita.get("data_situacao_cadastral"),
        "Motivo_Situacao":          receita.get("motivo_situacao_cadastral"),
        "Natureza_Juridica":        receita.get("natureza_juridica"),
        "Data_Inicio_Atividade":    receita.get("data_inicio_atividade"),
        "CNAE_Principal_Codigo":    receita.get("cnae_principal_codigo"),
        "CNAE_Principal_Descricao": receita.get("cnae_principal_descricao"),
        "Porte":                    receita.get("porte"),
        "Capital_Social":           receita.get("capital_social"),
        "Simples_Nacional":         receita.get("opcao_simples"),
        "MEI":                      receita.get("opcao_mei"),
        "Email":                    receita.get("email"),
        "Telefone":                 receita.get("telefone"),
        "CEP_Receita":              receita.get("receita_cep"),
        "Logradouro_Receita":       receita.get("receita_logradouro"),
        "Numero_Receita":           receita.get("receita_numero"),
        "Complemento_Receita":      receita.get("receita_complemento"),
        "Bairro_Receita":           receita.get("receita_bairro"),
        "Municipio_Receita":        receita.get("receita_municipio"),
        "UF_Receita":               receita.get("receita_uf"),
        "Data_Consulta_Receita":    receita.get("data_consulta"),
    }

    log_msg = (
        header
        + f"— {status_validacao} | "
        + f"{(receita.get('razao_social') or '')[:30]} | "
        + f"CEP Receita: {cep_receita}"
    )
    return (i, linha_val, linha_cad, linha_rel, log_msg)


print("Worker _processar_linha carregado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Processamento paralelo

# COMMAND ----------

col_doc     = _encontrar_coluna(df, ["CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ", "CPF", "DOC"])
col_cep     = _encontrar_coluna(df, ["CEP"])
col_fatura  = _encontrar_coluna(df, ["FATURA"])
col_id      = _encontrar_coluna(df, ["ID_CLIENTE_CONTRATO", "ID_CLIENTE", "ID"])
col_regra   = _encontrar_coluna(df, ["REGRA"])
col_cidade  = _encontrar_coluna(df, ["CIDADE", "CITY"])
col_bairro  = _encontrar_coluna(df, ["BAIRRO"])
col_uf      = _encontrar_coluna(df, ["UF", "ESTADO", "STATE"])
col_nome    = _encontrar_coluna(df, ["NOME_CLIENTE", "NOME", "CLIENTE"])
col_ie      = _encontrar_coluna(df, ["INSCRICAO_ESTADUAL", "IE"])
col_produto = _encontrar_coluna(df, ["PRODUTO"])
col_tpsvc   = _encontrar_coluna(df, ["TIPO_SERVICO"])
col_dssvc   = _encontrar_coluna(df, ["DESCRICAO_SERVICO"])
col_imposto = _encontrar_coluna(df, ["TIPO_IMPOSTO"])
col_promo   = _encontrar_coluna(df, ["PROMOCAO"])
col_grupo   = _encontrar_coluna(df, ["GRUPO_LOCALIDADE"])
col_lote    = _encontrar_coluna(df, ["ID_LOTE"])
col_seg     = _encontrar_coluna(df, ["SEGMENTO"])
col_crm     = _encontrar_coluna(df, ["CRM", "SISTEMA_ORIGEM"])

if col_doc is None:
    raise ValueError("Coluna de documento não encontrada.")
if col_cep is None:
    raise ValueError("Coluna CEP não encontrada.")

col_map = {
    "doc": col_doc, "cep": col_cep, "fatura": col_fatura, "id": col_id,
    "regra": col_regra, "cidade": col_cidade, "bairro": col_bairro, "uf": col_uf,
    "nome": col_nome, "ie": col_ie, "produto": col_produto, "tpsvc": col_tpsvc,
    "dssvc": col_dssvc, "imposto": col_imposto, "promo": col_promo,
    "grupo": col_grupo, "lote": col_lote, "seg": col_seg, "crm": col_crm,
}

total = len(df)
fonte_cep = "Correios API" if TOKEN else "ViaCEP (gratuito)"
print(f"Processando {total} registros | CEP via {fonte_cep} | Receita via cache ({len(_cache_receita)} CNPJs) | workers=20\n")

args_list = [(i, row.to_dict(), col_map, total) for i, row in df.iterrows()]

resultados: dict = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(_processar_linha, args): args[0] for args in args_list}
    for future in concurrent.futures.as_completed(futures):
        idx, linha_val, linha_cad, linha_rel, log_msg = future.result()
        resultados[idx] = (linha_val, linha_cad, linha_rel, log_msg)

linhas_validacao:  list[dict] = []
linhas_cadastrais: list[dict] = []
linhas_relatorio:  list[dict] = []

for idx in sorted(resultados.keys()):
    linha_val, linha_cad, linha_rel, log_msg = resultados[idx]
    if linha_val is not None:
        linhas_validacao.append(linha_val)
    if linha_cad is not None:
        linhas_cadastrais.append(linha_cad)
    if linha_rel is not None:
        linhas_relatorio.append(linha_rel)
    print(log_msg)

ok        = sum(1 for r in linhas_validacao if r.get("Status_Validacao") in ("Confere", "CEP encontrado"))
div       = sum(1 for r in linhas_validacao if r.get("Status_Validacao") == "Divergente")
sem_cache = sum(1 for r in linhas_validacao if r.get("Status_Validacao") == "nao_carregado_na_cache")
erro      = len(linhas_validacao) - ok - div - sem_cache
print(f"\nConcluído: {ok} OK | {div} divergente(s) | {sem_cache} sem cache | {erro} erro(s)")
if sem_cache:
    print(f"  ⚠ {sem_cache} CNPJs não estão em tb_dados_receita — rode carga_dados_receita_notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Grava resultados nas tabelas Delta

# COMMAND ----------

for _tbl, _col in [
    ("validacao_status_fatura",    "CRM"),
    ("validacao_enderecos",        "CRM"),
    ("validacao_dados_cadastrais", "CRM"),
]:
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.{_tbl} ALTER COLUMN {_col} DROP NOT NULL")
    except Exception:
        pass

_SCHEMA_CADASTRAIS = {
    "CNAE_Principal_Codigo": LongType(),
    "Capital_Social":        DoubleType(),
    "Simples_Nacional":      BooleanType(),
    "MEI":                   BooleanType(),
}

_VAZIOS = {"", "nan", "NaN", "None", "none", "null", "NULL", "NaT", "na", "NA"}

def _to_spark(linhas: list[dict], tipos: dict | None = None):
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

if linhas_validacao:
    _to_spark(linhas_validacao) \
        .write.mode("append").option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.validacao_enderecos")
    print(f"{len(linhas_validacao)} registros → {CATALOG}.validacao_enderecos")

if linhas_cadastrais:
    _to_spark(linhas_cadastrais, tipos=_SCHEMA_CADASTRAIS) \
        .write.mode("append").option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.validacao_dados_cadastrais")
    print(f"{len(linhas_cadastrais)} registros → {CATALOG}.validacao_dados_cadastrais")

if linhas_relatorio:
    _to_spark(linhas_relatorio) \
        .write.mode("append").option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.validacao_status_fatura")
    print(f"{len(linhas_relatorio)} registros → {CATALOG}.validacao_status_fatura")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verificação rápida dos resultados

# COMMAND ----------

display(spark.sql(f"SELECT STATUS, SUBSTATUS, COUNT(*) AS QTD FROM {CATALOG}.validacao_status_fatura GROUP BY 1, 2 ORDER BY 1, 2"))
