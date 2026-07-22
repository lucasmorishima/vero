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
    ROW_NUMBER() OVER (ORDER BY bc.codigocliente) + 1 AS FATURA,
    COALESCE(ca.CONTRATO, ca.ID_CLIENTE)               AS ID_CLIENTE_CONTRATO,
    ca.segmento                                        AS SEGMENTO,

    -- Endereço de instalação (onde o serviço está instalado)
    bc.cidade   AS cidade_instalacao,
    bc.bairro   AS bairro_instalacao,
    bc.cep      AS cep_instalacao,
    bc.uf       AS uf_instalacao,

    -- Endereço legal (endereço de cobrança / cadastro)
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
            time.sleep(0.35)   # throttle: ~3 req/s máximo para evitar 429
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


# Inicializa caches — garante que as variáveis existam mesmo se a célula falhar parcialmente
_cache_receita: dict[str, dict] = {}
_cep_cache:     dict[str, dict] = {}


def _cnpj_valido(cnpj: str) -> bool:
    """Valida CNPJ pelo algoritmo dos dígitos verificadores."""
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    for pesos, pos in (([5,4,3,2,9,8,7,6,5,4,3,2], 12), ([6,5,4,3,2,9,8,7,6,5,4,3,2], 13)):
        soma = sum(int(cnpj[i]) * pesos[i] for i in range(pos))
        resto = soma % 11
        dv = 0 if resto < 2 else 11 - resto
        if dv != int(cnpj[pos]):
            return False
    return True


# --- Coleta CNPJs do lote atual (resultado da query com LIMIT_REGISTROS) ---
_col_doc_raw = next((c for c in df.columns if c.upper() in ("CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ")), None)
_cnpjs_raw: set[str] = set()
if _col_doc_raw:
    for v in df[_col_doc_raw]:
        d = re.sub(r"\D", "", str(v).strip())
        if len(d) == 14:
            _cnpjs_raw.add(d)

cnpjs_lote: set[str] = {c for c in _cnpjs_raw if _cnpj_valido(c)}
invalidos   = len(_cnpjs_raw) - len(cnpjs_lote)
if invalidos:
    print(f"  {invalidos} CNPJ(s) com dígito verificador inválido ignorados.")

ja_ok: set[str] = {
    r["cnpj"]
    for r in spark.sql(
        f"SELECT cnpj FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'"
    ).collect()
}

cnpjs_buscar = sorted(cnpjs_lote - ja_ok)
print(f"CNPJs na base-fonte: {len(_cnpjs_raw)} | Válidos: {len(cnpjs_lote)} | Já na tabela (ok): {len(ja_ok)} | A buscar na API: {len(cnpjs_buscar)}")

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


# Coleta CEPs do lote atual (colunas de instalação e legal presentes no df)
ceps_lote: set[str] = set()
for col_cep_col in [c for c in df.columns if "cep" in c.lower()]:
    for v in df[col_cep_col]:
        cep = re.sub(r"\D", "", str(v)).zfill(8)
        if len(cep) == 8 and cep != "00000000":
            ceps_lote.add(cep)

for rec in _cache_receita.values():
    cep = (rec.get("receita_cep") or "").replace("-", "").strip().zfill(8)
    if len(cep) == 8 and cep != "00000000":
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

# ---------------------------------------------------------------------------
# Helpers internos dos sub-validadores
# ---------------------------------------------------------------------------

def _cep_end(cep_raw: str) -> dict:
    """Lê CEP do _cep_cache. Retorna Status_CEP='ERRO' se ausente."""
    try:
        return _consultar_cep_cached(cep_raw)
    except ValueError:
        return {"Status_CEP": "ERRO", "Cidade": "", "UF": "", "Logradouro": "", "Bairro": "", "Fonte_CEP": ""}


def _check_cidade_uf(cidade_base: str, uf_base: str, end: dict) -> list:
    divs = []
    cb = _normalizar(re.sub(r"\s*-\s*\w+$", "", cidade_base).strip())
    cc = _normalizar(end.get("Cidade", ""))
    ub = _normalizar(uf_base)
    uc = _normalizar(end.get("UF", ""))
    if cb and cc and cb != cc:
        divs.append(f"Cidade divergente: base '{cidade_base}' x CEP '{end.get('Cidade','')}'")
    if ub and uc and ub != uc:
        divs.append(f"UF divergente: base '{uf_base}' x CEP '{end.get('UF','')}'")
    return divs


def _divs_cep_generico(cep_raw: str, cidade_base: str, end: dict) -> list:
    cidade_limpa = re.sub(r"\s*-\s*\w+$", "", cidade_base).strip()
    nota = f"[CEP] CEP genérico: {cep_raw[:5]}-{cep_raw[5:]} (sede do município)"
    if _normalizar(cidade_limpa) and _normalizar(end.get("Cidade","")) \
            and _normalizar(cidade_limpa) != _normalizar(end.get("Cidade","")):
        return [f"{nota} | Município divergente: base '{cidade_limpa}' x CEP '{end.get('Cidade','')}'"]
    return [f"{nota} | Município confirmado: {end.get('Cidade','')}/{end.get('UF','')}"]


def _rel_base(base: dict, regra: str, status: str, substatus: str, obs: str,
               dados_billing: str, dados_contrato: str | None, dados_tv: str | None) -> dict:
    return {
        "FATURA": base["Fatura"], "ID_CONTA_CONTRATO": base["ID_Cliente"],
        "REGRA": regra, "SEGMENTO": base["Segmento"],
        "STATUS": status, "SUBSTATUS": substatus, "OBSERVACAO": obs,
        "DADOS_BILLING": dados_billing, "DADOS_CONTRATO": dados_contrato,
        "DADOS_TABELA_VERDADE": dados_tv,
        "ID_LOTE": base["ID_Lote"], "PRODUTO": base["Produto"],
        "TIPO_SERVICO": base["Tipo_Servico"], "DESCRICAO_SERVICO": base["Desc_Servico"],
        "TIPO_IMPOSTO": base["Tipo_Imposto"], "PROMOCAO": base.get("Promocao"),
        "GRUPO_LOCALIDADE": base["Grupo_Localidade"], "CRM": base["CRM"],
    }


# ---------------------------------------------------------------------------
# Sub-validador 1 — DADOS CADASTRAIS (CNPJ only)
# ---------------------------------------------------------------------------

def _val_dados_cadastrais(base: dict, doc_num: str, receita: dict | None, nome_base: str) -> tuple:
    regra = "DADOS_CADASTRAIS"
    billing = f"NOME: {nome_base} | DOC: {doc_num}"

    if receita is None:
        obs = "[CACHE] CNPJ não encontrado em tab_dados_receita. Execute a célula de populate."
        return (
            {**base, "REGRA": regra, "Documento": doc_num, "Tipo": "CNPJ",
             "Status_Validacao": "nao_carregado", "Observacao": obs},
            None,
            _rel_base(base, regra, "PENDENTE", "CACHE_VAZIO", obs, billing, None, None),
            f"{regra} — NÃO CARREGADO NO CACHE",
        )

    divs = []
    sit = (receita.get("situacao_cadastral") or "").upper()
    if sit and sit not in ("ATIVA",):
        divs.append(f"Situação: {receita.get('situacao_cadastral','')}")

    razao_rec  = _normalizar(receita.get("razao_social") or "")
    razao_base = _normalizar(nome_base)
    if razao_rec and razao_base and razao_rec != razao_base:
        divs.append(f"Razão social divergente: base '{nome_base[:40]}' x Receita '{(receita.get('razao_social') or '')[:40]}'")

    obs    = " | ".join(divs)
    sv     = "Divergente" if divs else "Confere"
    status = "INCORRETO" if divs else "CORRETO"

    linha_cad = {
        **base, "REGRA": regra,
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
    contrato = (f"RAZAO: {receita.get('razao_social','')} | "
                f"SITUACAO: {receita.get('situacao_cadastral','')} | "
                f"CEP: {receita.get('receita_cep','')} | "
                f"CIDADE: {receita.get('receita_municipio','')}/{receita.get('receita_uf','')}")
    return (
        {**base, "REGRA": regra, "Documento": doc_num, "Tipo": "CNPJ",
         "Razao_Social": receita.get("razao_social",""),
         "Situacao_Cadastral": receita.get("situacao_cadastral",""),
         "Status_Validacao": sv, "Observacao": obs},
        linha_cad,
        _rel_base(base, regra, status, "ERRO" if divs else "OK", obs, billing, contrato, None),
        f"{regra} — {sv}",
    )


# ---------------------------------------------------------------------------
# Sub-validador 2 — ENDEREÇO LEGAL
# CPF: valida via tab_dados_cep (cidade/uf)
# CNPJ: idem + cruza com endereço registrado na Receita
# ---------------------------------------------------------------------------

def _val_endereco_legal(base: dict, tipo: str, doc_num: str,
                         cep_raw: str, cidade: str, uf: str,
                         nome_base: str, ie_base: str,
                         receita: dict | None) -> tuple:
    regra = "ENDERECO_LEGAL"
    cidade_limpa = re.sub(r"\s*-\s*\w+$", "", cidade).strip()
    end  = _cep_end(cep_raw)
    divs = []

    if end["Status_CEP"] == "ERRO":
        divs.append(f"[CEP] CEP não encontrado no cache: {cep_raw}")
    elif _cep_generico(cep_raw):
        divs += _divs_cep_generico(cep_raw, cidade, end)
    else:
        divs += _check_cidade_uf(cidade, uf, end)

    # CNPJ: compara também com Receita Federal
    if tipo == "CNPJ":
        if receita is None:
            divs.append("[CACHE] CNPJ não encontrado em tab_dados_receita")
        else:
            cep_rec = (receita.get("receita_cep") or "")
            if cep_raw != cep_rec:
                divs.append(f"CEP divergente: base '{cep_raw}' x Receita '{cep_rec}'")
            mn_base = _normalizar(cidade_limpa)
            mn_rec  = _normalizar(receita.get("receita_municipio",""))
            if mn_base and mn_rec and mn_base != mn_rec:
                divs.append(f"Cidade divergente: base '{cidade_limpa}' x Receita '{receita.get('receita_municipio','')}'")
            uf_base_n = _normalizar(uf)
            uf_rec_n  = _normalizar(receita.get("receita_uf",""))
            if uf_base_n and uf_rec_n and uf_base_n != uf_rec_n:
                divs.append(f"UF divergente: base '{uf}' x Receita '{receita.get('receita_uf','')}'")

    obs    = " | ".join(divs)
    sv     = "Divergente" if divs else "Confere"
    status = "INCORRETO" if divs else "CORRETO"
    billing = f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade} | UF: {uf} | IE: {ie_base or '-'}"
    tv      = f"CEP: {cep_raw} | LOGRADOURO: {end.get('Logradouro','')} | CIDADE: {end.get('Cidade','')} | UF: {end.get('UF','')}"
    contrato = None
    if tipo == "CNPJ" and receita:
        contrato = (f"RAZAO: {receita.get('razao_social','')} | "
                    f"CEP: {receita.get('receita_cep','')} | "
                    f"CIDADE: {receita.get('receita_municipio','')}/{receita.get('receita_uf','')}")

    linha_val = {
        **base, "REGRA": regra,
        "Documento":      doc_num, "Tipo": tipo,
        "CEP_Legal":      cep_raw,
        "Logradouro_CEP": end.get("Logradouro",""),
        "Bairro_CEP":     end.get("Bairro",""),
        "Cidade_CEP":     end.get("Cidade",""),
        "UF_CEP":         end.get("UF",""),
        "Cidade_Confere": "Sim" if _normalizar(cidade_limpa)==_normalizar(end.get("Cidade","")) else "Não",
        "UF_Confere":     "Sim" if _normalizar(uf)==_normalizar(end.get("UF","")) else "Não",
        "Fonte_CEP":      end.get("Fonte_CEP","tab_dados_cep"),
        "Status_Validacao": sv, "Observacao": obs,
    }
    if tipo == "CNPJ" and receita:
        linha_val.update({
            "CEP_Receita":        receita.get("receita_cep",""),
            "Cidade_Receita":     receita.get("receita_municipio",""),
            "UF_Receita":         receita.get("receita_uf",""),
            "Razao_Social":       receita.get("razao_social",""),
            "Situacao_Cadastral": receita.get("situacao_cadastral",""),
        })
    return (linha_val, None, _rel_base(base, regra, status, "ERRO" if divs else "OK", obs, billing, contrato, tv),
            f"{regra} — {sv}")


# ---------------------------------------------------------------------------
# Sub-validador 3 — ENDEREÇO DE INSTALAÇÃO
# CPF e CNPJ: valida apenas via tab_dados_cep (sem cruzar com Receita)
# O endereço de instalação não precisa bater com o endereço legal/Receita
# ---------------------------------------------------------------------------

def _val_endereco_instalacao(base: dict, tipo: str, doc_num: str,
                              cep_raw: str, cidade: str, uf: str,
                              nome_base: str, ie_base: str) -> tuple:
    regra = "ENDERECO_INSTALACAO"
    cidade_limpa = re.sub(r"\s*-\s*\w+$", "", cidade).strip()
    end  = _cep_end(cep_raw)
    divs = []

    if end["Status_CEP"] == "ERRO":
        divs.append(f"[CEP] CEP não encontrado no cache: {cep_raw}")
    elif _cep_generico(cep_raw):
        divs += _divs_cep_generico(cep_raw, cidade, end)
    else:
        divs += _check_cidade_uf(cidade, uf, end)

    obs    = " | ".join(divs)
    sv     = "Divergente" if divs else "Confere"
    status = "INCORRETO" if divs else "CORRETO"
    billing = f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade} | UF: {uf} | IE: {ie_base or '-'}"
    tv      = f"CEP: {cep_raw} | LOGRADOURO: {end.get('Logradouro','')} | CIDADE: {end.get('Cidade','')} | UF: {end.get('UF','')}"

    linha_val = {
        **base, "REGRA": regra,
        "Documento":        doc_num, "Tipo": tipo,
        "CEP_Instalacao":   cep_raw,
        "Logradouro_CEP":   end.get("Logradouro",""),
        "Bairro_CEP":       end.get("Bairro",""),
        "Cidade_CEP":       end.get("Cidade",""),
        "UF_CEP":           end.get("UF",""),
        "Cidade_Confere":   "Sim" if _normalizar(cidade_limpa)==_normalizar(end.get("Cidade","")) else "Não",
        "UF_Confere":       "Sim" if _normalizar(uf)==_normalizar(end.get("UF","")) else "Não",
        "Fonte_CEP":        end.get("Fonte_CEP","tab_dados_cep"),
        "Status_Validacao": sv, "Observacao": obs,
    }
    return (linha_val, None, _rel_base(base, regra, status, "ERRO" if divs else "OK", obs, billing, None, tv),
            f"{regra} — {sv}")


# ---------------------------------------------------------------------------
# Worker principal — gera até 3 registros por linha
# ---------------------------------------------------------------------------

def _processar_linha(args: tuple) -> tuple:
    """Retorna (i, [(linha_val, linha_cad, linha_rel, log_msg), ...])"""
    i, row_dict, col_map, total = args

    def _v(col):
        v = str(row_dict.get(col, "")).strip() if col else ""
        return "" if v in ("nan", "None") else v

    doc_raw  = str(row_dict.get(col_map["doc"], "")).strip()
    tipo     = tipo_documento(doc_raw)
    doc_num  = _limpar_doc(doc_raw)

    fatura   = _v(col_map["fatura"])
    id_cli   = _v(col_map["id"])
    segmento = _v(col_map["seg"])
    nome_base= _v(col_map["nome"])
    ie_base  = _v(col_map["ie"])

    def _cep(col):
        return re.sub(r"\D", "", _v(col_map[col])).zfill(8)

    cep_legal = _cep("cep_legal")
    cidade_legal = _v(col_map["cidade_legal"])
    uf_legal     = _v(col_map["uf_legal"])

    cep_inst     = _cep("cep_inst")
    cidade_inst  = _v(col_map["cidade_inst"])
    uf_inst      = _v(col_map["uf_inst"])

    base = {
        "Fatura": fatura, "ID_Cliente": id_cli, "Segmento": segmento,
        "Produto": _v(col_map["produto"]), "Tipo_Servico": _v(col_map["tpsvc"]),
        "Desc_Servico": _v(col_map["dssvc"]), "Tipo_Imposto": _v(col_map["imposto"]),
        "Promocao": _v(col_map["promo"]) or None,
        "Grupo_Localidade": _v(col_map["grupo"]),
        "ID_Lote": _v(col_map["lote"]), "CRM": _v(col_map["crm"]),
    }

    header = f"  [{int(i)+1:4d}/{total}] {doc_raw[:18]:<20} ({tipo})"

    if tipo == "INVALIDO":
        obs = "[DOC] Documento inválido"
        r = _rel_base(base, "INVALIDO", "INCORRETO", "ERRO", obs,
                      f"DOC: {doc_raw}", None, None)
        val = {**base, "REGRA": "INVALIDO", "Documento": doc_raw,
               "Tipo": "INVALIDO", "Status_Validacao": "Documento inválido"}
        return (i, [(val, None, r, header + " — DOCUMENTO INVÁLIDO")])

    receita = _cache_receita.get(doc_num) if tipo == "CNPJ" else None
    results = []

    # 1. DADOS CADASTRAIS — somente CNPJ
    if tipo == "CNPJ":
        results.append(_val_dados_cadastrais(base, doc_num, receita, nome_base))

    # 2. ENDEREÇO LEGAL — CPF e CNPJ
    results.append(_val_endereco_legal(base, tipo, doc_num, cep_legal,
                                       cidade_legal, uf_legal, nome_base, ie_base, receita))

    # 3. ENDEREÇO DE INSTALAÇÃO — CPF e CNPJ (sem cruzar com Receita)
    results.append(_val_endereco_instalacao(base, tipo, doc_num, cep_inst,
                                            cidade_inst, uf_inst, nome_base, ie_base))

    return (i, results)


print("Workers carregados: _val_dados_cadastrais | _val_endereco_legal | _val_endereco_instalacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Processamento paralelo

# COMMAND ----------

col_doc          = _encontrar_coluna(df, ["CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ", "CPF", "DOC"])
col_fatura       = _encontrar_coluna(df, ["FATURA"])
col_id           = _encontrar_coluna(df, ["ID_CLIENTE_CONTRATO", "ID_CLIENTE", "ID"])
col_nome         = _encontrar_coluna(df, ["NOME_CLIENTE", "NOME", "CLIENTE"])
col_ie           = _encontrar_coluna(df, ["INSCRICAO_ESTADUAL", "IE"])
col_seg          = _encontrar_coluna(df, ["SEGMENTO"])
col_produto      = _encontrar_coluna(df, ["PRODUTO"])
col_tpsvc        = _encontrar_coluna(df, ["TIPO_SERVICO"])
col_dssvc        = _encontrar_coluna(df, ["DESCRICAO_SERVICO"])
col_imposto      = _encontrar_coluna(df, ["TIPO_IMPOSTO"])
col_promo        = _encontrar_coluna(df, ["PROMOCAO"])
col_grupo        = _encontrar_coluna(df, ["GRUPO_LOCALIDADE"])
col_lote         = _encontrar_coluna(df, ["ID_LOTE"])
col_crm          = _encontrar_coluna(df, ["CRM", "SISTEMA_ORIGEM"])

# Endereço de instalação (onde o cliente está instalado)
col_cep_inst    = _encontrar_coluna(df, ["cep_instalacao", "CEP_INSTALACAO", "CEP"])
col_cidade_inst = _encontrar_coluna(df, ["cidade_instalacao", "CIDADE_INSTALACAO", "CIDADE"])
col_uf_inst     = _encontrar_coluna(df, ["uf_instalacao", "UF_INSTALACAO", "UF"])

# Endereço legal / fiscal (cadastrado na Receita / boleto)
col_cep_legal    = _encontrar_coluna(df, ["cep_legal", "CEP_LEGAL"])
col_cidade_legal = _encontrar_coluna(df, ["cidade_legal", "CIDADE_LEGAL"])
col_uf_legal     = _encontrar_coluna(df, ["uf_legal", "UF_LEGAL"])

# Fallback: se não há colunas separadas, usa instalação para ambos
if col_cep_legal is None:
    col_cep_legal    = col_cep_inst
    col_cidade_legal = col_cidade_inst
    col_uf_legal     = col_uf_inst

if col_doc is None:
    raise ValueError("Coluna de documento não encontrada no DataFrame.")
if col_cep_inst is None:
    raise ValueError("Coluna CEP não encontrada no DataFrame.")

col_map = {
    "doc":          col_doc,
    "fatura":       col_fatura,
    "id":           col_id,
    "seg":          col_seg,
    "nome":         col_nome,
    "ie":           col_ie,
    "produto":      col_produto,
    "tpsvc":        col_tpsvc,
    "dssvc":        col_dssvc,
    "imposto":      col_imposto,
    "promo":        col_promo,
    "grupo":        col_grupo,
    "lote":         col_lote,
    "crm":          col_crm,
    # Instalação
    "cep_inst":     col_cep_inst,
    "cidade_inst":  col_cidade_inst,
    "uf_inst":      col_uf_inst,
    # Legal
    "cep_legal":    col_cep_legal,
    "cidade_legal": col_cidade_legal,
    "uf_legal":     col_uf_legal,
}

total = len(df)
print(f"Processando {total} registros | Receita cache={len(_cache_receita)} CNPJs | CEP cache={len(_cep_cache)} CEPs | workers=20\n")

args_list = [(i, row.to_dict(), col_map, total) for i, row in df.iterrows()]

_resultados_raw: dict = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(_processar_linha, args): args[0] for args in args_list}
    for future in concurrent.futures.as_completed(futures):
        idx, lista_resultados = future.result()
        _resultados_raw[idx] = lista_resultados

linhas_validacao:  list[dict] = []
linhas_cadastrais: list[dict] = []
linhas_relatorio:  list[dict] = []

n_cad = n_end_legal = n_end_inst = n_ok = n_div = n_erro = 0

for idx in sorted(_resultados_raw.keys()):
    for linha_val, linha_cad, linha_rel, log_msg in _resultados_raw[idx]:
        if linha_val is not None:
            linhas_validacao.append(linha_val)
            regra = linha_val.get("REGRA", "")
            sv    = linha_val.get("Status_Validacao", "")
            if regra == "DADOS_CADASTRAIS":
                n_cad += 1
            elif regra == "ENDERECO_LEGAL":
                n_end_legal += 1
            elif regra == "ENDERECO_INSTALACAO":
                n_end_inst += 1
            if sv in ("Confere", "OK"):
                n_ok += 1
            elif sv == "Divergente":
                n_div += 1
            elif sv not in ("", None):
                n_erro += 1
        if linha_cad is not None:
            linhas_cadastrais.append(linha_cad)
        if linha_rel is not None:
            linhas_relatorio.append(linha_rel)
        if log_msg:
            print(log_msg)

print(
    f"\nConcluído — registros gerados: "
    f"DADOS_CADASTRAIS={n_cad} | ENDERECO_LEGAL={n_end_legal} | ENDERECO_INSTALACAO={n_end_inst}"
    f"\nResultados: OK={n_ok} | Divergente={n_div} | Outros={n_erro}"
)

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
