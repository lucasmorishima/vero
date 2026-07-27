# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação de Endereços e Dados Cadastrais — V5
# MAGIC
# MAGIC **Diferença vs V3:** suporte a duas fontes para consulta de dados da Receita Federal.
# MAGIC
# MAGIC **Modo automático por token:**
# MAGIC - `SERPRO_TOKEN` preenchido → consulta SERPRO Consulta CNPJ (paralelo direto, sem rate-limit)
# MAGIC - `SERPRO_TOKEN` vazio → consulta BrasilAPI gratuita (`Semaphore(1)` + `sleep(0.35 s/req)`)
# MAGIC
# MAGIC **Arquitetura V5:**
# MAGIC ```
# MAGIC spark.sql() → JOIN tab_dados_receita + tab_dados_cep → CASE WHEN columns → SELECT por tabela → write
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
import threading
import time
import unicodedata
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)


print("V5 — Processamento nativo Spark (sem .toPandas()) | Dual-source: SERPRO / BrasilAPI")

spark = SparkSession.getActiveSession()

try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
except Exception:
    pass  # Serverless pode não permitir — o _sem_strings_vazias() cobre esse caso

try:
    TOKEN = dbutils.secrets.get(scope="vero", key="correios_token")
except Exception:
    TOKEN = ""

# SERPRO — Consulta CNPJ (deixe vazio "" para usar BrasilAPI gratuita)
# Obtenha em: https://servicos.serpro.gov.br → Consulta CNPJ
SERPRO_TOKEN = ""   # ex: "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."

_USAR_SERPRO = bool(SERPRO_TOKEN and SERPRO_TOKEN.strip())
print(f"Fonte Receita: {'SERPRO (sem rate-limit)' if _USAR_SERPRO else 'BrasilAPI (Semaphore 1, 0.35s/req)'}")

CATALOG        = "hive_metastore.accenture"
TABELA_RECEITA = f"{CATALOG}.tab_dados_receita"
TABELA_CEP     = f"{CATALOG}.tab_dados_cep"

# Limite de registros para processar. None = sem limite (produção).
LIMIT_REGISTROS = None  # ex: 1000 para teste

# Retry config para sessões HTTP
MAX_RETRIES = 4
BACKOFF     = 1.5

# BrasilAPI: 1 chamada simultânea para evitar 429
_brasilapi_sem = threading.Semaphore(1)

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
SELECT     TRY_CAST(REGEXP_REPLACE(ca.fatura_numero, '[^0-9]', '') AS BIGINT) AS FATURA,
    ca.fatura_numero                                                   AS NUMERO_FATURA,
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
    date_format(current_date(), 'yyyy-MM')   AS ID_LOTE,
    ca.sistema_origem                        AS CRM
FROM accenture.base_clientes_centralizada bc
INNER JOIN accenture.tb_dispersao_competencia_analitica ca
    ON bc.idcontrato = ca.CONTRATO
WHERE bc.crm <> 'NG'

UNION ALL

SELECT TRY_CAST(REGEXP_REPLACE(ca.fatura_numero, '[^0-9]', '') AS BIGINT) AS FATURA,
    ca.fatura_numero                                                   AS NUMERO_FATURA,
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
    date_format(current_date(), 'yyyy-MM')   AS ID_LOTE,
    ca.sistema_origem                        AS CRM
FROM accenture.base_clientes_centralizada bc
INNER JOIN accenture.tb_dispersao_competencia_analitica ca
    ON bc.codigocliente = ca.ID_CLIENTE
WHERE bc.crm = 'NG'
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
    f"INVALIDO: {_cnts.get('INVALIDO', 0)} | "
    f"NULO: {_cnts.get('NULO', 0)}"
)
if _cnts.get("CNPJ", 0) == 0:
    print("  AVISO: nenhum CNPJ no lote — DADOS_CADASTRAIS não será gerado.")

display(sdf.limit(5))

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


def _nova_sessao() -> requests.Session:
    retry = Retry(total=MAX_RETRIES, backoff_factor=BACKOFF,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if _USAR_SERPRO:
        _tok = SERPRO_TOKEN.strip()
        s.headers.update({
            "Authorization": _tok if _tok.lower().startswith("bearer ") else f"Bearer {_tok}",
            "Accept": "application/json",
            "User-Agent": "VeroValidacaoReceita/5.0",
        })
    else:
        s.headers.update({"User-Agent": "VeroValidacaoReceita/5.0"})
    return s


def _consultar_serpro(cnpj: str, session: requests.Session) -> dict[str, Any]:
    """Consulta SERPRO Consulta CNPJ e retorna dict normalizado para tab_dados_receita."""
    url = f"https://gateway.apiserpro.serpro.gov.br/consulta-cnpj/v1/basica/{cnpj}"
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return {"cnpj": cnpj, "data_consulta": data_consulta,
                    "status_consulta": f"erro_http_{resp.status_code}"}
        p = resp.json()
        # Endereço: pega o primeiro item de "enderecos" com tipo "ESTABELECIMENTO" ou qualquer um
        _enderecos = p.get("enderecos") or []
        _end = next((e for e in _enderecos if e.get("tipo") == "ESTABELECIMENTO"),
                    _enderecos[0] if _enderecos else {})
        _cep_raw = re.sub(r"\D", "", str(_end.get("cep") or ""))
        _telefones = p.get("telefones") or []
        _tel = f"{_telefones[0].get('ddd','')}{_telefones[0].get('numero','')}" if _telefones else None
        _cnae = p.get("cnaePrincipal") or {}
        _sit  = p.get("situacaoCadastral") or {}
        _nat  = p.get("naturezaJuridica") or {}
        _porte = p.get("porte") or {}
        # capital social vem como string "1000000.00"
        _cap = p.get("capitalSocial")
        try:
            _cap = float(str(_cap).replace(",", ".")) if _cap else None
        except (ValueError, TypeError):
            _cap = None
        return {
            "cnpj":                      cnpj,
            "razao_social":              p.get("nomeEmpresarial"),
            "nome_fantasia":             p.get("nomeFantasia"),
            "situacao_cadastral":        _sit.get("descricao") or _sit.get("codigo"),
            "data_situacao_cadastral":   _sit.get("data"),
            "motivo_situacao_cadastral": _sit.get("motivo"),
            "natureza_juridica":         _nat.get("descricao") or _nat.get("codigo"),
            "data_inicio_atividade":     p.get("dataAbertura"),
            "cnae_principal_codigo":     re.sub(r"\D", "", str(_cnae.get("codigo") or "")) or None,
            "cnae_principal_descricao":  _cnae.get("descricao"),
            "porte":                     _porte.get("descricao") or _porte.get("codigo"),
            "capital_social":            _cap,
            "opcao_simples":             None,  # SERPRO básica não retorna Simples
            "opcao_mei":                 None,
            "email":                     p.get("correioEletronico"),
            "telefone":                  _tel,
            "receita_cep":               _cep_raw.zfill(8) if len(_cep_raw) >= 7 else None,
            "receita_logradouro":        _end.get("logradouro"),
            "receita_numero":            _end.get("numero"),
            "receita_complemento":       _end.get("complemento"),
            "receita_bairro":            _end.get("bairro"),
            "receita_municipio":         (_end.get("municipio") or {}).get("descricao") or _end.get("municipio"),
            "receita_uf":                _end.get("uf"),
            "data_consulta":             data_consulta,
            "status_consulta":           "ok",
        }
    except requests.Timeout:
        return {"cnpj": cnpj, "data_consulta": data_consulta, "status_consulta": "timeout"}
    except requests.RequestException as exc:
        return {"cnpj": cnpj, "data_consulta": data_consulta, "status_consulta": f"erro_rede: {str(exc)[:120]}"}


def _consultar_brasilapi(cnpj: str, session: requests.Session) -> dict[str, Any]:
    """Consulta BrasilAPI e retorna dict normalizado para gravar no Delta."""
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(cnpj)}"
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
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


def _consultar_receita(cnpj: str, session: requests.Session) -> dict[str, Any]:
    """Roteador: usa SERPRO se token configurado, BrasilAPI caso contrário."""
    if _USAR_SERPRO:
        return _consultar_serpro(cnpj, session)
    else:
        return _consultar_brasilapi(cnpj, session)


def _worker(cnpj: str) -> dict[str, Any]:
    session = _nova_sessao()
    try:
        if _USAR_SERPRO:
            # SERPRO: sem rate-limit, paralelo direto
            return _consultar_receita(cnpj, session)
        else:
            # BrasilAPI: serializa via semáforo + throttle
            with _brasilapi_sem:
                result = _consultar_receita(cnpj, session)
                time.sleep(0.35)
            return result
    finally:
        session.close()


# --- Extrai CNPJs distintos usando Spark (sem .toPandas()) ---
_cnpj_rows = (
    sdf
    .select(F.regexp_replace(F.col("CPF_CNPJ"), r"\D", "").alias("doc_clean"))
    .filter(F.length(F.col("doc_clean")) == 14)
    .distinct()
    .collect()
)
_cnpjs_raw: set[str] = {r["doc_clean"] for r in _cnpj_rows}

cnpjs_lote: set[str] = {c for c in _cnpjs_raw if _cnpj_valido(c)}
invalidos_cnpj = len(_cnpjs_raw) - len(cnpjs_lote)
if invalidos_cnpj:
    print(f"  {invalidos_cnpj} CNPJ(s) com dígito verificador inválido ignorados.")

ja_ok: set[str] = {
    r["cnpj"]
    for r in spark.sql(
        f"SELECT cnpj FROM {TABELA_RECEITA} WHERE status_consulta = 'ok'"
    ).collect()
}

cnpjs_buscar = sorted(cnpjs_lote - ja_ok)
print(
    f"CNPJs na base-fonte: {len(_cnpjs_raw)} | Válidos: {len(cnpjs_lote)} | "
    f"Já na tabela (ok): {len(ja_ok)} | A buscar na API: {len(cnpjs_buscar)}"
)

if cnpjs_buscar:
    t0 = time.time()
    novos: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_worker, cnpj): cnpj for cnpj in cnpjs_buscar}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            cnpj = futures[future]
            try:
                rec = future.result()
                novos.append(rec)
                if rec.get("status_consulta") != "ok":
                    print(f"  [ERRO] {cnpj} — {rec.get('status_consulta')}")
            except Exception as exc:
                novos.append({"cnpj": cnpj, "status_consulta": f"excecao: {exc}",
                              "data_consulta": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
            if i % 50 == 0 or i == len(cnpjs_buscar):
                print(f"  {i}/{len(cnpjs_buscar)} CNPJs consultados ({time.time() - t0:.0f}s)")

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
    print(f"MERGE concluído: {ok_novos} gravados OK | {err_novos} com erro | {time.time() - t0:.1f}s")
else:
    print("Todos os CNPJs do lote já estão na tabela. Nenhuma chamada à API necessária.")

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
    """Consulta CEP em cascata: Correios API → ViaCEP → BrasilAPI → OpenCEP."""
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def _ok(fonte, d_logradouro="", d_bairro="", d_cidade="", d_uf="", d_complemento=""):
        return {"cep": cep, "logradouro": d_logradouro, "bairro": d_bairro,
                "cidade": d_cidade, "uf": d_uf, "complemento": d_complemento,
                "fonte": fonte, "data_consulta": data_consulta, "status_consulta": "ok"}

    # 1. Correios API (apenas se TOKEN configurado)
    if TOKEN:
        try:
            r = requests.get(
                f"https://api.correios.com.br/cep/v2/{cep}",
                headers={"Authorization": f"Bearer {TOKEN}"},
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()
            return _ok("Correios API",
                       d.get("logradouro", ""), d.get("bairro", ""),
                       d.get("localidade", ""), d.get("uf", ""), d.get("complemento", ""))
        except Exception:
            pass

    # 2. ViaCEP
    try:
        r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
        r.raise_for_status()
        d = r.json()
        if not d.get("erro"):
            return _ok("ViaCEP",
                       d.get("logradouro", ""), d.get("bairro", ""),
                       d.get("localidade", ""), d.get("uf", ""), d.get("complemento", ""))
    except Exception:
        pass

    # 3. BrasilAPI CEP v2 (agrega Correios + ViaCEP + outros)
    try:
        r = requests.get(f"https://brasilapi.com.br/api/cep/v2/{cep}", timeout=10)
        r.raise_for_status()
        d = r.json()
        if d.get("cep"):
            return _ok("BrasilAPI",
                       d.get("street", ""), d.get("neighborhood", ""),
                       d.get("city", ""), d.get("state", ""), "")
    except Exception:
        pass

    # 4. OpenCEP
    try:
        r = requests.get(f"https://opencep.com/v1/{cep}", timeout=10)
        r.raise_for_status()
        d = r.json()
        if d.get("cep"):
            return _ok("OpenCEP",
                       d.get("logradouro", ""), d.get("bairro", ""),
                       d.get("localidade", ""), d.get("uf", ""), d.get("complemento", ""))
    except Exception:
        pass

    return {"cep": cep, "data_consulta": data_consulta, "status_consulta": "nao_encontrado"}


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
# Normaliza a chave de join do cache (mesmo padrão de _cep_inst_norm: só dígitos, lpad 8)
_cep_raw     = _cep_raw.withColumn(
    "cep", F.lpad(F.regexp_replace(F.col("cep"), r"\D", ""), 8, "0")
)
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
        F.when(F.col("CPF_CNPJ").isNull() | (F.trim(F.col("CPF_CNPJ")) == ""), F.lit("NULO"))
         .when(F.length("_doc_norm") == 14, F.lit("CNPJ"))
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

_div_cidade_inst = (
    (F.col("_norm_cidade_inst") != "") &
    (F.col("_norm_ci_cidade")   != "") &
    (F.col("_norm_cidade_inst") != F.col("_norm_ci_cidade"))
)
_div_uf_inst = (
    (F.col("_norm_uf_inst") != "") &
    (F.col("_norm_ci_uf")   != "") &
    (F.col("_norm_uf_inst") != F.col("_norm_ci_uf"))
)

sdf_enriched = sdf_enriched.withColumn(
    "_obs_end_inst",
    F.when(
        F.col("ci_cep").isNull(),
        F.concat(F.lit("[CEP] CEP não encontrado no cache: "), F.col("_cep_inst_norm"))
    ).when(
        F.col("_cep_inst_generico") & (_div_cidade_inst | _div_uf_inst),
        F.concat(
            F.lit("CEP genérico — cidade/estado divergente: base '"),
            F.col("_cidade_inst_clean"), F.lit("/"), F.col("uf_instalacao"),
            F.lit("' x CEP '"),
            F.coalesce(F.col("ci_cidade"), F.lit("")), F.lit("/"),
            F.coalesce(F.col("ci_uf"), F.lit("")), F.lit("'")
        )
    ).when(
        F.col("_cep_inst_generico"),
        F.concat(
            F.lit("CEP genérico — município confirmado: "),
            F.coalesce(F.col("ci_cidade"), F.lit("")), F.lit("/"),
            F.coalesce(F.col("ci_uf"), F.lit(""))
        )
    ).otherwise(
        F.concat_ws(
            " | ",
            F.when(
                _div_cidade_inst,
                F.concat(
                    F.lit("Cidade divergente: base '"),
                    F.col("_cidade_inst_clean"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("ci_cidade"), F.lit("")), F.lit("'")
                )
            ),
            F.when(
                _div_uf_inst,
                F.concat(
                    F.lit("UF divergente: base '"),
                    F.col("uf_instalacao"),
                    F.lit("' x CEP '"),
                    F.coalesce(F.col("ci_uf"), F.lit("")), F.lit("'")
                )
            ),
        )
    )
)

sdf_enriched = sdf_enriched.withColumn(
    "_sv_end_inst",
    F.when(
        F.col("_cep_inst_generico") & (_div_cidade_inst | _div_uf_inst),
        F.lit("CepGenericoDiverge")
    ).when(
        F.col("_cep_inst_generico"),
        F.lit("CepGenericoOk")
    ).when(
        F.col("_obs_end_inst").isNull() | (F.col("_obs_end_inst") == ""),
        F.lit("Confere")
    ).otherwise(F.lit("Divergente"))
)

# ---------------------------------------------------------------------------
# 7f. Observação e status — ENDERECO_LEGAL
#     Parte CEP (igual ao instalação mas usando cl_* e cep_legal_*)
#     Parte Receita (somente CNPJ): compara CEP, cidade e UF com os dados da RF
# ---------------------------------------------------------------------------

_div_cidade_legal = (
    (F.col("_norm_cidade_legal") != "") &
    (F.col("_norm_cl_cidade")    != "") &
    (F.col("_norm_cidade_legal") != F.col("_norm_cl_cidade"))
)
_div_uf_legal = (
    (F.col("_norm_uf_legal") != "") &
    (F.col("_norm_cl_uf")    != "") &
    (F.col("_norm_uf_legal") != F.col("_norm_cl_uf"))
)

# Passo intermediário: obs baseada apenas no CEP legal
sdf_enriched = sdf_enriched.withColumn(
    "_obs_cep_legal",
    F.when(
        F.col("cl_cep").isNull(),
        F.concat(F.lit("[CEP] CEP não encontrado no cache: "), F.col("_cep_legal_norm"))
    ).when(
        F.col("_cep_legal_generico") & (_div_cidade_legal | _div_uf_legal),
        F.concat(
            F.lit("CEP genérico — cidade/estado divergente: base '"),
            F.col("_cidade_legal_clean"), F.lit("/"), F.col("uf_legal"),
            F.lit("' x CEP '"),
            F.coalesce(F.col("cl_cidade"), F.lit("")), F.lit("/"),
            F.coalesce(F.col("cl_uf"), F.lit("")), F.lit("'")
        )
    ).when(
        F.col("_cep_legal_generico"),
        F.concat(
            F.lit("CEP genérico — município confirmado: "),
            F.coalesce(F.col("cl_cidade"), F.lit("")), F.lit("/"),
            F.coalesce(F.col("cl_uf"), F.lit(""))
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
        F.col("_cep_legal_generico") & (_div_cidade_legal | _div_uf_legal),
        F.lit("CepGenericoDiverge")
    ).when(
        F.col("_cep_legal_generico") &
        (F.col("_obs_end_legal").isNull() | F.col("_obs_end_legal").contains("confirmado")),
        F.lit("CepGenericoOk")
    ).when(
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
sdf_end_inst = sdf_enriched.filter(~F.col("_doc_tipo").isin("INVALIDO", "NULO")).select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
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
    F.col("NUMERO_FATURA"),
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
_nulo_obs = F.lit("CPF/CNPJ não encontrado")

sdf_dados_cad_val = sdf_enriched.filter(F.col("_doc_tipo").isin("CNPJ", "NULO")).select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("DADOS_CADASTRAIS").alias("REGRA"),
    F.col("SEGMENTO"),
    F.col("_doc_norm").alias("Documento"),
    F.col("_doc_tipo").alias("Tipo"),
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
    # Validação — NULO → INCORRETO; CNPJ → lógica normal
    F.when(F.col("_doc_tipo") == "NULO", F.lit("INCORRETO"))
     .otherwise(F.col("_sv_cad")).alias("Status_Validacao"),
    F.when(F.col("_doc_tipo") == "NULO", _nulo_obs)
     .otherwise(F.col("_obs_cad")).alias("Observacao"),
    *_DIMS,
)

# ------------------------------------------------------------------
# 4) INVALIDOS
# ------------------------------------------------------------------
_inv_tipo_doc = F.when(
    F.col("CPF_CNPJ").isNull() | (F.trim(F.col("CPF_CNPJ")) == ""), F.lit("NULO")
).when(
    F.length(F.regexp_replace(F.col("CPF_CNPJ"), r"\D", "")) == 11, F.lit("CPF")
).when(
    F.length(F.regexp_replace(F.col("CPF_CNPJ"), r"\D", "")) == 14, F.lit("CNPJ")
).otherwise(F.lit("DOC"))

_inv_obs = F.when(
    F.col("CPF_CNPJ").isNull() | (F.trim(F.col("CPF_CNPJ")) == ""),
    F.lit("Nulo"),
).otherwise(F.concat(
    _inv_tipo_doc,
    F.lit("("),
    F.col("CPF_CNPJ"),
    F.lit(") - Inválido"),
))

sdf_invalidos = sdf_enriched.filter(F.col("_doc_tipo") == "INVALIDO").select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
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
    F.lit("INCORRETO").alias("Status_Validacao"),
    _inv_obs.alias("Observacao"),
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

validacao_dados_cadastrais = sdf_enriched.filter(F.col("_doc_tipo").isin("CNPJ", "NULO")).select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
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
        F.when(F.col(sv_col) == "Confere",            F.lit("CORRETO"))
         .when(F.col(sv_col) == "CepGenericoOk",      F.lit("CORRETO"))
         .when(F.col(sv_col) == "nao_carregado",      F.lit("PENDENTE"))
         .when(F.col(sv_col) == "CepGenericoDiverge", F.lit("INCORRETO"))
         .otherwise(F.lit("INCORRETO"))
    )
    substatus = (
        F.when(F.col(sv_col) == "Confere",            F.lit("OK"))
         .when(F.col(sv_col) == "CepGenericoOk",      F.lit("ALERTA"))
         .when(F.col(sv_col) == "nao_carregado",      F.lit("CACHE_VAZIO"))
         .when(F.col(sv_col) == "CepGenericoDiverge", F.lit("ERRO"))
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
_sf_inst = sdf_enriched.filter(~F.col("_doc_tipo").isin("INVALIDO", "NULO")).select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
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
    F.col("NUMERO_FATURA"),
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
_sf_cad = sdf_enriched.filter(F.col("_doc_tipo").isin("CNPJ", "NULO")).select(
    F.col("FATURA"),
    F.col("NUMERO_FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("DADOS_CADASTRAIS").alias("REGRA"),
    F.col("SEGMENTO"),
    F.when(F.col("_doc_tipo") == "NULO", F.lit("INCORRETO")).otherwise(_st_cad).alias("STATUS"),
    F.when(F.col("_doc_tipo") == "NULO", F.lit("ERRO")).otherwise(_ss_cad).alias("SUBSTATUS"),
    F.when(F.col("_doc_tipo") == "NULO", _nulo_obs).otherwise(F.col("_obs_cad")).alias("OBSERVACAO"),
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
    F.col("NUMERO_FATURA"),
    F.col("ID_CLIENTE_CONTRATO").alias("ID_CONTA_CONTRATO"),
    F.lit("INVALIDO").alias("REGRA"),
    F.col("SEGMENTO"),
    F.lit("INCORRETO").alias("STATUS"),
    F.lit("ERRO").alias("SUBSTATUS"),
    _inv_obs.alias("OBSERVACAO"),
    _inv_obs.alias("DADOS_BILLING"),
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


def _sem_strings_vazias(df):
    """Converte '' para NULL em colunas STRING — evita CAST_INVALID_INPUT em ANSI mode."""
    from pyspark.sql.types import StringType
    return df.select([
        F.when(F.col(c) == "", F.lit(None).cast(df.schema[c].dataType))
         .otherwise(F.col(c)).alias(c)
        if isinstance(df.schema[c].dataType, StringType)
        else F.col(c).alias(c)
        for c in df.columns
    ])

# Limpa dados anteriores preservando schema
for _tbl in ["validacao_enderecos", "validacao_dados_cadastrais"]:
    try:
        spark.sql(f"TRUNCATE TABLE {CATALOG}.{_tbl}")
        print(f"[TRUNCATE] {_tbl} limpa.")
    except Exception:
        pass  # tabela não existe ainda — será criada no write

try:
    spark.sql(f"""
        DELETE FROM {CATALOG}.validacao_status_fatura
        WHERE REGRA IN ('DADOS_CADASTRAIS', 'ENDERECO_INSTALACAO', 'ENDERECO_LEGAL', 'INVALIDO')
    """)
    print("[DELETE] validacao_status_fatura: registros do lote removidos.")
except Exception:
    pass  # tabela não existe ainda — será criada no write

# Grava as 3 tabelas de output
(
    _sem_strings_vazias(validacao_enderecos)
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.validacao_enderecos")
)

(
    _sem_strings_vazias(validacao_dados_cadastrais)
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.validacao_dados_cadastrais")
)

(
    _sem_strings_vazias(validacao_status_fatura)
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
