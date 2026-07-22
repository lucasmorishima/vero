# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga de Dados Cadastrais — Receita Federal (BrasilAPI)
# MAGIC
# MAGIC **Objetivo:** Busca todos os CNPJs únicos da base de clientes na API da Receita Federal
# MAGIC e persiste o resultado em `hive_metastore.accenture.tb_dados_receita`.
# MAGIC
# MAGIC **Modo de execução:**
# MAGIC - `incremental` *(padrão)* — consulta apenas CNPJs ausentes ou com erro anterior
# MAGIC - `full` — reprocessa todos os CNPJs (use para refresh completo)
# MAGIC
# MAGIC **Saída:** `hive_metastore.accenture.tb_dados_receita` (Delta, upsert por CNPJ)
# MAGIC
# MAGIC **Agendamento sugerido:** rodada noturna / fora do horário de pico.
# MAGIC Após a carga, execute `V_2_validar_enderecos_notebook` para o batimento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

from __future__ import annotations

import concurrent.futures
import re
import threading
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração

# COMMAND ----------

# "incremental" → busca apenas CNPJs ausentes ou com erro anterior
# "full"        → reprocessa todos os CNPJs (refresh completo)
MODO = "incremental"

CATALOG        = "hive_metastore.accenture"
TABELA_DESTINO = f"{CATALOG}.tb_dados_receita"

# Parallelismo e rate-limit
MAX_WORKERS = 20   # threads simultâneas para CEP/IO paralelo
# BrasilAPI tolera 1 chamada por vez sem 429
_brasilapi_sem = threading.Semaphore(1)

MAX_RETRIES   = 4
BACKOFF       = 1.5
TIMEOUT_SEG   = 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query — CNPJs únicos da base de clientes

# COMMAND ----------

_QUERY_CNPJS = """
SELECT DISTINCT
    REGEXP_REPLACE(ca.CPF_CNPJ, '[^0-9]', '') AS cnpj_limpo
FROM hive_metastore.accenture.tb_dispersao_competencia_analitica ca
WHERE ca.CPF_CNPJ IS NOT NULL
  AND LENGTH(REGEXP_REPLACE(ca.CPF_CNPJ, '[^0-9]', '')) = 14
"""

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


_cnpjs_raw: set[str] = {r["cnpj_limpo"] for r in spark.sql(_QUERY_CNPJS).collect()}
todos_cnpjs: set[str] = {c for c in _cnpjs_raw if _cnpj_valido(c)}
invalidos = len(_cnpjs_raw) - len(todos_cnpjs)
print(f"{len(_cnpjs_raw)} CNPJs únicos na base | {invalidos} inválidos (DV) ignorados | {len(todos_cnpjs)} válidos a processar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filtra CNPJs a processar (modo incremental)

# COMMAND ----------

if MODO == "incremental" and spark.catalog.tableExists(TABELA_DESTINO):
    # Considera "ok" apenas os registros consultados com sucesso
    ja_ok: set[str] = {
        r["cnpj"]
        for r in spark.sql(
            f"SELECT cnpj FROM {TABELA_DESTINO} WHERE status_consulta = 'ok'"
        ).collect()
    }
    cnpjs_processar = sorted(todos_cnpjs - ja_ok)
    print(
        f"Modo incremental: {len(ja_ok)} já carregados OK | "
        f"{len(cnpjs_processar)} a processar."
    )
else:
    cnpjs_processar = sorted(todos_cnpjs)
    print(f"Modo full: {len(cnpjs_processar)} CNPJs serão (re)consultados.")

if not cnpjs_processar:
    print("Nada a fazer. Tabela já atualizada.")
    dbutils.notebook.exit("OK — nenhum CNPJ novo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Funções de consulta à BrasilAPI

# COMMAND ----------

def _nova_sessao() -> requests.Session:
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "VeroCarregaDadosReceita/1.0"})
    return s


def consultar_receita(cnpj: str) -> dict[str, Any]:
    """Consulta BrasilAPI e retorna dict normalizado pronto para gravar em Delta."""
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(cnpj)}"
    session = _nova_sessao()
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        with _brasilapi_sem:
            resp = session.get(url, timeout=TIMEOUT_SEG)
            time.sleep(0.35)   # throttle: ~3 req/s máximo para evitar 429

        if resp.status_code != 200:
            return {
                "cnpj": cnpj,
                "data_consulta": data_consulta,
                "status_consulta": f"erro_http_{resp.status_code}",
            }

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
        return {"cnpj": cnpj, "data_consulta": data_consulta, "status_consulta": f"erro_rede: {str(exc)[:120]}"}
    finally:
        session.close()


print("Funções de consulta carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Processamento paralelo

# COMMAND ----------

t_inicio = time.time()
total    = len(cnpjs_processar)
resultados: list[dict] = []
sucessos = erros = 0

print(f"Iniciando consulta de {total} CNPJs | workers={MAX_WORKERS} | brasilapi_sem=1\n")

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(consultar_receita, cnpj): cnpj for cnpj in cnpjs_processar}

    for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
        cnpj = futures[future]
        try:
            rec = future.result()
            resultados.append(rec)
            if rec.get("status_consulta") == "ok":
                sucessos += 1
            else:
                erros += 1
                print(f"  [ERRO] {cnpj} — {rec.get('status_consulta')}")
        except Exception as exc:
            erros += 1
            resultados.append({"cnpj": cnpj, "status_consulta": f"excecao: {exc}", "data_consulta": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
            print(f"  [EXCEÇÃO] {cnpj} — {exc}")

        if i % 50 == 0 or i == total:
            pct = i / total * 100
            elapsed = time.time() - t_inicio
            eta = (elapsed / i) * (total - i) if i > 0 else 0
            print(f"  {i}/{total} ({pct:.0f}%) | ok={sucessos} | erros={erros} | ETA={eta:.0f}s")

print(f"\nConsultas concluídas em {time.time() - t_inicio:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Schema Delta — tb_dados_receita

# COMMAND ----------

_SCHEMA = StructType([
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

_VAZIOS = {"", "nan", "NaN", "None", "none", "null", "NULL", "NaT", "na", "NA"}

def _limpar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str) and v in _VAZIOS:
        return None
    return v

def _limpar_registro(r: dict) -> dict:
    """Garante que todas as colunas do schema estão presentes e limpas."""
    limpo: dict = {}
    for campo in [f.name for f in _SCHEMA.fields]:
        v = _limpar(r.get(campo))
        # Conversões de tipo explícitas para evitar ClassCastException
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
        elif campo in ("opcao_simples", "opcao_mei") and v is not None:
            if isinstance(v, bool):
                pass
            elif str(v).lower() in ("true", "1", "sim"):
                v = True
            elif str(v).lower() in ("false", "0", "nao", "não"):
                v = False
            else:
                v = None
        limpo[campo] = v
    return limpo

registros_limpos = [_limpar_registro(r) for r in resultados]
df_spark = spark.createDataFrame(registros_limpos, schema=_SCHEMA)
print(f"DataFrame criado: {df_spark.count()} registros | {len(df_spark.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Persiste na tabela Delta (MERGE / upsert por CNPJ)

# COMMAND ----------

if spark.catalog.tableExists(TABELA_DESTINO):
    # Upsert: atualiza se já existe, insere se é novo
    (
        DeltaTable.forName(spark, TABELA_DESTINO)
        .alias("destino")
        .merge(df_spark.alias("novo"), "destino.cnpj = novo.cnpj")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"MERGE concluído em {TABELA_DESTINO}")
else:
    # Primeira execução: cria a tabela
    (
        df_spark.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABELA_DESTINO)
    )
    print(f"Tabela criada: {TABELA_DESTINO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo da execução

# COMMAND ----------

t_total = time.time() - t_inicio
total_tabela = spark.sql(f"SELECT COUNT(*) AS n FROM {TABELA_DESTINO}").collect()[0]["n"]

print("=" * 55)
print("RESUMO DA CARGA")
print("=" * 55)
print(f"Modo                   : {MODO}")
print(f"CNPJs na base          : {len(todos_cnpjs)}")
print(f"CNPJs consultados      : {total}")
print(f"Sucesso (ok)           : {sucessos}")
print(f"Erros                  : {erros}")
print(f"Total na tabela destino: {total_tabela}")
print(f"Tempo total            : {t_total:.1f}s ({t_total/60:.1f} min)")
print("=" * 55)

display(
    spark.sql(f"""
        SELECT status_consulta, COUNT(*) AS qtd
        FROM {TABELA_DESTINO}
        GROUP BY status_consulta
        ORDER BY qtd DESC
    """)
)
