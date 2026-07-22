# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabela Verdade NFS-e — ISS Alíquotas por Município
# MAGIC
# MAGIC **Origem:** API oficial NFS-e Nacional + `tributario.tb_municipios`
# MAGIC
# MAGIC **Destino:** `tributario.tb_iss_aliquotas` (Delta, modo overwrite)
# MAGIC
# MAGIC **Processamento:** `ThreadPoolExecutor` paralelo · retry com backoff · sem pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

import concurrent.futures
import json
import time
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração — ajuste antes de executar

# COMMAND ----------

# ------------------------------------------------------------
# Endpoint base da API NFS-e Nacional.
# Exemplo: "https://www.nfse.gov.br/api/v1/aliquotas"
# Consulte a documentação oficial para o endpoint correto.
# ------------------------------------------------------------
URL_BASE = "https://www.nfse.gov.br/api/v1/aliquotas"  # <-- AJUSTAR

# Token de autenticação (Bearer). Deixe "" se não usar.
TOKEN = ""

# Caminho para certificado cliente (.pem) quando exigido pela API.
# Use False para desabilitar verificação SSL (não recomendado em prod).
CERTIFICADO = True  # <-- AJUSTAR: "/dbfs/caminho/cert.pem" ou False

# Headers adicionais exigidos pela API (acrescentar conforme documentação)
HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json",
}
if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"

# Tabelas
TABELA_ORIGEM  = "tributario.tb_municipios"
TABELA_DESTINO = "tributario.tb_iss_aliquotas"

# Paralelismo e retry
MAX_WORKERS = 20   # threads simultâneas
MAX_RETRIES = 3    # tentativas por município
TIMEOUT_SEG = 30   # timeout por chamada (segundos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carrega municípios da tabela Spark

# COMMAND ----------

# Lê apenas as colunas necessárias e converte para lista de dicts Python.
# O collect() é feito uma única vez — o restante do processamento é Python puro.
df_municipios = (
    spark.table(TABELA_ORIGEM)
         .select("codigo_ibge", "municipio", "uf")
)

municipios = [row.asDict() for row in df_municipios.collect()]
total_municipios = len(municipios)
print(f"{total_municipios} municípios carregados de {TABELA_ORIGEM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funções auxiliares — consulta API com retry

# COMMAND ----------

def _nova_sessao() -> requests.Session:
    """Sessão com retry automático para erros de rede (5xx).
    O 429 é tratado manualmente com backoff exponencial no loop principal."""
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s


def _registro_erro(mun: dict, data_consulta: str, status: str, json_raw: str = "") -> dict:
    """Retorna um registro com todos os campos obrigatórios preenchidos com NULL
    quando a API falha, para não interromper a execução."""
    return {
        "codigo_ibge":      mun["codigo_ibge"],
        "municipio":        mun["municipio"],
        "uf":               mun["uf"],
        "codigo_servico":   None,
        "codigo_trecho":    None,
        "aliquota":         None,
        "aliquota_efetiva": None,
        "inicio_vigencia":  None,
        "fim_vigencia":     None,
        "fonte":            "NFS-e Nacional",
        "data_consulta":    data_consulta,
        "json_retorno":     json_raw or None,
        "status":           status,
    }


def _extrair_itens(dados) -> list:
    """Normaliza o retorno da API: aceita lista raiz ou dict com chave de lista."""
    if isinstance(dados, list):
        return dados
    # Tenta chaves comuns de paginação/envelope
    for chave in ("itens", "aliquotas", "data", "result", "results", "content"):
        if isinstance(dados.get(chave), list):
            return dados[chave]
    # Retorno é um objeto único — envolve em lista
    return [dados]


def consultar_aliquotas(mun: dict) -> list[dict]:
    """Consulta a API NFS-e para um município.

    Retorna lista de dicts (um por código de serviço/trecho).
    Em caso de falha retorna lista com um único registro de erro.
    """
    codigo_ibge = mun["codigo_ibge"]
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    session = _nova_sessao()

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            url = f"{URL_BASE}/{codigo_ibge}"
            resp = session.get(url, timeout=TIMEOUT_SEG, verify=CERTIFICADO)

            # --- Sucesso ---
            if resp.status_code == 200:
                try:
                    dados = resp.json()
                except json.JSONDecodeError:
                    return [_registro_erro(mun, data_consulta, "json_invalido", resp.text[:500])]

                itens = _extrair_itens(dados)

                if not itens:
                    return [_registro_erro(mun, data_consulta, "sem_dados", resp.text[:500])]

                registros = []
                for item in itens:
                    # Campos mapeados com fallback para variações de nomenclatura
                    try:
                        aliquota = item.get("aliquota") or item.get("aliq") or item.get("taxaIss")
                        aliquota = float(aliquota) if aliquota is not None else None
                    except (TypeError, ValueError):
                        aliquota = None

                    try:
                        aliquota_ef = (
                            item.get("aliquotaEfetiva")
                            or item.get("aliquota_efetiva")
                            or item.get("aliqEfetiva")
                        )
                        aliquota_ef = float(aliquota_ef) if aliquota_ef is not None else None
                    except (TypeError, ValueError):
                        aliquota_ef = None

                    registros.append({
                        "codigo_ibge":      str(codigo_ibge),
                        "municipio":        str(mun["municipio"]),
                        "uf":               str(mun["uf"]),
                        "codigo_servico":   str(item.get("codigoServico") or item.get("codigo_servico") or item.get("codServico") or ""),
                        "codigo_trecho":    str(item.get("codigoTrecho") or item.get("codigo_trecho") or item.get("codTrecho") or ""),
                        "aliquota":         aliquota,
                        "aliquota_efetiva": aliquota_ef,
                        "inicio_vigencia":  str(item.get("inicioVigencia") or item.get("inicio_vigencia") or item.get("dtInicioVigencia") or ""),
                        "fim_vigencia":     str(item.get("fimVigencia") or item.get("fim_vigencia") or item.get("dtFimVigencia") or ""),
                        "fonte":            "NFS-e Nacional",
                        "data_consulta":    data_consulta,
                        "json_retorno":     json.dumps(item, ensure_ascii=False),
                        "status":           "ok",
                    })
                return registros

            # --- Rate limit: backoff exponencial e retry ---
            elif resp.status_code == 429:
                espera = 2 ** tentativa
                time.sleep(espera)
                continue

            # --- Erro HTTP definitivo ---
            else:
                if tentativa == MAX_RETRIES:
                    return [_registro_erro(mun, data_consulta, f"erro_http_{resp.status_code}", resp.text[:500])]
                time.sleep(tentativa)

        except requests.Timeout:
            if tentativa == MAX_RETRIES:
                return [_registro_erro(mun, data_consulta, "timeout")]
            time.sleep(tentativa)

        except requests.RequestException as exc:
            if tentativa == MAX_RETRIES:
                return [_registro_erro(mun, data_consulta, f"erro_rede: {exc}")]
            time.sleep(tentativa)

        finally:
            session.close()

    return [_registro_erro(mun, data_consulta, "max_tentativas_excedidas")]


print("Funções de consulta carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Processamento paralelo

# COMMAND ----------

t_inicio = time.time()

todos_registros: list[dict] = []
sucessos = 0
erros    = 0

print(f"Iniciando processamento de {total_municipios} municípios | workers={MAX_WORKERS}\n")

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submete todas as tarefas de uma vez
    futures = {
        executor.submit(consultar_aliquotas, mun): mun
        for mun in municipios
    }

    for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
        mun = futures[future]
        try:
            registros = future.result()
            todos_registros.extend(registros)

            # Contabiliza sucesso vs erro pelo campo status do primeiro registro
            if registros and registros[0]["status"] == "ok":
                sucessos += 1
            else:
                erros += 1
                print(f"  [ERRO] {mun['municipio']}/{mun['uf']} — {registros[0]['status']}")

        except Exception as exc:
            erros += 1
            print(f"  [EXCEÇÃO] {mun['municipio']}/{mun['uf']} — {exc}")

        # Log de progresso a cada 100 municípios
        if i % 100 == 0 or i == total_municipios:
            pct = i / total_municipios * 100
            print(f"  {i}/{total_municipios} ({pct:.0f}%) | ok={sucessos} | erros={erros}")

t_fim = time.time()
print(f"\nProcessamento concluído em {t_fim - t_inicio:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Converte para DataFrame Spark (sem pandas)

# COMMAND ----------

# Schema explícito garante tipos corretos e NULLs onde necessário
_SCHEMA = StructType([
    StructField("codigo_ibge",      StringType(), True),
    StructField("municipio",        StringType(), True),
    StructField("uf",               StringType(), True),
    StructField("codigo_servico",   StringType(), True),
    StructField("codigo_trecho",    StringType(), True),
    StructField("aliquota",         DoubleType(), True),
    StructField("aliquota_efetiva", DoubleType(), True),
    StructField("inicio_vigencia",  StringType(), True),
    StructField("fim_vigencia",     StringType(), True),
    StructField("fonte",            StringType(), True),
    StructField("data_consulta",    StringType(), True),
    StructField("json_retorno",     StringType(), True),
    StructField("status",           StringType(), True),
])

# Normaliza strings vazias para None → NULL no Spark
_VAZIOS = {"", "nan", "None", "none", "null", "NULL"}

def _limpar(v):
    if v is None:
        return None
    if isinstance(v, str) and v in _VAZIOS:
        return None
    return v

registros_limpos = [
    {k: _limpar(v) for k, v in r.items()}
    for r in todos_registros
]

df = spark.createDataFrame(registros_limpos, schema=_SCHEMA)

print(f"DataFrame criado: {df.count()} registros | {len(df.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Grava na tabela Delta

# COMMAND ----------

# Modo overwrite recria a tabela com os dados mais recentes.
# overwriteSchema=true permite atualizar o schema caso mude entre execuções.
(
    df.write
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TABELA_DESTINO)
)

print(f"Tabela gravada: {TABELA_DESTINO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumo da execução

# COMMAND ----------

total_registros = df.count()
tempo_total     = t_fim - t_inicio

print("=" * 50)
print("RESUMO DA EXECUÇÃO")
print("=" * 50)
print(f"Municípios processados : {total_municipios}")
print(f"Sucessos               : {sucessos}")
print(f"Erros                  : {erros}")
print(f"Total de registros     : {total_registros}")
print(f"Tempo total            : {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
print("=" * 50)

# Distribuição por status
display(
    df.groupBy("status", "uf")
      .count()
      .orderBy("status", "uf")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Visualização dos dados gravados

# COMMAND ----------

display(df)
