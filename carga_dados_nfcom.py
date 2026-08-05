# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga: accenture.tab_clientes_nfcom
# MAGIC **Vero Internet | Accenture**
# MAGIC
# MAGIC Consolida os dados de faturamento das 3 fontes (NG, ADAPTER, SIMETRA) em uma
# MAGIC tabela unificada com os campos necessários para validação da NFCom modelo 62.
# MAGIC
# MAGIC ### Fontes
# MAGIC | Sistema  | Tabela origem                                          | Chave contrato  |
# MAGIC |----------|--------------------------------------------------------|-----------------|
# MAGIC | NG       | negocio.base_faturamento_ng_julho2026                  | CONTA_NUMERO    |
# MAGIC | ADAPTER  | negocio.base_fechamento_faturamento_adapter_junho      | idcontrato      |
# MAGIC | SIMETRA  | negocio.base_faturamento_simetra (quando disponível)   | COD_CNTR        |
# MAGIC
# MAGIC ### Idempotência
# MAGIC DELETE por `sistema_origem + ciclo_faturamento` antes do INSERT.
# MAGIC Reprocessamento seguro para qualquer ciclo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ng",      "202607", "Ciclo NG (AAAAMM)")
dbutils.widgets.text("ciclo_adapter", "202607", "Ciclo ADAPTER (AAAAMM)")
dbutils.widgets.text("ciclo_simetra", "202607", "Ciclo SIMETRA (AAAAMM)")

CICLO_NG      = dbutils.widgets.get("ciclo_ng")
CICLO_ADAPTER = dbutils.widgets.get("ciclo_adapter")
CICLO_SIMETRA = dbutils.widgets.get("ciclo_simetra")

SCHEMA        = "accenture"
TBL_DEST      = f"{SCHEMA}.tab_clientes_nfcom"

TBL_NG        = "negocio.base_faturamento_ng_julho2026"
TBL_ADAPTER   = "negocio.base_fechamento_faturamento_adapter_junho"
TBL_SIMETRA   = "negocio.base_faturamento_simetra"   # quando disponível

print(f"Ciclo NG      : {CICLO_NG}")
print(f"Ciclo ADAPTER : {CICLO_ADAPTER}")
print(f"Ciclo SIMETRA : {CICLO_SIMETRA}")
print(f"Destino       : {TBL_DEST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark.conf.set("spark.sql.shuffle.partitions", "200")

print("✅ Setup concluído")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DDL — Tabela destino (idempotente)
# MAGIC
# MAGIC Campos selecionados com base nos requisitos de validação NFCom:
# MAGIC - Identificação do contrato, fatura e item
# MAGIC - Tributação real: ICMS, PIS, COFINS, FUST, FUNTTEL
# MAGIC - Classificação fiscal: CCLASS, CFOP, CST
# MAGIC - Dados geográficos: UF, cidade (para validação de CFOP intra/interestadual)
# MAGIC - Controle: sistema_origem, ciclo_faturamento, status NFCom

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_DEST} (

    -- Controle
    id_registro         STRING      COMMENT 'PK — MD5(sistema + contrato + fatura + item + ciclo)',
    sistema_origem      STRING      COMMENT 'NG | ADAPTER | SIMETRA',
    ciclo_faturamento   STRING      COMMENT 'AAAAMM',
    dt_carga            TIMESTAMP,

    -- Identificação do contrato e cliente
    id_conta_contrato   STRING      COMMENT 'CONTA_NUMERO (NG) | idcontrato (ADAPTER) | COD_CNTR (SIMETRA)',
    id_cliente          STRING      COMMENT 'COD_CLIENTE_SAP (NG/ADAPTER) | id_cliente (SIMETRA)',
    nome_assinante      STRING,
    tipo_pessoa         STRING      COMMENT 'PF | PJ — TipoPessoa (ADAPTER) | TIPO_ASSINANTE (NG)',
    empresa_prestadora  STRING,

    -- Identificação da fatura e item
    fatura_numero       STRING,
    fatura_data_emissao STRING,
    fatura_valor_atual  DOUBLE,
    nf_numero           STRING,
    nf_valor            DOUBLE,
    nf_item_cod_sap     STRING,
    nf_item_descricao   STRING,
    posicao_item        STRING,
    data_inicio_cobranca STRING,
    data_fim_cobranca   STRING,
    nf_item_valor       DOUBLE,

    -- Classificação fiscal (chave do motor NFCom)
    cclass              STRING      COMMENT 'Codigo CCLASS — chave principal do motor tributario',
    cfop                STRING      COMMENT 'CFOP do item (nulo para ISS/SVA/financeiro)',
    cst_icms            STRING      COMMENT 'CST ICMS do item (nulo para indSemCST)',

    -- Tributação real (standing)
    icms_aliquota       DOUBLE,
    icms_base_calculo   DOUBLE,
    icms_valor          DOUBLE,
    iss_aliquota        DOUBLE,
    iss_base_calculo    DOUBLE,
    iss_valor           DOUBLE,
    pis_aliquota        DOUBLE,
    pis_base_calculo    DOUBLE,
    pis_valor           DOUBLE,
    cofins_aliquota     DOUBLE,
    cofins_base_calculo DOUBLE,
    cofins_valor        DOUBLE,
    fust_aliquota       DOUBLE,
    fust_base_calculo   DOUBLE,
    fust_valor          DOUBLE,
    funttel_aliquota    DOUBLE,
    funttel_base_calculo DOUBLE,
    funttel_valor       DOUBLE,

    -- Dados geográficos (validação CFOP + ICMS)
    nf_uf               STRING      COMMENT 'UF do destinatário — para validação CFOP intra/interestadual',
    nf_cidade           STRING,

    -- Status NFCom
    status_nfcom        STRING,
    tipo_emissao_nfcom  STRING,
    cancelada           STRING,
    nota_substituta     STRING,
    nota_substituida    STRING,
    chave_acesso_nfcom  STRING,

    -- Regime especial
    regime_especial     STRING,

    -- Hash CDC
    hash_registro       STRING
)
USING DELTA
PARTITIONED BY (ciclo_faturamento, sistema_origem)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
""")

print(f"✅ Tabela {TBL_DEST} pronta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função auxiliar de normalização

# COMMAND ----------

def norm_str(col):
    """Normaliza string: trim + upper + nulos padronizados."""
    return F.when(
        F.upper(F.trim(col.cast(StringType()))).isin("", "NULL", "NAN", "NONE", "-"),
        F.lit(None).cast(StringType())
    ).otherwise(F.trim(col.cast(StringType())))

_NULL_STRS = ("", "null", "nan", "None", "NaN", "NULL", "none")

def norm_dbl(col):
    """Converte para double; strings nulas literais viram NULL antes do cast."""
    s = F.trim(col.cast(StringType()))
    return F.when(s.isNull() | s.isin(*_NULL_STRS), F.lit(None).cast("double")).otherwise(s.cast("double"))

def ciclo_from_date(col):
    """Extrai AAAAMM de campo de data."""
    return F.regexp_replace(F.substring(col.cast(StringType()), 1, 7), "-", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Leitura e mapeamento — NG
# MAGIC
# MAGIC Campos mapeados:
# MAGIC - Contrato   : CONTA_NUMERO
# MAGIC - Fatura     : FATURA_NUMERO
# MAGIC - CCLASS     : CCLASS (disponível na tabela)
# MAGIC - CST ICMS   : ICMS_CST
# MAGIC - CFOP       : CFOP
# MAGIC - Tributação : ICMS_*, PIS_*, COFINS_*, FUST_*, FUNTTEL_*
# MAGIC - UF/Cidade  : NF_UF, NF_CIDADE
# MAGIC - Tipo pessoa: TIPO_ASSINANTE → PF/PJ

# COMMAND ----------

df_ng_raw = (
    spark.table(TBL_NG)
    .filter(F.upper(F.trim(F.col("CANCELADA"))) == "NAO")
    .filter(F.trim(F.col("STATUS_NFCOM")) != "SUBSTITUIÇÃO")
    .filter(~((F.upper(F.trim(F.col("CATEGORIA_FISCAL"))) == "ICMS") &
              (F.col("ICMS_BASE_CALCULO").cast("double") == 0)))
)
cnt_ng_raw = df_ng_raw.count()
print(f"NG apos filtros: {cnt_ng_raw:,} registros")

df_ng = (
    df_ng_raw
    .withColumn("sistema_origem",      F.lit("NG"))
    .withColumn("ciclo_faturamento",   F.lit(CICLO_NG))
    .withColumn("dt_carga",            F.current_timestamp())
    # Identificação
    .withColumn("id_conta_contrato",   norm_str(F.col("CONTA_NUMERO")))
    .withColumn("id_cliente",          norm_str(F.col("COD_CLIENTE_SAP")))
    .withColumn("nome_assinante",      norm_str(F.col("NOME_ASSINANTE")))
    .withColumn("tipo_pessoa",         norm_str(F.col("TIPO_ASSINANTE")))
    .withColumn("empresa_prestadora",  norm_str(F.col("EMPRESA_PRESTADORA")))
    # Fatura e item
    .withColumn("fatura_numero",       norm_str(F.col("FATURA_NUMERO")))
    .withColumn("fatura_data_emissao", norm_str(F.col("FATURA_DATA_EMISSAO")))
    .withColumn("fatura_valor_atual",  norm_dbl(F.col("FATURA_VALOR_ATUAL")))
    .withColumn("nf_numero",           norm_str(F.col("NF_NUMERO")))
    .withColumn("nf_valor",            norm_dbl(F.col("NF_VALOR")))
    .withColumn("nf_item_cod_sap",     norm_str(F.col("NF_ITEM_COD_SAP")))
    .withColumn("nf_item_descricao",   norm_str(F.col("NF_ITEM_DESCRICAO")))
    .withColumn("posicao_item",        F.col("POSICAO_ITEM").cast(StringType()))
    .withColumn("data_inicio_cobranca",norm_str(F.col("DATA_INICIO_COBRANCA")))
    .withColumn("data_fim_cobranca",   norm_str(F.col("DATA_FIM_COBRANCA")))
    .withColumn("nf_item_valor",       norm_dbl(F.col("NF_ITEM_VALOR")))
    # Classificação fiscal — chaves do motor NFCom
    .withColumn("cclass",              norm_str(F.col("CCLASS")))
    .withColumn("cfop",               norm_str(F.col("CFOP")))
    .withColumn("cst_icms",           norm_str(F.col("ICMS_CST")))
    # Tributação real
    .withColumn("icms_aliquota",      norm_dbl(F.col("ICMS_ALIQUOTA")))
    .withColumn("icms_base_calculo",  norm_dbl(F.col("ICMS_BASE_CALCULO")))
    .withColumn("icms_valor",         norm_dbl(F.col("ICMS_VALOR_IMPOSTO")))
    .withColumn("iss_aliquota",       norm_dbl(F.col("ISS_ALIQUOTA")))
    .withColumn("iss_base_calculo",   norm_dbl(F.col("ISS_BASE_CALCULO")))
    .withColumn("iss_valor",          norm_dbl(F.col("ISS_VALOR_IMPOSTO")))
    .withColumn("pis_aliquota",       norm_dbl(F.col("PIS_ALIQUOTA")))
    .withColumn("pis_base_calculo",   norm_dbl(F.col("PIS_BASE_CALCULO")))
    .withColumn("pis_valor",          norm_dbl(F.col("PIS_VALOR_IMPOSTO")))
    .withColumn("cofins_aliquota",    norm_dbl(F.col("COFINS_ALIQUOTA")))
    .withColumn("cofins_base_calculo",norm_dbl(F.col("COFINS_BASE_CALCULO")))
    .withColumn("cofins_valor",       norm_dbl(F.col("COFINS_VALOR_IMPOSTO")))
    .withColumn("fust_aliquota",      norm_dbl(F.col("FUST_ALIQUOTA")))
    .withColumn("fust_base_calculo",  norm_dbl(F.col("FUST_BASE_CALCULO")))
    .withColumn("fust_valor",         norm_dbl(F.col("FUST_VALOR_IMPOSTO")))
    .withColumn("funttel_aliquota",   norm_dbl(F.col("FUNTTEL_ALIQUOTA")))
    .withColumn("funttel_base_calculo",norm_dbl(F.col("FUNTTEL_BASE_CALCULO")))
    .withColumn("funttel_valor",      norm_dbl(F.col("FUNTTEL_VALOR_IMPOSTO")))
    # Geográficos
    .withColumn("nf_uf",              norm_str(F.col("NF_UF")))
    .withColumn("nf_cidade",          norm_str(F.col("NF_CIDADE")))
    # Status NFCom
    .withColumn("status_nfcom",       norm_str(F.col("STATUS_NFCOM")))
    .withColumn("tipo_emissao_nfcom", norm_str(F.col("TIPO_EMISSAO_NFCOM")))
    .withColumn("cancelada",          norm_str(F.col("CANCELADA")))
    .withColumn("nota_substituta",    norm_str(F.col("NOTA_SUBSTITUTA")))
    .withColumn("nota_substituida",   norm_str(F.col("NOTA_SUBSTITUIDA")))
    .withColumn("chave_acesso_nfcom", norm_str(F.col("CHAVE_ACESSO_NFCOM")))
    .withColumn("regime_especial",    norm_str(F.col("REGIME_ESPECIAL")))
)

print(f"✅ NG lido: {df_ng_raw.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Leitura e mapeamento — ADAPTER
# MAGIC
# MAGIC Campos mapeados:
# MAGIC - Contrato   : idcontrato
# MAGIC - Fatura     : FATURA_NUMERO
# MAGIC - CCLASS     : **ausente na fonte** — campo nulo, aguarda enriquecimento via catálogo de produtos
# MAGIC - CST ICMS   : **ausente na fonte** — idem
# MAGIC - CFOP       : CFOP (disponível)
# MAGIC - Tributação : ICMS_*, PIS_*, COFINS_*, FUST_*, FUNTTEL_*
# MAGIC - UF/Cidade  : NF_UF, NF_CIDADE
# MAGIC - Tipo pessoa: TipoPessoa

# COMMAND ----------

df_adapter_raw = spark.table(TBL_ADAPTER)

df_adapter = (
    df_adapter_raw
    .withColumn("sistema_origem",      F.lit("ADAPTER"))
    .withColumn("ciclo_faturamento",   F.lit(CICLO_ADAPTER))
    .withColumn("dt_carga",            F.current_timestamp())
    # Identificação
    .withColumn("id_conta_contrato",   norm_str(F.col("idcontrato")))
    .withColumn("id_cliente",          norm_str(F.col("COD_CLIENTE_SAP")))
    .withColumn("nome_assinante",      norm_str(F.col("NOME_ASSINANTE")))
    .withColumn("tipo_pessoa",         norm_str(F.col("TipoPessoa")))
    .withColumn("empresa_prestadora",  norm_str(F.col("EMPRESA_PRESTADORA")))
    # Fatura e item
    .withColumn("fatura_numero",       norm_str(F.col("FATURA_NUMERO")))
    .withColumn("fatura_data_emissao", norm_str(F.col("FATURA_DATA_EMISSAO")))
    .withColumn("fatura_valor_atual",  norm_dbl(F.col("FATURA_VALOR_ATUAL")))
    .withColumn("nf_numero",           norm_str(F.col("NF_NUMERO")))
    .withColumn("nf_valor",            norm_dbl(F.col("NF_VALOR")))
    .withColumn("nf_item_cod_sap",     norm_str(F.col("NF_ITEM_COD_SAP")))
    .withColumn("nf_item_descricao",   norm_str(F.col("NF_ITEM_DESCRICAO")))
    .withColumn("posicao_item",        F.col("POSICAO_ITEM").cast(StringType()))
    .withColumn("data_inicio_cobranca",norm_str(F.col("DATA_INICIO_COBRANCA")))
    .withColumn("data_fim_cobranca",   norm_str(F.col("DATA_FIM_COBRANCA")))
    .withColumn("nf_item_valor",       norm_dbl(F.col("NF_ITEM_VALOR")))
    # Classificação fiscal
    # CCLASS e CST_ICMS ausentes no ADAPTER — virão via JOIN com catálogo de produtos (base de produtos)
    .withColumn("cclass",              F.lit(None).cast(StringType()))
    .withColumn("cfop",               norm_str(F.col("CFOP")))
    .withColumn("cst_icms",           F.lit(None).cast(StringType()))
    # Tributação real
    .withColumn("icms_aliquota",      norm_dbl(F.col("ICMS_ALIQUOTA")))
    .withColumn("icms_base_calculo",  norm_dbl(F.col("ICMS_BASE_CALCULO")))
    .withColumn("icms_valor",         norm_dbl(F.col("ICMS_VALOR_IMPOSTO")))
    .withColumn("iss_aliquota",       norm_dbl(F.col("ISS_ALIQUOTA")))
    .withColumn("iss_base_calculo",   norm_dbl(F.col("ISS_BASE_CALCULO")))
    .withColumn("iss_valor",          norm_dbl(F.col("ISS_VALOR_IMPOSTO")))
    .withColumn("pis_aliquota",       norm_dbl(F.col("PIS_ALIQUOTA")))
    .withColumn("pis_base_calculo",   norm_dbl(F.col("PIS_BASE_CALCULO")))
    .withColumn("pis_valor",          norm_dbl(F.col("PIS_VALOR_IMPOSTO")))
    .withColumn("cofins_aliquota",    norm_dbl(F.col("COFINS_ALIQUOTA")))
    .withColumn("cofins_base_calculo",norm_dbl(F.col("COFINS_BASE_CALCULO")))
    .withColumn("cofins_valor",       norm_dbl(F.col("COFINS_VALOR_IMPOSTO")))
    .withColumn("fust_aliquota",      norm_dbl(F.col("FUST_ALIQUOTA")))
    .withColumn("fust_base_calculo",  norm_dbl(F.col("FUST_BASE_CALCULO")))
    .withColumn("fust_valor",         norm_dbl(F.col("FUST_VALOR_IMPOSTO")))
    .withColumn("funttel_aliquota",   norm_dbl(F.col("FUNTTEL_ALIQUOTA")))
    .withColumn("funttel_base_calculo",norm_dbl(F.col("FUNTTEL_BASE_CALCULO")))
    .withColumn("funttel_valor",      norm_dbl(F.col("FUNTTEL_VALOR_IMPOSTO")))
    # Geográficos
    .withColumn("nf_uf",              norm_str(F.col("NF_UF")))
    .withColumn("nf_cidade",          norm_str(F.col("NF_CIDADE")))
    # Status NFCom — campos com nomes diferentes no ADAPTER
    .withColumn("status_nfcom",       norm_str(F.col("StatusNotaFiscal")))
    .withColumn("tipo_emissao_nfcom", norm_str(F.col("TipoEmissao")))
    .withColumn("cancelada",          norm_str(F.col("CANCELADA")))
    .withColumn("nota_substituta",    F.lit(None).cast(StringType()))
    .withColumn("nota_substituida",   F.lit(None).cast(StringType()))
    .withColumn("chave_acesso_nfcom", F.lit(None).cast(StringType()))
    .withColumn("regime_especial",    F.lit(None).cast(StringType()))
)

print(f"✅ ADAPTER lido: {df_adapter_raw.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Leitura e mapeamento — SIMETRA
# MAGIC
# MAGIC **Tabela ainda não disponível.**
# MAGIC Slot reservado — quando a tabela `negocio.base_faturamento_simetra` for disponibilizada,
# MAGIC descomentar o bloco abaixo ajustando os nomes de coluna conforme a estrutura recebida.
# MAGIC
# MAGIC Campos esperados (a confirmar):
# MAGIC - Contrato   : COD_CNTR
# MAGIC - Fatura     : FT_NFISCAL
# MAGIC - CCLASS     : CCLASS (interno SIMETRA — pode não ter mapeamento SEFAZ)
# MAGIC - CST ICMS   : a confirmar
# MAGIC - CFOP       : a confirmar
# MAGIC - Tributação : a confirmar

# COMMAND ----------

# ── SIMETRA — descomentar quando a tabela estiver disponível ─────────────────
#
# df_simetra_raw = spark.table(TBL_SIMETRA)
#
# df_simetra = (
#     df_simetra_raw
#     .withColumn("sistema_origem",      F.lit("SIMETRA"))
#     .withColumn("ciclo_faturamento",   F.lit(CICLO_SIMETRA))
#     .withColumn("dt_carga",            F.current_timestamp())
#     .withColumn("id_conta_contrato",   norm_str(F.col("COD_CNTR")))
#     .withColumn("id_cliente",          norm_str(F.col("ID_CLIENTE")))
#     .withColumn("nome_assinante",      norm_str(F.col("NOME_CLIENTE")))
#     .withColumn("tipo_pessoa",         norm_str(F.col("TP_PESSOA")))
#     .withColumn("empresa_prestadora",  norm_str(F.col("EMPRESA")))
#     .withColumn("fatura_numero",       norm_str(F.col("FT_NFISCAL")))
#     .withColumn("fatura_data_emissao", norm_str(F.col("DT_EMISSAO")))
#     .withColumn("fatura_valor_atual",  norm_dbl(F.col("VL_FATURA")))
#     .withColumn("nf_numero",           norm_str(F.col("NF_NUMERO")))
#     .withColumn("nf_valor",            norm_dbl(F.col("NF_VALOR")))
#     .withColumn("nf_item_cod_sap",     norm_str(F.col("COD_PRODUTO")))
#     .withColumn("nf_item_descricao",   norm_str(F.col("DESC_PRODUTO")))
#     .withColumn("posicao_item",        F.col("POSICAO").cast(StringType()))
#     .withColumn("data_inicio_cobranca",norm_str(F.col("DT_INI_COBRANCA")))
#     .withColumn("data_fim_cobranca",   norm_str(F.col("DT_FIM_COBRANCA")))
#     .withColumn("nf_item_valor",       norm_dbl(F.col("VL_ITEM")))
#     .withColumn("cclass",              norm_str(F.col("CCLASS")))
#     .withColumn("cfop",               norm_str(F.col("CFOP")))
#     .withColumn("cst_icms",           norm_str(F.col("CST_ICMS")))
#     .withColumn("icms_aliquota",      norm_dbl(F.col("ICMS_ALIQUOTA")))
#     # ... demais campos tributários
#     .withColumn("nf_uf",              norm_str(F.col("UF_DEST")))
#     .withColumn("nf_cidade",          norm_str(F.col("MUNICIPIO_DEST")))
#     .withColumn("status_nfcom",       F.lit(None).cast(StringType()))
#     .withColumn("tipo_emissao_nfcom", F.lit(None).cast(StringType()))
#     .withColumn("cancelada",          norm_str(F.col("CANCELADA")))
#     .withColumn("nota_substituta",    F.lit(None).cast(StringType()))
#     .withColumn("nota_substituida",   F.lit(None).cast(StringType()))
#     .withColumn("chave_acesso_nfcom", F.lit(None).cast(StringType()))
#     .withColumn("regime_especial",    F.lit(None).cast(StringType()))
# )
# ─────────────────────────────────────────────────────────────────────────────

print("⚠ SIMETRA: tabela ainda não disponível — slot reservado no job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Seleção dos campos finais e PK

# COMMAND ----------

COLS_FINAIS = [
    "id_registro", "sistema_origem", "ciclo_faturamento", "dt_carga",
    "id_conta_contrato", "id_cliente", "nome_assinante", "tipo_pessoa", "empresa_prestadora",
    "fatura_numero", "fatura_data_emissao", "fatura_valor_atual",
    "nf_numero", "nf_valor", "nf_item_cod_sap", "nf_item_descricao",
    "posicao_item", "data_inicio_cobranca", "data_fim_cobranca", "nf_item_valor",
    "cclass", "cfop", "cst_icms",
    "icms_aliquota", "icms_base_calculo", "icms_valor",
    "iss_aliquota",  "iss_base_calculo",  "iss_valor",
    "pis_aliquota",  "pis_base_calculo",  "pis_valor",
    "cofins_aliquota","cofins_base_calculo","cofins_valor",
    "fust_aliquota", "fust_base_calculo", "fust_valor",
    "funttel_aliquota","funttel_base_calculo","funttel_valor",
    "nf_uf", "nf_cidade",
    "status_nfcom", "tipo_emissao_nfcom", "cancelada",
    "nota_substituta", "nota_substituida", "chave_acesso_nfcom",
    "regime_especial", "hash_registro",
]

def preparar(df, sistema, ciclo):
    return (
        df
        .withColumn("id_registro",
            F.md5(F.concat_ws("|",
                F.lit(sistema),
                F.coalesce(F.col("id_conta_contrato"), F.lit("")),
                F.coalesce(F.col("fatura_numero"),     F.lit("")),
                F.coalesce(F.col("nf_item_cod_sap"),   F.lit("")),
                F.coalesce(F.col("posicao_item"),      F.lit("")),
                F.lit(ciclo),
            ))
        )
        .withColumn("hash_registro",
            F.sha2(F.concat_ws("|",
                F.coalesce(F.col("cclass"),          F.lit("")),
                F.coalesce(F.col("cfop"),            F.lit("")),
                F.coalesce(F.col("cst_icms"),        F.lit("")),
                F.coalesce(F.col("icms_aliquota").cast(StringType()), F.lit("")),
                F.coalesce(F.col("pis_aliquota").cast(StringType()),  F.lit("")),
                F.coalesce(F.col("nf_uf"),           F.lit("")),
            ), 256)
        )
        .select(*COLS_FINAIS)
    )

df_ng_final      = preparar(df_ng,      "NG",      CICLO_NG)
df_adapter_final = preparar(df_adapter, "ADAPTER", CICLO_ADAPTER)

# Union das fontes disponíveis
# Adicionar df_simetra_final quando SIMETRA estiver disponível
df_union = df_ng_final.union(df_adapter_final)

total = df_union.count()
print(f"✅ Union concluída: {total:,} registros")
print(f"   NG      : {df_ng_final.count():,}")
print(f"   ADAPTER : {df_adapter_final.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Idempotência — DELETE por sistema + ciclo antes do INSERT

# COMMAND ----------

# Apaga apenas os ciclos que serão recarregados — não afeta outros ciclos
for sistema, ciclo in [("NG", CICLO_NG), ("ADAPTER", CICLO_ADAPTER)]:
    spark.sql(f"""
        DELETE FROM {TBL_DEST}
        WHERE sistema_origem    = '{sistema}'
          AND ciclo_faturamento = '{ciclo}'
    """)
    print(f"✅ DELETE: sistema={sistema} | ciclo={ciclo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. INSERT

# COMMAND ----------

(
    df_union
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(TBL_DEST)
)

print(f"✅ INSERT concluído: {total:,} registros gravados em {TBL_DEST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. QA — Resumo por sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     ciclo_faturamento,
# MAGIC     COUNT(*)                          AS registros,
# MAGIC     COUNT(DISTINCT id_conta_contrato) AS contratos,
# MAGIC     COUNT(DISTINCT fatura_numero)     AS faturas,
# MAGIC     COUNT(DISTINCT nf_item_cod_sap)   AS produtos,
# MAGIC     SUM(CASE WHEN cclass  IS NULL THEN 1 ELSE 0 END) AS sem_cclass,
# MAGIC     SUM(CASE WHEN cfop    IS NULL THEN 1 ELSE 0 END) AS sem_cfop,
# MAGIC     SUM(CASE WHEN cst_icms IS NULL THEN 1 ELSE 0 END) AS sem_cst,
# MAGIC     SUM(CASE WHEN nf_uf   IS NULL THEN 1 ELSE 0 END) AS sem_uf
# MAGIC FROM accenture.tab_clientes_nfcom
# MAGIC GROUP BY sistema_origem, ciclo_faturamento
# MAGIC ORDER BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. QA — ADAPTER: produtos sem CCLASS (pendência base de produtos)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nf_item_cod_sap,
# MAGIC     nf_item_descricao,
# MAGIC     COUNT(*)              AS ocorrencias,
# MAGIC     COUNT(DISTINCT fatura_numero) AS faturas
# MAGIC FROM accenture.tab_clientes_nfcom
# MAGIC WHERE sistema_origem  = 'ADAPTER'
# MAGIC   AND cclass IS NULL
# MAGIC GROUP BY nf_item_cod_sap, nf_item_descricao
# MAGIC ORDER BY ocorrencias DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. QA — NG: distribuição de CCLASS por grupo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cclass,
# MAGIC     SUBSTR(cclass, 1, 1) AS grupo_cclass,
# MAGIC     cfop,
# MAGIC     cst_icms,
# MAGIC     COUNT(*)             AS ocorrencias,
# MAGIC     ROUND(AVG(icms_aliquota), 4) AS icms_medio,
# MAGIC     ROUND(AVG(pis_aliquota),  4) AS pis_medio
# MAGIC FROM accenture.tab_clientes_nfcom
# MAGIC WHERE sistema_origem = 'NG'
# MAGIC GROUP BY cclass, cfop, cst_icms
# MAGIC ORDER BY ocorrencias DESC
# MAGIC LIMIT 30