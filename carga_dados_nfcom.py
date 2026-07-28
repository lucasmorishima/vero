# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Standing `dados_nfcom_cliente`
# MAGIC **Vero Internet | Accenture | v4.0**
# MAGIC
# MAGIC UNION das três fontes normalizadas para um standing fiscal único.
# MAGIC O que não existe na tabela de origem chega como `null`.
# MAGIC
# MAGIC | Fonte | Tabela | Sistema |
# MAGIC |---|---|---|
# MAGIC | Adapter (fiscal) | `bronze.adapter_aliquotas` | ADAPTER |
# MAGIC | Faturamento NG | `hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS` | NG |
# MAGIC | Protheus/SIMETRA | `NEGOCIO.TB_FATURAMENTO_PROTHEUS_FILTRADA` | SIMETRA |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ref",         "202607", "Ciclo (AAAAMM)")
dbutils.widgets.text("executar_optimize", "true",   "Executar OPTIMIZE?")

CICLO_REF         = dbutils.widgets.get("ciclo_ref")
EXECUTAR_OPTIMIZE = dbutils.widgets.get("executar_optimize").lower() == "true"

TABELA_DEST  = "accenture.dados_nfcom_cliente"
TBL_ADAPTER  = "bronze.adapter_aliquotas"
TBL_NG       = "hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS"
TBL_PROTHEUS = "NEGOCIO.TB_FATURAMENTO_PROTHEUS_FILTRADA"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DecimalType, IntegerType, StringType, TimestampType

log = logging.getLogger("ba.standing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",   "true")
spark.conf.set("spark.sql.adaptive.enabled",                   "true")

# Constantes de tipo nulo — evita repetição
NUL_STR  = F.lit(None).cast(StringType())
NUL_D2   = F.lit(None).cast(DecimalType(18, 2))
NUL_D4   = F.lit(None).cast(DecimalType(8,  4))
NUL_DATE = F.lit(None).cast(DateType())
NUL_INT  = F.lit(None).cast(IntegerType())

# Helpers
def _s(c):  return F.trim(c.cast(StringType()))
def _up(c): return F.upper(F.trim(c.cast(StringType())))
def _d2(c): return c.cast(DecimalType(18, 2))
def _d4(c): return c.cast(DecimalType(8,  4))

def _ciclo_ts(c):       return F.date_format(F.to_timestamp(c), "yyyyMM")
def _ciclo_mesano(c):   return F.regexp_replace(F.trim(c.cast(StringType())), "-", "")
def _ciclo_aaaammdd(c): return F.substring(c.cast(StringType()), 1, 6)

def _tp_pessoa(c):
    u = F.upper(F.trim(c.cast(StringType())))
    return F.when(u.contains("FISICA"), F.lit("PF")).when(u.contains("JURIDICA"), F.lit("PJ")).otherwise(u)

log.info("Setup concluído | ciclo=%s | destino=%s", CICLO_REF, TABELA_DEST)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL — Tabela destino

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS accenture")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_DEST} (

    id_registro       STRING        COMMENT 'PK MD5',
    sistema_origem    STRING        COMMENT 'ADAPTER | NG | SIMETRA',
    ciclo_faturamento STRING        COMMENT 'AAAAMM',
    dt_carga          TIMESTAMP,
    data_emissao      DATE,

    -- Nota Fiscal
    nf_numero         STRING,   -- Adapter: NF_NUMERO        | Protheus: FT_NFISCAL
    fatura_numero     STRING,   -- Adapter: FATURA_NUMERO    | NG: FATURA_NUMERO
    id_item           STRING,   -- Adapter: ID_FATURAMENTO_MES | Protheus: FT_ITEM
    tipo_emissao      STRING,   -- Adapter: TipoEmissao
    tipo_modelo_nf    STRING,   -- Adapter: TipoModeloNotaFiscal | Protheus: C5_TIPOF
    status_contrato   STRING,   -- Adapter: CANCELADA        | NG: DescricaoStatusContrato

    -- Emitente / Empresa
    empresa_prestadora STRING,  -- Adapter: EMPRESA_PRESTADORA | NG: operacao

    -- Cliente / Contrato
    id_contrato       STRING,   -- Adapter: CONTA_NUMERO | NG: idcontrato | Protheus: C6_CONTRT
    id_cliente        STRING,   -- Adapter: COD_CLIENTE_SAP | NG: idcliente | Protheus: FT_CLIEFOR
    cpf_cnpj          STRING,   -- NG: CPF_CNPJ | Protheus: A1_CGC
    nome_cliente      STRING,   -- Adapter: NOME_ASSINANTE | Protheus: A1_NOME
    tp_pessoa         STRING,   -- NG: TipoPessoa → PF|PJ
    uf_dest           STRING,   -- Adapter: NF_UF | Protheus: FT_ESTADO
    cidade_dest       STRING,   -- Adapter: NF_CIDADE | NG: NomeCidade
    data_ativacao     DATE,     -- NG: DataAtivacao
    aging             INTEGER,  -- NG: Aging

    -- Produto / Serviço
    cod_produto       STRING,   -- Adapter: NF_ITEM_COD_SAP | Protheus: B1_COD
    descricao_item    STRING,   -- Adapter: NF_ITEM_DESCRICAO | NG: DescricaoPlano | Protheus: B1_DESC
    cclass            STRING,   -- NG: GrupoN2 | Protheus: B1_XCCLASS
    tipo_servico      STRING,   -- NG: servico | Protheus: TIPO_PROD
    tipo_receita      STRING,   -- NG: Tipo_Receita

    -- Financeiro
    valor_fatura      DECIMAL(18,2), -- Adapter: FATURA_VALOR_ORIGINAL
    valor_nf          DECIMAL(18,2), -- Adapter: NF_VALOR | Protheus: FT_VALCONT
    valor_item        DECIMAL(18,2), -- Adapter: NF_ITEM_VALOR | NG: ReceitaBruta | Protheus: FT_TOTAL

    -- CFOP
    cfop              STRING,   -- Adapter: CFOP | Protheus: FT_CFOP

    -- ICMS
    icms_base         DECIMAL(18,2), -- Adapter: ICMS_BASE_CALCULO | Protheus: FT_BASEICM
    icms_aliquota     DECIMAL(8,4),  -- Adapter: ICMS_ALIQUOTA     | Protheus: FT_ALIQICM
    icms_valor        DECIMAL(18,2), -- Adapter: ICMS_VALOR_IMPOSTO| Protheus: FT_VALICM

    -- ISS (somente Adapter)
    iss_base          DECIMAL(18,2), -- Adapter: ISS_BASE_CALCULO
    iss_aliquota      DECIMAL(8,4),  -- Adapter: ISS_ALIQUOTA
    iss_valor         DECIMAL(18,2), -- Adapter: ISS_VALOR_IMPOSTO

    -- PIS
    pis_base          DECIMAL(18,2), -- Adapter: PIS_BASE_CALCULO  | Protheus: FT_BASEPIS
    pis_aliquota      DECIMAL(8,4),  -- Adapter: PIS_ALIQUOTA      | Protheus: FT_ALIQPIS
    pis_valor         DECIMAL(18,2), -- Adapter: PIS_VALOR_IMPOSTO | Protheus: FT_VALPIS
    cst_pis           STRING,        -- Protheus: FT_CSTPIS

    -- COFINS
    cofins_base       DECIMAL(18,2), -- Adapter: COFINS_BASE_CALCULO | Protheus: FT_BASECOF
    cofins_aliquota   DECIMAL(8,4),  -- Adapter: COFINS_ALIQUOTA     | Protheus: FT_ALIQCOF
    cofins_valor      DECIMAL(18,2), -- Adapter: COFINS_VALOR_IMPOSTO| Protheus: FT_VALCOF
    cst_cofins        STRING,        -- Protheus: FT_CSTCOF

    -- FUST
    fust_base         DECIMAL(18,2), -- Adapter: FUST_BASE_CALCULO | Protheus: FT_BASIMP5
    fust_aliquota     DECIMAL(8,4),  -- Adapter: FUST_ALIQUOTA     | Protheus: FT_ALQIMP5
    fust_valor        DECIMAL(18,2), -- Adapter: FUST_VALOR_IMPOSTO| Protheus: FT_VALIMP5

    -- FUNTTEL
    funttel_base      DECIMAL(18,2), -- Adapter: FUNTTEL_BASE_CALCULO | Protheus: FT_BASIMP6
    funttel_aliquota  DECIMAL(8,4),  -- Adapter: FUNTTEL_ALIQUOTA     | Protheus: FT_ALQIMP6
    funttel_valor     DECIMAL(18,2), -- Adapter: FUNTTEL_VALOR_IMPOSTO| Protheus: FT_VALIMP6

    -- Reforma Tributária (somente Protheus)
    cbs_aliquota      DECIMAL(8,4),  -- Protheus: ALIQ_CBS
    cbs_valor         DECIMAL(18,2), -- Protheus: VALOR_CBS
    ibs_aliquota      DECIMAL(8,4),  -- Protheus: ALIQ_IBS
    ibs_valor         DECIMAL(18,2), -- Protheus: VALOR_IBS

    -- Derivados
    ind_sem_cst       BOOLEAN COMMENT 'iss_valor > 0 → TRUE',
    faturamento_zerado BOOLEAN COMMENT 'valor_item = 0 → TRUE (PB-000)',
    fatura_sem_numero  BOOLEAN COMMENT 'nf_numero nulo → TRUE',

    -- CDC
    hash_registro     STRING,
    dt_processamento  TIMESTAMP
)
USING DELTA
PARTITIONED BY (ciclo_faturamento, sistema_origem)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
""")

print(f"✅ {TABELA_DEST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — `bronze.adapter_aliquotas`
# MAGIC Filtro: `FATURA_DATA_EMISSAO` → ciclo_ref

# COMMAND ----------

df_adapter = (
    spark.table(TBL_ADAPTER)
    .filter(_ciclo_ts(F.col("FATURA_DATA_EMISSAO")) == CICLO_REF)
    .select(
        F.lit("ADAPTER").alias("sistema_origem"),
        _ciclo_ts(F.col("FATURA_DATA_EMISSAO")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.to_date(F.to_timestamp(F.col("NF_DATA_EMISSAO"))).alias("data_emissao"),
        _s(F.col("NF_NUMERO")).alias("nf_numero"),
        _s(F.col("FATURA_NUMERO")).alias("fatura_numero"),
        _s(F.col("ID_FATURAMENTO_MES")).alias("id_item"),
        _s(F.col("TipoEmissao")).alias("tipo_emissao"),
        _s(F.col("TipoModeloNotaFiscal")).alias("tipo_modelo_nf"),
        _s(F.col("CANCELADA")).alias("status_contrato"),
        _s(F.col("EMPRESA_PRESTADORA")).alias("empresa_prestadora"),
        _s(F.col("CONTA_NUMERO")).alias("id_contrato"),
        _s(F.col("COD_CLIENTE_SAP")).alias("id_cliente"),
        NUL_STR.alias("cpf_cnpj"),
        _s(F.col("NOME_ASSINANTE")).alias("nome_cliente"),
        NUL_STR.alias("tp_pessoa"),
        _up(F.col("NF_UF")).alias("uf_dest"),
        _s(F.col("NF_CIDADE")).alias("cidade_dest"),
        NUL_DATE.alias("data_ativacao"),
        NUL_INT.alias("aging"),
        _s(F.col("NF_ITEM_COD_SAP")).alias("cod_produto"),
        _s(F.col("NF_ITEM_DESCRICAO")).alias("descricao_item"),
        NUL_STR.alias("cclass"),
        NUL_STR.alias("tipo_servico"),
        NUL_STR.alias("tipo_receita"),
        _d2(F.col("FATURA_VALOR_ORIGINAL")).alias("valor_fatura"),
        _d2(F.col("NF_VALOR")).alias("valor_nf"),
        _d2(F.col("NF_ITEM_VALOR")).alias("valor_item"),
        _s(F.col("CFOP")).alias("cfop"),
        _d2(F.col("ICMS_BASE_CALCULO")).alias("icms_base"),
        _d4(F.col("ICMS_ALIQUOTA")).alias("icms_aliquota"),
        _d2(F.col("ICMS_VALOR_IMPOSTO")).alias("icms_valor"),
        _d2(F.col("ISS_BASE_CALCULO")).alias("iss_base"),
        _d4(F.col("ISS_ALIQUOTA")).alias("iss_aliquota"),
        _d2(F.col("ISS_VALOR_IMPOSTO")).alias("iss_valor"),
        _d2(F.col("PIS_BASE_CALCULO")).alias("pis_base"),
        _d4(F.col("PIS_ALIQUOTA")).alias("pis_aliquota"),
        _d2(F.col("PIS_VALOR_IMPOSTO")).alias("pis_valor"),
        NUL_STR.alias("cst_pis"),
        _d2(F.col("COFINS_BASE_CALCULO")).alias("cofins_base"),
        _d4(F.col("COFINS_ALIQUOTA")).alias("cofins_aliquota"),
        _d2(F.col("COFINS_VALOR_IMPOSTO")).alias("cofins_valor"),
        NUL_STR.alias("cst_cofins"),
        _d2(F.col("FUST_BASE_CALCULO")).alias("fust_base"),
        _d4(F.col("FUST_ALIQUOTA")).alias("fust_aliquota"),
        _d2(F.col("FUST_VALOR_IMPOSTO")).alias("fust_valor"),
        _d2(F.col("FUNTTEL_BASE_CALCULO")).alias("funttel_base"),
        _d4(F.col("FUNTTEL_ALIQUOTA")).alias("funttel_aliquota"),
        _d2(F.col("FUNTTEL_VALOR_IMPOSTO")).alias("funttel_valor"),
        NUL_D4.alias("cbs_aliquota"), NUL_D2.alias("cbs_valor"),
        NUL_D4.alias("ibs_aliquota"), NUL_D2.alias("ibs_valor"),
        F.current_timestamp().alias("dt_processamento"),
    )
)
print(f"✅ ADAPTER | {df_adapter.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — `hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS`
# MAGIC Filtro: `MESANO` → ciclo_ref
# MAGIC > Dados fiscais (ICMS, PIS, CFOP etc.) **não existem** nesta tabela → null.

# COMMAND ----------

df_ng = (
    spark.table(TBL_NG)
    .filter(_ciclo_mesano(F.col("MESANO")) == CICLO_REF)
    .select(
        F.lit("NG").alias("sistema_origem"),
        _ciclo_mesano(F.col("MESANO")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.to_date(F.col("DataEmissaoNF")).alias("data_emissao"),
        NUL_STR.alias("nf_numero"),
        _s(F.col("FATURA_NUMERO")).alias("fatura_numero"),
        NUL_STR.alias("id_item"),
        NUL_STR.alias("tipo_emissao"),
        NUL_STR.alias("tipo_modelo_nf"),
        _s(F.col("DescricaoStatusContrato")).alias("status_contrato"),
        _s(F.col("operacao")).alias("empresa_prestadora"),
        _s(F.col("idcontrato")).alias("id_contrato"),
        _s(F.col("idcliente")).alias("id_cliente"),
        _s(F.col("CPF_CNPJ")).alias("cpf_cnpj"),
        NUL_STR.alias("nome_cliente"),
        _tp_pessoa(F.col("TipoPessoa")).alias("tp_pessoa"),
        NUL_STR.alias("uf_dest"),
        _s(F.col("NomeCidade")).alias("cidade_dest"),
        F.to_date(F.col("DataAtivacao")).alias("data_ativacao"),
        F.col("Aging").cast(IntegerType()).alias("aging"),
        NUL_STR.alias("cod_produto"),
        _s(F.col("DescricaoPlano")).alias("descricao_item"),
        _s(F.col("GrupoN2")).alias("cclass"),
        _s(F.col("servico")).alias("tipo_servico"),
        _s(F.col("Tipo_Receita")).alias("tipo_receita"),
        NUL_D2.alias("valor_fatura"),
        NUL_D2.alias("valor_nf"),
        _d2(F.col("ReceitaBruta")).alias("valor_item"),
        NUL_STR.alias("cfop"),
        NUL_D2.alias("icms_base"),   NUL_D4.alias("icms_aliquota"),   NUL_D2.alias("icms_valor"),
        NUL_D2.alias("iss_base"),    NUL_D4.alias("iss_aliquota"),    NUL_D2.alias("iss_valor"),
        NUL_D2.alias("pis_base"),    NUL_D4.alias("pis_aliquota"),    NUL_D2.alias("pis_valor"),
        NUL_STR.alias("cst_pis"),
        NUL_D2.alias("cofins_base"), NUL_D4.alias("cofins_aliquota"), NUL_D2.alias("cofins_valor"),
        NUL_STR.alias("cst_cofins"),
        NUL_D2.alias("fust_base"),   NUL_D4.alias("fust_aliquota"),   NUL_D2.alias("fust_valor"),
        NUL_D2.alias("funttel_base"),NUL_D4.alias("funttel_aliquota"),NUL_D2.alias("funttel_valor"),
        NUL_D4.alias("cbs_aliquota"), NUL_D2.alias("cbs_valor"),
        NUL_D4.alias("ibs_aliquota"), NUL_D2.alias("ibs_valor"),
        F.current_timestamp().alias("dt_processamento"),
    )
)
print(f"✅ NG | {df_ng.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — `NEGOCIO.TB_FATURAMENTO_PROTHEUS_FILTRADA`
# MAGIC Filtro: `FT_EMISSAO` (AAAAMMDD) → ciclo_ref
# MAGIC > FUST = `FT_ALQIMP5 / FT_VALIMP5` | FUNTTEL = `FT_ALQIMP6 / FT_VALIMP6`
# MAGIC > CBS/IBS exclusivos desta tabela.

# COMMAND ----------

df_protheus = (
    spark.table(TBL_PROTHEUS)
    .filter(_ciclo_aaaammdd(F.col("FT_EMISSAO")) == CICLO_REF)
    .select(
        F.lit("SIMETRA").alias("sistema_origem"),
        _ciclo_aaaammdd(F.col("FT_EMISSAO")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.to_date(F.col("FT_EMISSAO").cast(StringType()), "yyyyMMdd").alias("data_emissao"),
        _s(F.col("FT_NFISCAL")).alias("nf_numero"),
        NUL_STR.alias("fatura_numero"),
        _s(F.col("FT_ITEM")).alias("id_item"),
        NUL_STR.alias("tipo_emissao"),
        _s(F.col("C5_TIPOF")).alias("tipo_modelo_nf"),
        NUL_STR.alias("status_contrato"),
        NUL_STR.alias("empresa_prestadora"),
        _s(F.col("C6_CONTRT")).alias("id_contrato"),
        _s(F.col("FT_CLIEFOR")).alias("id_cliente"),
        _s(F.col("A1_CGC")).alias("cpf_cnpj"),
        _s(F.col("A1_NOME")).alias("nome_cliente"),
        NUL_STR.alias("tp_pessoa"),
        _up(F.col("FT_ESTADO")).alias("uf_dest"),
        NUL_STR.alias("cidade_dest"),
        NUL_DATE.alias("data_ativacao"),
        NUL_INT.alias("aging"),
        _s(F.col("B1_COD")).alias("cod_produto"),
        _s(F.col("B1_DESC")).alias("descricao_item"),
        _s(F.col("B1_XCCLASS")).alias("cclass"),
        _s(F.col("TIPO_PROD")).alias("tipo_servico"),
        NUL_STR.alias("tipo_receita"),
        NUL_D2.alias("valor_fatura"),
        _d2(F.col("FT_VALCONT")).alias("valor_nf"),
        _d2(F.col("FT_TOTAL")).alias("valor_item"),
        _s(F.col("FT_CFOP")).alias("cfop"),
        _d2(F.col("FT_BASEICM")).alias("icms_base"),
        _d4(F.col("FT_ALIQICM")).alias("icms_aliquota"),
        _d2(F.col("FT_VALICM")).alias("icms_valor"),
        NUL_D2.alias("iss_base"), NUL_D4.alias("iss_aliquota"), NUL_D2.alias("iss_valor"),
        _d2(F.col("FT_BASEPIS")).alias("pis_base"),
        _d4(F.col("FT_ALIQPIS")).alias("pis_aliquota"),
        _d2(F.col("FT_VALPIS")).alias("pis_valor"),
        _s(F.col("FT_CSTPIS")).alias("cst_pis"),
        _d2(F.col("FT_BASECOF")).alias("cofins_base"),
        _d4(F.col("FT_ALIQCOF")).alias("cofins_aliquota"),
        _d2(F.col("FT_VALCOF")).alias("cofins_valor"),
        _s(F.col("FT_CSTCOF")).alias("cst_cofins"),
        _d2(F.col("FT_BASIMP5")).alias("fust_base"),
        _d4(F.col("FT_ALQIMP5")).alias("fust_aliquota"),
        _d2(F.col("FT_VALIMP5")).alias("fust_valor"),
        _d2(F.col("FT_BASIMP6")).alias("funttel_base"),
        _d4(F.col("FT_ALQIMP6")).alias("funttel_aliquota"),
        _d2(F.col("FT_VALIMP6")).alias("funttel_valor"),
        _d4(F.col("ALIQ_CBS")).alias("cbs_aliquota"),
        _d2(F.col("VALOR_CBS")).alias("cbs_valor"),
        _d4(F.col("ALIQ_IBS")).alias("ibs_aliquota"),
        _d2(F.col("VALOR_IBS")).alias("ibs_valor"),
        F.current_timestamp().alias("dt_processamento"),
    )
)
print(f"✅ SIMETRA | {df_protheus.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION ALL + Campos Derivados + Hash CDC

# COMMAND ----------

zero2 = F.lit(0).cast(DecimalType(18, 2))

df_final = (
    df_adapter
    .unionByName(df_ng,       allowMissingColumns=False)
    .unionByName(df_protheus, allowMissingColumns=False)
    # ind_sem_cst: ISS > 0 → item sem ICMS (somente Adapter tem ISS)
    .withColumn("ind_sem_cst",
        F.coalesce((F.col("iss_valor") > zero2).cast(BooleanType()), F.lit(False))
    )
    # faturamento_zerado: PB-000
    .withColumn("faturamento_zerado",
        F.coalesce((F.col("valor_item") == zero2).cast(BooleanType()), F.lit(True))
    )
    # fatura_sem_numero
    .withColumn("fatura_sem_numero",
        (F.col("nf_numero").isNull() | (F.trim(F.col("nf_numero").cast(StringType())) == ""))
        .cast(BooleanType())
    )
    # PK
    .withColumn("id_registro",
        F.md5(F.concat_ws("|",
            F.col("sistema_origem"),
            F.col("ciclo_faturamento"),
            F.coalesce(_s(F.col("nf_numero")),     F.lit("")),
            F.coalesce(_s(F.col("fatura_numero")), F.lit("")),
            F.coalesce(_s(F.col("id_item")),       F.lit("")),
            F.coalesce(_s(F.col("id_contrato")),   F.lit("")),
        ))
    )
    # Hash CDC
    .withColumn("hash_registro",
        F.sha2(F.concat_ws("|",
            F.coalesce(_s(F.col("cfop")),                        F.lit("")),
            F.coalesce(F.col("icms_aliquota").cast(StringType()), F.lit("")),
            F.coalesce(F.col("icms_valor").cast(StringType()),    F.lit("")),
            F.coalesce(F.col("iss_valor").cast(StringType()),     F.lit("")),
            F.coalesce(F.col("pis_aliquota").cast(StringType()),  F.lit("")),
            F.coalesce(F.col("pis_valor").cast(StringType()),     F.lit("")),
            F.coalesce(F.col("cofins_aliquota").cast(StringType()),F.lit("")),
            F.coalesce(F.col("cofins_valor").cast(StringType()),  F.lit("")),
            F.coalesce(F.col("fust_valor").cast(StringType()),    F.lit("")),
            F.coalesce(F.col("funttel_valor").cast(StringType()), F.lit("")),
            F.coalesce(F.col("valor_item").cast(StringType()),    F.lit("")),
            F.coalesce(_s(F.col("uf_dest")),                      F.lit("")),
        ), 256)
    )
)

total = df_final.count()
print(f"✅ UNION ALL | {total:,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO `dados_nfcom_cliente`

# COMMAND ----------

campos_update = {c: f"src.{c}" for c in df_final.columns if c != "id_registro"}

(
    DeltaTable.forName(spark, TABELA_DEST).alias("tgt")
    .merge(
        df_final.alias("src"),
        "tgt.id_registro       = src.id_registro AND "
        "tgt.ciclo_faturamento = src.ciclo_faturamento AND "
        "tgt.sistema_origem    = src.sistema_origem",
    )
    .whenMatchedUpdate(condition="tgt.hash_registro <> src.hash_registro", set=campos_update)
    .whenNotMatchedInsertAll()
    .execute()
)

h = DeltaTable.forName(spark, TABELA_DEST).history(1).select("operationMetrics").collect()
m = h[0]["operationMetrics"] if h else {}
inserted = int(m.get("numTargetRowsInserted", 0))
updated  = int(m.get("numTargetRowsUpdated",  0))
print(f"✅ MERGE | inseridos={inserted:,} | atualizados={updated:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Resumo por sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     COUNT(*)                                                          AS itens,
# MAGIC     COUNT(DISTINCT id_contrato)                                      AS contratos,
# MAGIC     SUM(CASE WHEN faturamento_zerado  THEN 1 ELSE 0 END)             AS zerados,
# MAGIC     SUM(CASE WHEN fatura_sem_numero   THEN 1 ELSE 0 END)             AS sem_nf,
# MAGIC     SUM(CASE WHEN ind_sem_cst         THEN 1 ELSE 0 END)             AS itens_iss,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst     THEN 1 ELSE 0 END)             AS itens_icms,
# MAGIC     ROUND(SUM(COALESCE(valor_item,    0)), 2)                        AS valor_itens_r,
# MAGIC     ROUND(SUM(COALESCE(icms_valor,    0)), 2)                        AS icms_r,
# MAGIC     ROUND(SUM(COALESCE(iss_valor,     0)), 2)                        AS iss_r,
# MAGIC     ROUND(SUM(COALESCE(pis_valor,     0)), 2)                        AS pis_r,
# MAGIC     ROUND(SUM(COALESCE(cofins_valor,  0)), 2)                        AS cofins_r,
# MAGIC     ROUND(SUM(COALESCE(fust_valor,    0)), 2)                        AS fust_r,
# MAGIC     ROUND(SUM(COALESCE(funttel_valor, 0)), 2)                        AS funttel_r,
# MAGIC     ROUND(SUM(COALESCE(cbs_valor,     0)), 2)                        AS cbs_r,
# MAGIC     ROUND(SUM(COALESCE(ibs_valor,     0)), 2)                        AS ibs_r
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem ORDER BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — CFOP por UF

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH validos AS (
# MAGIC     SELECT explode(array(
# MAGIC         '5301','5302','5303','5304','5305','5306','5307','5933',
# MAGIC         '6301','6302','6303','6304','6305','6306','6307','6933',
# MAGIC         '1205','7301'
# MAGIC     )) AS cfop_valido
# MAGIC )
# MAGIC SELECT
# MAGIC     s.sistema_origem, s.uf_dest, s.cfop, s.ind_sem_cst,
# MAGIC     COUNT(*) AS qtd, ROUND(SUM(s.valor_item), 2) AS valor_r,
# MAGIC     CASE
# MAGIC         WHEN s.cfop IS NULL                                      THEN '⚪ sem CFOP nesta fonte'
# MAGIC         WHEN s.ind_sem_cst                                       THEN '🟡 ISS — indSemCST'
# MAGIC         WHEN v.cfop_valido IS NULL                               THEN '🔴 CFOP_INVALIDO'
# MAGIC         WHEN s.uf_dest = 'SP' AND LEFT(s.cfop,1) = '6'         THEN '🔴 deveria ser 5xxx'
# MAGIC         WHEN s.uf_dest <> 'SP' AND LEFT(s.cfop,1) = '5'        THEN '🔴 deveria ser 6xxx'
# MAGIC         ELSE '✅ OK'
# MAGIC     END AS status
# MAGIC FROM accenture.dados_nfcom_cliente s
# MAGIC LEFT JOIN validos v ON v.cfop_valido = s.cfop
# MAGIC WHERE s.ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY s.sistema_origem, s.uf_dest, s.cfop, s.ind_sem_cst, v.cfop_valido
# MAGIC ORDER BY qtd DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Tributação (ICMS / ISS / PIS / COFINS)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     SUM(CASE WHEN icms_valor > 0 AND iss_valor > 0                   THEN 1 ELSE 0 END) AS icms_e_iss_simultaneos,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND icms_aliquota = 0
# MAGIC                   AND icms_aliquota IS NOT NULL                       THEN 1 ELSE 0 END) AS icms_aliq_zero,
# MAGIC     SUM(CASE WHEN ind_sem_cst AND (pis_valor > 0 OR cofins_valor > 0) THEN 1 ELSE 0 END) AS pis_cofins_indevido_iss,
# MAGIC     SUM(CASE WHEN icms_aliquota IS NULL                               THEN 1 ELSE 0 END) AS sem_icms_na_fonte,
# MAGIC     COUNT(*) AS total
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — FUST / FUNTTEL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     CASE WHEN ind_sem_cst THEN 'ISS/SVA' ELSE 'ICMS' END AS tipo,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND fust_valor IS NOT NULL AND fust_valor = 0    THEN 1 ELSE 0 END) AS fust_zero_em_icms,
# MAGIC     SUM(CASE WHEN ind_sem_cst     AND fust_valor IS NOT NULL AND fust_valor > 0    THEN 1 ELSE 0 END) AS fust_indevido_iss,
# MAGIC     SUM(CASE WHEN fust_valor IS NULL                                               THEN 1 ELSE 0 END) AS sem_fust_na_fonte,
# MAGIC     ROUND(SUM(COALESCE(fust_valor,    0)), 2) AS total_fust_r,
# MAGIC     ROUND(SUM(COALESCE(funttel_valor, 0)), 2) AS total_funttel_r,
# MAGIC     COUNT(*) AS total
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem, ind_sem_cst ORDER BY sistema_origem, ind_sem_cst

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — PB-000 Faturamento Zerado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     COUNT(DISTINCT id_contrato)                              AS contratos,
# MAGIC     SUM(CASE WHEN faturamento_zerado THEN 1 ELSE 0 END)     AS itens_zerados,
# MAGIC     SUM(CASE WHEN fatura_sem_numero  THEN 1 ELSE 0 END)     AS sem_nf_numero
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC   AND (faturamento_zerado OR fatura_sem_numero)
# MAGIC GROUP BY sistema_origem

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem, id_contrato, id_cliente, cpf_cnpj, uf_dest,
# MAGIC     nf_numero, fatura_numero, valor_item, descricao_item, cfop,
# MAGIC     CASE
# MAGIC         WHEN faturamento_zerado AND fatura_sem_numero THEN '🔴 ZERADO + SEM NF'
# MAGIC         WHEN fatura_sem_numero                        THEN '🔴 SEM NF_NUMERO'
# MAGIC         ELSE                                               '🔴 ITEM ZERADO'
# MAGIC     END AS tag
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC   AND (faturamento_zerado OR fatura_sem_numero)
# MAGIC ORDER BY sistema_origem LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Cobertura de campos por sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     COUNT(*) AS total,
# MAGIC     ROUND(SUM(CASE WHEN cfop       IS NOT NULL AND cfop<>''    THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cfop,
# MAGIC     ROUND(SUM(CASE WHEN cclass     IS NOT NULL AND cclass<>''  THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cclass,
# MAGIC     ROUND(SUM(CASE WHEN uf_dest    IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_uf,
# MAGIC     ROUND(SUM(CASE WHEN cpf_cnpj   IS NOT NULL AND cpf_cnpj<>''THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cpf_cnpj,
# MAGIC     ROUND(SUM(CASE WHEN icms_aliquota IS NOT NULL              THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_icms,
# MAGIC     ROUND(SUM(CASE WHEN pis_aliquota  IS NOT NULL              THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_pis,
# MAGIC     ROUND(SUM(CASE WHEN fust_valor    IS NOT NULL              THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_fust,
# MAGIC     ROUND(SUM(CASE WHEN cbs_aliquota  IS NOT NULL              THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cbs
# MAGIC FROM accenture.dados_nfcom_cliente
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem ORDER BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE

# COMMAND ----------

if EXECUTAR_OPTIMIZE:
    spark.sql(f"""
        OPTIMIZE {TABELA_DEST}
        WHERE ciclo_faturamento = '{CICLO_REF}'
        ZORDER BY (sistema_origem, uf_dest, cfop)
    """)
    print(f"✅ OPTIMIZE | ciclo={CICLO_REF}")