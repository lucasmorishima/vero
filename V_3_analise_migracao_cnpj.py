# Databricks notebook source
# Foco: CNPJs que indicam migração (receita transitou de SIMETRA para NG)
# Análise da variação de receita sistema a sistema ao longo do tempo

from pyspark.sql import functions as F
from pyspark.sql import Window

spark = globals().get("spark")

MES_INICIO_NG = "2026-03"
MESES = "('2025-12','2026-01','2026-02','2026-03','2026-04','2026-05','2026-06','2026-07')"

# --------------------------------------------------------------------------
# Carga
# --------------------------------------------------------------------------
sql_query = f"""
SELECT
    'SIMETRA'                                                AS SISTEMA,
    np.A1_CGC                                               AS CPF_CNPJ,
    DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM') AS ANO_MES,
    REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')          AS FATURA,
    COUNT(DISTINCT np.C6_CONTRT)                             AS QTD_CONTRATO,
    SUM(FT_VALCONT)                                          AS VALOR_FATURADO
FROM NEGOCIO.TB_FATURAMENTO_PROTHEUS_COMPLETA np
WHERE DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM') IN {MESES}
GROUP BY np.A1_CGC,
         DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM'),
         REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')

UNION ALL

SELECT
    'NG'                                                      AS SISTEMA,
    CPF_CNPJ,
    try_cast(MESANO AS STRING)                                AS ANO_MES,
    FATURA_NUMERO                                             AS FATURA,
    COUNT(idcontrato)                                         AS QTD_CONTRATO,
    SUM(try_cast(NULLIF(trim(ReceitaBruta), '') AS DOUBLE))   AS VALOR_FATURADO
FROM hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS
WHERE try_cast(MESANO AS STRING) IN {MESES}
  AND crm = 'NG'
GROUP BY CPF_CNPJ,
         try_cast(MESANO AS STRING),
         FATURA_NUMERO
"""

sdf = spark.sql(sql_query).filter(F.col("VALOR_FATURADO") > 0)
sdf.createOrReplaceTempView("vw_fat")
sdf = spark.table("vw_fat")
print(f"Registros carregados: {sdf.count():,}")

# COMMAND ----------

# --------------------------------------------------------------------------
# Identificação dos CNPJs com indicação de migração
# Critério: aparece em SIMETRA em algum mês E em NG em algum mês
# --------------------------------------------------------------------------
cnpjs_simetra = sdf.filter(F.col("SISTEMA") == "SIMETRA").select("CPF_CNPJ").distinct()
cnpjs_ng      = sdf.filter(F.col("SISTEMA") == "NG").select("CPF_CNPJ").distinct()

cnpjs_migrados = cnpjs_simetra.join(cnpjs_ng, on="CPF_CNPJ", how="inner")
qtd_migrados = cnpjs_migrados.count()
print(f"CNPJs com indicação de migração (presentes nos dois sistemas): {qtd_migrados:,}")

# Stats por sistema para esses CNPJs
simetra_stats = (
    sdf
    .filter(F.col("SISTEMA") == "SIMETRA")
    .join(cnpjs_migrados, on="CPF_CNPJ", how="inner")
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_SIMETRA"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_SIMETRA"),
        F.min("ANO_MES").alias("PRIMEIRO_MES_SIMETRA"),
        F.max("ANO_MES").alias("ULTIMO_MES_SIMETRA"),
        F.countDistinct("ANO_MES").alias("MESES_SIMETRA"),
    )
    .withColumn("MEDIA_SIMETRA",
        F.when(F.col("MESES_SIMETRA") > 0, F.col("TOTAL_SIMETRA") / F.col("MESES_SIMETRA"))
    )
)

ng_stats = (
    sdf
    .filter(F.col("SISTEMA") == "NG")
    .join(cnpjs_migrados, on="CPF_CNPJ", how="inner")
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_NG"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_NG"),
        F.min("ANO_MES").alias("PRIMEIRO_MES_NG"),
        F.max("ANO_MES").alias("ULTIMO_MES_NG"),
        F.countDistinct("ANO_MES").alias("MESES_NG"),
    )
    .withColumn("MEDIA_NG",
        F.when(F.col("MESES_NG") > 0, F.col("TOTAL_NG") / F.col("MESES_NG"))
    )
)

migrados = (
    simetra_stats.join(ng_stats, on="CPF_CNPJ", how="inner")
    .withColumn("VAR_MEDIA_ABS", F.col("MEDIA_NG") - F.col("MEDIA_SIMETRA"))
    .withColumn("VAR_MEDIA_PCT",
        F.when(F.col("MEDIA_SIMETRA") > 0,
            F.col("VAR_MEDIA_ABS") / F.col("MEDIA_SIMETRA") * 100)
    )
    .withColumn("DUPLICIDADE",
        # ainda faturando nos dois sistemas simultaneamente após a virada
        F.col("ULTIMO_MES_SIMETRA") >= MES_INICIO_NG
    )
    .withColumn("STATUS",
        F.when(F.col("DUPLICIDADE"),                   "Duplicidade ativa")
         .when(F.col("VAR_MEDIA_PCT") >  10,           "Cresceu na migração")
         .when(F.col("VAR_MEDIA_PCT") < -10,           "Perdeu valor")
         .otherwise("Manteve valor")
    )
    .orderBy(F.desc("TOTAL_SIMETRA"))
)

migrados.createOrReplaceTempView("vw_migrados")
migrados = spark.table("vw_migrados")

# COMMAND ----------

# --------------------------------------------------------------------------
# Resumo geral dos migrados
# --------------------------------------------------------------------------
print("=" * 60)
print("RESUMO — CNPJs COM INDICAÇÃO DE MIGRAÇÃO")
print("=" * 60)

display(
    migrados
    .groupBy("STATUS")
    .agg(
        F.count("CPF_CNPJ").alias("QTD_CNPJ"),
        F.sum("TOTAL_SIMETRA").alias("TOTAL_SIMETRA"),
        F.sum("TOTAL_NG").alias("TOTAL_NG"),
        F.avg("VAR_MEDIA_PCT").alias("VAR_MEDIA_PCT_MEDIA"),
    )
    .orderBy(F.desc("QTD_CNPJ"))
)

# COMMAND ----------

# --------------------------------------------------------------------------
# Visão individual: para cada CNPJ migrado, receita mês a mês por sistema
# SIMETRA e NG lado a lado + total combinado + variação M/M do total
# --------------------------------------------------------------------------
w_cnpj = Window.partitionBy("CPF_CNPJ").orderBy("ANO_MES")

pivot_mensal = (
    sdf
    .join(cnpjs_migrados, on="CPF_CNPJ", how="inner")
    .groupBy("CPF_CNPJ", "ANO_MES")
    .pivot("SISTEMA", ["SIMETRA", "NG"])
    .agg(F.sum("VALOR_FATURADO"))
    .withColumnRenamed("SIMETRA", "RECEITA_SIMETRA")
    .withColumnRenamed("NG",      "RECEITA_NG")
    .withColumn("RECEITA_SIMETRA", F.coalesce("RECEITA_SIMETRA", F.lit(0.0)))
    .withColumn("RECEITA_NG",      F.coalesce("RECEITA_NG",      F.lit(0.0)))
    .withColumn("TOTAL_MES", F.col("RECEITA_SIMETRA") + F.col("RECEITA_NG"))
    .withColumn("TOTAL_MES_ANTERIOR", F.lag("TOTAL_MES").over(w_cnpj))
    .withColumn("VAR_MES_ABS", F.col("TOTAL_MES") - F.col("TOTAL_MES_ANTERIOR"))
    .withColumn("VAR_MES_PCT",
        F.when(F.col("TOTAL_MES_ANTERIOR") > 0,
            F.col("VAR_MES_ABS") / F.col("TOTAL_MES_ANTERIOR") * 100)
    )
    .orderBy("CPF_CNPJ", "ANO_MES")
)

print("=" * 60)
print("RECEITA MÊS A MÊS POR SISTEMA (SIMETRA × NG) — CNPJs MIGRADOS")
print("=" * 60)
display(pivot_mensal)

# COMMAND ----------

# --------------------------------------------------------------------------
# Foco: CNPJs que perderam valor na migração
# Ordenado pela maior perda de receita média mensal
# --------------------------------------------------------------------------
perderam = (
    migrados
    .filter(F.col("STATUS") == "Perdeu valor")
    .select(
        "CPF_CNPJ",
        "PRIMEIRO_MES_SIMETRA", "ULTIMO_MES_SIMETRA", "MESES_SIMETRA",
        "TOTAL_SIMETRA", "MEDIA_SIMETRA", "CONTRATOS_SIMETRA",
        "PRIMEIRO_MES_NG", "ULTIMO_MES_NG", "MESES_NG",
        "TOTAL_NG", "MEDIA_NG", "CONTRATOS_NG",
        "VAR_MEDIA_ABS", "VAR_MEDIA_PCT",
    )
    .orderBy("VAR_MEDIA_ABS")
)

print("=" * 60)
print("CNPJs QUE PERDERAM VALOR NA MIGRAÇÃO (ordenado pela maior perda)")
print("=" * 60)
print(f"Total: {perderam.count():,} CNPJs")
display(perderam)

# COMMAND ----------

# --------------------------------------------------------------------------
# Foco: CNPJs com duplicidade ativa (faturando nos dois sistemas pós-Mar/26)
# Risco de cobrança dupla
# --------------------------------------------------------------------------
duplicados = (
    migrados
    .filter(F.col("DUPLICIDADE"))
    .select(
        "CPF_CNPJ",
        "ULTIMO_MES_SIMETRA", "MESES_SIMETRA", "TOTAL_SIMETRA", "MEDIA_SIMETRA",
        "ULTIMO_MES_NG",      "MESES_NG",      "TOTAL_NG",      "MEDIA_NG",
        "VAR_MEDIA_ABS", "VAR_MEDIA_PCT",
    )
    .orderBy(F.desc("TOTAL_SIMETRA"))
)

print("=" * 60)
print("CNPJs COM DUPLICIDADE ATIVA (ambos os sistemas após Mar/26)")
print("=" * 60)
print(f"Total: {duplicados.count():,} CNPJs")
display(duplicados)

# COMMAND ----------

# --------------------------------------------------------------------------
# Mês da transição: quando cada CNPJ parou no SIMETRA e quando entrou no NG
# --------------------------------------------------------------------------
transicao = (
    migrados
    .filter(~F.col("DUPLICIDADE"))
    .withColumn("GAP_MESES",
        # diferença em meses entre fim do SIMETRA e início do NG
        F.months_between(
            F.to_date(F.col("PRIMEIRO_MES_NG"),    "yyyy-MM"),
            F.to_date(F.col("ULTIMO_MES_SIMETRA"), "yyyy-MM"),
        ).cast("int")
    )
    .withColumn("TIPO_TRANSICAO",
        F.when(F.col("GAP_MESES") == 1,  "Transição direta (1 mês)")
         .when(F.col("GAP_MESES") <= 0,  "Sobreposição (simultâneo)")
         .when(F.col("GAP_MESES") <= 3,  "Gap curto (2-3 meses)")
         .otherwise("Gap longo (4+ meses)")
    )
    .select(
        "CPF_CNPJ", "STATUS",
        "ULTIMO_MES_SIMETRA", "PRIMEIRO_MES_NG",
        "GAP_MESES", "TIPO_TRANSICAO",
        "MEDIA_SIMETRA", "MEDIA_NG", "VAR_MEDIA_ABS", "VAR_MEDIA_PCT",
    )
    .orderBy("ULTIMO_MES_SIMETRA", "CPF_CNPJ")
)

print("=" * 60)
print("ANÁLISE DO MOMENTO DA TRANSIÇÃO ENTRE SISTEMAS")
print("=" * 60)

display(
    transicao
    .groupBy("TIPO_TRANSICAO", "STATUS")
    .agg(
        F.count("CPF_CNPJ").alias("QTD"),
        F.avg("VAR_MEDIA_PCT").alias("VAR_MEDIA_PCT_MEDIA"),
    )
    .orderBy("TIPO_TRANSICAO", "STATUS")
)

print()
print("Detalhe por CNPJ:")
display(transicao)
