# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import Window

MES_CORTE_SIMETRA = "2026-02"
MES_INICIO_NG     = "2026-03"
MES_PAROU         = "2026-04"
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

sdf = spark.sql(sql_query)
sdf.createOrReplaceTempView("vw_fat")
sdf = spark.table("vw_fat")
print(f"Registros carregados: {sdf.count():,}")

# COMMAND ----------

# --------------------------------------------------------------------------
# 1. Faturamento mensal por sistema
# --------------------------------------------------------------------------
print("=" * 60)
print("FATURAMENTO MENSAL POR SISTEMA")
print("=" * 60)

fat_mensal = (
    sdf
    .filter(F.col("VALOR_FATURADO") > 0)
    .groupBy("ANO_MES", "SISTEMA")
    .agg(
        F.sum("VALOR_FATURADO").alias("FATURAMENTO"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS"),
        F.countDistinct("FATURA").alias("FATURAS"),
        F.countDistinct("CPF_CNPJ").alias("CNPJS_ATIVOS"),
    )
    .withColumn("TICKET_MEDIO",
        F.when(F.col("CONTRATOS") > 0, F.col("FATURAMENTO") / F.col("CONTRATOS"))
    )
    .orderBy("ANO_MES", "SISTEMA")
)

display(fat_mensal)

# COMMAND ----------

# --------------------------------------------------------------------------
# 2. Tendência por CNPJ
# --------------------------------------------------------------------------
cnpj_mes = (
    sdf
    .filter(F.col("VALOR_FATURADO").isNotNull() & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ", "ANO_MES")
    .agg(
        F.sum("VALOR_FATURADO").alias("VALOR"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS"),
    )
)

w_asc  = Window.partitionBy("CPF_CNPJ").orderBy("ANO_MES")
w_desc = Window.partitionBy("CPF_CNPJ").orderBy(F.desc("ANO_MES"))

cnpj_ranked = (
    cnpj_mes
    .withColumn("rn_asc",  F.row_number().over(w_asc))
    .withColumn("rn_desc", F.row_number().over(w_desc))
)

first_vals = (
    cnpj_ranked.filter(F.col("rn_asc") == 1)
    .select("CPF_CNPJ",
            F.col("ANO_MES").alias("PRIMEIRO_MES"),
            F.col("VALOR").alias("PRIMEIRO_VALOR"))
)
last_vals = (
    cnpj_ranked.filter(F.col("rn_desc") == 1)
    .select("CPF_CNPJ",
            F.col("ANO_MES").alias("ULTIMO_MES"),
            F.col("VALOR").alias("ULTIMO_VALOR"))
)

cnpj_stats = (
    cnpj_mes
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR").alias("TOTAL_FATURADO"),
        F.sum("CONTRATOS").alias("TOTAL_CONTRATOS"),
        F.countDistinct("ANO_MES").alias("MESES_ATIVOS"),
        F.avg("VALOR").alias("MEDIA_MENSAL"),
    )
    .join(first_vals, on="CPF_CNPJ", how="left")
    .join(last_vals,  on="CPF_CNPJ", how="left")
    .withColumn("TENDENCIA",
        F.when(F.col("MESES_ATIVOS") == 1, "DADOS INSUFICIENTES")
         .when(F.col("ULTIMO_MES") <= MES_PAROU, "PAROU")
         .when(F.col("ULTIMO_VALOR") > F.col("PRIMEIRO_VALOR") * 1.10, "CRESCIMENTO")
         .when(F.col("ULTIMO_VALOR") < F.col("PRIMEIRO_VALOR") * 0.90, "QUEDA")
         .otherwise("ESTAVEL")
    )
    .withColumn("VAR_PCT",
        F.when(F.col("PRIMEIRO_VALOR") > 0,
            (F.col("ULTIMO_VALOR") - F.col("PRIMEIRO_VALOR")) / F.col("PRIMEIRO_VALOR") * 100)
    )
)

print("=" * 60)
print("RESUMO DE TENDÊNCIAS")
print("=" * 60)

resumo_tend = (
    cnpj_stats
    .groupBy("TENDENCIA")
    .agg(
        F.count("CPF_CNPJ").alias("QTD_CNPJ"),
        F.sum("TOTAL_FATURADO").alias("FATURAMENTO_TOTAL"),
    )
    .orderBy(F.desc("QTD_CNPJ"))
)
display(resumo_tend)

# COMMAND ----------

# --------------------------------------------------------------------------
# 3. Análise de migração SIMETRA → NG
# --------------------------------------------------------------------------
cnpjs_simetra_fev = (
    sdf.filter((F.col("SISTEMA") == "SIMETRA") & (F.col("ANO_MES") == MES_CORTE_SIMETRA))
    .select("CPF_CNPJ").distinct()
    .withColumn("ERA_SIMETRA_FEV", F.lit(True))
)
cnpjs_ng_mar = (
    sdf.filter((F.col("SISTEMA") == "NG") & (F.col("ANO_MES") == MES_INICIO_NG))
    .select("CPF_CNPJ").distinct()
    .withColumn("FOI_NG_MAR", F.lit(True))
)

cnpj_stats = (
    cnpj_stats
    .join(cnpjs_simetra_fev, on="CPF_CNPJ", how="left")
    .join(cnpjs_ng_mar,      on="CPF_CNPJ", how="left")
    .withColumn("ERA_SIMETRA_FEV", F.coalesce("ERA_SIMETRA_FEV", F.lit(False)))
    .withColumn("FOI_NG_MAR",      F.coalesce("FOI_NG_MAR",      F.lit(False)))
    .withColumn("STATUS_MIGRACAO",
        F.when(F.col("ERA_SIMETRA_FEV") & F.col("FOI_NG_MAR"),  "MIGROU")
         .when(F.col("ERA_SIMETRA_FEV") & ~F.col("FOI_NG_MAR"), "SUMIU")
         .otherwise("N/A")
    )
)

print("=" * 60)
print("STATUS DE MIGRAÇÃO (CNPJs do SIMETRA Fev/26)")
print("=" * 60)

display(
    cnpj_stats
    .filter(F.col("ERA_SIMETRA_FEV"))
    .groupBy("STATUS_MIGRACAO")
    .agg(
        F.count("CPF_CNPJ").alias("QTD"),
        F.sum("TOTAL_FATURADO").alias("FATURAMENTO_TOTAL"),
        F.avg("TOTAL_FATURADO").alias("TICKET_MEDIO_CNPJ"),
    )
    .orderBy(F.desc("QTD"))
)

# COMMAND ----------

# --------------------------------------------------------------------------
# 4. CNPJs que SUMIRAM — quanto valiam antes e quanto faturam agora
# --------------------------------------------------------------------------
MES_PRE       = ["2025-12", "2026-01", "2026-02"]
MES_POST      = ["2026-03", "2026-04", "2026-05", "2026-06", "2026-07"]
# Mar/26 excluído: sumidos não foram ao NG em Mar, logo não há o quê comparar nesse mês
MES_POS_SUMIDOS = ["2026-04", "2026-05", "2026-06", "2026-07"]

# CNPJs que sumiram (estavam no SIMETRA em Fev mas não foram pro NG em Mar)
cnpjs_sumiram = cnpj_stats.filter(F.col("STATUS_MIGRACAO") == "SUMIU").select("CPF_CNPJ")

# Faturamento histórico no período PRÉ (todos os meses pré-migração, qualquer sistema)
fat_pre_sumidos = (
    sdf
    .join(cnpjs_sumiram, on="CPF_CNPJ", how="inner")
    .filter(F.col("ANO_MES").isin(MES_PRE) & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_PRE"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_PRE"),
        F.countDistinct("ANO_MES").alias("MESES_PRE"),
        F.max("ANO_MES").alias("ULTIMO_MES_PRE"),
    )
    .withColumn("MEDIA_MENSAL_PRE",
        F.when(F.col("MESES_PRE") > 0, F.col("TOTAL_PRE") / F.col("MESES_PRE"))
    )
)

# Faturamento PÓS-migração — qualquer sistema (Abr–Jul, sem Mar pois sumidos não foram ao NG)
fat_pos_sumidos = (
    sdf
    .join(cnpjs_sumiram, on="CPF_CNPJ", how="inner")
    .filter(F.col("ANO_MES").isin(MES_POS_SUMIDOS) & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_POS"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_POS"),
        F.countDistinct("ANO_MES").alias("MESES_POS"),
        F.collect_set("SISTEMA").alias("SISTEMAS_POS"),
        F.max("ANO_MES").alias("ULTIMO_MES_POS"),
    )
    .withColumn("MEDIA_MENSAL_POS",
        F.when(F.col("MESES_POS") > 0, F.col("TOTAL_POS") / F.col("MESES_POS"))
    )
)

# Junta e calcula impacto
sumidos_analise = (
    fat_pre_sumidos
    .join(fat_pos_sumidos, on="CPF_CNPJ", how="left")
    .withColumn("TOTAL_POS",        F.coalesce("TOTAL_POS",        F.lit(0.0)))
    .withColumn("MEDIA_MENSAL_POS", F.coalesce("MEDIA_MENSAL_POS", F.lit(0.0)))
    .withColumn("CONTRATOS_POS",    F.coalesce("CONTRATOS_POS",    F.lit(0)))
    .withColumn("MESES_POS",        F.coalesce("MESES_POS",        F.lit(0)))
    .withColumn("STATUS_ATUAL",
        F.when(F.col("TOTAL_POS") > 0, "Reapareceu (outro sistema)")
         .otherwise("Zerou completamente")
    )
    .withColumn("PERDA_MENSAL",
        F.col("MEDIA_MENSAL_PRE") - F.col("MEDIA_MENSAL_POS")
    )
    .withColumn("PERDA_PCT",
        F.when(F.col("MEDIA_MENSAL_PRE") > 0,
            F.col("PERDA_MENSAL") / F.col("MEDIA_MENSAL_PRE") * 100)
    )
    .select(
        "CPF_CNPJ",
        "ULTIMO_MES_PRE",
        "TOTAL_PRE",
        "MEDIA_MENSAL_PRE",
        "CONTRATOS_PRE",
        "TOTAL_POS",
        "MEDIA_MENSAL_POS",
        "CONTRATOS_POS",
        "MESES_POS",
        "ULTIMO_MES_POS",
        "SISTEMAS_POS",
        "STATUS_ATUAL",
        "PERDA_MENSAL",
        "PERDA_PCT",
    )
    .orderBy(F.desc("MEDIA_MENSAL_PRE"))
)

# Resumo agregado
total_pre  = fat_pre_sumidos.agg(F.sum("TOTAL_PRE").alias("T")).collect()[0]["T"] or 0
total_pos  = fat_pos_sumidos.agg(F.sum("TOTAL_POS").alias("T")).collect()[0]["T"] or 0
qtd_sumiu  = cnpjs_sumiram.count()
qtd_reapar = fat_pos_sumidos.count()

print("=" * 60)
print("CNPJs QUE SUMIRAM — ANTES vs AGORA")
print("=" * 60)
print(f"Total de CNPJs que sumiram:       {qtd_sumiu:,}")
print(f"  Reapareceram em outro sistema:  {qtd_reapar:,}")
print(f"  Zeraram completamente:          {qtd_sumiu - qtd_reapar:,}")
print(f"Faturamento total PRE (Dez/25-Fev/26): R$ {total_pre:,.2f}")
print(f"Faturamento total POS (Mar/26-Jul/26): R$ {total_pos:,.2f}")
print(f"Perda estimada total:                  R$ {total_pre - total_pos:,.2f}")
print()
print("Detalhe por CNPJ (ordenado pelo maior faturador antes):")
display(sumidos_analise)

# COMMAND ----------

# --------------------------------------------------------------------------
# 5. Impacto pré vs pós migração por CNPJ (queda de faturamento)
# --------------------------------------------------------------------------

pre_mig = (
    sdf.filter(F.col("ANO_MES").isin(MES_PRE) & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_PRE"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_PRE"),
        F.countDistinct("ANO_MES").alias("MESES_PRE"),
    )
    .withColumn("MEDIA_PRE",  F.when(F.col("MESES_PRE")    > 0, F.col("TOTAL_PRE") / F.col("MESES_PRE")))
    .withColumn("TICKET_PRE", F.when(F.col("CONTRATOS_PRE") > 0, F.col("TOTAL_PRE") / F.col("CONTRATOS_PRE")))
)

post_mig = (
    sdf.filter(F.col("ANO_MES").isin(MES_POST) & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_POS"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_POS"),
        F.countDistinct("ANO_MES").alias("MESES_POS"),
    )
    .withColumn("MEDIA_POS",  F.when(F.col("MESES_POS")    > 0, F.col("TOTAL_POS") / F.col("MESES_POS")))
    .withColumn("TICKET_POS", F.when(F.col("CONTRATOS_POS") > 0, F.col("TOTAL_POS") / F.col("CONTRATOS_POS")))
)

impacto = (
    pre_mig.join(post_mig, on="CPF_CNPJ", how="inner")
    .withColumn("VAR_MEDIA_ABS", F.col("MEDIA_POS") - F.col("MEDIA_PRE"))
    .withColumn("VAR_MEDIA_PCT",
        F.when(F.col("MEDIA_PRE") > 0,
            (F.col("MEDIA_POS") - F.col("MEDIA_PRE")) / F.col("MEDIA_PRE") * 100)
    )
    .withColumn("VAR_CONTRATO_PCT",
        F.when(F.col("CONTRATOS_PRE") > 0,
            (F.col("CONTRATOS_POS") / F.col("MESES_POS") - F.col("CONTRATOS_PRE") / F.col("MESES_PRE"))
            / (F.col("CONTRATOS_PRE") / F.col("MESES_PRE")) * 100)
    )
    .withColumn("VAR_TICKET_PCT",
        F.when(F.col("TICKET_PRE") > 0,
            (F.col("TICKET_POS") - F.col("TICKET_PRE")) / F.col("TICKET_PRE") * 100)
    )
    .withColumn("CAUSA",
        F.when(F.col("VAR_MEDIA_ABS") >= 0, "Crescimento")
         .when(F.abs(F.col("VAR_CONTRATO_PCT")) >= F.abs(F.col("VAR_TICKET_PCT")), "Reducao de contratos")
         .when(F.abs(F.col("VAR_TICKET_PCT"))   >  F.abs(F.col("VAR_CONTRATO_PCT")), "Reducao de ticket")
         .otherwise("Mista")
    )
    .filter(F.col("VAR_MEDIA_ABS") < 0)
    .orderBy("VAR_MEDIA_ABS")
    .select("CPF_CNPJ",
            "MEDIA_PRE", "MEDIA_POS", "VAR_MEDIA_ABS", "VAR_MEDIA_PCT",
            "VAR_CONTRATO_PCT", "VAR_TICKET_PCT", "CAUSA")
)

print("=" * 60)
print("CNPJs COM QUEDA PÓS-MIGRAÇÃO (média mensal pré vs pós)")
print("=" * 60)
display(impacto)

# COMMAND ----------

# --------------------------------------------------------------------------
# 5. Top 100 maior queda de faturamento
# --------------------------------------------------------------------------
w_lag = Window.partitionBy("CPF_CNPJ").orderBy("ANO_MES")

top100_queda = (
    cnpj_mes
    .withColumn("VALOR_ANTERIOR", F.lag("VALOR").over(w_lag))
    .filter(F.col("VALOR_ANTERIOR").isNotNull())
    .withColumn("VARIACAO_MES", F.col("VALOR") - F.col("VALOR_ANTERIOR"))
    .withColumn("rn_pior", F.row_number().over(
        Window.partitionBy("CPF_CNPJ").orderBy("VARIACAO_MES")
    ))
    .filter(F.col("rn_pior") == 1)
    .join(
        cnpj_stats.select("CPF_CNPJ", "TOTAL_FATURADO", "MESES_ATIVOS",
                          "PRIMEIRO_MES", "PRIMEIRO_VALOR",
                          "ULTIMO_MES",   "ULTIMO_VALOR", "TENDENCIA"),
        on="CPF_CNPJ", how="left"
    )
    .filter(F.col("VARIACAO_MES") < 0)
    .withColumn("QUEDA_PERCENTUAL",
        F.when(F.col("VALOR_ANTERIOR") > 0,
            F.col("VARIACAO_MES") / F.col("VALOR_ANTERIOR") * 100)
    )
    .select(
        "CPF_CNPJ", "TENDENCIA",
        F.col("ANO_MES").alias("MES_MAIOR_QUEDA"),
        "VALOR_ANTERIOR", "VALOR", "VARIACAO_MES", "QUEDA_PERCENTUAL",
        "MESES_ATIVOS", "TOTAL_FATURADO",
    )
    .orderBy("VARIACAO_MES")
    .limit(100)
)

print("=" * 60)
print("TOP 100 CNPJ — MAIOR QUEDA MENSAL")
print("=" * 60)
display(top100_queda)
