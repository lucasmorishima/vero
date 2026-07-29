# Databricks notebook source
# Foco: CNPJs do SIMETRA — a receita migrou para o NG?
# Base: todos que faturaram no SIMETRA no período PRÉ (Dez/25–Fev/26)
# Comparação: SIMETRA PRÉ  →  NG PÓS + SIMETRA PÓS

from pyspark.sql import functions as F
from pyspark.sql import Window

spark   = globals().get("spark")
dbutils = globals().get("dbutils")

PRE  = ["2025-12", "2026-01", "2026-02"]
POS  = ["2026-03", "2026-04", "2026-05", "2026-06", "2026-07"]
MESES_SQL = "('" + "','".join(PRE + POS) + "')"

# Labels de DESTINO — definidos uma única vez, reutilizados em withColumn e filter
D_CONTINUA_SIMETRA  = "Continua faturando no Simetra"
D_SEM_NG            = "Ja faturou no Simetra e nao faturou no NG"
D_DOIS_SISTEMAS     = "Fatura nos 2 sistemas"
D_MIGROU_GANHOU     = "Migrou - Ganhou receita"
D_MIGROU_PERDEU     = "Migrou - Perdeu receita"

# --------------------------------------------------------------------------
# Carga
# --------------------------------------------------------------------------
sql_query = f"""
SELECT
    'SIMETRA'                                                       AS SISTEMA,
    REGEXP_REPLACE(TRIM(np.A1_CGC), '[^0-9]', '')                   AS CPF_CNPJ,
    DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM')         AS ANO_MES,
    REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')                  AS FATURA,
    COUNT(DISTINCT np.C6_CONTRT)                                     AS QTD_CONTRATO,
    SUM(FT_VALCONT)                                                  AS VALOR_FATURADO
FROM NEGOCIO.TB_FATURAMENTO_PROTHEUS_COMPLETA np
WHERE DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM') IN {MESES_SQL}
GROUP BY REGEXP_REPLACE(TRIM(np.A1_CGC), '[^0-9]', ''),
         DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM'),
         REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')

UNION ALL

SELECT
    'NG'                                                             AS SISTEMA,
    REGEXP_REPLACE(TRIM(CPF_CNPJ), '[^0-9]', '')                    AS CPF_CNPJ,
    try_cast(MESANO AS STRING)                                       AS ANO_MES,
    FATURA_NUMERO                                                    AS FATURA,
    COUNT(idcontrato)                                                AS QTD_CONTRATO,
    SUM(try_cast(NULLIF(trim(ReceitaBruta), '') AS DOUBLE))          AS VALOR_FATURADO
FROM hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS
WHERE try_cast(MESANO AS STRING) IN {MESES_SQL}
  AND crm = 'NG'
GROUP BY REGEXP_REPLACE(TRIM(CPF_CNPJ), '[^0-9]', ''),
         try_cast(MESANO AS STRING),
         FATURA_NUMERO
"""

sdf = spark.sql(sql_query).filter(F.col("VALOR_FATURADO") > 0)
sdf.createOrReplaceTempView("vw_fat")
sdf = spark.table("vw_fat")

# COMMAND ----------

# --------------------------------------------------------------------------
# Base: CNPJs que faturaram no SIMETRA no período PRÉ
# ÚNICA origem — contratos NG-only nunca entram nessa base
# --------------------------------------------------------------------------
base_simetra = (
    sdf
    .filter(
        (F.col("SISTEMA") == "SIMETRA")
        & F.col("ANO_MES").isin(PRE)
        & F.col("CPF_CNPJ").isNotNull()
        & (F.trim(F.col("CPF_CNPJ")) != "")
    )
    .select("CPF_CNPJ").distinct()
)
base_simetra.createOrReplaceTempView("vw_base_simetra")
base_simetra = spark.table("vw_base_simetra")

qtd_base = base_simetra.count()
print(f"CNPJs SIMETRA PRE (antes do filtro de ultimo mes): {qtd_base:,}")

# Validação: nenhum CNPJ que entrou na base pode ser exclusivo do NG
cnpj_ng_only = (
    sdf.filter(F.col("SISTEMA") == "NG").select("CPF_CNPJ").distinct()
    .join(sdf.filter(F.col("SISTEMA") == "SIMETRA").select("CPF_CNPJ").distinct(),
          on="CPF_CNPJ", how="left_anti")
)
vazamento = base_simetra.join(cnpj_ng_only, on="CPF_CNPJ", how="inner").count()
print(f"Vazamento (NG-only na base SIMETRA): {vazamento} — esperado 0")

# COMMAND ----------

# --------------------------------------------------------------------------
# Receita de cada "braço" para os CNPJs da base
# --------------------------------------------------------------------------

# 1) SIMETRA PRÉ — ponto de partida
CORTE_ULTIMO_MES_PRE = "2026-02"  # só entra quem faturou no SIMETRA até este mês

simetra_pre = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .filter((F.col("SISTEMA") == "SIMETRA") & F.col("ANO_MES").isin(PRE))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("RECEITA_SIM_PRE"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_SIM_PRE"),
        F.countDistinct("ANO_MES").alias("MESES_SIM_PRE"),
        F.max("ANO_MES").alias("ULTIMO_MES_SIM_PRE"),
    )
    .withColumn("MEDIA_SIM_PRE",
        F.when(F.col("MESES_SIM_PRE") > 0,
            F.col("RECEITA_SIM_PRE") / F.col("MESES_SIM_PRE"))
    )
    # universo: somente CNPJs cujo último mês no PRE foi >= CORTE_ULTIMO_MES_PRE
    .filter(F.col("ULTIMO_MES_SIM_PRE") >= CORTE_ULTIMO_MES_PRE)
)

# reconstrói base_simetra a partir do universo filtrado
base_simetra = simetra_pre.select("CPF_CNPJ").distinct()
base_simetra.createOrReplaceTempView("vw_base_simetra")
base_simetra = spark.table("vw_base_simetra")

qtd_universo = simetra_pre.count()
print(f"Universo apos filtro (ultimo mes PRE >= {CORTE_ULTIMO_MES_PRE}): {qtd_universo:,} CNPJs")

# 2) NG PÓS — para onde deveria ter ido a receita
ng_pos = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .filter((F.col("SISTEMA") == "NG") & F.col("ANO_MES").isin(POS))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("RECEITA_NG_POS"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_NG_POS"),
        F.countDistinct("ANO_MES").alias("MESES_NG_POS"),
        F.min("ANO_MES").alias("PRIMEIRO_MES_NG"),
        F.max("ANO_MES").alias("ULTIMO_MES_NG"),
    )
    .withColumn("MEDIA_NG_POS",
        F.when(F.col("MESES_NG_POS") > 0,
            F.col("RECEITA_NG_POS") / F.col("MESES_NG_POS"))
    )
)

# 2b) CNPJs que existem no NG — lookup direto no sdf bruto, SEM filtrar por base_simetra
# Evita falso-negativo quando o join CNPJ não casa mas o CNPJ realmente existe no NG
cnpjs_ng = (
    sdf
    .filter(F.col("SISTEMA") == "NG")
    .select("CPF_CNPJ")
    .distinct()
    .withColumn("EXISTE_NO_NG", F.lit(True))
)

# 2c) NG TOTAL — métricas para os CNPJs da base (valor, linhas, meses)
ng_total = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .filter(F.col("SISTEMA") == "NG")
    .groupBy("CPF_CNPJ")
    .agg(
        F.count("*").alias("LINHAS_NG_TOTAL"),
        F.sum("VALOR_FATURADO").alias("RECEITA_NG_TOTAL"),
    )
)

# 3) SIMETRA PÓS — ficou no SIMETRA mesmo após a virada?
simetra_pos = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .filter((F.col("SISTEMA") == "SIMETRA") & F.col("ANO_MES").isin(POS))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("RECEITA_SIM_POS"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_SIM_POS"),
        F.countDistinct("ANO_MES").alias("MESES_SIM_POS"),
        F.max("ANO_MES").alias("ULTIMO_MES_SIM_POS"),
    )
    .withColumn("MEDIA_SIM_POS",
        F.when(F.col("MESES_SIM_POS") > 0,
            F.col("RECEITA_SIM_POS") / F.col("MESES_SIM_POS"))
    )
)

# --------------------------------------------------------------------------
# Junta tudo e classifica o destino da receita
# --------------------------------------------------------------------------
destino = (
    simetra_pre
    .join(ng_pos,      on="CPF_CNPJ", how="left")
    .join(ng_total,    on="CPF_CNPJ", how="left")
    .join(cnpjs_ng,    on="CPF_CNPJ", how="left")
    .join(simetra_pos, on="CPF_CNPJ", how="left")
    # preenche zeros onde não houve faturamento
    .withColumn("RECEITA_NG_POS",    F.coalesce("RECEITA_NG_POS",    F.lit(0.0)))
    .withColumn("MEDIA_NG_POS",      F.coalesce("MEDIA_NG_POS",      F.lit(0.0)))
    .withColumn("CONTRATOS_NG_POS",  F.coalesce("CONTRATOS_NG_POS",  F.lit(0)))
    .withColumn("MESES_NG_POS",      F.coalesce("MESES_NG_POS",      F.lit(0)))
    .withColumn("LINHAS_NG_TOTAL",   F.coalesce("LINHAS_NG_TOTAL",   F.lit(0)))
    .withColumn("EXISTE_NO_NG",     F.coalesce("EXISTE_NO_NG",     F.lit(False)))
    .withColumn("RECEITA_NG_TOTAL",  F.coalesce("RECEITA_NG_TOTAL",  F.lit(0.0)))
    .withColumn("RECEITA_SIM_POS",   F.coalesce("RECEITA_SIM_POS",   F.lit(0.0)))
    .withColumn("MEDIA_SIM_POS",     F.coalesce("MEDIA_SIM_POS",     F.lit(0.0)))
    .withColumn("CONTRATOS_SIM_POS", F.coalesce("CONTRATOS_SIM_POS", F.lit(0)))
    .withColumn("MESES_SIM_POS",     F.coalesce("MESES_SIM_POS",     F.lit(0)))
    # receita total pós (NG + qualquer SIMETRA restante)
    .withColumn("MEDIA_TOTAL_POS", F.col("MEDIA_NG_POS") + F.col("MEDIA_SIM_POS"))
    # variação da receita média pré → pós
    .withColumn("VAR_ABS",
        F.col("MEDIA_TOTAL_POS") - F.col("MEDIA_SIM_PRE")
    )
    .withColumn("VAR_PCT",
        F.when(F.col("MEDIA_SIM_PRE") > 0,
            F.col("VAR_ABS") / F.col("MEDIA_SIM_PRE") * 100)
    )
    # % da receita pré que aparece no NG pós
    .withColumn("COBERTURA_NG_PCT",
        F.when(F.col("MEDIA_SIM_PRE") > 0,
            F.col("MEDIA_NG_POS") / F.col("MEDIA_SIM_PRE") * 100)
    )
    # classificação do destino da receita — 5 categorias de negócio
    # EXISTE_NO_NG: lookup direto no sdf bruto — True se o CNPJ aparece em qualquer linha do NG
    # Mais robusto que contar linhas via join, evita falso-negativo por mismatch de CNPJ
    .withColumn("DESTINO",
        F.when(
            (~F.col("EXISTE_NO_NG")) & (F.col("MESES_SIM_POS") > 0),
            F.lit(D_CONTINUA_SIMETRA)
        ).when(
            (~F.col("EXISTE_NO_NG")) & (F.col("MESES_SIM_POS") == 0),
            F.lit(D_SEM_NG)
        ).when(
            (F.col("MESES_NG_POS") > 0) & (F.col("MESES_SIM_POS") > 0),
            F.lit(D_DOIS_SISTEMAS)
        ).when(
            (F.col("MESES_NG_POS") > 0) & (F.col("MEDIA_NG_POS") >= F.col("MEDIA_SIM_PRE")),
            F.lit(D_MIGROU_GANHOU)
        ).when(
            F.col("MESES_NG_POS") > 0,
            F.lit(D_MIGROU_PERDEU)
        ).otherwise("Indefinido")
    )
    .orderBy(F.desc("RECEITA_SIM_PRE"))
)

destino.createOrReplaceTempView("vw_destino")
destino = spark.table("vw_destino")

# COMMAND ----------

# --------------------------------------------------------------------------
# Resumo: para onde foi a receita do SIMETRA?
# --------------------------------------------------------------------------
print("=" * 60)
print("DESTINO DA RECEITA DO SIMETRA PÓS-MIGRAÇÃO")
print("=" * 60)

resumo = (
    destino
    .groupBy("DESTINO")
    .agg(
        F.count("CPF_CNPJ").alias("QTD_CNPJ"),
        F.sum("RECEITA_SIM_PRE").alias("RECEITA_PRE_TOTAL"),
        F.sum("RECEITA_NG_POS").alias("RECEITA_NG_TOTAL"),
        F.sum("RECEITA_SIM_POS").alias("RECEITA_SIM_POS_TOTAL"),
        F.avg("COBERTURA_NG_PCT").alias("COBERTURA_NG_MEDIA_PCT"),
        F.avg("VAR_PCT").alias("VAR_MEDIA_PCT"),
    )
    .withColumn("RECEITA_TOTAL_POS",
        F.col("RECEITA_NG_TOTAL") + F.col("RECEITA_SIM_POS_TOTAL")
    )
    .withColumn("PERDA_TOTAL",
        F.col("RECEITA_PRE_TOTAL") - F.col("RECEITA_TOTAL_POS")
    )
    .orderBy(F.desc("RECEITA_PRE_TOTAL"))
)
display(resumo)

# COMMAND ----------

# --------------------------------------------------------------------------
# Detalhe por CNPJ — todos os CNPJs da base com seu destino
# --------------------------------------------------------------------------
print("=" * 60)
print("DETALHE POR CNPJ — RECEITA PRÉ × PÓS MIGRAÇÃO")
print("=" * 60)

df_validacao = (
    destino
    .select(
        "CPF_CNPJ", "DESTINO",
        "ULTIMO_MES_SIM_PRE",
        "MESES_SIM_PRE", "RECEITA_SIM_PRE", "MEDIA_SIM_PRE", "CONTRATOS_SIM_PRE",
        "PRIMEIRO_MES_NG", "ULTIMO_MES_NG",
        "MESES_NG_POS", "RECEITA_NG_POS", "MEDIA_NG_POS", "CONTRATOS_NG_POS",
        "LINHAS_NG_TOTAL", "RECEITA_NG_TOTAL",
        "MESES_SIM_POS", "RECEITA_SIM_POS", "MEDIA_SIM_POS",
        "MEDIA_TOTAL_POS", "VAR_ABS", "VAR_PCT", "COBERTURA_NG_PCT",
    )
    .orderBy("VAR_PCT")
)

df_validacao.createOrReplaceTempView("vw_validacao")
df_validacao = spark.table("vw_validacao")

print(f"Total CNPJs no universo de validação: {df_validacao.count():,}")
display(df_validacao)

# COMMAND ----------

# --------------------------------------------------------------------------
# Grava tabela para análise — accenture.validacao_hipoteses
# --------------------------------------------------------------------------
df_validacao.createOrReplaceTempView("vw_validacao_export")

spark.sql("DROP TABLE IF EXISTS accenture.validacao_hipoteses")
spark.sql("""
    CREATE TABLE accenture.validacao_hipoteses
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    AS SELECT * FROM vw_validacao_export
""")

qtd_gravada = spark.sql("SELECT COUNT(*) AS n FROM accenture.validacao_hipoteses").collect()[0]["n"]
print(f"Tabela accenture.validacao_hipoteses gravada: {qtd_gravada:,} CNPJs")

# COMMAND ----------

# --------------------------------------------------------------------------
# Faturamento mês a mês por sistema — uma linha por CNPJ por mês
# Mesma base do detalhe acima, sem médias
# --------------------------------------------------------------------------
dbutils.widgets.text("filtro_cnpj", "", "Filtrar por CNPJ (vazio = todos)")
filtro_cnpj = dbutils.widgets.get("filtro_cnpj").strip()

# Para fixar um CNPJ no código, descomente a linha abaixo (sobrescreve o widget):
filtro_cnpj = "27275722801"

mensal_por_cnpj = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .groupBy("CPF_CNPJ", "ANO_MES")
    .pivot("SISTEMA", ["SIMETRA", "NG"])
    .agg(F.sum("VALOR_FATURADO"))
    .withColumnRenamed("SIMETRA", "SIMETRA")
    .withColumnRenamed("NG",      "NG")
    .withColumn("SIMETRA", F.coalesce("SIMETRA", F.lit(0.0)))
    .withColumn("NG",      F.coalesce("NG",      F.lit(0.0)))
    .withColumn("TOTAL",   F.col("SIMETRA") + F.col("NG"))
    .withColumn("PERIODO",
        F.when(F.col("ANO_MES").isin(PRE), "PRE").otherwise("POS")
    )
    .join(destino.select("CPF_CNPJ", "DESTINO", "RECEITA_SIM_PRE"), on="CPF_CNPJ", how="left")
    .orderBy("CPF_CNPJ", "ANO_MES")
    .withColumn("SISTEMA_ATIVO",
        F.when((F.col("SIMETRA") > 0) & (F.col("NG") > 0), "SIMETRA + NG")
         .when(F.col("SIMETRA") > 0, "SIMETRA")
         .when(F.col("NG")      > 0, "NG")
         .otherwise("SEM FATURAMENTO")
    )
    .select(
        "CPF_CNPJ", "DESTINO", "PERIODO", "ANO_MES",
        "SISTEMA_ATIVO", "SIMETRA", "NG", "TOTAL", "RECEITA_SIM_PRE",
    )
)

if filtro_cnpj:
    mensal_por_cnpj = mensal_por_cnpj.filter(F.col("CPF_CNPJ") == filtro_cnpj)
    print(f"Filtrado por CNPJ: {filtro_cnpj}")
else:
    print("Exibindo todos os CNPJs")

print("=" * 60)
print("FATURAMENTO MÊS A MÊS POR SISTEMA — BASE SIMETRA PRÉ")
print("=" * 60)
display(mensal_por_cnpj)

# COMMAND ----------

# --------------------------------------------------------------------------
# Mês a mês: SIMETRA PRÉ → SIMETRA PÓS + NG PÓS lado a lado
# Para entender quando a receita transitou
# --------------------------------------------------------------------------
w_cnpj = Window.partitionBy("CPF_CNPJ").orderBy("ANO_MES")

mensal = (
    sdf
    .join(base_simetra, on="CPF_CNPJ", how="inner")
    .groupBy("CPF_CNPJ", "ANO_MES", "SISTEMA")
    .agg(
        F.sum("VALOR_FATURADO").alias("RECEITA"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS"),
    )
    .groupBy("CPF_CNPJ", "ANO_MES")
    .pivot("SISTEMA", ["SIMETRA", "NG"])
    .agg(F.sum("RECEITA"))
    .withColumnRenamed("SIMETRA", "SIMETRA")
    .withColumnRenamed("NG", "NG")
    .withColumn("SIMETRA", F.coalesce("SIMETRA", F.lit(0.0)))
    .withColumn("NG",      F.coalesce("NG",      F.lit(0.0)))
    .withColumn("TOTAL",   F.col("SIMETRA") + F.col("NG"))
    .withColumn("TOTAL_ANT", F.lag("TOTAL").over(w_cnpj))
    .withColumn("VAR_MES_PCT",
        F.when(F.col("TOTAL_ANT") > 0,
            (F.col("TOTAL") - F.col("TOTAL_ANT")) / F.col("TOTAL_ANT") * 100)
    )
    .withColumn("PERIODO",
        F.when(F.col("ANO_MES").isin(PRE), "PRE")
         .otherwise("POS")
    )
    # enriquece com o destino classificado
    .join(destino.select("CPF_CNPJ", "DESTINO"), on="CPF_CNPJ", how="left")
    .orderBy("CPF_CNPJ", "ANO_MES")
)

print("=" * 60)
print("EVOLUÇÃO MENSAL — SIMETRA × NG (base: CNPJs do SIMETRA PRÉ)")
print("=" * 60)
display(mensal)

# COMMAND ----------

# --------------------------------------------------------------------------
# Foco: sumidos — não foram ao NG e pararam de faturar no SIMETRA também
# --------------------------------------------------------------------------
sumidos = (
    destino
    .filter(F.col("DESTINO") == D_SEM_NG)
    .select(
        "CPF_CNPJ",
        "ULTIMO_MES_SIM_PRE",
        "MESES_SIM_PRE", "RECEITA_SIM_PRE", "MEDIA_SIM_PRE", "CONTRATOS_SIM_PRE",
    )
    .orderBy(F.desc("RECEITA_SIM_PRE"))
)

print("=" * 60)
print("SUMIDOS — SEM NG E SEM SIMETRA PÓS-MIGRAÇÃO")
print("=" * 60)
print(f"Total: {sumidos.count():,} CNPJs")
display(sumidos)
