# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga faturas_principal_vero
# MAGIC **Fonte:** `accenture.validacao_status_fatura` (coracao do processo)
# MAGIC **Destino:** `accenture.faturas_principal_vero` (espelho identico a faturas_principal_v4)
# MAGIC
# MAGIC ### Logica
# MAGIC - Aglutina por FATURA + ID_CONTA
# MAGIC - Linhas INCORRETAS: concatena no campo PROBLEMA
# MAGIC - Se qualquer linha INCORRETA → STATUS=INCORRETO, STATUS_VALIDACAO=PENDENTE
# MAGIC - ANALISTA e OBSERVACAO → nulo (preenchimento manual)
# MAGIC - VALOR → 0 se nulo
# MAGIC - Campos incrementais preenchidos automaticamente
# MAGIC - Campos nao listados → nulo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros

# COMMAND ----------

from datetime import datetime

# Ciclo automatico: ano-mes atual
_CICLO_AUTO = datetime.now().strftime("%Y-%m")

dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ref", _CICLO_AUTO, "Ciclo (AAAA-MM)")
CICLO_REF = dbutils.widgets.get("ciclo_ref")

TBL_FONTE   = "accenture.validacao_status_fatura"
TBL_DESTINO = "accenture.faturas_principal_vero"

print(f"Fonte: {TBL_FONTE} → Destino: {TBL_DESTINO} | Ciclo: {CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DDL — schema identico a faturas_principal_v4

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_DESTINO} (
    FATURA                  STRING,
    ID_CONTA                STRING,
    STATUS                  STRING,
    STATUS_VALIDACAO        STRING,
    ANALISTA                STRING,
    OBSERVACAO              STRING,
    PROBLEMA                STRING,
    VALOR                   DOUBLE,
    CRIADO_EM               STRING,
    STATUS_RETORNO          STRING,
    CHAMADO                 STRING,
    RESUMO                  STRING,
    Ordem_Status            INT,
    DATA_ABERTURA_CHAMADO   DATE,
    DT_EMISSAO              DATE,
    Valor_Positive          STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")
print(f"DDL {TBL_DESTINO} OK — 16 colunas identicas a faturas_principal_v4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura da fonte

# COMMAND ----------

df_fonte = (
    spark.table(TBL_FONTE)
    .filter(F.col("ID_Lote") == CICLO_REF)
)
cnt_fonte = df_fonte.count()
print(f"Fonte: {cnt_fonte:,} linhas no ciclo {CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aglutinar por FATURA + ID_CONTA
# MAGIC
# MAGIC ### Mapeamento campo a campo (referencia → logica)
# MAGIC | # | Campo destino         | Origem / Logica                                              |
# MAGIC |---|----------------------|--------------------------------------------------------------|
# MAGIC | 1 | FATURA               | fonte.FATURA                                                 |
# MAGIC | 2 | ID_CONTA             | fonte.ID_CONTA_CONTRATO (cast double)                        |
# MAGIC | 3 | STATUS               | INCORRETO se qualquer linha INCORRETA, senao CORRETO         |
# MAGIC | 4 | STATUS_VALIDACAO      | PENDENTE se INCORRETO, VALIDADO se CORRETO                   |
# MAGIC | 5 | ANALISTA             | null                                                         |
# MAGIC | 6 | OBSERVACAO           | null                                                         |
# MAGIC | 7 | PROBLEMA             | Concatenacao de REGRA das linhas INCORRETAS (pipe-separated)  |
# MAGIC | 8 | VALOR                | SUM(VALOR_BILLING), 0 se nulo                                |
# MAGIC | 9 | CRIADO_EM            | current_timestamp (string ISO)                                |
# MAGIC |10 | STATUS_RETORNO       | null                                                         |
# MAGIC |11 | CHAMADO              | null                                                         |
# MAGIC |12 | RESUMO               | Concatenacao TAG: obs | SEVERIDADE: substatus                 |
# MAGIC |13 | Ordem_Status         | 4 se INCORRETO, 1 se CORRETO                                 |
# MAGIC |14 | DATA_ABERTURA_CHAMADO| current_date                                                  |
# MAGIC |15 | DT_EMISSAO           | MIN(DT_EMISSAO) do grupo — ou current_date se nulo           |
# MAGIC |16 | Valor_Positive       | SIM se VALOR > 0, NAO caso contrario                         |

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5a. Preparar colunas auxiliares antes do groupBy
# ---------------------------------------------------------------------------

# Regras que usam Produto como TAG (em vez de REGRA)
_REGRAS_TAG_PRODUTO = ["VALOR_OFERTA", "DIVERGENCIA_CONTRATO_PRODUTO", "VALOR ZERADO", "VALOR FATURA"]

df_prep = (
    df_fonte
    # TAG: se REGRA in lista especial → Produto, senao → REGRA
    .withColumn("_tag",
        F.when(
            F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
            F.coalesce(F.col("Produto"), F.col("REGRA"))
        ).otherwise(F.col("REGRA"))
    )
    # Coluna com REGRA somente quando INCORRETO (para concatenar no PROBLEMA)
    .withColumn("_regra_inc",
        F.when(F.col("STATUS") == "INCORRETO", F.col("REGRA"))
    )
    # Coluna com TAG: OBSERVACAO somente quando INCORRETO
    .withColumn("_obs_inc",
        F.when(
            (F.col("STATUS") == "INCORRETO") & F.col("OBSERVACAO").isNotNull() & (F.trim(F.col("OBSERVACAO")) != ""),
            F.concat(F.col("_tag"), F.lit(": "), F.col("OBSERVACAO"))
        ).when(
            F.col("STATUS") == "INCORRETO",
            F.col("_tag")
        )
    )
)

# ---------------------------------------------------------------------------
# 5b. Agregar TUDO em um unico groupBy (sem JOIN — evita problema NaN)
# ---------------------------------------------------------------------------

df_agg = (
    df_prep
    .groupBy("FATURA", "ID_CONTA_CONTRATO")
    .agg(
        # Flag: tem ao menos 1 INCORRETO?
        F.max(
            F.when(F.col("STATUS") == "INCORRETO", F.lit(1)).otherwise(F.lit(0))
        ).alias("_tem_incorreto"),

        # PROBLEMA: regras distintas das linhas INCORRETAS
        F.concat_ws(" | ", F.collect_set(F.col("_regra_inc"))).alias("_PROBLEMA"),

        # OBSERVACAO: todas as obs das linhas INCORRETAS
        F.concat_ws(" | ", F.collect_list(F.col("_obs_inc"))).alias("_OBSERVACAO"),

        # RESUMO: idem OBSERVACAO
        F.concat_ws(" | ", F.collect_list(F.col("_obs_inc"))).alias("_RESUMO"),

        # Soma de VALOR_BILLING (0 se nulo ou vazio)
        F.sum(
            F.coalesce(
                F.when(
                    F.col("VALOR_BILLING").isNotNull() & (F.trim(F.col("VALOR_BILLING").cast(StringType())) != ""),
                    F.col("VALOR_BILLING").cast(DoubleType())
                ),
                F.lit(0.0)
            )
        ).alias("_valor_soma"),
    )
)

# ---------------------------------------------------------------------------
# 5c. Montar as 16 colunas exatas
# ---------------------------------------------------------------------------

df_result = (
    df_agg
    .select(
        # 1. FATURA (STRING)
        F.col("FATURA").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA (STRING)
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. STATUS
        F.when(F.col("_tem_incorreto") == 1, F.lit("INCORRETO"))
         .otherwise(F.lit("CORRETO"))
         .alias("STATUS"),

        # 4. STATUS_VALIDACAO
        F.when(F.col("_tem_incorreto") == 1, F.lit("PENDENTE"))
         .otherwise(F.lit("VALIDADO"))
         .alias("STATUS_VALIDACAO"),

        # 5. ANALISTA — nulo
        F.lit(None).cast(StringType()).alias("ANALISTA"),

        # 6. OBSERVACAO — concatena obs das linhas INCORRETAS
        F.when(
            (F.col("_tem_incorreto") == 1) & (F.col("_OBSERVACAO") != ""),
            F.col("_OBSERVACAO")
        ).otherwise(F.lit(None).cast(StringType()))
         .alias("OBSERVACAO"),

        # 7. PROBLEMA — concatena REGRA das linhas INCORRETAS
        F.when(
            (F.col("_tem_incorreto") == 1) & (F.col("_PROBLEMA") != ""),
            F.col("_PROBLEMA")
        ).otherwise(F.lit(None).cast(StringType()))
         .alias("PROBLEMA"),

        # 8. VALOR — soma, 0 se nulo
        F.coalesce(F.col("_valor_soma"), F.lit(0.0))
         .cast(DoubleType())
         .alias("VALOR"),

        # 9. CRIADO_EM — timestamp atual em ISO string
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
         .alias("CRIADO_EM"),

        # 10. STATUS_RETORNO — nulo
        F.lit(None).cast(StringType()).alias("STATUS_RETORNO"),

        # 11. CHAMADO — nulo
        F.lit(None).cast(StringType()).alias("CHAMADO"),

        # 12. RESUMO — concatena obs com regra
        F.when(
            (F.col("_tem_incorreto") == 1) & (F.col("_RESUMO") != ""),
            F.col("_RESUMO")
        ).otherwise(F.lit(None).cast(StringType()))
         .alias("RESUMO"),

        # 13. Ordem_Status — 4=INCORRETO/PENDENTE, 1=CORRETO/VALIDADO
        F.when(F.col("_tem_incorreto") == 1, F.lit(4))
         .otherwise(F.lit(1))
         .cast(IntegerType())
         .alias("Ordem_Status"),

        # 14. DATA_ABERTURA_CHAMADO — data atual
        F.current_date().cast(DateType()).alias("DATA_ABERTURA_CHAMADO"),

        # 15. DT_EMISSAO — data atual (ou derivar da fonte se disponivel)
        F.current_date().cast(DateType()).alias("DT_EMISSAO"),

        # 16. Valor_Positive
        F.when(
            F.coalesce(F.col("_valor_soma"), F.lit(0.0)) > 0,
            F.lit("SIM")
        ).otherwise(F.lit("NAO"))
         .alias("Valor_Positive"),
    )
)

# Remove duplicatas por FATURA + ID_CONTA
df_result = df_result.dropDuplicates(["FATURA", "ID_CONTA"])

# Somente faturas com STATUS INCORRETO
df_result = df_result.filter(F.col("STATUS") == "INCORRETO")

cnt = df_result.count()
print(f"Resultado: {cnt:,} faturas INCORRETAS (unicas por FATURA+ID_CONTA)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravar — overwrite do ciclo

# COMMAND ----------

# Limpa ciclo atual (idempotente)
try:
    spark.sql(f"DELETE FROM {TBL_DESTINO}")
except:
    pass  # Tabela vazia na primeira execucao

# Append somente INCORRETOS unicos
df_result.write.format("delta").mode("append").saveAsTable(TBL_DESTINO)

cnt_final = spark.table(TBL_DESTINO).count()
print(f"Gravado: {cnt_final:,} registros em {TBL_DESTINO} (somente INCORRETO)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. QA — Resumo

# COMMAND ----------

spark.sql(f"""
SELECT STATUS, STATUS_VALIDACAO, Ordem_Status,
    COUNT(*) AS faturas,
    COUNT(DISTINCT ID_CONTA) AS contas,
    ROUND(SUM(VALOR), 2) AS valor_total,
    SUM(CASE WHEN PROBLEMA IS NOT NULL THEN 1 ELSE 0 END) AS com_problema,
    SUM(CASE WHEN Valor_Positive = 'SIM' THEN 1 ELSE 0 END) AS com_valor
FROM {TBL_DESTINO}
GROUP BY STATUS, STATUS_VALIDACAO, Ordem_Status
ORDER BY Ordem_Status
""").show(truncate=False)

# COMMAND ----------

# Top problemas
spark.sql(f"""
SELECT PROBLEMA, COUNT(*) qtd
FROM {TBL_DESTINO}
WHERE PROBLEMA IS NOT NULL
GROUP BY PROBLEMA ORDER BY qtd DESC LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Amostra incorretas
spark.sql(f"""
SELECT FATURA, ID_CONTA, STATUS, STATUS_VALIDACAO, ANALISTA, OBSERVACAO,
    PROBLEMA, VALOR, RESUMO, Ordem_Status, Valor_Positive
FROM {TBL_DESTINO} WHERE STATUS = 'INCORRETO' LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# Amostra corretas
spark.sql(f"""
SELECT FATURA, ID_CONTA, STATUS, STATUS_VALIDACAO, ANALISTA, OBSERVACAO,
    PROBLEMA, VALOR, Ordem_Status, Valor_Positive
FROM {TBL_DESTINO} WHERE STATUS = 'CORRETO' LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sanidade

# COMMAND ----------

checks = []

# 1. Contagem bate
n_src = df_fonte.select("FATURA","ID_CONTA_CONTRATO").distinct().count()
n_dst = spark.table(TBL_DESTINO).count()
c1 = n_src == n_dst
checks.append(c1)
print(f"1. Faturas fonte={n_src:,} vs destino={n_dst:,} {'✅' if c1 else '❌'}")

# 2. ANALISTA sempre nulo
c2 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ANALISTA IS NOT NULL").collect()[0][0] == 0
checks.append(c2)
print(f"2. ANALISTA sempre nulo: {'✅' if c2 else '❌'}")

# 3. OBSERVACAO sempre nulo
c3 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE OBSERVACAO IS NOT NULL").collect()[0][0] == 0
checks.append(c3)
print(f"3. OBSERVACAO sempre nulo: {'✅' if c3 else '❌'}")

# 4. VALOR nunca nulo
c4 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE VALOR IS NULL").collect()[0][0] == 0
checks.append(c4)
print(f"4. VALOR nunca nulo: {'✅' if c4 else '❌'}")

# 5. CORRETO nao tem PROBLEMA
c5 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE STATUS='CORRETO' AND PROBLEMA IS NOT NULL").collect()[0][0] == 0
checks.append(c5)
print(f"5. CORRETO sem PROBLEMA: {'✅' if c5 else '❌'}")

# 6. INCORRETO tem PROBLEMA
c6 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE STATUS='INCORRETO' AND (PROBLEMA IS NULL OR PROBLEMA='')").collect()[0][0] == 0
checks.append(c6)
print(f"6. INCORRETO com PROBLEMA: {'✅' if c6 else '❌'}")

# 7. Schema identico (16 colunas)
cols_ref = ["FATURA","ID_CONTA","STATUS","STATUS_VALIDACAO","ANALISTA","OBSERVACAO",
            "PROBLEMA","VALOR","CRIADO_EM","STATUS_RETORNO","CHAMADO","RESUMO",
            "Ordem_Status","DATA_ABERTURA_CHAMADO","DT_EMISSAO","Valor_Positive"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c7 = cols_dst == cols_ref
checks.append(c7)
print(f"7. Schema 16 colunas identicas: {'✅' if c7 else '❌'}")
if not c7:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")

print(f"\n{'='*60}")
print(f"{'✅ CARGA OK' if all(checks) else '⚠️ VER ISSUES'}")