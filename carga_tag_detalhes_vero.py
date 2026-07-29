# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga tag_detalhes_vero
# MAGIC **Fonte:** `accenture.validacao_status_fatura`
# MAGIC **Destino:** `accenture.tag_detalhes_vero` (espelho 1:1, somente linhas com TAG)
# MAGIC
# MAGIC ### Logica
# MAGIC - Espelho da fonte, mesma logica de detalhes_da_fatura_vero
# MAGIC - **Filtro:** somente carrega linhas que possuem TAG (STATUS = INCORRETO com observacao)
# MAGIC - Schema identico a `tag_detalhes_v4` (27 colunas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros

# COMMAND ----------

from datetime import datetime

_CICLO_AUTO = datetime.now().strftime("%Y-%m")

dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ref", _CICLO_AUTO, "Ciclo (AAAA-MM)")
CICLO_REF = dbutils.widgets.get("ciclo_ref")

TBL_FONTE   = "accenture.validacao_status_fatura"
TBL_DESTINO = "accenture.tag_detalhes_vero"

print(f"Fonte: {TBL_FONTE} → Destino: {TBL_DESTINO} | Ciclo: {CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DDL — schema identico a tag_detalhes_v4 (27 colunas)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_DESTINO} (
    FATURA                          STRING,
    ID_CONTA                        STRING,
    ASSET                           STRING,
    REGRA                           STRING,
    STATUS                          STRING,
    SUBSTATUS                       STRING,
    OBSERVACAO                      STRING,
    DADOS_KENAN                     STRING,
    DADOS_TABELA_VERDADE            STRING,
    ID_LOTE                         STRING,
    PRODUTO                         STRING,
    COMPONENT_ID                    STRING,
    TIPO_SERVICO                    STRING,
    ID_SERVICO                      STRING,
    DESCRICAO_SERVICO               STRING,
    TIPO_IMPOSTO                    STRING,
    PROMOCAO                        STRING,
    GRUPO                           STRING,
    STATUS_VALIDACAO                STRING,
    TAG                             STRING,
    ANALISTA                        STRING,
    STATUS_RETORNO                  STRING,
    CHAMADO                         STRING,
    RESUMO                          STRING,
    _FILTRA_PAGE_TAG                STRING,
    DATA_ABERTURA_CHAMADO           DATE,
    DT_EMISSAO                      DATE
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")
print(f"DDL {TBL_DESTINO} OK — 27 colunas identicas a tag_detalhes_v4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura da fonte — somente linhas INCORRETAS (com TAG)

# COMMAND ----------

df_fonte = (
    spark.table(TBL_FONTE)
    .filter(F.col("ID_Lote") == CICLO_REF)
)
cnt_total = df_fonte.count()

# Filtro 1: somente faturas que existem na principal (INCORRETAS)
TBL_PRINCIPAL = "accenture.faturas_principal_vero"
df_principal = spark.table(TBL_PRINCIPAL).select(
    F.col("FATURA").alias("_p_fat"),
    F.col("ID_CONTA").alias("_p_cta")
)

df_fonte = (
    df_fonte
    .join(
        df_principal,
        (df_fonte["FATURA"].cast(StringType()) == df_principal["_p_fat"]) &
        (df_fonte["ID_CONTA_CONTRATO"].cast(StringType()) == df_principal["_p_cta"]),
        how="inner"
    )
    .drop("_p_fat", "_p_cta")
)

# Filtro 2: somente INCORRETO com OBSERVACAO (que gera TAG)
df_com_tag = (
    df_fonte
    .filter(F.col("STATUS") == "INCORRETO")
    .filter(F.col("OBSERVACAO").isNotNull() & (F.trim(F.col("OBSERVACAO")) != ""))
)
cnt_tag = df_com_tag.count()
print(f"Fonte total: {cnt_total:,} | Na principal: {df_fonte.count():,} | Com TAG: {cnt_tag:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mapeamento 1:1 — 27 colunas
# MAGIC
# MAGIC | # | Destino                   | Fonte                   | Logica                              |
# MAGIC |---|---------------------------|-------------------------|-------------------------------------|
# MAGIC | 1 | FATURA                    | FATURA                  | direto                              |
# MAGIC | 2 | ID_CONTA                  | ID_CONTA_CONTRATO       | direto                              |
# MAGIC | 3 | ASSET                     | CRM                     | sistema origem                      |
# MAGIC | 4 | REGRA                     | REGRA                   | direto                              |
# MAGIC | 5 | STATUS                    | STATUS                  | direto (sempre INCORRETO)           |
# MAGIC | 6 | SUBSTATUS                 | SUBSTATUS               | direto                              |
# MAGIC | 7 | OBSERVACAO                | OBSERVACAO              | direto                              |
# MAGIC | 8 | DADOS_KENAN               | DADOS_BILLING           | renomear                            |
# MAGIC | 9 | DADOS_TABELA_VERDADE      | DADOS_TABELA_VERDADE    | direto                              |
# MAGIC |10 | ID_LOTE                   | ID_Lote                 | direto                              |
# MAGIC |11 | PRODUTO                   | SEGMENTO                | B2C/B2B                             |
# MAGIC |12 | COMPONENT_ID              | —                       | nulo                                |
# MAGIC |13 | TIPO_SERVICO              | Tipo_Servico            | direto                              |
# MAGIC |14 | ID_SERVICO                | —                       | nulo                                |
# MAGIC |15 | DESCRICAO_SERVICO         | Desc_Servico            | direto                              |
# MAGIC |16 | TIPO_IMPOSTO              | Tipo_Imposto            | direto                              |
# MAGIC |17 | PROMOCAO                  | Promocao                | direto                              |
# MAGIC |18 | GRUPO                     | Grupo_Localidade        | direto                              |
# MAGIC |19 | STATUS_VALIDACAO          | —                       | PENDENTE (todos sao INCORRETO)      |
# MAGIC |20 | TAG                       | OBSERVACAO              | texto antes do primeiro ":"         |
# MAGIC |21 | ANALISTA                  | —                       | nulo                                |
# MAGIC |22 | STATUS_RETORNO            | —                       | nulo                                |
# MAGIC |23 | CHAMADO                   | —                       | nulo                                |
# MAGIC |24 | RESUMO                    | REGRA + OBSERVACAO      | concatenado                         |
# MAGIC |25 | _FILTRA_PAGE_TAG          | REGRA + STATUS          | concatenado                         |
# MAGIC |26 | DATA_ABERTURA_CHAMADO     | —                       | current_date                        |
# MAGIC |27 | DT_EMISSAO                | —                       | current_date                        |

# COMMAND ----------

_null = F.lit(None).cast(StringType())

# Regras que usam Produto como TAG
_REGRAS_TAG_PRODUTO = ["VALOR_OFERTA", "DIVERGENCIA_CONTRATO_PRODUTO", "VALOR ZERADO", "VALOR FATURA"]

df_result = (
    df_com_tag
    .select(
        # 1. FATURA
        F.col("FATURA").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. ASSET
        F.coalesce(F.col("CRM"), F.lit("NAO_IDENTIFICADO")).alias("ASSET"),

        # 4. REGRA
        F.col("REGRA").cast(StringType()).alias("REGRA"),

        # 5. STATUS
        F.col("STATUS").cast(StringType()).alias("STATUS"),

        # 6. SUBSTATUS
        F.col("SUBSTATUS").cast(StringType()).alias("SUBSTATUS"),

        # 7. OBSERVACAO
        F.col("OBSERVACAO").cast(StringType()).alias("OBSERVACAO"),

        # 8. DADOS_KENAN — condicional por REGRA (com fallback)
        F.coalesce(
            F.when(F.col("REGRA") == "BATIMENTO_PRODUTOS_FATURA",
                   F.col("VALOR_CONTRATO").cast(StringType()))
             .when(F.col("REGRA").isin("VALOR FATURA", "VALOR ZERADO"),
                   F.col("VALOR_BILLING").cast(StringType()))
             .otherwise(F.col("DADOS_BILLING").cast(StringType())),
            F.col("DADOS_BILLING").cast(StringType()),
            F.col("VALOR_BILLING").cast(StringType()),
            F.col("VALOR_CONTRATO").cast(StringType())
        ).alias("DADOS_KENAN"),

        # 9. DADOS_TABELA_VERDADE — condicional por REGRA (com fallback)
        F.coalesce(
            F.when(F.col("REGRA") == "BATIMENTO_PRODUTOS_FATURA",
                   F.col("VALOR_TABELA_VERDADE").cast(StringType()))
             .when(F.col("REGRA").isin("VALOR FATURA", "VALOR ZERADO"),
                   F.col("VALOR_CONTRATO").cast(StringType()))
             .otherwise(F.col("DADOS_TABELA_VERDADE").cast(StringType())),
            F.col("DADOS_TABELA_VERDADE").cast(StringType()),
            F.col("VALOR_TABELA_VERDADE").cast(StringType()),
            F.col("VALOR_CONTRATO").cast(StringType())
        ).alias("DADOS_TABELA_VERDADE"),

        # 10. ID_LOTE
        F.col("ID_Lote").cast(StringType()).alias("ID_LOTE"),

        # 11. PRODUTO (= SEGMENTO)
        F.coalesce(F.col("SEGMENTO"), F.lit("NAO IDENTIFICADO")).alias("PRODUTO"),

        # 12. COMPONENT_ID — nulo
        _null.alias("COMPONENT_ID"),

        # 13. TIPO_SERVICO
        F.col("Tipo_Servico").cast(StringType()).alias("TIPO_SERVICO"),

        # 14. ID_SERVICO — nulo
        _null.alias("ID_SERVICO"),

        # 15. DESCRICAO_SERVICO
        F.col("Desc_Servico").cast(StringType()).alias("DESCRICAO_SERVICO"),

        # 16. TIPO_IMPOSTO
        F.col("Tipo_Imposto").cast(StringType()).alias("TIPO_IMPOSTO"),

        # 17. PROMOCAO
        F.col("Promocao").cast(StringType()).alias("PROMOCAO"),

        # 18. GRUPO
        F.col("Grupo_Localidade").cast(StringType()).alias("GRUPO"),

        # 19. STATUS_VALIDACAO — sempre PENDENTE (sao todos INCORRETO)
        F.lit("PENDENTE").alias("STATUS_VALIDACAO"),

        # 20. TAG — VALOR_OFERTA/DIVERGENCIA/ZERADO → Produto, senao → REGRA
        F.when(
            F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
            F.coalesce(F.col("Produto"), F.col("REGRA"))
        ).otherwise(F.col("REGRA"))
         .alias("TAG"),

        # 21. ANALISTA — nulo
        _null.alias("ANALISTA"),

        # 22. STATUS_RETORNO — nulo
        _null.alias("STATUS_RETORNO"),

        # 23. CHAMADO — nulo
        _null.alias("CHAMADO"),

        # 24. RESUMO — TAG | OBSERVACAO
        F.concat(
            F.when(F.col("REGRA").isin(_REGRAS_TAG_PRODUTO), F.coalesce(F.col("Produto"), F.col("REGRA"))).otherwise(F.col("REGRA")),
            F.lit(" | "),
            F.col("OBSERVACAO")
        ).alias("RESUMO"),

        # 25. _FILTRA_PAGE_TAG — REGRA_STATUS
        F.concat(F.col("REGRA"), F.lit("_"), F.col("STATUS")).alias("_FILTRA_PAGE_TAG"),

        # 26. DATA_ABERTURA_CHAMADO
        F.current_date().cast(DateType()).alias("DATA_ABERTURA_CHAMADO"),

        # 27. DT_EMISSAO
        F.current_date().cast(DateType()).alias("DT_EMISSAO"),
    )
)

# Remove duplicatas
df_result = df_result.dropDuplicates()

cnt = df_result.count()
print(f"Resultado: {cnt:,} linhas (somente com TAG)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravar

# COMMAND ----------

try:
    spark.sql(f"DELETE FROM {TBL_DESTINO} WHERE ID_LOTE = '{CICLO_REF}'")
except:
    pass

df_result.write.format("delta").mode("append").saveAsTable(TBL_DESTINO)

cnt_final = spark.table(TBL_DESTINO).count()
print(f"Gravado: {cnt_final:,} registros em {TBL_DESTINO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. QA

# COMMAND ----------

spark.sql(f"""
SELECT REGRA, STATUS, TAG, STATUS_VALIDACAO,
    COUNT(*) AS linhas,
    COUNT(DISTINCT FATURA) AS faturas
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}'
GROUP BY REGRA, STATUS, TAG, STATUS_VALIDACAO
ORDER BY linhas DESC
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT FATURA, ID_CONTA, ASSET, REGRA, TAG, OBSERVACAO, RESUMO, _FILTRA_PAGE_TAG
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}'
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sanidade

# COMMAND ----------

checks = []

# 1. Somente INCORRETO
c1 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND STATUS != 'INCORRETO'").collect()[0][0] == 0
checks.append(c1)
print(f"1. Somente INCORRETO: {'✅' if c1 else '❌'}")

# 2. TAG nunca nula
c2 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND (TAG IS NULL OR TAG = '')").collect()[0][0] == 0
checks.append(c2)
print(f"2. TAG sempre preenchida: {'✅' if c2 else '❌'}")

# 3. STATUS_VALIDACAO sempre PENDENTE
c3 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND STATUS_VALIDACAO != 'PENDENTE'").collect()[0][0] == 0
checks.append(c3)
print(f"3. STATUS_VALIDACAO=PENDENTE: {'✅' if c3 else '❌'}")

# 4. Schema 27 colunas
cols_ref = ["FATURA","ID_CONTA","ASSET","REGRA","STATUS","SUBSTATUS","OBSERVACAO",
            "DADOS_KENAN","DADOS_TABELA_VERDADE","ID_LOTE","PRODUTO","COMPONENT_ID",
            "TIPO_SERVICO","ID_SERVICO","DESCRICAO_SERVICO","TIPO_IMPOSTO","PROMOCAO",
            "GRUPO","STATUS_VALIDACAO","TAG","ANALISTA","STATUS_RETORNO","CHAMADO",
            "RESUMO","_FILTRA_PAGE_TAG","DATA_ABERTURA_CHAMADO","DT_EMISSAO"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c4 = cols_dst == cols_ref
checks.append(c4)
print(f"4. Schema 27 colunas: {'✅' if c4 else '❌'}")
if not c4:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")

# 5. ANALISTA nulo
c5 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND ANALISTA IS NOT NULL").collect()[0][0] == 0
checks.append(c5)
print(f"5. ANALISTA nulo: {'✅' if c5 else '❌'}")

# 6. Contagem compativel com fonte INCORRETA
n_inc_fonte = df_com_tag.count()
n_dst = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}'").collect()[0][0]
c6 = n_inc_fonte == n_dst
checks.append(c6)
print(f"6. Contagem: fonte_incorreta={n_inc_fonte:,} vs destino={n_dst:,} {'✅' if c6 else '❌'}")

print(f"\n{'='*60}")
print(f"{'✅ CARGA OK' if all(checks) else '⚠️ VER ISSUES'}")