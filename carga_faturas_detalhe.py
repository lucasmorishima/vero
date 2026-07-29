# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga detalhes_da_fatura_vero
# MAGIC **Fonte:** `accenture.validacao_status_fatura` (coracao do processo)
# MAGIC **Destino:** `accenture.detalhes_da_fatura_vero` (espelho 1:1, sem group by)
# MAGIC
# MAGIC ### Logica
# MAGIC - Cada linha da fonte gera exatamente 1 linha no destino
# MAGIC - Schema identico a `detalhes_da_fatura_v4` (28 colunas)
# MAGIC - Campos mapeados direto, campos incrementais auto, campos sem origem = nulo

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
TBL_DESTINO = "accenture.detalhes_da_fatura_vero"

print(f"Fonte: {TBL_FONTE} → Destino: {TBL_DESTINO} | Ciclo: {CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DDL — schema identico a detalhes_da_fatura_v4 (28 colunas)

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
    STATUS_RETORNO                  STRING,
    CHAMADO                         STRING,
    RESUMO                          STRING,
    _Ordem_Status_DET               INT,
    _Prioridade_Final_da_Fatura     INT,
    _FILTRA_PAGE                    STRING,
    DATA_ABERTURA_CHAMADO           DATE,
    DT_EMISSAO                      DATE
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")
print(f"DDL {TBL_DESTINO} OK — 28 colunas identicas a detalhes_da_fatura_v4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura da fonte

# COMMAND ----------

df_fonte = (
    spark.table(TBL_FONTE)
    .filter(F.col("ID_Lote") == CICLO_REF)
)
cnt_fonte = df_fonte.count()

# Filtro: somente faturas que existem na principal (INCORRETAS)
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

cnt_filtrado = df_fonte.count()
print(f"Fonte total: {cnt_fonte:,} | Apos filtro pela principal: {cnt_filtrado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mapeamento 1:1 — 28 colunas
# MAGIC
# MAGIC | # | Destino                       | Fonte                         | Logica                                  |
# MAGIC |---|-------------------------------|-------------------------------|-----------------------------------------|
# MAGIC | 1 | FATURA                        | FATURA                        | direto (string)                         |
# MAGIC | 2 | ID_CONTA                      | ID_CONTA_CONTRATO             | direto (string)                         |
# MAGIC | 3 | ASSET                         | CRM                           | sistema origem                          |
# MAGIC | 4 | REGRA                         | REGRA                         | direto                                  |
# MAGIC | 5 | STATUS                        | STATUS                        | direto                                  |
# MAGIC | 6 | SUBSTATUS                     | SUBSTATUS                     | direto                                  |
# MAGIC | 7 | OBSERVACAO                    | OBSERVACAO                    | direto                                  |
# MAGIC | 8 | DADOS_KENAN                   | DADOS_BILLING                 | renomear                                |
# MAGIC | 9 | DADOS_TABELA_VERDADE          | DADOS_TABELA_VERDADE          | direto                                  |
# MAGIC |10 | ID_LOTE                       | ID_Lote                       | direto                                  |
# MAGIC |11 | PRODUTO                       | SEGMENTO                      | B2C/B2B                                 |
# MAGIC |12 | COMPONENT_ID                  | —                             | nulo                                    |
# MAGIC |13 | TIPO_SERVICO                  | Tipo_Servico                  | direto                                  |
# MAGIC |14 | ID_SERVICO                    | —                             | nulo                                    |
# MAGIC |15 | DESCRICAO_SERVICO             | Desc_Servico                  | direto                                  |
# MAGIC |16 | TIPO_IMPOSTO                  | Tipo_Imposto                  | direto                                  |
# MAGIC |17 | PROMOCAO                      | Promocao                      | direto                                  |
# MAGIC |18 | GRUPO                         | Grupo_Localidade              | direto                                  |
# MAGIC |19 | STATUS_VALIDACAO              | —                             | PENDENTE se INCORRETO, VALIDADO se nao  |
# MAGIC |20 | TAG                           | —                             | extraido da OBSERVACAO (antes do :)     |
# MAGIC |21 | STATUS_RETORNO                | —                             | nulo                                    |
# MAGIC |22 | CHAMADO                       | —                             | nulo                                    |
# MAGIC |23 | RESUMO                        | —                             | REGRA + OBSERVACAO                      |
# MAGIC |24 | _Ordem_Status_DET             | —                             | 3=INCORRETO, 1=CORRETO                  |
# MAGIC |25 | _Prioridade_Final_da_Fatura   | —                             | 4=INCORRETO, 1=CORRETO                  |
# MAGIC |26 | _FILTRA_PAGE                  | —                             | REGRA_STATUS                            |
# MAGIC |27 | DATA_ABERTURA_CHAMADO         | —                             | current_date                            |
# MAGIC |28 | DT_EMISSAO                    | —                             | current_date                            |

# COMMAND ----------

_null = F.lit(None).cast(StringType())

# Regras que usam Produto como TAG
_REGRAS_TAG_PRODUTO = ["VALOR_OFERTA", "DIVERGENCIA_CONTRATO_PRODUTO", "VALOR ZERADO", "VALOR FATURA"]

df_result = (
    df_fonte
    .select(
        # 1. FATURA
        F.col("FATURA").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. ASSET (sistema origem)
        F.coalesce(F.col("CRM"), F.lit("NAO_IDENTIFICADO")).alias("ASSET"),

        # 4. REGRA
        F.col("REGRA").cast(StringType()).alias("REGRA"),

        # 5. STATUS
        F.col("STATUS").cast(StringType()).alias("STATUS"),

        # 6. SUBSTATUS
        F.col("SUBSTATUS").cast(StringType()).alias("SUBSTATUS"),

        # 7. OBSERVACAO
        F.col("OBSERVACAO").cast(StringType()).alias("OBSERVACAO"),

        # 8. DADOS_KENAN — condicional por REGRA (com fallback para nao ficar nulo)
        #    BATIMENTO_PRODUTOS_FATURA → VALOR_CONTRATO
        #    VALOR FATURA / VALOR ZERADO → VALOR_BILLING
        #    Demais → DADOS_BILLING
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
        #    BATIMENTO_PRODUTOS_FATURA → VALOR_TABELA_VERDADE
        #    VALOR FATURA / VALOR ZERADO → VALOR_CONTRATO
        #    Demais → DADOS_TABELA_VERDADE
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

        # 19. STATUS_VALIDACAO
        F.when(F.col("STATUS") == "INCORRETO", F.lit("PENDENTE"))
         .otherwise(F.lit("VALIDADO"))
         .alias("STATUS_VALIDACAO"),

        # 20. TAG — regra especial: VALOR_OFERTA/DIVERGENCIA/ZERADO → Produto, senao → REGRA
        F.when(
            F.col("STATUS") == "INCORRETO",
            F.when(
                F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
                F.coalesce(F.col("Produto"), F.col("REGRA"))
            ).otherwise(F.col("REGRA"))
        ).otherwise(_null)
         .alias("TAG"),

        # 21. STATUS_RETORNO — nulo
        _null.alias("STATUS_RETORNO"),

        # 22. CHAMADO — nulo
        _null.alias("CHAMADO"),

        # 23. RESUMO — TAG | OBSERVACAO
        F.when(
            F.col("OBSERVACAO").isNotNull() & (F.trim(F.col("OBSERVACAO")) != ""),
            F.concat(
                F.when(F.col("REGRA").isin(_REGRAS_TAG_PRODUTO), F.coalesce(F.col("Produto"), F.col("REGRA"))).otherwise(F.col("REGRA")),
                F.lit(" | "),
                F.col("OBSERVACAO")
            )
        ).otherwise(
            F.when(F.col("REGRA").isin(_REGRAS_TAG_PRODUTO), F.coalesce(F.col("Produto"), F.col("REGRA"))).otherwise(F.col("REGRA"))
        ).alias("RESUMO"),

        # 24. _Ordem_Status_DET
        F.when(F.col("STATUS") == "INCORRETO", F.lit(3))
         .otherwise(F.lit(1))
         .cast(IntegerType())
         .alias("_Ordem_Status_DET"),

        # 25. _Prioridade_Final_da_Fatura
        F.when(F.col("STATUS") == "INCORRETO", F.lit(4))
         .otherwise(F.lit(1))
         .cast(IntegerType())
         .alias("_Prioridade_Final_da_Fatura"),

        # 26. _FILTRA_PAGE — REGRA_STATUS
        F.concat(F.col("REGRA"), F.lit("_"), F.col("STATUS"))
         .alias("_FILTRA_PAGE"),

        # 27. DATA_ABERTURA_CHAMADO
        F.current_date().cast(DateType()).alias("DATA_ABERTURA_CHAMADO"),

        # 28. DT_EMISSAO
        F.current_date().cast(DateType()).alias("DT_EMISSAO"),
    )
)

# Remove duplicatas
df_result = df_result.dropDuplicates()

cnt = df_result.count()
print(f"Resultado: {cnt:,} linhas (espelho 1:1, esperado {cnt_fonte:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravar

# COMMAND ----------

# Limpa ciclo atual (idempotente)
try:
    spark.sql(f"DELETE FROM {TBL_DESTINO} WHERE ID_LOTE = '{CICLO_REF}'")
except:
    pass

df_result.write.format("delta").mode("append").saveAsTable(TBL_DESTINO)

cnt_final = spark.table(TBL_DESTINO).count()
print(f"Gravado: {cnt_final:,} registros em {TBL_DESTINO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. QA — Resumo

# COMMAND ----------

spark.sql(f"""
SELECT REGRA, STATUS, SUBSTATUS, STATUS_VALIDACAO,
    COUNT(*) AS linhas,
    COUNT(DISTINCT FATURA) AS faturas,
    COUNT(DISTINCT ID_CONTA) AS contas
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}'
GROUP BY REGRA, STATUS, SUBSTATUS, STATUS_VALIDACAO
ORDER BY REGRA, STATUS
""").show(truncate=False)

# COMMAND ----------

# Amostra incorretas
spark.sql(f"""
SELECT FATURA, ID_CONTA, ASSET, REGRA, STATUS, SUBSTATUS,
    OBSERVACAO, TAG, RESUMO, _FILTRA_PAGE
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}' AND STATUS = 'INCORRETO'
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# Amostra corretas
spark.sql(f"""
SELECT FATURA, ID_CONTA, ASSET, REGRA, STATUS, SUBSTATUS,
    STATUS_VALIDACAO, _Ordem_Status_DET, _FILTRA_PAGE
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}' AND STATUS = 'CORRETO'
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sanidade

# COMMAND ----------

checks = []

# 1. Contagem 1:1
n_src = df_fonte.count()
n_dst = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE = '{CICLO_REF}'").collect()[0][0]
c1 = n_src == n_dst
checks.append(c1)
print(f"1. Espelho 1:1: fonte={n_src:,} vs destino={n_dst:,} {'✅' if c1 else '❌'}")

# 2. Schema 28 colunas
cols_ref = ["FATURA","ID_CONTA","ASSET","REGRA","STATUS","SUBSTATUS","OBSERVACAO",
            "DADOS_KENAN","DADOS_TABELA_VERDADE","ID_LOTE","PRODUTO","COMPONENT_ID",
            "TIPO_SERVICO","ID_SERVICO","DESCRICAO_SERVICO","TIPO_IMPOSTO","PROMOCAO",
            "GRUPO","STATUS_VALIDACAO","TAG","STATUS_RETORNO","CHAMADO","RESUMO",
            "_Ordem_Status_DET","_Prioridade_Final_da_Fatura","_FILTRA_PAGE",
            "DATA_ABERTURA_CHAMADO","DT_EMISSAO"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c2 = cols_dst == cols_ref
checks.append(c2)
print(f"2. Schema 28 colunas: {'✅' if c2 else '❌'}")
if not c2:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")

# 3. STATUS_VALIDACAO consistente
c3 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}'
    AND ((STATUS='INCORRETO' AND STATUS_VALIDACAO!='PENDENTE')
      OR (STATUS='CORRETO' AND STATUS_VALIDACAO!='VALIDADO'))
""").collect()[0][0] == 0
checks.append(c3)
print(f"3. STATUS_VALIDACAO consistente: {'✅' if c3 else '❌'}")

# 4. INCORRETO tem TAG preenchida
c4 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}' AND STATUS='INCORRETO' AND (TAG IS NULL OR TAG='')
""").collect()[0][0] == 0
checks.append(c4)
print(f"4. INCORRETO com TAG: {'✅' if c4 else '❌'}")

# 5. _FILTRA_PAGE preenchido
c5 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}' AND (_FILTRA_PAGE IS NULL OR _FILTRA_PAGE='')
""").collect()[0][0] == 0
checks.append(c5)
print(f"5. _FILTRA_PAGE preenchido: {'✅' if c5 else '❌'}")

print(f"\n{'='*60}")
print(f"{'✅ CARGA OK' if all(checks) else '⚠️ VER ISSUES'}")