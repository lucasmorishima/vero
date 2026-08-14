# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga detalhes_da_fatura_vero
# MAGIC **Fonte:** `accenture.validacao_status_fatura` (coracao do processo)
# MAGIC **Destino:** `accenture.detalhes_da_fatura_vero` (espelho 1:1, sem group by)
# MAGIC
# MAGIC ### Logica
# MAGIC - Cada linha da fonte gera exatamente 1 linha no destino
# MAGIC - Schema v2: 23 colunas (SISTEMA, SEGMENTO, POSSUI_PREBILLING)
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
    SISTEMA                         STRING,
    REGRA                           STRING,
    STATUS                          STRING,
    SUBSTATUS                       STRING,
    OBSERVACAO                      STRING,
    DADOS_KENAN                     STRING,
    DADOS_TABELA_VERDADE            STRING,
    ID_LOTE                         STRING,
    SEGMENTO                        STRING,
    POSSUI_PREBILLING               STRING,
    TIPO_SERVICO                    STRING,
    DESCRICAO_SERVICO               STRING,
    TIPO_IMPOSTO                    STRING,
    STATUS_VALIDACAO                STRING,
    TAG                             STRING,
    RESUMO                          STRING,
    _Ordem_Status_DET               INT,
    _Prioridade_Final_da_Fatura     INT,
    _FILTRA_PAGE                    STRING,
    DATA_ABERTURA_CHAMADO           DATE,
    DT_EMISSAO                      DATE
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'        = 'name',
    'delta.minReaderVersion'          = '2',
    'delta.minWriterVersion'          = '5',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")
print(f"DDL {TBL_DESTINO} OK — 22 colunas (schema v2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura da fonte

# COMMAND ----------

df_fonte = (
    spark.table(TBL_FONTE)
    .filter(F.col("ID_Lote") == CICLO_REF)
)
cnt_fonte = df_fonte.count()
print(f"[DIAG 1] Fonte bruta (ID_Lote='{CICLO_REF}'): {cnt_fonte:,}")

# Distribuicao de ID_Lote na fonte — confirma que o ciclo esta correto
print("[DIAG 2] ID_Lote distintos na fonte:")
df_fonte.groupBy("ID_Lote").count().orderBy("ID_Lote").show(truncate=False)

# Distribuicao de REGRA na fonte — confirma quais regras existem no ciclo
print("[DIAG 3] REGRAs na fonte:")
df_fonte.groupBy("REGRA").count().orderBy("REGRA").show(truncate=False)

# Diagnostico especifico ENCARGOS
_enc = df_fonte.filter(F.col("REGRA") == "VALIDACAO_ENCARGOS_MULTA_JUROS")
print(f"[DIAG 3b] VALIDACAO_ENCARGOS_MULTA_JUROS na fonte: {_enc.count():,} linhas")
if _enc.count() > 0:
    _enc.groupBy("REGRA", "STATUS", "ID_Lote").count().show(truncate=False)

if cnt_fonte == 0:
    raise Exception(f"STOP: fonte vazia para ID_Lote='{CICLO_REF}' — verifique o ciclo")

# Filtro: somente contas que existem na principal (INCORRETAS)
# EXCECAO: VALIDACAO_NFCOM, VALIDACAO_IMPOSTOS e VALIDACAO_ENCARGOS_MULTA_JUROS
# carregam integralmente — geradas por pipelines proprios, nao passam pela principal.
TBL_PRINCIPAL = "accenture.faturas_principal_vero"
# Principal e tabela de estado corrente (full DELETE + reload a cada ciclo):
# le sem filtro de lote — sempre reflete o ciclo mais recente carregado.
df_principal = spark.table(TBL_PRINCIPAL).select(
    F.col("ID_CONTA").cast(StringType()).alias("_p_cta")
).dropDuplicates(["_p_cta"])

_REGRAS_DIRETAS = ["VALIDACAO_NFCOM", "VALIDACAO_IMPOSTOS", "VALIDACAO_ENCARGOS_MULTA_JUROS"]

# Split: regras diretas passam sem join; demais filtradas pelo principal
df_diretas = df_fonte.filter(F.col("REGRA").isin(_REGRAS_DIRETAS))
df_outras   = df_fonte.filter(~F.col("REGRA").isin(_REGRAS_DIRETAS))

cnt_diretas_pre = df_diretas.count()
print(f"[DIAG 4] Diretas (NFCOM/IMPOSTOS/ENCARGOS) antes do union: {cnt_diretas_pre:,}")
print("[DIAG 5] Diretas por REGRA+STATUS:")
df_diretas.groupBy("REGRA","STATUS").count().orderBy("REGRA","STATUS").show(truncate=False)

# Breakdown das diretas por REGRA — mostra se ENCARGOS tem dados na fonte
print("[DIAG 5b] Breakdown das diretas por REGRA+STATUS:")
df_diretas.groupBy("REGRA", "STATUS").count().orderBy("REGRA", "STATUS").show(truncate=False)

df_outras = (
    df_outras
    .join(
        df_principal,
        df_outras["ID_CONTA_CONTRATO"].cast(StringType()) == df_principal["_p_cta"],
        how="inner"
    )
    .drop("_p_cta")
)

# union por nome para evitar mistura de colunas por posicao
df_fonte = df_diretas.unionByName(df_outras)

cnt_filtrado  = df_fonte.count()
cnt_outras    = df_outras.count()
print(f"[DIAG 6] Apos union — NFCOM/IMPOSTOS: {cnt_diretas_pre:,} | Outras: {cnt_outras:,} | Total: {cnt_filtrado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mapeamento 1:1 — 23 colunas (schema v2)
# MAGIC
# MAGIC | # | Destino                       | Fonte                         | Logica                                  |
# MAGIC |---|-------------------------------|-------------------------------|-----------------------------------------|
# MAGIC | 1 | FATURA                        | FATURA                        | direto (string)                         |
# MAGIC | 2 | ID_CONTA                      | ID_CONTA_CONTRATO             | direto (string)                         |
# MAGIC | 3 | SISTEMA                       | CRM                           | sistema origem                          |
# MAGIC | 4 | REGRA                         | REGRA                         | direto                                  |
# MAGIC | 5 | STATUS                        | STATUS                        | direto                                  |
# MAGIC | 6 | SUBSTATUS                     | SUBSTATUS                     | direto                                  |
# MAGIC | 7 | OBSERVACAO                    | OBSERVACAO                    | direto                                  |
# MAGIC | 8 | DADOS_KENAN                   | DADOS_BILLING                 | renomear                                |
# MAGIC | 9 | DADOS_TABELA_VERDADE          | DADOS_TABELA_VERDADE          | direto                                  |
# MAGIC |10 | ID_LOTE                       | ID_Lote                       | direto                                  |
# MAGIC |11 | SEGMENTO                      | SEGMENTO                      | B2C/B2B (coalesce NAO IDENTIFICADO)     |
# MAGIC |12 | POSSUI_PREBILLING             | REGRA (qualquer status)       | SIM se contrato tem PRE BILLING/PRE-BILLING |
# MAGIC |13 | TIPO_SERVICO                  | Tipo_Servico                  | direto                                  |
# MAGIC |14 | DESCRICAO_SERVICO             | Desc_Servico                  | direto                                  |
# MAGIC |15 | TIPO_IMPOSTO                  | Tipo_Imposto                  | direto                                  |
# MAGIC |16 | STATUS_VALIDACAO              | —                             | PENDENTE se INCORRETO, VALIDADO se nao  |
# MAGIC |17 | TAG                           | —                             | extraido da OBSERVACAO (antes do :)     |
# MAGIC |18 | RESUMO                        | —                             | REGRA + OBSERVACAO                      |
# MAGIC |19 | _Ordem_Status_DET             | —                             | 3=INCORRETO, 1=CORRETO                  |
# MAGIC |20 | _Prioridade_Final_da_Fatura   | —                             | 4=INCORRETO, 1=CORRETO                  |
# MAGIC |21 | _FILTRA_PAGE                  | —                             | REGRA_STATUS                            |
# MAGIC |22 | DATA_ABERTURA_CHAMADO         | —                             | current_date                            |
# MAGIC |23 | DT_EMISSAO                    | —                             | current_date                            |

# COMMAND ----------

_null = F.lit(None).cast(StringType())

# ---------------------------------------------------------------------------
# Normalizacao do sistema (CRM → SISTEMA)
# SIMETRA(AN) e AN sao o mesmo sistema — padronizar para SIMETRA
# ---------------------------------------------------------------------------
def _norm_crm(col):
    return (
        F.when(F.upper(F.trim(col)).isin("AN", "SIMETRA(AN)"), F.lit("SIMETRA"))
         .otherwise(F.coalesce(F.trim(col), F.lit("NAO_IDENTIFICADO")))
    )

# Regras que usam o nome do Produto como TAG
_REGRAS_TAG_PRODUTO = [
    "VALOR_OFERTA",
    "VALOR FATURA",
    "VALOR ZERADO",
    "VALOR_ZERADO",
    "PRE BILLING",
    "PRE-BILLING",
    "DIVERGENCIA_CONTRATO_PRODUTO",
]

# Regras NFCom/Impostos: TAG extraida da OBSERVACAO (formato "TAG_NAME: descricao")
_REGRAS_TAG_OBS = ["VALIDACAO_NFCOM", "VALIDACAO_IMPOSTOS"]

# Regras que usam a propria REGRA como TAG (fallback .otherwise — listadas aqui so para documentacao):
#   GAP_FATURAMENTO, VALIDACAO_ENCARGOS_MULTA_JUROS, FATURAS_NAO_FATURAVEIS,
#   ENDERECO_LEGAL, DADOS_CADASTRAIS, ENDERECO_INSTALACAO

# Helper: extrai a TAG antes do primeiro ":" na OBSERVACAO
# Ex: "CFOP_INVALIDO: CFOP fora da lista..." → "CFOP_INVALIDO"
_tag_from_obs = F.trim(F.split(F.col("OBSERVACAO"), ":").getItem(0))

# ---------------------------------------------------------------------------
# POSSUI_PREBILLING: verifica se o contrato possui a regra PRE BILLING
# em qualquer linha do ciclo (qualquer status), entao propaga para todas
# as linhas do mesmo contrato via broadcast join.
# ---------------------------------------------------------------------------
_pb_accts = (
    df_fonte
    .filter(F.upper(F.trim(F.col("REGRA"))).isin("PRE-BILLING", "PRE BILLING"))
    .select(F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("_pb_cta"))
    .distinct()
)
df_fonte = (
    df_fonte
    .join(
        F.broadcast(_pb_accts),
        df_fonte["ID_CONTA_CONTRATO"].cast(StringType()) == _pb_accts["_pb_cta"],
        how="left"
    )
    .withColumn("_possui_pb",
        F.when(F.col("_pb_cta").isNotNull(), F.lit("SIM")).otherwise(F.lit("NAO"))
    )
    .drop("_pb_cta")
)

df_result = (
    df_fonte
    .select(
        # 1. FATURA
        F.col("FATURA").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. SISTEMA (sistema origem — normalizado: AN / SIMETRA(AN) → SIMETRA)
        _norm_crm(F.col("CRM")).alias("SISTEMA"),

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
        #    DIVERGENCIA_CONTRATO_PRODUTO → DADOS_CONTRATO (descricao billing do contrato)
        #    BATIMENTO_PRODUTOS_FATURA    → VALOR_CONTRATO
        #    VALOR FATURA / VALOR ZERADO  → VALOR_BILLING
        #    Demais                       → DADOS_BILLING
        F.coalesce(
            F.when(F.col("REGRA") == "DIVERGENCIA_CONTRATO_PRODUTO",
                   F.col("DADOS_CONTRATO").cast(StringType()))
             .when(F.col("REGRA") == "BATIMENTO_PRODUTOS_FATURA",
                   F.col("VALOR_CONTRATO").cast(StringType()))
             .when(F.col("REGRA").isin("VALOR FATURA", "VALOR ZERADO"),
                   F.col("VALOR_BILLING").cast(StringType()))
             .otherwise(F.col("DADOS_BILLING").cast(StringType())),
            F.col("DADOS_BILLING").cast(StringType()),
            F.col("VALOR_BILLING").cast(StringType()),
            F.col("VALOR_CONTRATO").cast(StringType())
        ).alias("DADOS_KENAN"),

        # 9. DADOS_TABELA_VERDADE — condicional por REGRA (com fallback)
        #    DIVERGENCIA_CONTRATO_PRODUTO → DADOS_TABELA_VERDADE direto (verdade do produto esperado)
        #    VALOR FATURA / VALOR ZERADO  → VALOR_CONTRATO
        #    Demais                       → DADOS_CONTRATO se preenchido, senao DADOS_TABELA_VERDADE
        F.when(
            F.col("REGRA") == "DIVERGENCIA_CONTRATO_PRODUTO",
            F.col("DADOS_TABELA_VERDADE").cast(StringType())
        ).when(
            F.col("REGRA").isin("VALOR FATURA", "VALOR ZERADO"),
            F.coalesce(
                F.col("VALOR_CONTRATO").cast(StringType()),
                F.col("DADOS_TABELA_VERDADE").cast(StringType())
            )
        ).otherwise(
            F.coalesce(
                F.when(
                    F.col("DADOS_CONTRATO").isNotNull() & (F.trim(F.col("DADOS_CONTRATO").cast(StringType())) != ""),
                    F.col("DADOS_CONTRATO").cast(StringType())
                ),
                F.col("DADOS_TABELA_VERDADE").cast(StringType())
            )
        ).alias("DADOS_TABELA_VERDADE"),

        # 10. ID_LOTE
        F.col("ID_Lote").cast(StringType()).alias("ID_LOTE"),

        # 11. SEGMENTO (origem: coluna SEGMENTO da fonte = B2C/B2B/etc.)
        F.coalesce(F.col("SEGMENTO"), F.lit("NAO IDENTIFICADO")).alias("SEGMENTO"),

        # 12. POSSUI_PREBILLING — SIM se o contrato tem PRE BILLING em qualquer linha do ciclo
        F.col("_possui_pb").alias("POSSUI_PREBILLING"),

        # 13. TIPO_SERVICO
        F.col("Tipo_Servico").cast(StringType()).alias("TIPO_SERVICO"),

        # 13. DESCRICAO_SERVICO
        F.col("Desc_Servico").cast(StringType()).alias("DESCRICAO_SERVICO"),

        # 14. TIPO_IMPOSTO
        F.col("Tipo_Imposto").cast(StringType()).alias("TIPO_IMPOSTO"),

        # 15. STATUS_VALIDACAO
        #     INCORRETO → PENDENTE | ALERTA → PENDENTE | CORRETO → VALIDADO
        F.when(F.col("STATUS").isin("INCORRETO", "ALERTA"), F.lit("PENDENTE"))
         .otherwise(F.lit("VALIDADO"))
         .alias("STATUS_VALIDACAO"),

        # 20. TAG — INCORRETO e ALERTA recebem TAG; CORRETO recebe null
        #     NFCOM/IMPOSTOS → extrai da OBSERVACAO (antes do ":")
        #     demais regras com Produto → Produto | restante → REGRA
        F.when(
            F.col("STATUS").isin("INCORRETO", "ALERTA"),
            F.when(
                F.col("REGRA").isin(_REGRAS_TAG_OBS),
                F.coalesce(_tag_from_obs, F.col("REGRA"))
            ).when(
                F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
                F.coalesce(F.col("Produto"), F.col("REGRA"))
            ).otherwise(F.col("REGRA"))
        ).otherwise(_null)
         .alias("TAG"),

        # 17. RESUMO — TAG | OBSERVACAO
        #     TAG calculada igual à coluna 20 (NFCOM/IMPOSTOS → obs, Produto → produto, resto → regra)
        F.when(
            F.col("OBSERVACAO").isNotNull() & (F.trim(F.col("OBSERVACAO")) != ""),
            F.concat(
                F.when(F.col("REGRA").isin(_REGRAS_TAG_OBS),
                       F.coalesce(_tag_from_obs, F.col("REGRA")))
                 .when(F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
                       F.coalesce(F.col("Produto"), F.col("REGRA")))
                 .otherwise(F.col("REGRA")),
                F.lit(" | "),
                F.col("OBSERVACAO")
            )
        ).otherwise(
            F.when(F.col("REGRA").isin(_REGRAS_TAG_OBS),
                   F.coalesce(_tag_from_obs, F.col("REGRA")))
             .when(F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
                   F.coalesce(F.col("Produto"), F.col("REGRA")))
             .otherwise(F.col("REGRA"))
        ).alias("RESUMO"),

        # 24. _Ordem_Status_DET  — INCORRETO=3, ALERTA=2, CORRETO=1
        F.when(F.col("STATUS") == "INCORRETO", F.lit(3))
         .when(F.col("STATUS") == "ALERTA",    F.lit(2))
         .otherwise(F.lit(1))
         .cast(IntegerType())
         .alias("_Ordem_Status_DET"),

        # 25. _Prioridade_Final_da_Fatura — INCORRETO=4, ALERTA=3, CORRETO=1
        F.when(F.col("STATUS") == "INCORRETO", F.lit(4))
         .when(F.col("STATUS") == "ALERTA",    F.lit(3))
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

# Remove duplicatas pela chave de negocio
# (exclui DATA_ABERTURA_CHAMADO/DT_EMISSAO que sao current_date e nao definem unicidade)
_CHAVE_DEDUP = [
    "FATURA", "ID_CONTA", "REGRA", "STATUS", "SUBSTATUS",
    "OBSERVACAO", "SEGMENTO", "TIPO_SERVICO", "DESCRICAO_SERVICO",
    "TIPO_IMPOSTO", "ID_LOTE",
]
df_result = df_result.dropDuplicates(_CHAVE_DEDUP)

cnt = df_result.count()
print(f"[DIAG 7] Apos select+dedup por chave negocio: {cnt:,} linhas")
print("[DIAG 8] NFCOM/IMPOSTOS no resultado apos select+dedup:")
df_result.filter(F.col("REGRA").isin("VALIDACAO_NFCOM","VALIDACAO_IMPOSTOS")) \
    .groupBy("REGRA","STATUS","SUBSTATUS").count().orderBy("REGRA","STATUS").show(truncate=False)
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

print(f"[DIAG 9] NFCOM/IMPOSTOS gravados em {TBL_DESTINO} (ID_LOTE='{CICLO_REF}'):")
spark.sql(f"""
    SELECT REGRA, STATUS, SUBSTATUS, COUNT(*) n, COUNT(DISTINCT FATURA) fat
    FROM {TBL_DESTINO}
    WHERE ID_LOTE = '{CICLO_REF}'
      AND REGRA IN ('VALIDACAO_NFCOM','VALIDACAO_IMPOSTOS')
    GROUP BY 1,2,3 ORDER BY 1,2
""").show(truncate=False)

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
SELECT FATURA, ID_CONTA, SISTEMA, SEGMENTO, REGRA, STATUS, SUBSTATUS,
    OBSERVACAO, TAG, RESUMO, _FILTRA_PAGE
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}' AND STATUS = 'INCORRETO'
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# Amostra corretas
spark.sql(f"""
SELECT FATURA, ID_CONTA, SISTEMA, SEGMENTO, REGRA, STATUS, SUBSTATUS,
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

# 2. Schema 23 colunas (v2)
cols_ref = ["FATURA","ID_CONTA","SISTEMA","REGRA","STATUS","SUBSTATUS","OBSERVACAO",
            "DADOS_KENAN","DADOS_TABELA_VERDADE","ID_LOTE","SEGMENTO","POSSUI_PREBILLING",
            "TIPO_SERVICO","DESCRICAO_SERVICO","TIPO_IMPOSTO",
            "STATUS_VALIDACAO","TAG","RESUMO",
            "_Ordem_Status_DET","_Prioridade_Final_da_Fatura","_FILTRA_PAGE",
            "DATA_ABERTURA_CHAMADO","DT_EMISSAO"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c2 = set(cols_ref).issubset(set(cols_dst))
checks.append(c2)
print(f"2. Schema 23 colunas (v2): {'✅' if c2 else '❌'}")
if not c2:
    faltando = set(cols_ref) - set(cols_dst)
    if faltando: print(f"   Faltando: {faltando}")
if not c2:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")

# 3. STATUS_VALIDACAO consistente
#    INCORRETO/ALERTA → PENDENTE | CORRETO → VALIDADO
c3 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}'
    AND ((STATUS IN ('INCORRETO','ALERTA') AND STATUS_VALIDACAO!='PENDENTE')
      OR (STATUS='CORRETO' AND STATUS_VALIDACAO!='VALIDADO'))
""").collect()[0][0] == 0
checks.append(c3)
print(f"3. STATUS_VALIDACAO consistente: {'✅' if c3 else '❌'}")

# 4. INCORRETO e ALERTA têm TAG preenchida
c4 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}' AND STATUS IN ('INCORRETO','ALERTA') AND (TAG IS NULL OR TAG='')
""").collect()[0][0] == 0
checks.append(c4)
print(f"4. INCORRETO/ALERTA com TAG: {'✅' if c4 else '❌'}")

# 5. _FILTRA_PAGE preenchido
c5 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}' AND (_FILTRA_PAGE IS NULL OR _FILTRA_PAGE='')
""").collect()[0][0] == 0
checks.append(c5)
print(f"5. _FILTRA_PAGE preenchido: {'✅' if c5 else '❌'}")

print(f"\n{'='*60}")
print(f"{'✅ CARGA OK' if all(checks) else '⚠️ VER ISSUES'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Correcao retroativa de TAG — regras que usam REGRA como TAG
# MAGIC
# MAGIC Registros carregados antes da padronizacao da lista `_REGRAS_TAG_PRODUTO` podem ter
# MAGIC TAG = nome do produto em vez do nome da regra. Este bloco corrige **toda a tabela**
# MAGIC (todos os lotes) para as regras que devem usar a propria REGRA como TAG.

# COMMAND ----------

# Regras que devem ter TAG = REGRA (mesmo nome da regra), nao nome do produto
_REGRAS_TAG_IGUAL_REGRA = [
    "GAP_FATURAMENTO",
    "ENDERECO_INSTALACAO",
    "ENDERECO_LEGAL",
    "DADOS_CADASTRAIS",
    "FATURAS_NAO_FATURAVEIS",
    "VALIDACAO_ENCARGOS_MULTA_JUROS",
]

_lista_sql = ", ".join(f"'{r}'" for r in _REGRAS_TAG_IGUAL_REGRA)

# Contagem antes
n_erradas = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE REGRA IN ({_lista_sql})
      AND STATUS IN ('INCORRETO', 'ALERTA')
      AND (TAG != REGRA OR TAG IS NULL)
""").collect()[0][0]
print(f"[RETRO] Registros com TAG incorreta antes da correcao: {n_erradas:,}")

if n_erradas > 0:
    spark.sql(f"""
        UPDATE {TBL_DESTINO}
        SET
            TAG    = REGRA,
            RESUMO = CASE
                         WHEN OBSERVACAO IS NOT NULL AND TRIM(OBSERVACAO) != ''
                         THEN CONCAT(REGRA, ' | ', OBSERVACAO)
                         ELSE REGRA
                     END
        WHERE REGRA IN ({_lista_sql})
          AND STATUS IN ('INCORRETO', 'ALERTA')
          AND (TAG != REGRA OR TAG IS NULL)
    """)
    print(f"[RETRO] {n_erradas:,} registros corrigidos (TAG = REGRA) em {TBL_DESTINO}")
else:
    print("[RETRO] Nenhum registro com TAG incorreta — tabela ja consistente ✅")

# Verificacao pos-correcao
n_restantes = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE REGRA IN ({_lista_sql})
      AND STATUS IN ('INCORRETO', 'ALERTA')
      AND (TAG != REGRA OR TAG IS NULL)
""").collect()[0][0]
print(f"[RETRO] Registros com TAG incorreta apos correcao: {n_restantes:,} {'✅' if n_restantes == 0 else '❌'}")