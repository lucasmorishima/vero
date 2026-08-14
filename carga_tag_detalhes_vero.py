# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Carga tag_detalhes_vero
# MAGIC **Fonte:** `accenture.validacao_status_fatura`
# MAGIC **Destino:** `accenture.tag_detalhes_vero` (espelho 1:1, somente linhas com TAG)
# MAGIC
# MAGIC ### Logica
# MAGIC - Espelho da fonte, mesma logica de detalhes_da_fatura_vero
# MAGIC - **Filtro:** somente carrega linhas que possuem TAG (STATUS = INCORRETO com observacao)
# MAGIC - Schema v2: 22 colunas (SISTEMA, SEGMENTO, POSSUI_PREBILLING)

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
    ANALISTA                        STRING,
    RESUMO                          STRING,
    _FILTRA_PAGE_TAG                STRING,
    DATA_ABERTURA_CHAMADO           DATE,
    DT_EMISSAO                      DATE
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")
print(f"DDL {TBL_DESTINO} OK — 22 colunas (schema v2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura da fonte — somente linhas INCORRETAS (com TAG)

# COMMAND ----------

df_fonte = (
    spark.table(TBL_FONTE)
    .filter(F.col("ID_Lote") == CICLO_REF)
)
cnt_total = df_fonte.count()
print(f"[DIAG 1] Fonte bruta (ID_Lote='{CICLO_REF}'): {cnt_total:,}")

# Principal e tabela de estado corrente (full DELETE + reload a cada ciclo):
# le sem filtro de lote — sempre reflete o ciclo mais recente carregado.
# (NFCOM/IMPOSTOS/ENCARGOS bypassam esse join via _REGRAS_DIRETAS)
TBL_PRINCIPAL = "accenture.faturas_principal_vero"
df_principal = spark.table(TBL_PRINCIPAL).select(
    F.col("ID_CONTA").cast(StringType()).alias("_p_cta")
).dropDuplicates(["_p_cta"])

# Regras diretas: carregam integralmente sem filtro pela principal
# — geradas por pipelines proprios; garante alinhamento com detalhes_da_fatura_vero para JOIN.
_REGRAS_DIRETAS = ["VALIDACAO_NFCOM", "VALIDACAO_IMPOSTOS", "VALIDACAO_ENCARGOS_MULTA_JUROS"]

df_diretas = df_fonte.filter(F.col("REGRA").isin(_REGRAS_DIRETAS))
df_outras   = df_fonte.filter(~F.col("REGRA").isin(_REGRAS_DIRETAS))

df_outras = (
    df_outras
    .join(
        df_principal,
        df_outras["ID_CONTA_CONTRATO"].cast(StringType()) == df_principal["_p_cta"],
        how="inner"
    )
    .drop("_p_cta")
)

# Union por nome (evita mistura de colunas por posicao)
df_fonte = df_diretas.unionByName(df_outras)

# Filtro final: INCORRETO e ALERTA — TAG sempre preenchida (Produto, OBSERVACAO ou propria REGRA)
df_com_tag  = df_fonte.filter(F.col("STATUS").isin("INCORRETO", "ALERTA"))
cnt_tag     = df_com_tag.count()
cnt_dir_tag = df_diretas.filter(F.col("STATUS").isin("INCORRETO", "ALERTA")).count()
print(f"[DIAG 2] Diretas (NFCOM/IMPOSTOS/ENCARGOS) INCORRETO/ALERTA: {cnt_dir_tag:,} | Demais: {cnt_tag - cnt_dir_tag:,} | Total: {cnt_tag:,}")

# Breakdown das diretas por REGRA — mostra se ENCARGOS tem dados na fonte
print("[DIAG 2b] Breakdown das diretas por REGRA+STATUS:")
df_diretas.groupBy("REGRA", "STATUS").count().orderBy("REGRA", "STATUS").show(truncate=False)

# Breakdown das demais por REGRA — mostra todas as regras que passaram pelo join
print("[DIAG 2c] Breakdown das demais (via principal) por REGRA:")
df_outras.filter(F.col("STATUS").isin("INCORRETO", "ALERTA")) \
    .groupBy("REGRA").count().orderBy("REGRA").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mapeamento 1:1 — 22 colunas (schema v2)
# MAGIC
# MAGIC | # | Destino                   | Fonte                   | Logica                              |
# MAGIC |---|---------------------------|-------------------------|-------------------------------------|
# MAGIC | 1 | FATURA                    | FATURA                  | direto                              |
# MAGIC | 2 | ID_CONTA                  | ID_CONTA_CONTRATO       | direto                              |
# MAGIC | 3 | SISTEMA                   | CRM                     | sistema origem                      |
# MAGIC | 4 | REGRA                     | REGRA                   | direto                              |
# MAGIC | 5 | STATUS                    | STATUS                  | direto (INCORRETO ou ALERTA)        |
# MAGIC | 6 | SUBSTATUS                 | SUBSTATUS               | direto                              |
# MAGIC | 7 | OBSERVACAO                | OBSERVACAO              | direto                              |
# MAGIC | 8 | DADOS_KENAN               | DADOS_BILLING           | renomear                            |
# MAGIC | 9 | DADOS_TABELA_VERDADE      | DADOS_TABELA_VERDADE    | direto                              |
# MAGIC |10 | ID_LOTE                   | ID_Lote                 | direto                              |
# MAGIC |11 | SEGMENTO                  | SEGMENTO                | B2C/B2B (coalesce NAO IDENTIFICADO) |
# MAGIC |12 | POSSUI_PREBILLING         | REGRA (qualquer status) | SIM se contrato tem PRE BILLING/PRE-BILLING |
# MAGIC |13 | TIPO_SERVICO              | Tipo_Servico            | direto                              |
# MAGIC |14 | DESCRICAO_SERVICO         | Desc_Servico            | direto                              |
# MAGIC |15 | TIPO_IMPOSTO              | Tipo_Imposto            | direto                              |
# MAGIC |16 | STATUS_VALIDACAO          | —                       | PENDENTE (INCORRETO ou ALERTA)      |
# MAGIC |17 | TAG                       | OBSERVACAO / Produto / REGRA | NFCOM/IMPOSTOS→obs; Produto→produto; demais→REGRA |
# MAGIC |18 | ANALISTA                  | —                       | nulo                                |
# MAGIC |19 | RESUMO                    | REGRA + OBSERVACAO      | concatenado                         |
# MAGIC |20 | _FILTRA_PAGE_TAG          | REGRA + STATUS          | concatenado                         |
# MAGIC |21 | DATA_ABERTURA_CHAMADO     | —                       | current_date                        |
# MAGIC |22 | DT_EMISSAO                | —                       | current_date                        |

# COMMAND ----------

_null = F.lit(None).cast(StringType())

# Regras que usam o nome do Produto como TAG  (sincronizado com carga_faturas_detalhe_vero)
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
_tag_from_obs = F.trim(F.split(F.col("OBSERVACAO"), ":").getItem(0))

# ---------------------------------------------------------------------------
# POSSUI_PREBILLING: contratos que possuem a regra PRE BILLING no ciclo
# (qualquer status) — broadcast join para propagar a todos as linhas do contrato.
# ---------------------------------------------------------------------------
_pb_accts = (
    df_fonte
    .filter(F.upper(F.trim(F.col("REGRA"))).isin("PRE-BILLING", "PRE BILLING"))
    .select(F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("_pb_cta"))
    .distinct()
)
df_com_tag = (
    df_com_tag
    .join(
        F.broadcast(_pb_accts),
        df_com_tag["ID_CONTA_CONTRATO"].cast(StringType()) == _pb_accts["_pb_cta"],
        how="left"
    )
    .withColumn("_possui_pb",
        F.when(F.col("_pb_cta").isNotNull(), F.lit("SIM")).otherwise(F.lit("NAO"))
    )
    .drop("_pb_cta")
)

df_result = (
    df_com_tag
    .select(
        # 1. FATURA
        F.col("FATURA").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. SISTEMA
        F.coalesce(F.col("CRM"), F.lit("NAO_IDENTIFICADO")).alias("SISTEMA"),

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
        #    DIVERGENCIA_CONTRATO_PRODUTO / VALOR FATURA / VALOR ZERADO → logica especifica
        #    Demais → DADOS_CONTRATO se preenchido, senao DADOS_TABELA_VERDADE
        F.when(
            F.col("REGRA").isin("DIVERGENCIA_CONTRATO_PRODUTO", "VALOR FATURA", "VALOR ZERADO"),
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

        # 11. SEGMENTO
        F.coalesce(F.col("SEGMENTO"), F.lit("NAO IDENTIFICADO")).alias("SEGMENTO"),

        # 12. POSSUI_PREBILLING — SIM se o contrato tem PRE BILLING em qualquer linha do ciclo
        F.col("_possui_pb").alias("POSSUI_PREBILLING"),

        # 13. TIPO_SERVICO
        F.col("Tipo_Servico").cast(StringType()).alias("TIPO_SERVICO"),

        # 13. DESCRICAO_SERVICO
        F.col("Desc_Servico").cast(StringType()).alias("DESCRICAO_SERVICO"),

        # 14. TIPO_IMPOSTO
        F.col("Tipo_Imposto").cast(StringType()).alias("TIPO_IMPOSTO"),

        # 15. STATUS_VALIDACAO — INCORRETO e ALERTA → PENDENTE
        F.when(F.col("STATUS").isin("INCORRETO", "ALERTA"), F.lit("PENDENTE"))
         .otherwise(F.lit("VALIDADO"))
         .alias("STATUS_VALIDACAO"),

        # 20. TAG — NFCOM/IMPOSTOS → extrai da OBSERVACAO (antes do ":")
        #          regras de produto → nome do Produto
        #          demais regras    → propria REGRA
        F.when(
            F.col("REGRA").isin(_REGRAS_TAG_OBS),
            F.coalesce(_tag_from_obs, F.col("REGRA"))
        ).when(
            F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
            F.coalesce(F.col("Produto"), F.col("REGRA"))
        ).otherwise(F.col("REGRA"))
         .alias("TAG"),

        # 17. ANALISTA — nulo
        _null.alias("ANALISTA"),

        # 18. RESUMO — TAG | OBSERVACAO  (OBSERVACAO pode ser nula para regras sem detalhe)
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

        # 19. _FILTRA_PAGE_TAG — REGRA_STATUS
        F.concat(F.col("REGRA"), F.lit("_"), F.col("STATUS")).alias("_FILTRA_PAGE_TAG"),

        # 20. DATA_ABERTURA_CHAMADO
        F.current_date().cast(DateType()).alias("DATA_ABERTURA_CHAMADO"),

        # 21. DT_EMISSAO
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
print(f"[DIAG 3] Apos select+dedup por chave negocio: {cnt:,} linhas")

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
SELECT FATURA, ID_CONTA, SISTEMA, REGRA, TAG, OBSERVACAO, RESUMO, _FILTRA_PAGE_TAG
FROM {TBL_DESTINO}
WHERE ID_LOTE = '{CICLO_REF}'
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sanidade

# COMMAND ----------

checks = []

# 1. Somente INCORRETO ou ALERTA
c1 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND STATUS NOT IN ('INCORRETO','ALERTA')").collect()[0][0] == 0
checks.append(c1)
print(f"1. Somente INCORRETO/ALERTA: {'✅' if c1 else '❌'}")

# 2. TAG nunca nula
c2 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND (TAG IS NULL OR TAG = '')").collect()[0][0] == 0
checks.append(c2)
print(f"2. TAG sempre preenchida: {'✅' if c2 else '❌'}")

# 3. STATUS_VALIDACAO consistente (INCORRETO/ALERTA → PENDENTE)
c3 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE ID_LOTE='{CICLO_REF}'
      AND STATUS IN ('INCORRETO','ALERTA') AND STATUS_VALIDACAO != 'PENDENTE'
""").collect()[0][0] == 0
checks.append(c3)
print(f"3. STATUS_VALIDACAO=PENDENTE para INCORRETO/ALERTA: {'✅' if c3 else '❌'}")

# 4. Schema 22 colunas (schema v2: SISTEMA, SEGMENTO, POSSUI_PREBILLING)
cols_ref = ["FATURA","ID_CONTA","SISTEMA","REGRA","STATUS","SUBSTATUS","OBSERVACAO",
            "DADOS_KENAN","DADOS_TABELA_VERDADE","ID_LOTE","SEGMENTO","POSSUI_PREBILLING",
            "TIPO_SERVICO","DESCRICAO_SERVICO","TIPO_IMPOSTO",
            "STATUS_VALIDACAO","TAG","ANALISTA",
            "RESUMO","_FILTRA_PAGE_TAG","DATA_ABERTURA_CHAMADO","DT_EMISSAO"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c4 = set(cols_ref).issubset(set(cols_dst))
checks.append(c4)
print(f"4. Schema 22 colunas (v2): {'✅' if c4 else '❌'}")
if not c4:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")
    faltando = set(cols_ref) - set(cols_dst)
    if faltando: print(f"   Faltando: {faltando}")

# 5. ANALISTA nulo
c5 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}' AND ANALISTA IS NOT NULL").collect()[0][0] == 0
checks.append(c5)
print(f"5. ANALISTA nulo: {'✅' if c5 else '❌'}")

# 6. Contagem compativel com fonte INCORRETO+ALERTA
n_inc_fonte = df_com_tag.count()
n_dst = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ID_LOTE='{CICLO_REF}'").collect()[0][0]
c6 = n_inc_fonte == n_dst
checks.append(c6)
print(f"6. Contagem: fonte_INCORRETO/ALERTA={n_inc_fonte:,} vs destino={n_dst:,} {'✅' if c6 else '❌'}")

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