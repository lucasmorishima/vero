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

TBL_FONTE    = "accenture.validacao_status_fatura"
TBL_CLIENTES = "accenture.base_clientes_centralizada"
TBL_DESTINO  = "accenture.faturas_principal_vero"

print(f"Fonte: {TBL_FONTE} | Clientes: {TBL_CLIENTES} → Destino: {TBL_DESTINO} | Ciclo: {CICLO_REF}")

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
    NOME_CLIENTE            STRING,
    CPF_CNPJ                STRING,
    ASSET                   STRING,
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
for _col_ddl in ["ASSET STRING", "NOME_CLIENTE STRING", "CPF_CNPJ STRING"]:
    try:
        spark.sql(f"ALTER TABLE {TBL_DESTINO} ADD COLUMNS ({_col_ddl})")
    except Exception:
        pass  # coluna ja existe
print(f"DDL {TBL_DESTINO} OK — 19 colunas")

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

_cli_raw = (
    spark.table(TBL_CLIENTES)
    .filter(F.col("nome").isNotNull() & (F.trim(F.col("nome")) != ""))
)

# Lookup por IDCONTA
df_cli_conta = (
    _cli_raw
    .select(
        F.col("IDCONTA").cast(StringType()).alias("_k"),
        F.col("nome").cast(StringType()).alias("NOME_CLIENTE"),
        F.col("CPF_CNPJ").cast(StringType()).alias("CPF_CNPJ"),
    )
    .dropDuplicates(["_k"])
)

# Lookup por IDCONTRATO (fallback 1)
df_cli_contrato = (
    _cli_raw
    .select(
        F.col("IDCONTRATO").cast(StringType()).alias("_k2"),
        F.col("nome").cast(StringType()).alias("NOME_CLIENTE_2"),
        F.col("CPF_CNPJ").cast(StringType()).alias("CPF_CNPJ_2"),
    )
    .dropDuplicates(["_k2"])
)

# Lookup por CODIGOCLIENTE (fallback 2)
df_cli_codigo = (
    _cli_raw
    .select(
        F.col("CODIGOCLIENTE").cast(StringType()).alias("_k3"),
        F.col("nome").cast(StringType()).alias("NOME_CLIENTE_3"),
        F.col("CPF_CNPJ").cast(StringType()).alias("CPF_CNPJ_3"),
    )
    .dropDuplicates(["_k3"])
)

print(f"Clientes por IDCONTA: {df_cli_conta.count():,} | por IDCONTRATO: {df_cli_contrato.count():,} | por CODIGOCLIENTE: {df_cli_codigo.count():,}")

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
# MAGIC | 5 | ANALISTA             | "PRE-BILLING" se essa regra esta incorreta, senao null        |
# MAGIC | 6 | OBSERVACAO           | null                                                         |
# MAGIC | 7 | PROBLEMA             | Concatenacao de REGRA das linhas INCORRETAS (pipe-separated)  |
# MAGIC | 8 | VALOR                | SUM(VALOR_BILLING), 0 se nulo                                |
# MAGIC | 9 | CRIADO_EM            | current_timestamp (string ISO)                                |
# MAGIC |10 | STATUS_RETORNO       | null                                                         |
# MAGIC |11 | CHAMADO              | null                                                         |
# MAGIC |12 | RESUMO               | Concatenacao TAG: obs | SEVERIDADE: substatus                 |
# MAGIC |13 | Ordem_Status         | soma ponderada regras distintas incorretas (PRE-BILLING=2, demais=1) |
# MAGIC |14 | DATA_ABERTURA_CHAMADO| current_date                                                  |
# MAGIC |15 | DT_EMISSAO           | MIN(DT_EMISSAO) do grupo — ou current_date se nulo           |
# MAGIC |16 | Valor_Positive       | SIM se VALOR > 0, NAO caso contrario                         |

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5a. Preparar colunas auxiliares antes do groupBy
# ---------------------------------------------------------------------------

# Colunas opcionais — podem nao existir em versoes antigas da fonte
_cols_fonte  = df_fonte.columns
_produto_col = F.col("Produto") if "Produto" in _cols_fonte else F.lit(None).cast(StringType())
print(f"[INFO] CRM na fonte: {'sim' if 'CRM' in _cols_fonte else 'nao'}  |  Produto: {'sim' if 'Produto' in _cols_fonte else 'nao'}")

# ── listas de TAG alinhadas com carga_detalhes ──────────────────────────────
# Regras NFCom/Impostos: TAG extraida da OBSERVACAO (formato "NOME_TAG: descricao")
_REGRAS_TAG_OBS = ["VALIDACAO_NFCOM", "VALIDACAO_IMPOSTOS"]

# Regras cujo TAG é o Produto (em vez da REGRA literal)
_REGRAS_TAG_PRODUTO = [
    "ENDERECO_INSTALACAO", "VALOR FATURA", "DIVERGENCIA_CONTRATO_PRODUTO",
    "VALOR ZERADO", "ENDERECO_LEGAL", "GAP_FATURAMENTO", "PRE BILLING",
    "DADOS_CADASTRAIS", "FATURAS_NAO_FATURAVEIS", "VALOR_OFERTA",
]

# Helper: extrai TAG antes do primeiro ":" na OBSERVACAO
# Ex: "FUST_INCORRETO: FUST diverge..." → "FUST_INCORRETO"
_tag_from_obs = F.trim(F.split(F.col("OBSERVACAO"), ":").getItem(0))

df_prep = (
    df_fonte
    # ── 1. Normalizar CRM → _crm_norm (ANTES de qualquer agregacao) ──────────
    # AN e SIMETRA(AN) sao o mesmo sistema — padronizar para SIMETRA
    .withColumn("_crm_norm",
        F.when(
            F.col("CRM").isNotNull() &
            F.upper(F.trim(F.col("CRM"))).isin("AN", "SIMETRA(AN)"),
            F.lit("SIMETRA")
        ).otherwise(
            F.when(F.col("CRM").isNotNull(), F.trim(F.col("CRM")))
        )
        if "CRM" in _cols_fonte else F.lit(None).cast(StringType())
    )
    # TAG: NFCOM/IMPOSTOS → extrai da OBSERVACAO | Produto-rules → Produto | resto → REGRA
    .withColumn("_tag",
        F.when(
            F.col("REGRA").isin(_REGRAS_TAG_OBS),
            F.coalesce(_tag_from_obs, F.col("REGRA"))
        ).when(
            F.col("REGRA").isin(_REGRAS_TAG_PRODUTO),
            F.coalesce(_produto_col, F.col("REGRA"))
        ).otherwise(F.col("REGRA"))
    )
    # Coluna com REGRA quando INCORRETO ou ALERTA (para concatenar no PROBLEMA)
    .withColumn("_regra_inc",
        F.when(F.col("STATUS").isin("INCORRETO", "ALERTA"), F.col("REGRA"))
    )
    # Coluna com TAG: OBSERVACAO quando INCORRETO ou ALERTA
    .withColumn("_obs_inc",
        F.when(
            F.col("STATUS").isin("INCORRETO", "ALERTA") &
            F.col("OBSERVACAO").isNotNull() & (F.trim(F.col("OBSERVACAO")) != ""),
            F.concat(F.col("_tag"), F.lit(": "), F.col("OBSERVACAO"))
        ).when(
            F.col("STATUS").isin("INCORRETO", "ALERTA"),
            F.col("_tag")
        )
    )
)

# ---------------------------------------------------------------------------
# 5b. Agregar TUDO em um unico groupBy (sem JOIN — evita problema NaN)
# ---------------------------------------------------------------------------

df_agg = (
    df_prep
    .groupBy("ID_CONTA_CONTRATO")
    .agg(
        # Flag: tem ao menos 1 INCORRETO?
        F.max(
            F.when(F.col("STATUS") == "INCORRETO", F.lit(1)).otherwise(F.lit(0))
        ).alias("_tem_incorreto"),

        # Flag: tem ao menos 1 ALERTA (sem INCORRETO)?
        F.max(
            F.when(F.col("STATUS") == "ALERTA", F.lit(1)).otherwise(F.lit(0))
        ).alias("_tem_alerta"),

        # PROBLEMA: regras distintas das linhas INCORRETAS
        F.concat_ws(" | ", F.collect_set(F.col("_regra_inc"))).alias("_PROBLEMA"),

        # OBSERVACAO: todas as obs das linhas INCORRETAS
        F.concat_ws(" | ", F.collect_list(F.col("_obs_inc"))).alias("_OBSERVACAO"),

        # RESUMO: idem OBSERVACAO
        F.concat_ws(" | ", F.collect_list(F.col("_obs_inc"))).alias("_RESUMO"),

        # Faturas distintas associadas a esta conta
        F.concat_ws(" | ", F.collect_set(F.col("FATURA"))).alias("_FATURAS"),

        # Sistema da conta — sempre 1 valor
        # _crm_norm já foi normalizado por withColumn (AN/SIMETRA(AN)→SIMETRA)
        # max() garante valor único e determinístico, nunca concatena
        F.max(F.col("_crm_norm")).alias("_SISTEMAS"),

        # Flag: tem regra PRE-BILLING incorreta? (para campo ANALISTA)
        F.max(
            F.when(
                (F.col("STATUS") == "INCORRETO") & (F.upper(F.trim(F.col("REGRA"))) == F.lit("PRE-BILLING")),
                F.lit(1)
            ).otherwise(F.lit(0))
        ).alias("_tem_prebilling"),

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
# 5b-bis. Ordem_Status: total de regras distintas por contrato (todas, nao so incorretas)
# PRE-BILLING = 2, demais = 1. Deduplicar (ID_CONTA_CONTRATO, REGRA) antes de somar.
# ---------------------------------------------------------------------------

_pesos = (
    df_prep
    .select("ID_CONTA_CONTRATO", "REGRA")
    .dropDuplicates(["ID_CONTA_CONTRATO", "REGRA"])
    .withColumn("_peso",
        F.when(F.upper(F.trim(F.col("REGRA"))) == F.lit("PRE-BILLING"), F.lit(2))
         .otherwise(F.lit(1)))
    .groupBy("ID_CONTA_CONTRATO")
    .agg(F.sum("_peso").cast(IntegerType()).alias("_ordem_status"))
)

df_agg = df_agg.join(_pesos, on="ID_CONTA_CONTRATO", how="left")

# ---------------------------------------------------------------------------
# 5c. Montar as 16 colunas exatas
# ---------------------------------------------------------------------------

df_result = (
    df_agg
    .select(
        # 1. FATURA — faturas distintas da conta (pode ser mais de uma)
        F.col("_FATURAS").cast(StringType()).alias("FATURA"),

        # 2. ID_CONTA (STRING)
        F.col("ID_CONTA_CONTRATO").cast(StringType()).alias("ID_CONTA"),

        # 3. ASSET — sistemas (CRM) distintos da conta
        F.col("_SISTEMAS").cast(StringType()).alias("ASSET"),

        # 5. STATUS — alinhado com detalhe: INCORRETO > ALERTA > CORRETO
        F.when(F.col("_tem_incorreto") == 1, F.lit("INCORRETO"))
         .when(F.col("_tem_alerta")    == 1, F.lit("ALERTA"))
         .otherwise(F.lit("CORRETO"))
         .alias("STATUS"),

        # 4. STATUS_VALIDACAO — INCORRETO ou ALERTA → PENDENTE
        F.when(
            (F.col("_tem_incorreto") == 1) | (F.col("_tem_alerta") == 1),
            F.lit("PENDENTE")
        ).otherwise(F.lit("VALIDADO"))
         .alias("STATUS_VALIDACAO"),

        # 5. ANALISTA — "PRE-BILLING" se essa regra estiver incorreta, senao nulo
        F.when(F.col("_tem_prebilling") == 1, F.lit("PRE-BILLING"))
         .otherwise(F.lit(None).cast(StringType()))
         .alias("ANALISTA"),

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

        # 13. Ordem_Status — soma ponderada de regras distintas incorretas
        # PRE-BILLING=2, demais=1; 0 se nenhuma regra incorreta
        F.coalesce(F.col("_ordem_status"), F.lit(0)).cast(IntegerType())
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

# Remove duplicatas por ID_CONTA
df_result = df_result.dropDuplicates(["ID_CONTA"])

# JOIN com clientes: tenta IDCONTA → IDCONTRATO → CODIGOCLIENTE
_base = df_result
df_result = (
    _base
    .join(df_cli_conta,    _base["ID_CONTA"] == df_cli_conta["_k"],    how="left")
    .join(df_cli_contrato, _base["ID_CONTA"] == df_cli_contrato["_k2"], how="left")
    .join(df_cli_codigo,   _base["ID_CONTA"] == df_cli_codigo["_k3"],   how="left")
    .select(
        _base["FATURA"],
        _base["ID_CONTA"],
        F.coalesce(df_cli_conta["NOME_CLIENTE"], df_cli_contrato["NOME_CLIENTE_2"], df_cli_codigo["NOME_CLIENTE_3"]).alias("NOME_CLIENTE"),
        F.coalesce(df_cli_conta["CPF_CNPJ"],     df_cli_contrato["CPF_CNPJ_2"],     df_cli_codigo["CPF_CNPJ_3"]).alias("CPF_CNPJ"),
        _base["ASSET"],
        _base["STATUS"], _base["STATUS_VALIDACAO"], _base["ANALISTA"],
        _base["OBSERVACAO"], _base["PROBLEMA"], _base["VALOR"], _base["CRIADO_EM"],
        _base["STATUS_RETORNO"], _base["CHAMADO"], _base["RESUMO"],
        _base["Ordem_Status"], _base["DATA_ABERTURA_CHAMADO"],
        _base["DT_EMISSAO"], _base["Valor_Positive"],
    )
)

# Somente faturas INCORRETO ou ALERTA (ambos geram pendencia de validacao)
df_result = df_result.filter(F.col("STATUS").isin("INCORRETO", "ALERTA"))

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
df_result.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_DESTINO)

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

# 2. ANALISTA: somente "PRE-BILLING" ou nulo
c2 = spark.sql(f"SELECT COUNT(*) FROM {TBL_DESTINO} WHERE ANALISTA IS NOT NULL AND ANALISTA != 'PRE-BILLING'").collect()[0][0] == 0
checks.append(c2)
print(f"2. ANALISTA somente PRE-BILLING ou nulo: {'✅' if c2 else '❌'}")

# 3. INCORRETO/ALERTA tem STATUS_VALIDACAO = PENDENTE
c3 = spark.sql(f"""
    SELECT COUNT(*) FROM {TBL_DESTINO}
    WHERE STATUS IN ('INCORRETO','ALERTA') AND STATUS_VALIDACAO != 'PENDENTE'
""").collect()[0][0] == 0
checks.append(c3)
print(f"3. INCORRETO/ALERTA → STATUS_VALIDACAO=PENDENTE: {'✅' if c3 else '❌'}")

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
cols_ref = ["FATURA","ID_CONTA","NOME_CLIENTE","CPF_CNPJ","ASSET","STATUS","STATUS_VALIDACAO",
            "ANALISTA","OBSERVACAO","PROBLEMA","VALOR","CRIADO_EM","STATUS_RETORNO",
            "CHAMADO","RESUMO","Ordem_Status","DATA_ABERTURA_CHAMADO","DT_EMISSAO","Valor_Positive"]
cols_dst = [c.name for c in spark.table(TBL_DESTINO).schema.fields]
c7 = cols_dst == cols_ref
checks.append(c7)
print(f"7. Schema 19 colunas: {'✅' if c7 else '❌'}")
if not c7:
    print(f"   Esperado: {cols_ref}")
    print(f"   Obtido:   {cols_dst}")

print(f"\n{'='*60}")
print(f"{'✅ CARGA OK' if all(checks) else '⚠️ VER ISSUES'}")