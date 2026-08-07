# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Validacao NFCOM x Tabelas Verdade v8
# MAGIC **Vero Internet | Accenture**
# MAGIC
# MAGIC ### Regra de saida
# MAGIC | REGRA              | Validacoes agrupadas |
# MAGIC |--------------------|----------------------------------------------|
# MAGIC | VALIDACAO_IMPOSTOS | ICMS, PIS, COFINS, FUST, FUNTTEL, CST        |
# MAGIC | VALIDACAO_NFCOM    | CCLASS, CFOP, UF, estrutura NFCom            |
# MAGIC | OK                 | Nenhuma divergencia                          |
# MAGIC
# MAGIC ### Modelo: 1 linha por categoria de regra disparada
# MAGIC - Item com erro de imposto E de NFCOM gera 2 linhas
# MAGIC - Item sem erro gera 1 linha com regra OK

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros

# COMMAND ----------
from datetime import datetime as _dt
dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ref", _dt.now().strftime("%Y-%m"), "Ciclo (AAAA-MM)")

CICLO_REF  = dbutils.widgets.get("ciclo_ref")

# ID_Lote no formato YYYY-MM — normaliza antes: aceita tanto "202607" quanto "2026-07"
_ciclo_norm = CICLO_REF.replace("-", "")           # garante YYYYMM sem hífen
ID_LOTE = f"{_ciclo_norm[:4]}-{_ciclo_norm[4:]}"   # sempre YYYY-MM
dbutils.widgets.text("ciclo_ref_lote", ID_LOTE, "Ciclo lote (AAAA-MM)")

TBL_STANDING  = "accenture.tab_validacoes_NFCOM_v4"
TBL_IMPOSTOS  = "accenture.tab_impostos_verdade_nova"
TBL_MESTRE    = "accenture.tab_mestre_nfcom_nova"
TBL_RESULTADO = "accenture.validacao_status_fatura"
TOL = 0.005   # tolerância: ±0.005 pp — aceita arredondamento de 3 casas decimais

# UF da emissora da NFCom — parametrizável por widget
# VERO INTERNET: sede no RS → UF_EMISSORA = "RS"
# ATENÇÃO: se o standing já tem UF_EMIT_PARAMETRIZADA correta, usar essa coluna em vez do widget
dbutils.widgets.text("uf_emissora", "RS", "UF Emissora NFCom")
UF_EMISSORA = dbutils.widgets.get("uf_emissora").upper().strip()

UFS = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
CFOPS_OK = ["5301","5302","5303","5304","5305","5306","5307","5933","6301","6302","6303","6304","6305","6306","6307","6933","1205","7301"]
CFOPS_ISS = ["5933","6933"]
GRUPOS_FIN = ["100","110"]

print(f"Ciclo={CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, DoubleType, StructType, StructField

spark.conf.set("spark.sql.shuffle.partitions","200")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura e filtro

# COMMAND ----------

df_st = (spark.table(TBL_STANDING)
    .withColumn("_ciclo", F.regexp_replace(F.trim(F.col("CICLO").cast(StringType())),"-",""))
    .filter(F.col("_ciclo") == CICLO_REF.replace("-","")))

df_im = spark.table(TBL_IMPOSTOS)
df_ms = spark.table(TBL_MESTRE)

cnt = df_st.count()
print(f"Standing ciclo={CICLO_REF}: {cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. JOIN Mestre

# COMMAND ----------

# Remove zeros a esquerda mantendo "0" sozinho intacto
_strip_zeros = lambda c: F.regexp_replace(F.trim(c.cast(StringType())), r"^0+(?=\d)", "")

# ---- Prepara standing: normaliza CCLASS e limpa CST ----
_cst_clean = F.regexp_replace(F.trim(F.col("CST_ICMS").cast(StringType())), r"\.0$", "")
_cst_vazio = _cst_clean.isNull() | _cst_clean.isin("", "nan", "null", "None", "NaN")

df_st = (df_st
    .withColumn("_cclass_norm",   _strip_zeros(F.col("CCLASS")))
    .withColumn("_cclass7d_norm", _strip_zeros(F.col("CCLASS_7D")))
    # TAG construída a partir do CST do standing (será fallback, não fonte primária)
    .withColumn("_tag_from_cst",
        F.when(_cst_vazio, F.lit("SEM_IMPOSTO_CST_NULO"))
         .otherwise(F.concat(F.lit("ICMS_CST_"), _cst_clean))
    )
)

# ---- JOIN com tab_mestre por CCLASS ----
ms = df_ms.select(
    _strip_zeros(F.col("CCLASS_NFCON")).alias("_m_cc"),
    F.col("CST").alias("_m_cst"),
    F.col("TAX_UF_MUNICIPIO").alias("_m_tipo"),
).dropDuplicates(["_m_cc"])

ms_fb = ms.select(
    F.col("_m_cc") .alias("_m_cc_fb"),
    F.col("_m_cst").alias("_m_cst_fb"),
    F.col("_m_tipo").alias("_m_tipo_fb"),
)

df = (df_st
    .join(ms,    F.col("_cclass_norm")   == ms["_m_cc"],       how="left")
    .join(ms_fb, F.col("_cclass7d_norm") == ms_fb["_m_cc_fb"], how="left")
    .withColumn("_m_cc",   F.coalesce(F.col("_m_cc"),   F.col("_m_cc_fb")))
    .withColumn("_m_cst",  F.coalesce(F.col("_m_cst"),  F.col("_m_cst_fb")))
    .withColumn("_m_tipo", F.coalesce(F.col("_m_tipo"), F.col("_m_tipo_fb")))
    .drop("_m_cc_fb", "_m_cst_fb", "_m_tipo_fb")
)

# ---- TAG final: 1o CST do standing → 2o mestre fallback → 3o validação ----
# Prioridade:
#   1. _tag_from_cst — derivada do CST declarado no item (reflete o que o billing emitiu)
#   2. _m_tipo da tab_mestre — fallback quando CST é nulo/vazio
# TAGs válidas (novas e antigas para transição):
_TAGS_VALIDAS = [
    # TAGs novas (pós-rebuild)
    "ICMS_CST_0", "ICMS_CST_40", "ICMS_CST_51",
    "SEM_IMPOSTO_CST_NULO", "PIS_COFINS_CST_NULO",
    # TAGs antigas (pré-rebuild — mantidas para transição)
    "ICMS", "ICMS_CONFAZ", "ICMS_ESTADUAL",
    "SEM", "PIS_COFINS",
]

# _tag: usa CST primeiro (sempre disponível no standing); mestre como fallback
df = df.withColumn("_tag_raw",
    F.when(F.col("_tag_from_cst").isin(*_TAGS_VALIDAS), F.col("_tag_from_cst"))
     .when(F.col("_m_tipo").isin(*_TAGS_VALIDAS), F.col("_m_tipo"))
     .otherwise(F.col("_tag_from_cst"))  # mantém para log mesmo se inválida
)

# Valida: TAG que não está na lista → None (motor não busca alíquota inválida)
df = df.withColumn("_tag",
    F.when(F.col("_tag_raw").isin(*_TAGS_VALIDAS), F.col("_tag_raw"))
     .otherwise(F.lit(None).cast(StringType()))
)

# _tag_cst: sempre a TAG construída do CST (para comparar com _m_tipo na validação CST_INCOMPATIVEL)
df = df.withColumn("_tag_cst", F.col("_tag_from_cst"))

# item_icms: TRUE se o item É de ICMS. Duas fontes:
#   1. _tag (derivada do CST) indica ICMS → CST preenchido com valor ICMS
#   2. _m_tipo (da tab_mestre) indica ICMS → mestre diz que o CCLASS é ICMS
# O caso crítico: CST nulo em item ICMS → _tag = SEM_IMPOSTO_CST_NULO mas _m_tipo = ICMS_CST_0
# Sem a verificação da mestre, o motor trata o item como "sem ICMS" e dispara
# INDSEMCST_COM_CFOP em vez de CST_ICMS_NULO (que é o correto)
_icms_por_tag = F.col("_tag").isNotNull() & (
    F.col("_tag").startswith("ICMS_CST_") |
    F.col("_tag").isin("ICMS", "ICMS_CONFAZ", "ICMS_ESTADUAL")
)
_icms_por_mestre = F.col("_m_tipo").isNotNull() & (
    F.col("_m_tipo").startswith("ICMS_CST_") |
    F.col("_m_tipo").isin("ICMS", "ICMS_CONFAZ", "ICMS_ESTADUAL")
)
df = df.withColumn("_item_icms", _icms_por_tag | _icms_por_mestre)

# Log para diagnóstico
print("TAGs (1o mestre → 2o CST → validada):")
df.groupBy("_tag").count().orderBy("_tag").show(truncate=False)
print(f"TAGs inválidas (rejeitadas):")
df.filter(F.col("_tag").isNull() & F.col("_tag_raw").isNotNull()).groupBy("_tag_raw").count().show(truncate=False)

cnt_mestre = df.count()
cnt_mapped = df.filter(F.col("_m_cc").isNotNull()).count()
print(f"Secao 5 JOIN Mestre: {cnt_mestre:,} registros | mapeados={cnt_mapped:,} | nao_mapeados={cnt_mestre-cnt_mapped:,}")
if cnt_mestre == 0: raise Exception("STOP Secao 5: 0 registros apos JOIN Mestre")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. JOIN Impostos

# COMMAND ----------

def _imp(tags, alias):
    """Busca aliquota na tab_impostos — aceita lista de TAGs (transição antiga → nova)."""
    return (df_im.filter(F.upper(F.trim(F.col("TIPO_IMPOSTO"))).isin(*tags) & (F.col("ALIQUOTA") > 0))
        .select(F.upper(F.trim(F.col("ESTADO"))).alias(f"_ie_{alias}"),
                F.col("ALIQUOTA").cast(DecimalType(8,4)).alias(f"_i_{alias}"))
        .dropDuplicates([f"_ie_{alias}"]))

# JOIN com tab_impostos — aceita TAGs antigas E novas:
# CST=0  → ICMS ou ICMS_CST_0           → aliquota nominal por UF
# CST=51 → ICMS_CONFAZ ou ICMS_CST_51   → aliquota 0% (diferimento)
# CST=40 → ICMS_ESTADUAL ou ICMS_CST_40 → aliquota 0% (isento)
for tags, a in [
    (["ICMS", "ICMS_CST_0"],           "icms"),
    (["ICMS_CONFAZ", "ICMS_CST_51"],   "confaz"),
    (["ICMS_ESTADUAL", "ICMS_CST_40"], "estadual"),
]:
    df = df.join(_imp(tags, a), F.upper(F.trim(F.col("UF_DEST"))) == F.col(f"_ie_{a}"), how="left")

# PIS/COFINS não-cumulativo — aceita TAG antiga ou nova
df_nc = (df_im.filter(F.upper(F.trim(F.col("TIPO_IMPOSTO"))).isin("PIS_COFINS", "PIS_COFINS_CST_NULO"))
    .select(F.col("PIS").cast(DecimalType(8,4)).alias("_pis_nc"),
            F.col("COFINS").cast(DecimalType(8,4)).alias("_cofins_nc")).limit(1))
df = df.crossJoin(df_nc)
cnt_imp = df.count()
print(f"Secao 6 JOIN Impostos: {cnt_imp:,} registros")
if cnt_imp == 0: raise Exception("STOP Secao 6: 0 registros apos JOIN Impostos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Helpers

# COMMAND ----------

def adiv(a, b):
    """Diverge se o valor não bate nem como percentual direto nem como percentual/100.
    Exemplo: esperado=17.0 → aceita billing=17.0 (direto) OU billing=0.17 (/100).
    Trata a característica de sistemas que enviam alíquota em formato decimal."""
    _tol = F.lit(TOL).cast(DecimalType(8,4))
    _100 = F.lit(100).cast(DecimalType(8,4))
    diverge_direto = F.abs(a - b) > _tol          # 17.0 vs 17.0 → OK
    diverge_x100   = F.abs(a * _100 - b) > _tol   # 0.17 × 100 = 17.0 vs 17.0 → OK
    return a.isNotNull() & b.isNotNull() & diverge_direto & diverge_x100

mapeado = F.col("_m_cc").isNotNull()
# item_icms: usa _item_icms calculado na seção 5 (derivado da TAG validada)
item_icms = F.col("_item_icms")
simples = F.upper(F.trim(F.col("REGIME_TRIB").cast(StringType()))).contains("SIMPLES")

icms_st=F.col("ICMS_STANDING").cast(DecimalType(8,4))
# PIS/COFINS: COALESCE com 0 — NULL = não informado = não cobrado
# Sem isso, sistemas que não extraem PIS/COFINS escapam da validação
pis_st=F.coalesce(F.col("PIS_STANDING").cast(DecimalType(8,4)), F.lit(0).cast(DecimalType(8,4)))
cof_st=F.coalesce(F.col("COFINS_STANDING").cast(DecimalType(8,4)), F.lit(0).cast(DecimalType(8,4)))
# FUST/FUNTTEL: COALESCE com 0 — NULL = não informado = não cobrado
# Sem isso, SIMETRA (que não extrai FUST/FUNTTEL) escapa da validação (falso negativo)
fust_st=F.coalesce(F.col("FUST_STANDING").cast(DecimalType(8,4)), F.lit(0).cast(DecimalType(8,4)))
ftl_st=F.coalesce(F.col("FUNTTEL_STANDING").cast(DecimalType(8,4)), F.lit(0).cast(DecimalType(8,4)))
pis_esp=F.col("PIS_ESPERADO").cast(DecimalType(8,4))
cof_esp=F.col("COFINS_ESPERADO").cast(DecimalType(8,4))
fust_esp=F.col("FUST_ESPERADO").cast(DecimalType(8,4))
ftl_esp=F.col("FUNTTEL_ESPERADO").cast(DecimalType(8,4))

# PIS/COFINS não-cumulativo — valores corretos hardcoded (não depende de tab_impostos)
# Cumulativo: PIS=0.65% COFINS=3.0% | Não-cumulativo: PIS=1.65% COFINS=7.6%
_pis_nc_val  = F.lit(1.65).cast(DecimalType(8,4))
_cof_nc_val  = F.lit(7.60).cast(DecimalType(8,4))
_pis_cum_val = F.lit(0.65).cast(DecimalType(8,4))
_cof_cum_val = F.lit(3.00).cast(DecimalType(8,4))

cst_n = F.regexp_replace(F.trim(F.col("CST_ICMS").cast(StringType())), r"\.0$","")
df = df.withColumn("_cst", cst_n)
cst_v = F.col("_cst").isNull() | F.col("_cst").isin("","nan","null","None","NaN")

uf_d = F.upper(F.trim(F.col("UF_DEST").cast(StringType())))
uf_e = F.upper(F.trim(F.coalesce(F.col("UF_EMIT_PARAMETRIZADA"), F.lit(UF_EMISSORA))))
df = df.withColumn("CFOP", F.regexp_replace(F.trim(F.col("CFOP").cast(StringType())), r"\.0$", ""))
cfop = F.col("CFOP")
cfop_ok = cfop.isNotNull() & ~cfop.isin("","null","nan","None")
grupo = F.col("GRUPO_CCLASS").cast(StringType())

cst_mestre_n = F.regexp_replace(F.trim(F.col("_m_cst").cast(StringType())),r"\.0$","")

# icms_ref: aliquota esperada conforme CST declarado no Standing
# CST=0  → ICMS_CST_0  → aliquota nominal por UF (_i_icms)
# CST=51 → ICMS_CST_51 → 0% (diferimento CONFAZ — ICMS nao destacado) (_i_confaz = 0)
# CST=40 → ICMS_CST_40 → 0% (isento estadual — nacional) (_i_estadual = 0)
icms_ref = (F.when(F.col("_cst") == "0",  F.col("_i_icms"))
    .when(F.col("_cst") == "51", F.col("_i_confaz"))
    .when(F.col("_cst") == "40", F.col("_i_estadual"))
    .otherwise(F.coalesce(F.col("_i_icms"), F.col("ICMS_ESPERADO").cast(DecimalType(8,4)))))
df = df.withColumn("_icms_ref", icms_ref)

# RN004 — FUST e FUNTTEL são determinísticos pela TAG de join (não pelo standing):
#   Com ICMS (ICMS_CST_0, ICMS_CST_51, ICMS_CST_40) → FUST=1.0% FUNTTEL=0.5%
#   Sem ICMS (SEM_IMPOSTO_CST_NULO, PIS_COFINS_CST_NULO) → FUST=0% FUNTTEL=0%
# Usa a TAG da tab_mestre como fonte de verdade — não confia no FUST_ESPERADO do standing
_fust_ref = F.when(item_icms, F.lit(1.0).cast(DecimalType(8,4))).otherwise(F.lit(0.0).cast(DecimalType(8,4)))
_ftl_ref  = F.when(item_icms, F.lit(0.5).cast(DecimalType(8,4))).otherwise(F.lit(0.0).cast(DecimalType(8,4)))
df = df.withColumn("_fust_ref", _fust_ref)
df = df.withColumn("_ftl_ref",  _ftl_ref)

print("Secao 7 Helpers: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validacoes
# MAGIC
# MAGIC Cada validacao tem: (col_bool, tag, categoria, severidade, observacao)
# MAGIC - categoria: IMPOSTOS ou NFCOM
# MAGIC - tag vai dentro da observacao, nao na regra

# COMMAND ----------

# --- VALIDACAO_IMPOSTOS ---
df = df.withColumn("_vi_cst_incomp",
    # Compara a TAG derivada do CST do standing (_tag_cst) contra a TAG da tab_mestre (_m_tipo)
    # _tag_cst  = "ICMS_CST_{cst}" construída do CST que o billing declarou
    # _m_tipo   = TAX_UF_MUNICIPIO da tab_mestre (o que o CCLASS deveria ter)
    # Se _tag_cst != _m_tipo → o CST declarado não corresponde ao esperado para o CCLASS
    # Normaliza TAGs antigas para comparação:
    #   ICMS → ICMS_CST_0 | ICMS_CONFAZ → ICMS_CST_51 | ICMS_ESTADUAL → ICMS_CST_40
    mapeado & item_icms & ~cst_v &
    F.col("_m_tipo").isNotNull() &
    F.col("_tag_cst").isNotNull() &
    (F.col("_tag_cst") !=
        F.when(F.col("_m_tipo") == "ICMS",          F.lit("ICMS_CST_0"))
         .when(F.col("_m_tipo") == "ICMS_CONFAZ",   F.lit("ICMS_CST_51"))
         .when(F.col("_m_tipo") == "ICMS_ESTADUAL", F.lit("ICMS_CST_40"))
         .otherwise(F.col("_m_tipo"))
    )
)
# CST_ICMS_NULO: CST vazio para item ICMS
# EXCEÇÃO: quando CST_MESTRE é 40 (isento) ou 41 (não tributado), a operadora pode
# legitimamente optar por indSemCST em vez de gICMS40/gICMS41 no XML da NFCom.
# Ambas abordagens são aceitas pela SEFAZ para itens isentos/não tributados.
_cst_m_clean = F.regexp_replace(F.trim(F.col("_m_cst").cast(StringType())), r"\.0$", "")
_cst_mestre_isento = _cst_m_clean.isin("40", "41")
df = df.withColumn("_vi_cst_nulo", mapeado & item_icms & cst_v & ~_cst_mestre_isento)
df = df.withColumn("_vi_icms_div", mapeado & item_icms & ~cst_v & adiv(icms_st, F.col("_icms_ref")))
# PIS: diverge se não bate com cumulativo (0.65) NEM com não-cumulativo (1.65) — em ambos formatos (% e %/100)
# COALESCE garante que NULL→0, então isNotNull não é mais necessário
df = df.withColumn("_vi_pis",
    mapeado & ~simples & item_icms & adiv(pis_st, _pis_cum_val) & adiv(pis_st, _pis_nc_val))
# COFINS: diverge se não bate com cumulativo (3.0) NEM com não-cumulativo (7.6) — em ambos formatos
df = df.withColumn("_vi_cofins",
    mapeado & ~simples & item_icms & adiv(cof_st, _cof_cum_val) & adiv(cof_st, _cof_nc_val))
# Simples Nacional: mesma lógica mas severidade ALERTA
df = df.withColumn("_vi_pis_cof_simples", mapeado & simples & (
    (adiv(pis_st, _pis_cum_val) & adiv(pis_st, _pis_nc_val)) |
    (adiv(cof_st, _cof_cum_val) & adiv(cof_st, _cof_nc_val))))
# RN004 — FUST/FUNTTEL: compara standing contra regra determinística (não contra FUST_ESPERADO do standing)
df = df.withColumn("_vi_fust",    mapeado & adiv(fust_st, F.col("_fust_ref")))
df = df.withColumn("_vi_funttel", mapeado & adiv(ftl_st,  F.col("_ftl_ref")))
# ICMS_SEM_ALIQUOTA: item ICMS com alíquota zero — mas NÃO para CSTs onde zero é esperado
# CSTs onde ICMS=0 é correto (não é erro de parametrização):
#   40 = isento estadual | 41 = não tributado | 50 = suspensão
#   51 = diferimento CONFAZ | 60 = ST pago anteriormente
_cst_zero_ok = F.col("_cst").isin("40", "41", "50", "51", "60")
df = df.withColumn("_vi_icms_zero",
    item_icms & ~_cst_zero_ok &
    (icms_st.isNull() | (icms_st == F.lit(0).cast(DecimalType(8,4))))
)

# --- VALIDACAO_NFCOM ---
df = df.withColumn("_vn_cclass", ~mapeado)
df = df.withColumn("_vn_cfop_inv", item_icms & cfop_ok & ~cfop.isin(CFOPS_OK))
df = df.withColumn("_vn_cfop_uf", item_icms & cfop_ok & cfop.isin(CFOPS_OK) & uf_d.isin(UFS) &
    (((uf_d==uf_e)&(F.substring(cfop,1,1)=="6"))|((uf_d!=uf_e)&(F.substring(cfop,1,1)=="5"))))
# R17 — CFOP_INCOMPATIVEL_TRIBUTO
# Aderência entre CFOP e tipo de tributação do item (TAX_UF_MUNICIPIO da tabela mestre):
# ATENÇÃO: para itens ISS/SVA com CFOP de ICMS, existem exceções legítimas:
#   - Bundle/pacote único: SVA agregado ao serviço de telecom, tributado por ICMS
#   - SVA tributado por ICMS: interpretação fiscal ou configuração histórica do ERP
#   - Legislação estadual: alguns estados possuem entendimentos específicos
# Por isso, esta regra é ALERTA (não BLOQUEANTE) para itens ISS/SVA
CFOPS_ICMS_OK = ["5301","5302","5303","5304","5305","5306","5307",
                 "6301","6302","6303","6304","6305","6306","6307","1205","7301"]
GRUPOS_ICMS = ["10","20","30","40","70"]
GRUPOS_ISS  = ["60","80","130","590"]

# Caso 1 (BLOQUEANTE): item ICMS com CFOP de ISS → sempre errado
df = df.withColumn("_vn_cfop_tributo_icms",
    cfop_ok & mapeado & grupo.isin(GRUPOS_ICMS) & cfop.isin(CFOPS_ISS)
)

# Caso 2 (ALERTA): item ISS/SVA com CFOP de ICMS → forte indício de erro,
# mas pode ter exceções (bundle, enquadramento tributário, legislação estadual)
df = df.withColumn("_vn_cfop_tributo_sva",
    cfop_ok & mapeado & grupo.isin(GRUPOS_ISS) & cfop.isin(CFOPS_ICMS_OK)
)
df = df.withColumn("_vn_cfop933", cfop.isin(CFOPS_ISS) & ~cst_v)

# F1 — CFOP_AUSENTE (Rejeição 540 MOC NFCom)
# Item com CST informado (ICMS) obrigatoriamente deve ter CFOP preenchido
df = df.withColumn("_vn_cfop_ausente",
    item_icms & ~cst_v &
    (F.col("CFOP").isNull() | F.trim(F.col("CFOP").cast(StringType())).isin("","null","nan","None"))
)

# F2 — INDSEMCST_COM_CFOP (Rejeição 541 MOC NFCom)
# Item sem ICMS (ISS/SVA) com CFOP preenchido (exceto 5933/6933)
# ATENÇÃO: Não afirmar categoricamente que a SEFAZ rejeitaria.
# Depende do modelo do documento fiscal e das demais informações tributárias.
# Em muitos casos, o documento é autorizado e o problema é identificado
# posteriormente em fiscalização ou cruzamentos fiscais.
# Severidade: ALERTA (não bloqueante) — validar exceções de bundle/enquadramento
df = df.withColumn("_vn_indsemcst_com_cfop",
    mapeado & (~item_icms) & cfop_ok &
    (~cfop.isin(CFOPS_ISS)) &  # 5933/6933 já cobertos em _vn_cfop933
    (~grupo.isin(GRUPOS_FIN))   # financeiros tratados em _vn_fin_cfop
)

# F3 — COFATURAMENTO_COM_ICMS (Rejeição 266 MOC NFCom)
# GRUPO_CCLASS=130 (cofaturamento) não pode ter ICMS destacado
# cClass 1300101 deve usar indSemCST sem CFOP
df = df.withColumn("_vn_cofat_com_icms",
    grupo.isin(["130","13"]) &
    (icms_st.isNotNull()) & (icms_st > F.lit(0).cast(DecimalType(8,4)))
)

# F4 — FAT_CENTRALIZADO_COM_ICMS (Rejeição 269 MOC NFCom)
# GRUPO_CCLASS=120 (faturamento centralizado) não pode ter ICMS destacado
df = df.withColumn("_vn_fatcent_com_icms",
    grupo.isin(["120","12"]) &
    (icms_st.isNotNull()) & (icms_st > F.lit(0).cast(DecimalType(8,4)))
)

# F5 — MUNICIPIO_PRESTACAO_AUSENTE
# Campo obrigatório pela SEFAZ — ausência bloqueia a emissão
df = df.withColumn("_vn_mun_prestacao",
    F.col("MUNICIPIO_PRESTACAO").isNull() |
    F.upper(F.trim(F.col("MUNICIPIO_PRESTACAO").cast(StringType()))).isin("","NULL","NAN","NONE","NAO INFORMADO")
)

# F6 — UF_DEST_AUSENTE_ICMS removida: é subconjunto de UF_DEST_INVALIDA
# UF_DEST_INVALIDA já cobre ausência e valor inválido para qualquer tipo de item
# Manter apenas UF_DEST_INVALIDA evita duplicidade de tags no mesmo registro

# F7 — TIPO_FAT_SUBSTITUICAO
# Standing contém NFCom de substituição no ciclo — deve ser revisada pela equipe fiscal
# TIPO_FAT = 'Substituição' indica nota que substituiu outra já emitida
df = df.withColumn("_vn_tipo_fat_subst",
    F.upper(F.trim(F.col("TIPO_FAT").cast(StringType()))) == "SUBSTITUIÇÃO"
)

# F8 — MUNICIPIO_DEST_AUSENTE
# UF_DEST preenchida mas MUNICIPIO_DEST ausente — inconsistência geográfica
df = df.withColumn("_vn_mun_dest_ausente",
    (F.col("UF_DEST").isNotNull()) &
    (~F.upper(F.trim(F.col("UF_DEST").cast(StringType()))).isin("","NULL","NAN","NONE")) &
    (
        F.col("MUNICIPIO_DEST").isNull() |
        F.upper(F.trim(F.col("MUNICIPIO_DEST").cast(StringType()))).isin("","NULL","NAN","NONE")
    )
)

# F9 — CCLASS_7D_DIVERGENTE
# CCLASS_7D (código interno do sistema) difere do CCLASS oficial de 6 dígitos
# Indica código interno sem mapeamento correto para o cClass SEFAZ
df = df.withColumn("_vn_cclass7d_div",
    mapeado &
    F.col("CCLASS_7D").isNotNull() &
    F.col("CCLASS").isNotNull() &
    (_strip_zeros(F.col("CCLASS_7D")) != _strip_zeros(F.col("CCLASS")))
)
df = df.withColumn("_vn_fin_cfop", mapeado & grupo.isin(GRUPOS_FIN) & cfop_ok)
df = df.withColumn("_vn_uf_inv",
    F.col("UF_DEST").isNull() | F.trim(F.col("UF_DEST").cast(StringType())).isin("","null","NULL","nan","NAN","None") | (~uf_d.isin(UFS)))
df = df.withColumn("_vn_fat_num",
    F.col("FATURA_NUMERO").isNull() | F.upper(F.trim(F.col("FATURA_NUMERO").cast(StringType()))).isin("","NAN","NULL","NONE"))

print(f"Validacoes calculadas: {df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Catalogo de validacoes

# COMMAND ----------

# (col_bool, tag, categoria, severidade, observacao_texto)
VALIDACOES = [
    # IMPOSTOS
    ("_vi_cst_incomp",       "CST_INCOMPATIVEL_TRIBUTO",   "VALIDACAO_IMPOSTOS","ALERTA",
     "CST declarado diverge do esperado para o CCLASS conforme tabela mestre fiscal. Existem diversos CST ICMS possiveis (00, 10, 20, 40, 41, 51, 60, 90). Validar enquadramento estadual e decisao fiscal"),
    ("_vi_cst_nulo",         "CST_ICMS_NULO",              "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "CST ICMS ausente para item com ICMS aplicavel (CST obrigatorio). Nao dispara quando CST esperado e 40 (isento) ou 41 (nao tributado) — nesses casos indSemCST e alternativa valida. Rejeicao 539 SEFAZ"),
    ("_vi_icms_div",         "ICMS_DIVERGENTE",            "VALIDACAO_IMPOSTOS","ALERTA",
     "Aliquota ICMS informada diverge da esperada conforme tabela de referencia por UF. Pode haver beneficio fiscal (cBenef), convenio ICMS ou regime especial. Aceita formato percentual e decimal"),
    ("_vi_pis",              "PIS_DIVERGENTE",             "VALIDACAO_IMPOSTOS","ALERTA",
     "Aliquota PIS informada diverge da esperada conforme regime tributario aplicavel. Aceita formato percentual e decimal (/100). Validar regime monofasico, aliquota zero ou suspensao"),
    ("_vi_cofins",           "COFINS_DIVERGENTE",          "VALIDACAO_IMPOSTOS","ALERTA",
     "Aliquota COFINS informada diverge da esperada conforme regime tributario aplicavel. Aceita formato percentual e decimal (/100). Validar regime monofasico, aliquota zero ou suspensao"),
    ("_vi_pis_cof_simples",  "PIS_COFINS_SIMPLES_ALERTA",  "VALIDACAO_IMPOSTOS","ALERTA",
     "PIS ou COFINS divergente em Simples Nacional. Aliquotas DAS diferem das aliquotas padrao. Aceita formato percentual e decimal"),
    ("_vi_fust",             "FUST_INCORRETO",             "VALIDACAO_IMPOSTOS","ALERTA",
     "FUST diverge da aliquota esperada conforme regra fiscal aplicavel ao servico. Campo nao presente no XML NFCom. Validar enquadramento e forma de calculo do standing"),
    ("_vi_funttel",          "FUNTTEL_INCORRETO",          "VALIDACAO_IMPOSTOS","ALERTA",
     "FUNTTEL diverge da aliquota esperada conforme regra fiscal aplicavel. Campo nao presente no XML NFCom. Validar enquadramento e forma de calculo do standing"),
    ("_vi_icms_zero",        "ICMS_SEM_ALIQUOTA",          "VALIDACAO_IMPOSTOS","ALERTA",
     "Item ICMS (CST 0/10/20/70/90) com aliquota zero. Pode haver beneficio fiscal com aliquota zero temporaria ou item em migracao. Nao dispara para CST 40/41/50/51/60"),
    # NFCOM
    ("_vn_cclass",           "CCLASS_NAO_MAPEADO",         "VALIDACAO_NFCOM",  "ALERTA",
     "CCLASS nao encontrado na tabela mestre fiscal. Gap interno de configuracao — SEFAZ nao valida CCLASS. Sem mapeamento, o motor nao consegue validar tributacao do item"),
    ("_vn_cfop_inv",         "CFOP_INVALIDO",              "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CFOP fora da lista oficial de 17 CFOPs validos para NFCom modelo 62 (MOC SEFAZ)"),
    ("_vn_cfop_uf",          "CFOP_INCOMPATIVEL_UF",       "VALIDACAO_NFCOM",  "ALERTA",
     "CFOP incompativel com direcao geografica da operacao. Dentro da mesma UF exige 5xxx, interestadual exige 6xxx. SEFAZ nao rejeita na autorizacao — risco identificado em malha fiscal posterior"),
    ("_vn_cfop_tributo_icms","CFOP_ICMS_COM_ISS",          "VALIDACAO_NFCOM",  "ALERTA",
     "Item ICMS com CFOP de ISS (5933/6933). SEFAZ pode nao rejeitar na autorizacao — risco de autuacao em cruzamento fiscal posterior. Validar configuracao do produto"),
    ("_vn_cfop_tributo_sva", "CFOP_SVA_COM_ICMS",          "VALIDACAO_NFCOM",  "ALERTA",
     "Item SVA/ISS com CFOP compativel com prestacao de servico de comunicacao. Forte indicio de configuracao incorreta. Validar excecoes de bundle, enquadramento tributario e tabela fiscal vigente"),
    ("_vn_cfop933",          "CFOP_933_COM_CST",           "VALIDACAO_NFCOM",  "ALERTA",
     "CFOP 5933/6933 (ISS) com CST ICMS preenchido. Em bundle comercial o CST pode vir preenchido mesmo com CFOP ISS. Validar configuracao do produto"),
    ("_vn_fin_cfop",         "ITEM_FINANCEIRO_COM_CFOP",   "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item financeiro (GRP 100/110) com CFOP preenchido. Itens sem incidencia de ICMS utilizam indicador indSemCST e nao devem possuir CFOP (Rejeicao 541 SEFAZ)"),
    ("_vn_uf_inv",           "UF_DEST_INVALIDA",           "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "UF destino inexistente ou incompativel com operacao fiscal. Campo obrigatorio para composicao da chave NFCom e calculo de ICMS"),
    ("_vn_fat_num",          "FATURA_SEM_NUMERO",          "VALIDACAO_NFCOM",  "ALERTA",
     "Numero de fatura ausente ou invalido. Possivel erro de geracao ou migracao"),
    # Novas — MOC NFCom SEFAZ
    ("_vn_cfop_ausente",     "CFOP_AUSENTE",               "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item com CST ICMS informado sem CFOP. Rejeicao 540 SEFAZ: CST obriga CFOP"),
    ("_vn_indsemcst_com_cfop","INDSEMCST_COM_CFOP",        "VALIDACAO_NFCOM",  "ALERTA",
     "Item informado como indSemCST possui CFOP preenchido. Itens sem tributacao ICMS nao devem possuir CFOP conforme regra NFCom. Validar excecoes de bundle e enquadramento tributario"),
    ("_vn_cofat_com_icms",   "COFATURAMENTO_COM_ICMS",     "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item de cofaturamento (GRUPO 130) com ICMS destacado. Rejeicao 266 SEFAZ: cClass cofaturamento nao pode ter tributacao ICMS"),
    ("_vn_fatcent_com_icms", "FAT_CENTRALIZADO_COM_ICMS",  "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item de faturamento centralizado (GRUPO 120) com ICMS destacado. Rejeicao 269 SEFAZ: cClass faturamento centralizado nao pode ser tributado"),
    ("_vn_mun_prestacao",    "MUNICIPIO_PRESTACAO_AUSENTE","VALIDACAO_NFCOM",  "ALERTA",
     "Municipio de prestacao ausente. Campo obrigatorio pela SEFAZ, mas servicos digitais ou itens sem localidade definida podem nao ter municipio. Validar com cadastro"),
    ("_vn_tipo_fat_subst",   "TIPO_FAT_SUBSTITUICAO",      "VALIDACAO_NFCOM",  "ALERTA",
     "NFCom de substituicao identificada no ciclo. Verificar se referencia corretamente a NFCom original e se o motivo da substituicao e valido"),
    ("_vn_mun_dest_ausente", "MUNICIPIO_DEST_AUSENTE",     "VALIDACAO_NFCOM",  "ALERTA",
     "UF de destino preenchida mas municipio do destinatario ausente. Inconsistencia geografica no cadastro do cliente"),
    ("_vn_cclass7d_div",     "CCLASS_7D_DIVERGENTE",       "VALIDACAO_NFCOM",  "ALERTA",
     "Codigo interno CCLASS_7D difere do CCLASS oficial SEFAZ de 6 digitos. Indica produto sem mapeamento correto para NFCom"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Explodir por categoria (IMPOSTOS / NFCOM / OK)

# COMMAND ----------

def _sv(v):
    """Safe value para montar string — converte None/nan para vazio."""
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan","none","null") else s

def _billing_impostos(d):
    """Monta dados_billing para VALIDACAO_IMPOSTOS."""
    return (
        f"TAG_VALIDADA: {_sv(d.get('_tag'))} | "
        f"TAG_CST: {_sv(d.get('_tag_cst'))} | "
        f"TAG_RAW: {_sv(d.get('_tag_raw'))} | "
        f"TAG_MESTRE: {_sv(d.get('_m_tipo'))} | "
        f"ICMS: {_sv(d.get('ICMS_STANDING'))} | "
        f"PIS: {_sv(d.get('PIS_STANDING'))} | "
        f"COFINS: {_sv(d.get('COFINS_STANDING'))} | "
        f"FUST: {_sv(d.get('FUST_STANDING'))} | "
        f"FUNTTEL: {_sv(d.get('FUNTTEL_STANDING'))} | "
        f"CST: {_sv(d.get('_cst'))} | "
        f"REGIME: {_sv(d.get('REGIME_TRIB'))}"
    )

def _verdade_impostos(d):
    """Monta dados_tabela_verdade para VALIDACAO_IMPOSTOS."""
    cst = _sv(d.get('_cst'))
    # ICMS esperado conforme CST e TAG de join da tab_mestre
    if cst == "0":
        icms_esp = f"ICMS_CST_0 (aliq nominal UF): {_sv(d.get('_i_icms'))}"
    elif cst == "51":
        icms_esp = f"ICMS_CST_51 (diferimento CONFAZ): {_sv(d.get('_i_confaz'))} — ICMS nao destacado"
    elif cst == "40":
        icms_esp = f"ICMS_CST_40 (isento estadual): {_sv(d.get('_i_estadual'))} — aliq=0% nacional"
    else:
        icms_esp = f"ICMS_CST_0 (UF): {_sv(d.get('_i_icms'))} | ICMS_ESPERADO_STANDING: {_sv(d.get('ICMS_ESPERADO'))}"
    return (
        f"{icms_esp} | "
        f"PIS_CUMUL: 0.65 | PIS_NC: 1.65 | "
        f"COFINS_CUMUL: 3.0 | COFINS_NC: 7.6 | "
        f"FUST_RN004: {_sv(d.get('_fust_ref'))} | FUNTTEL_RN004: {_sv(d.get('_ftl_ref'))} | "
        f"CST_MESTRE: {_sv(d.get('_m_cst'))} | TAG_JOIN: {_sv(d.get('_m_tipo'))}"
    )

def _billing_nfcom(d):
    """Monta dados_billing para VALIDACAO_NFCOM."""
    return (
        f"TAG_VALIDADA: {_sv(d.get('_tag'))} | "
        f"TAG_CST: {_sv(d.get('_tag_cst'))} | "
        f"TAG_RAW: {_sv(d.get('_tag_raw'))} | "
        f"TAG_MESTRE: {_sv(d.get('_m_tipo'))} | "
        f"CCLASS: {_sv(d.get('CCLASS'))} | "
        f"CFOP: {_sv(d.get('CFOP'))} | "
        f"UF_DEST: {_sv(d.get('UF_DEST'))} | "
        f"CST: {_sv(d.get('_cst'))} | "
        f"GRUPO_CCLASS: {_sv(d.get('GRUPO_CCLASS'))} | "
        f"FATURA: {_sv(d.get('FATURA_NUMERO'))}"
    )

def _verdade_nfcom(d):
    """Monta dados_tabela_verdade para VALIDACAO_NFCOM."""
    uf_emit = _sv(d.get('UF_EMIT_PARAMETRIZADA')) or "SP"
    return (
        f"UF_EMISSORA: {uf_emit} | "
        f"CFOPS_VALIDOS: 5301-5307,5933,6301-6307,6933,1205,7301 | "
        f"REGRA_CFOP: mesma UF=5xxx outra UF=6xxx | "
        f"TAG_JOIN: {_sv(d.get('_m_tipo'))} | "
        f"CST_MESTRE: {_sv(d.get('_m_cst'))} | "
        f"GRUPOS_FIN: 100,110 (indSemCST sem CFOP)"
    )

def _billing_ok(d):
    return (
        f"ICMS: {_sv(d.get('ICMS_STANDING'))} | "
        f"PIS: {_sv(d.get('PIS_STANDING'))} | "
        f"COFINS: {_sv(d.get('COFINS_STANDING'))} | "
        f"FUST: {_sv(d.get('FUST_STANDING'))} | "
        f"FUNTTEL: {_sv(d.get('FUNTTEL_STANDING'))} | "
        f"CST: {_sv(d.get('_cst'))} | "
        f"CFOP: {_sv(d.get('CFOP'))} | "
        f"UF_DEST: {_sv(d.get('UF_DEST'))}"
    )

def _verdade_ok(d):
    return (
        f"ICMS_UF: {_sv(d.get('_i_icms'))} | "
        f"PIS_ESP: {_sv(d.get('PIS_ESPERADO'))} | "
        f"COFINS_ESP: {_sv(d.get('COFINS_ESPERADO'))} | "
        f"FUST_ESP: {_sv(d.get('FUST_ESPERADO'))} | "
        f"FUNTTEL_ESP: {_sv(d.get('FUNTTEL_ESPERADO'))} | "
        f"CST_MESTRE: {_sv(d.get('_m_cst'))}"
    )

def _row(d, regra, status, substatus, observacao, dados_billing, dados_tabela_verdade):
    return {
        "ciclo_faturamento":d.get("_ciclo"),
        "fatura_numero":str(d.get("FATURA_NUMERO") or ""),
        "id_cliente":str(d.get("ID_CLIENTE") or ""),
        "sistema_origem":str(d.get("SISTEMA_ORIGEM") or ""),
        "cclass":str(d.get("CCLASS") or ""),
        "cfop":str(d.get("CFOP") or ""),
        "uf_dest":str(d.get("UF_DEST") or ""),
        "uf_emit":str(d.get("UF_EMIT_PARAMETRIZADA") or UF_EMISSORA),
        "tipo_servico":str(d.get("TIPO_SERVICO_ESPERADO_CCLASS") or ""),
        "regime_trib":str(d.get("REGIME_TRIB") or ""),
        "tipo_imposto_mestre":str(d.get("TIPO_IMPOSTO_MESTRE") or ""),
        "grupo_cclass":str(d.get("GRUPO_CCLASS") or ""),
        "cst_icms":str(d.get("_cst") or ""),
        "regra":regra, "status":status, "substatus":substatus,
        "observacao":observacao,
        "dados_billing":dados_billing,
        "dados_tabela_verdade":dados_tabela_verdade,
        "icms_standing":d.get("ICMS_STANDING"), "icms_esperado":d.get("ICMS_ESPERADO"),
        "icms_verdade_uf":d.get("_i_icms"), "icms_confaz_uf":d.get("_i_confaz"),
        "icms_estadual_uf":d.get("_i_estadual"),
        "pis_standing":d.get("PIS_STANDING"), "cofins_standing":d.get("COFINS_STANDING"),
        "fust_standing":d.get("FUST_STANDING"), "funttel_standing":d.get("FUNTTEL_STANDING"),
        "pis_esperado":d.get("PIS_ESPERADO"), "cofins_esperado":d.get("COFINS_ESPERADO"),
        "fust_esperado":d.get("FUST_ESPERADO"), "funttel_esperado":d.get("FUNTTEL_ESPERADO"),
        "fust_rn004":d.get("_fust_ref"), "funttel_rn004":d.get("_ftl_ref"),
        "pis_nc":d.get("_pis_nc"), "cofins_nc":d.get("_cofins_nc"),
        "cst_esperado_mestre":str(d.get("_m_cst") or ""),
        "tipo_trib_mestre":str(d.get("_m_tipo") or ""),
        "tag_join_final":str(d.get("_tag") or ""),
        "impacto_icms_r":d.get("IMPACTO_ICMS_ESTIMADO_R$"),
    }

# Colunas necessarias na explosao
cols_base = [
    "_ciclo","FATURA_NUMERO","ID_CLIENTE","SISTEMA_ORIGEM","CCLASS","CFOP",
    "UF_DEST","UF_EMIT_PARAMETRIZADA","TIPO_SERVICO_ESPERADO_CCLASS",
    "REGIME_TRIB","TIPO_IMPOSTO_MESTRE","GRUPO_CCLASS","_cst",
    "ICMS_STANDING","ICMS_ESPERADO","_i_icms","_i_confaz","_i_estadual",
    "PIS_STANDING","COFINS_STANDING","FUST_STANDING","FUNTTEL_STANDING",
    "PIS_ESPERADO","COFINS_ESPERADO","FUST_ESPERADO","FUNTTEL_ESPERADO",
    "_pis_nc","_cofins_nc","_fust_ref","_ftl_ref","_m_cst","_m_tipo","_tag","_tag_cst","_tag_raw","_item_icms","IMPACTO_ICMS_ESTIMADO_R$",
] + [c for c,*_ in VALIDACOES]
cols_base = list(dict.fromkeys(cols_base))  # remove duplicatas mantendo ordem
df_base = df.select(*[c for c in cols_base if c in df.columns])
cnt_base = df_base.count()
print(f"Secao 10 df_base: {cnt_base:,} registros | colunas={df_base.columns}")
if cnt_base == 0: raise Exception("STOP Secao 10: df_base vazio antes do mapInPandas")

SEVER_RANK = {"BLOQUEANTE":2,"ALERTA":1,"OK":0}

OUT_SCHEMA = StructType([
    StructField("ciclo_faturamento",    StringType()),
    StructField("fatura_numero",        StringType()),
    StructField("id_cliente",           StringType()),
    StructField("sistema_origem",       StringType()),
    StructField("cclass",               StringType()),
    StructField("cfop",                 StringType()),
    StructField("uf_dest",              StringType()),
    StructField("uf_emit",              StringType()),
    StructField("tipo_servico",         StringType()),
    StructField("regime_trib",          StringType()),
    StructField("tipo_imposto_mestre",  StringType()),
    StructField("grupo_cclass",         StringType()),
    StructField("cst_icms",             StringType()),
    StructField("regra",                StringType()),
    StructField("status",               StringType()),
    StructField("substatus",            StringType()),
    StructField("observacao",           StringType()),
    StructField("dados_billing",        StringType()),
    StructField("dados_tabela_verdade", StringType()),
    StructField("icms_standing",        DoubleType()),
    StructField("icms_esperado",        DoubleType()),
    StructField("icms_verdade_uf",      DoubleType()),
    StructField("icms_confaz_uf",       DoubleType()),
    StructField("icms_estadual_uf",     DoubleType()),
    StructField("pis_standing",         DoubleType()),
    StructField("cofins_standing",      DoubleType()),
    StructField("fust_standing",        DoubleType()),
    StructField("funttel_standing",     DoubleType()),
    StructField("pis_esperado",         DoubleType()),
    StructField("cofins_esperado",      DoubleType()),
    StructField("fust_esperado",        DoubleType()),
    StructField("funttel_esperado",     DoubleType()),
    StructField("fust_rn004",           DoubleType()),
    StructField("funttel_rn004",        DoubleType()),
    StructField("pis_nc",               DoubleType()),
    StructField("cofins_nc",            DoubleType()),
    StructField("cst_esperado_mestre",  StringType()),
    StructField("tipo_trib_mestre",     StringType()),
    StructField("tag_join_final",       StringType()),
    StructField("impacto_icms_r",       DoubleType()),
])

_OUT_COLS = [f.name for f in OUT_SCHEMA]

def explodir_pandas(iterator):
    for pdf in iterator:
        linhas = []
        for _, row in pdf.iterrows():
            d = row.to_dict()

            # Separa as tags disparadas por categoria
            cats = {"VALIDACAO_IMPOSTOS": [], "VALIDACAO_NFCOM": []}
            for col, tag, cat, sev, obs in VALIDACOES:
                if d.get(col):
                    cats[cat].append((tag, sev, obs))

            for cat in ["VALIDACAO_IMPOSTOS", "VALIDACAO_NFCOM"]:
                items = cats[cat]
                billing = _billing_impostos(d) if cat == "VALIDACAO_IMPOSTOS" else _billing_nfcom(d)
                verdade_base = _verdade_impostos(d) if cat == "VALIDACAO_IMPOSTOS" else _verdade_nfcom(d)

                if items:
                    # Monta resumo de TODAS as tags + regras disparadas para este item/categoria
                    # Formato: TAG_1[SEV] | TAG_2[SEV] | ...
                    todas_regras = " | ".join(
                        f"{tag}[{sev}]" for tag, sev, _ in items
                    )
                    verdade = f"REGRAS_DISPARADAS({len(items)}): {todas_regras} | {verdade_base}"

                    # 1 linha por tag disparada — sem concatenação
                    for tag, sev, obs in items:
                        # STATUS diferenciado por severidade:
                        #   BLOQUEANTE → INCORRETO (erro objetivo, deve corrigir)
                        #   ALERTA     → ALERTA    (anomalia, pode ter exceção — validar)
                        _status = "INCORRETO" if sev == "BLOQUEANTE" else "ALERTA"
                        linhas.append(_row(
                            d, cat, _status, sev,
                            f"{tag}: {obs}",   # observacao = apenas esta tag
                            billing, verdade
                        ))
                else:
                    # Item sem erro nesta categoria → 1 linha CORRETO/OK
                    linhas.append(_row(d, cat, "CORRETO", "OK", "Validacoes aprovadas", billing, verdade_base))

        if linhas:
            yield pd.DataFrame(linhas, columns=_OUT_COLS)
        else:
            yield pd.DataFrame(columns=_OUT_COLS)

df_exp = df_base.mapInPandas(explodir_pandas, schema=OUT_SCHEMA)

cnt_t = df_exp.count()
cnt_ok = df_exp.filter(F.col("status")=="CORRETO").count()
cnt_er = df_exp.filter(F.col("status")=="INCORRETO").count()
cnt_al = df_exp.filter(F.col("status")=="ALERTA").count()
print(f"Secao 10 Explosao: {cnt_t:,} linhas | CORRETO={cnt_ok:,} | INCORRETO={cnt_er:,} | ALERTA={cnt_al:,}")
if cnt_t == 0: raise Exception("STOP Secao 10: df_exp vazio apos mapInPandas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. PK, cast, hash

# COMMAND ----------

for c in ["icms_standing","icms_esperado","icms_verdade_uf","icms_confaz_uf","icms_estadual_uf",
          "pis_standing","cofins_standing","fust_standing","funttel_standing",
          "pis_esperado","cofins_esperado","fust_esperado","funttel_esperado",
          "fust_rn004","funttel_rn004","pis_nc","cofins_nc"]:
    df_exp = df_exp.withColumn(c, F.col(c).cast(DecimalType(8,4)))
df_exp = df_exp.withColumn("impacto_icms_r", F.col("impacto_icms_r").cast(DecimalType(18,2)))

df_final = (df_exp
    .withColumn("id_validacao", F.md5(F.concat_ws("|",
        *[F.coalesce(F.col(c),F.lit("")) for c in
          ["sistema_origem","fatura_numero","id_cliente","cclass","cfop","regra"]]+[F.lit(CICLO_REF)])))
    .withColumn("hash_registro", F.sha2(F.concat_ws("|",
        F.col("status"),F.col("substatus"),F.coalesce(F.col("observacao"),F.lit(""))),256))
    .withColumn("dt_carga",F.current_timestamp())
    .withColumn("dt_processamento",F.current_timestamp()))

cnt_final = df_final.count()
print(f"Secao 11 df_final: {cnt_final:,} registros prontos para MERGE")
if cnt_final == 0: raise Exception("STOP Secao 11: df_final vazio, MERGE nao sera executado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. MERGE

# COMMAND ----------

# ID_Lote na tabela usa formato YYYY-MM (ex: "2026-07") — reutiliza _ciclo_norm
ID_LOTE = f"{_ciclo_norm[:4]}-{_ciclo_norm[4:]}"

spark.sql(f"""
    DELETE FROM {TBL_RESULTADO}
    WHERE ID_Lote = '{ID_LOTE}'
      AND REGRA IN ('VALIDACAO_IMPOSTOS', 'VALIDACAO_NFCOM')
""")
print(f"Secao 12 DELETE: ID_Lote={ID_LOTE} regras VALIDACAO_IMPOSTOS/VALIDACAO_NFCOM — OK")

# Mapeia df_final para o schema da tabela
df_insert = df_final.select(
    F.col("fatura_numero")                                          .alias("FATURA"),
    F.col("id_cliente")                                             .alias("ID_CONTA_CONTRATO"),
    F.col("regra")                                                  .alias("REGRA"),
    F.col("status")                                                 .alias("STATUS"),
    F.col("substatus")                                              .alias("SUBSTATUS"),
    F.col("observacao")                                             .alias("OBSERVACAO"),
    F.col("dados_billing")                                          .alias("DADOS_BILLING"),
    F.lit(None).cast("string")                                      .alias("DADOS_CONTRATO"),
    F.col("dados_tabela_verdade")                                   .alias("DADOS_TABELA_VERDADE"),
    F.lit(None).cast("string")                                      .alias("Produto"),
    F.col("tipo_servico")                                           .alias("Tipo_Servico"),
    F.lit(None).cast("string")                                      .alias("Desc_Servico"),
    F.col("tipo_imposto_mestre")                                    .alias("Tipo_Imposto"),
    F.lit(None).cast("string")                                      .alias("Promocao"),
    F.lit(None).cast("string")                                      .alias("Grupo_Localidade"),
    F.lit(ID_LOTE)                                                  .alias("ID_Lote"),
    F.col("sistema_origem")                                         .alias("CRM"),
    F.lit(None).cast("double")                                      .alias("JUROS_MULTA"),
    F.lit(None).cast("double")                                      .alias("VALOR_DESCONTO"),
    F.col("dt_carga")                                               .alias("DT_CARGA"),
    F.col("dt_processamento")                                       .alias("DT_ATUALIZACAO"),
    F.col("fatura_numero")                                          .alias("NUMERO_FATURA"),
    F.lit(None).cast("double")                                      .alias("VALOR_BILLING"),
    F.lit(None).cast("double")                                      .alias("VALOR_CONTRATO"),
    F.lit(None).cast("double")                                      .alias("VALOR_TABELA_VERDADE"),
    F.lit(None).cast("string")                                      .alias("DESCONTOS_NOMES"),
    F.lit(None).cast("string")                                      .alias("dt_cancelamento"),
)

df_insert.write.format("delta").mode("append").saveAsTable(TBL_RESULTADO)
cnt_tbl = spark.sql(f"SELECT COUNT(*) n FROM {TBL_RESULTADO} WHERE ID_Lote='{ID_LOTE}'").collect()[0]["n"]
print(f"Secao 12 INSERT: {cnt_final:,} inseridos | total na tabela ID_Lote={ID_LOTE}: {cnt_tbl:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. QA Resumo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CRM, REGRA, STATUS, SUBSTATUS,
# MAGIC   COUNT(*) n, COUNT(DISTINCT ID_CONTA_CONTRATO) cli, COUNT(DISTINCT FATURA) fat
# MAGIC FROM accenture.validacao_status_fatura WHERE ID_Lote='${ciclo_ref_lote}'
# MAGIC   AND REGRA IN ('VALIDACAO_IMPOSTOS','VALIDACAO_NFCOM')
# MAGIC GROUP BY 1,2,3,4 ORDER BY 1,2,3,4

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. QA Detalhamento por observacao

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT REGRA, SUBSTATUS, OBSERVACAO, COUNT(*) n, COUNT(DISTINCT ID_CONTA_CONTRATO) cli
# MAGIC FROM accenture.validacao_status_fatura WHERE ID_Lote='${ciclo_ref_lote}' AND STATUS='INCORRETO'
# MAGIC   AND REGRA IN ('VALIDACAO_IMPOSTOS','VALIDACAO_NFCOM')
# MAGIC GROUP BY 1,2,3 ORDER BY REGRA, n DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. QA ICMS por CST e UF

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OBSERVACAO, COUNT(*) n, COUNT(DISTINCT ID_CONTA_CONTRATO) cli
# MAGIC FROM accenture.validacao_status_fatura
# MAGIC WHERE ID_Lote='${ciclo_ref_lote}' AND REGRA='VALIDACAO_IMPOSTOS'
# MAGIC   AND OBSERVACAO LIKE '%ICMS_DIVERGENTE%'
# MAGIC GROUP BY 1 ORDER BY n DESC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. QA Resumo executivo

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(DISTINCT FATURA) fat, COUNT(DISTINCT ID_CONTA_CONTRATO) cli,
  SUM(CASE WHEN STATUS='CORRETO'   THEN 1 ELSE 0 END) ok,
  SUM(CASE WHEN STATUS='INCORRETO' THEN 1 ELSE 0 END) incorreto,
  SUM(CASE WHEN STATUS='ALERTA'    THEN 1 ELSE 0 END) alerta,
  SUM(CASE WHEN REGRA='VALIDACAO_IMPOSTOS' THEN 1 ELSE 0 END) impostos,
  SUM(CASE WHEN REGRA='VALIDACAO_NFCOM' THEN 1 ELSE 0 END) nfcom,
  ROUND(COUNT(DISTINCT CASE WHEN STATUS='CORRETO' THEN FATURA END)
    *100.0/NULLIF(COUNT(DISTINCT FATURA),0),2) pct_ok,
  ROUND(COUNT(DISTINCT CASE WHEN STATUS='INCORRETO' THEN FATURA END)
    *100.0/NULLIF(COUNT(DISTINCT FATURA),0),2) pct_incorreto,
  ROUND(COUNT(DISTINCT CASE WHEN STATUS='ALERTA' THEN FATURA END)
    *100.0/NULLIF(COUNT(DISTINCT FATURA),0),2) pct_alerta
FROM {TBL_RESULTADO} WHERE ID_Lote='{ID_LOTE}'
  AND REGRA IN ('VALIDACAO_IMPOSTOS','VALIDACAO_NFCOM')
""").show(truncate=False)