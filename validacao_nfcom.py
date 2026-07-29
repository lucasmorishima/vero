# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Validacao NFCOM x Tabelas Verdade v7
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

# ID_Lote no formato YYYY-MM usado pela tabela de resultado
ID_LOTE = f"{CICLO_REF[:4]}-{CICLO_REF[4:]}"
dbutils.widgets.text("ciclo_ref_lote", ID_LOTE, "Ciclo lote (AAAA-MM)")

TBL_STANDING  = "accenture.tab_validacoes_NFCOM_v4"
TBL_IMPOSTOS  = "accenture.tab_impostos_verdade_nova"
TBL_MESTRE    = "accenture.tab_mestre_nfcom_nova"
TBL_RESULTADO = "accenture.validacao_status_fatura"
TOL = 0.01

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

ms = df_ms.select(
    F.col("CCLASS_NFCON").alias("_m_cc"),
    F.col("CST").alias("_m_cst"),
    F.col("TAX_UF_MUNICIPIO").alias("_m_tipo"),
    F.col("GRUPO_CCLASS").alias("_m_gr"))

ms_icms = ms.filter(F.col("_m_tipo")=="ICMS")
ms_sem  = ms.filter(F.col("_m_tipo").isin("SEM","PIS_COFINS")).dropDuplicates(["_m_cc"])

df_i = (df_st.filter(F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE")))=="ICMS")
    .join(ms_icms, F.col("CCLASS")==ms_icms["_m_cc"], how="left"))
df_o = (df_st.filter((F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE")))!="ICMS")|F.col("TIPO_IMPOSTO_MESTRE").isNull())
    .join(ms_sem, F.col("CCLASS")==ms_sem["_m_cc"], how="left"))
df = df_i.unionByName(df_o, allowMissingColumns=True)
cnt_mestre = df.count()
print(f"Secao 5 JOIN Mestre: {cnt_mestre:,} registros | df_i={df_i.count():,} df_o={df_o.count():,}")
if cnt_mestre == 0: raise Exception("STOP Secao 5: 0 registros apos JOIN Mestre")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. JOIN Impostos

# COMMAND ----------

def _imp(tipo, alias):
    return (df_im.filter((F.upper(F.trim(F.col("TIPO_IMPOSTO")))==tipo)&(F.col("ALIQUOTA")>0))
        .select(F.upper(F.trim(F.col("ESTADO"))).alias(f"_ie_{alias}"),
                F.col("ALIQUOTA").cast(DecimalType(8,4)).alias(f"_i_{alias}"))
        .dropDuplicates([f"_ie_{alias}"]))

for t,a in [("ICMS","icms"),("ICMS_CONFAZ","confaz"),("ICMS_ESTADUAL","estadual")]:
    df = df.join(_imp(t,a), F.upper(F.trim(F.col("UF_DEST")))==F.col(f"_ie_{a}"), how="left")

df_nc = (df_im.filter(F.upper(F.trim(F.col("TIPO_IMPOSTO")))=="PIS_COFINS")
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

def adiv(a,b):
    return a.isNotNull() & b.isNotNull() & (F.abs(a-b)>F.lit(TOL).cast(DecimalType(8,4)))

mapeado = F.col("_m_cc").isNotNull()
item_icms = (F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE")))=="ICMS")
simples = F.upper(F.trim(F.col("REGIME_TRIB").cast(StringType()))).contains("SIMPLES")

icms_st=F.col("ICMS_STANDING").cast(DecimalType(8,4))
pis_st=F.col("PIS_STANDING").cast(DecimalType(8,4))
cof_st=F.col("COFINS_STANDING").cast(DecimalType(8,4))
fust_st=F.col("FUST_STANDING").cast(DecimalType(8,4))
ftl_st=F.col("FUNTTEL_STANDING").cast(DecimalType(8,4))
pis_esp=F.col("PIS_ESPERADO").cast(DecimalType(8,4))
cof_esp=F.col("COFINS_ESPERADO").cast(DecimalType(8,4))
fust_esp=F.col("FUST_ESPERADO").cast(DecimalType(8,4))
ftl_esp=F.col("FUNTTEL_ESPERADO").cast(DecimalType(8,4))

cst_n = F.regexp_replace(F.trim(F.col("CST_ICMS").cast(StringType())), r"\.0$","")
df = df.withColumn("_cst", cst_n)
cst_v = F.col("_cst").isNull() | F.col("_cst").isin("","nan","null","None","NaN")

uf_d = F.upper(F.trim(F.col("UF_DEST").cast(StringType())))
uf_e = F.upper(F.trim(F.coalesce(F.col("UF_EMIT_PARAMETRIZADA"),F.lit("SP"))))
df = df.withColumn("CFOP", F.regexp_replace(F.trim(F.col("CFOP").cast(StringType())), r"\.0$", ""))
cfop = F.col("CFOP")
cfop_ok = cfop.isNotNull() & ~cfop.isin("","null","nan","None")
grupo = F.col("GRUPO_CCLASS").cast(StringType())

cst_mestre_n = F.regexp_replace(F.trim(F.col("_m_cst").cast(StringType())),r"\.0$","")

icms_ref = (F.when(F.col("_cst")=="0", F.col("_i_icms"))
    .when(F.col("_cst")=="51", F.col("_i_confaz"))
    .when(F.col("_cst")=="40", F.lit(0).cast(DecimalType(8,4)))
    .otherwise(F.coalesce(F.col("_i_icms"),F.col("ICMS_ESPERADO").cast(DecimalType(8,4)))))
df = df.withColumn("_icms_ref", icms_ref)
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
    # CST esperado é determinado pelo TIPO_IMPOSTO_MESTRE do item:
    # ICMS         → CST deve ser '0'  (tributação normal)
    # ICMS_CONFAZ  → CST deve ser '51' (diferimento/CONFAZ)
    # ICMS_ESTADUAL→ CST deve ser '40' (isenção estadual)
    # Comparar com o CST declarado no standing
    mapeado & item_icms & ~cst_v & (
        ((F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE"))) == "ICMS")          & (F.col("_cst") != "0"))  |
        ((F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE"))) == "ICMS_CONFAZ")   & (F.col("_cst") != "51")) |
        ((F.upper(F.trim(F.col("TIPO_IMPOSTO_MESTRE"))) == "ICMS_ESTADUAL") & (F.col("_cst") != "40"))
    )
)
df = df.withColumn("_vi_cst_nulo", mapeado & item_icms & cst_v)
df = df.withColumn("_vi_icms_div", mapeado & item_icms & ~cst_v & adiv(icms_st, F.col("_icms_ref")))
df = df.withColumn("_vi_pis",
    mapeado & ~simples & item_icms & pis_st.isNotNull() & adiv(pis_st, pis_esp) & adiv(pis_st, F.col("_pis_nc")))
df = df.withColumn("_vi_cofins",
    mapeado & ~simples & item_icms & cof_st.isNotNull() & adiv(cof_st, cof_esp) & adiv(cof_st, F.col("_cofins_nc")))
df = df.withColumn("_vi_pis_cof_simples", mapeado & simples & (
    (pis_st.isNotNull() & adiv(pis_st, pis_esp) & adiv(pis_st, F.col("_pis_nc"))) |
    (cof_st.isNotNull() & adiv(cof_st, cof_esp) & adiv(cof_st, F.col("_cofins_nc")))))
df = df.withColumn("_vi_fust", mapeado & adiv(fust_st, fust_esp))
df = df.withColumn("_vi_funttel", mapeado & adiv(ftl_st, ftl_esp))
df = df.withColumn("_vi_icms_zero", item_icms & (icms_st.isNull()|(icms_st==F.lit(0).cast(DecimalType(8,4)))))

# --- VALIDACAO_NFCOM ---
df = df.withColumn("_vn_cclass", ~mapeado)
df = df.withColumn("_vn_cfop_inv", item_icms & cfop_ok & ~cfop.isin(CFOPS_OK))
df = df.withColumn("_vn_cfop_uf", item_icms & cfop_ok & cfop.isin(CFOPS_OK) & uf_d.isin(UFS) &
    (((uf_d==uf_e)&(F.substring(cfop,1,1)=="6"))|((uf_d!=uf_e)&(F.substring(cfop,1,1)=="5"))))
# R17 — CFOP_INCOMPATIVEL_TRIBUTO
# Aderência entre CFOP e tipo de tributação do item (TAX_UF_MUNICIPIO da tabela mestre):
# - Item ICMS (grupos 10,20,40,70)   → CFOP deve ser 5301-5307 ou 6301-6307 (nunca 5933/6933)
# - Item ISS/SVA/SEM (grupos 60,80,130,590) → CFOP deve ser 5933 ou 6933 (nunca 5301-5307)
# - Item financeiro (grupos 100,110)  → sem CFOP (tratado em _vn_fin_cfop)
CFOPS_ICMS_OK = ["5301","5302","5303","5304","5305","5306","5307",
                 "6301","6302","6303","6304","6305","6306","6307","1205","7301"]
GRUPOS_ICMS = ["10","20","40","70"]
GRUPOS_ISS  = ["60","80","130","590"]

df = df.withColumn("_vn_cfop_tributo",
    cfop_ok & mapeado & (
        # Item ICMS mas CFOP é 933 (ISS)
        (grupo.isin(GRUPOS_ICMS) & cfop.isin(CFOPS_ISS))
        |
        # Item ISS/SVA mas CFOP é de ICMS
        (grupo.isin(GRUPOS_ISS) & cfop.isin(CFOPS_ICMS_OK))
    )
)
df = df.withColumn("_vn_cfop933", cfop.isin(CFOPS_ISS) & ~cst_v)

# F1 — CFOP_AUSENTE (Rejeição 540 MOC NFCom)
# Item com CST informado (ICMS) obrigatoriamente deve ter CFOP preenchido
df = df.withColumn("_vn_cfop_ausente",
    item_icms & ~cst_v &
    (F.col("CFOP").isNull() | F.trim(F.col("CFOP").cast(StringType())).isin("","null","nan","None"))
)

# F2 — INDSEMCST_COM_CFOP (Rejeição 541 MOC NFCom)
# Item sem ICMS (ISS/SVA/financeiro) com CFOP preenchido — mais abrangente que _vn_cfop933
# Guarda: mapeado — itens sem mapeamento já disparam CCLASS_NAO_MAPEADO, não duplicar
df = df.withColumn("_vn_indsemcst_com_cfop",
    mapeado & (~item_icms) & cfop_ok &
    (~cfop.isin(CFOPS_ISS))   # 5933/6933 já cobertos em _vn_cfop933, evitar duplicidade
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
    (F.col("CCLASS_7D").cast(StringType()) != F.col("CCLASS").cast(StringType()))
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
    ("_vi_cst_incomp",       "CST_INCOMPATIVEL_TRIBUTO",   "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "CST declarado nao corresponde ao tipo de tributacao do CCLASS. CST 0=ICMS 51=CONFAZ 40=ESTADUAL isento"),
    ("_vi_cst_nulo",         "CST_ICMS_NULO",              "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "Item ICMS com CST nulo ou em branco"),
    ("_vi_icms_div",         "ICMS_DIVERGENTE",            "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "Aliquota ICMS diverge do esperado por UF e CST. CST 0=normal 51=CONFAZ 40=isento"),
    ("_vi_pis",              "PIS_DIVERGENTE",             "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "PIS diverge do cumulativo 0.65 e do nao-cumulativo 1.65"),
    ("_vi_cofins",           "COFINS_DIVERGENTE",          "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "COFINS diverge do cumulativo 3.0 e do nao-cumulativo 7.6"),
    ("_vi_pis_cof_simples",  "PIS_COFINS_SIMPLES_ALERTA",  "VALIDACAO_IMPOSTOS","ALERTA",
     "PIS ou COFINS divergente em Simples Nacional. Aliquotas DAS diferem das aliquotas padrao"),
    ("_vi_fust",             "FUST_INCORRETO",             "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "FUST diverge. MOC NFCom 1% ICMS e 0% sem ICMS"),
    ("_vi_funttel",          "FUNTTEL_INCORRETO",          "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "FUNTTEL diverge. MOC NFCom 0.5% ICMS e 0% sem ICMS"),
    ("_vi_icms_zero",        "ICMS_SEM_ALIQUOTA",          "VALIDACAO_IMPOSTOS","BLOQUEANTE",
     "Item ICMS com aliquota nula ou zero. Erro de parametrizacao no faturamento"),
    # NFCOM
    ("_vn_cclass",           "CCLASS_NAO_MAPEADO",         "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CCLASS nao existe na Tabela Mestre NFCOM. Codigo interno sem mapeamento fiscal valido"),
    ("_vn_cfop_inv",         "CFOP_INVALIDO",              "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CFOP fora da lista oficial de 17 CFOPs validos para NFCom modelo 62 (MOC SEFAZ)"),
    ("_vn_cfop_uf",          "CFOP_INCOMPATIVEL_UF",       "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CFOP incompativel com direcao geografica. Intraestadual exige 5xxx interestadual exige 6xxx"),
    ("_vn_cfop_tributo",     "CFOP_INCOMPATIVEL_TRIBUTO",  "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CFOP incompativel com o tipo de tributacao do item. ICMS exige CFOP 5301-5307 ou 6301-6307. ISS/SVA exige CFOP 5933 ou 6933"),
    ("_vn_cfop933",          "CFOP_933_COM_CST",           "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "CFOP 5933 ou 6933 (ISS) com CST ICMS preenchido. Itens ISS devem usar indSemCST sem CST"),
    ("_vn_fin_cfop",         "ITEM_FINANCEIRO_COM_CFOP",   "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item financeiro grupo 100 ou 110 com CFOP. Itens financeiros devem usar indSemCST sem CFOP"),
    ("_vn_uf_inv",           "UF_DEST_INVALIDA",           "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "UF destinatario ausente ou invalida. Campo obrigatorio para CFOP e ICMS"),
    ("_vn_fat_num",          "FATURA_SEM_NUMERO",          "VALIDACAO_NFCOM",  "ALERTA",
     "Numero de fatura ausente ou invalido. Possivel erro de geracao ou migracao"),
    # Novas — MOC NFCom SEFAZ
    ("_vn_cfop_ausente",     "CFOP_AUSENTE",               "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item com CST ICMS informado sem CFOP. Rejeicao 540 SEFAZ: CST obriga CFOP"),
    ("_vn_indsemcst_com_cfop","INDSEMCST_COM_CFOP",        "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item sem ICMS (ISS/SVA) com CFOP preenchido. Rejeicao 541 SEFAZ: indSemCST veda uso de CFOP"),
    ("_vn_cofat_com_icms",   "COFATURAMENTO_COM_ICMS",     "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item de cofaturamento (GRUPO 130) com ICMS destacado. Rejeicao 266 SEFAZ: cClass cofaturamento nao pode ter tributacao ICMS"),
    ("_vn_fatcent_com_icms", "FAT_CENTRALIZADO_COM_ICMS",  "VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Item de faturamento centralizado (GRUPO 120) com ICMS destacado. Rejeicao 269 SEFAZ: cClass faturamento centralizado nao pode ser tributado"),
    ("_vn_mun_prestacao",    "MUNICIPIO_PRESTACAO_AUSENTE","VALIDACAO_NFCOM",  "BLOQUEANTE",
     "Municipio de prestacao do servico ausente. Campo obrigatorio pela SEFAZ para emissao da NFCom"),
    ("_vn_tipo_fat_subst",   "TIPO_FAT_SUBSTITUICAO",      "VALIDACAO_NFCOM",  "ALERTA",
     "NFCom de substituicao detectada no ciclo. Revisar com area fiscal se substituicao foi corretamente emitida e referenciada"),
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
    # ICMS esperado conforme CST
    if cst == "0":
        icms_esp = f"ICMS_UF (TIPO=ICMS): {_sv(d.get('_i_icms'))}"
    elif cst == "51":
        icms_esp = f"ICMS_CONFAZ_UF: {_sv(d.get('_i_confaz'))}"
    elif cst == "40":
        icms_esp = "ICMS_ESTADUAL: 0 (isento hardcoded)"
    else:
        icms_esp = f"ICMS_UF: {_sv(d.get('_i_icms'))} | ICMS_ESPERADO_STANDING: {_sv(d.get('ICMS_ESPERADO'))}"
    return (
        f"{icms_esp} | "
        f"PIS_CUMUL: {_sv(d.get('PIS_ESPERADO'))} | PIS_NC: {_sv(d.get('_pis_nc'))} | "
        f"COFINS_CUMUL: {_sv(d.get('COFINS_ESPERADO'))} | COFINS_NC: {_sv(d.get('_cofins_nc'))} | "
        f"FUST_ESPERADO: {_sv(d.get('FUST_ESPERADO'))} | FUNTTEL_ESPERADO: {_sv(d.get('FUNTTEL_ESPERADO'))} | "
        f"CST_MESTRE: {_sv(d.get('_m_cst'))} | TIPO_TRIB_MESTRE: {_sv(d.get('_m_tipo'))}"
    )

def _billing_nfcom(d):
    """Monta dados_billing para VALIDACAO_NFCOM."""
    return (
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
        f"TIPO_TRIB_MESTRE: {_sv(d.get('_m_tipo'))} | "
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
        "uf_emit":str(d.get("UF_EMIT_PARAMETRIZADA") or "SP"),
        "tipo_servico":str(d.get("TIPO_SERVICO_ESPERADO_CCLASS") or ""),
        "regime_trib":str(d.get("REGIME_TRIB") or ""),
        "segmento":str(d.get("SEGMENTO") or ""),
        "tipo_imposto_mestre":str(d.get("TIPO_IMPOSTO_MESTRE") or ""),
        "grupo_cclass":str(d.get("GRUPO_CCLASS") or ""),
        "cst_icms":str(d.get("_cst") or ""),
        "regra":regra, "status":status, "substatus":substatus,
        "observacao":observacao,
        "dados_billing":dados_billing,
        "dados_tabela_verdade":dados_tabela_verdade,
        "icms_standing":d.get("ICMS_STANDING"), "icms_esperado":d.get("ICMS_ESPERADO"),
        "icms_verdade_uf":d.get("_i_icms"), "icms_confaz_uf":d.get("_i_confaz"),
        "pis_standing":d.get("PIS_STANDING"), "cofins_standing":d.get("COFINS_STANDING"),
        "fust_standing":d.get("FUST_STANDING"), "funttel_standing":d.get("FUNTTEL_STANDING"),
        "pis_esperado":d.get("PIS_ESPERADO"), "cofins_esperado":d.get("COFINS_ESPERADO"),
        "fust_esperado":d.get("FUST_ESPERADO"), "funttel_esperado":d.get("FUNTTEL_ESPERADO"),
        "pis_nc":d.get("_pis_nc"), "cofins_nc":d.get("_cofins_nc"),
        "cst_esperado_mestre":str(d.get("_m_cst") or ""),
        "tipo_trib_mestre":str(d.get("_m_tipo") or ""),
        "impacto_icms_r":d.get("IMPACTO_ICMS_ESTIMADO_R$"),
    }

# Colunas necessarias na explosao
cols_base = [
    "_ciclo","FATURA_NUMERO","ID_CLIENTE","SISTEMA_ORIGEM","CCLASS","CFOP",
    "UF_DEST","UF_EMIT_PARAMETRIZADA","TIPO_SERVICO_ESPERADO_CCLASS",
    "REGIME_TRIB","SEGMENTO","TIPO_IMPOSTO_MESTRE","GRUPO_CCLASS","_cst",
    "ICMS_STANDING","ICMS_ESPERADO","_i_icms","_i_confaz",
    "PIS_STANDING","COFINS_STANDING","FUST_STANDING","FUNTTEL_STANDING",
    "PIS_ESPERADO","COFINS_ESPERADO","FUST_ESPERADO","FUNTTEL_ESPERADO",
    "_pis_nc","_cofins_nc","_m_cst","_m_tipo","IMPACTO_ICMS_ESTIMADO_R$",
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
    StructField("segmento",             StringType()),
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
    StructField("pis_standing",         DoubleType()),
    StructField("cofins_standing",      DoubleType()),
    StructField("fust_standing",        DoubleType()),
    StructField("funttel_standing",     DoubleType()),
    StructField("pis_esperado",         DoubleType()),
    StructField("cofins_esperado",      DoubleType()),
    StructField("fust_esperado",        DoubleType()),
    StructField("funttel_esperado",     DoubleType()),
    StructField("pis_nc",               DoubleType()),
    StructField("cofins_nc",            DoubleType()),
    StructField("cst_esperado_mestre",  StringType()),
    StructField("tipo_trib_mestre",     StringType()),
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
                verdade = _verdade_impostos(d) if cat == "VALIDACAO_IMPOSTOS" else _verdade_nfcom(d)

                if items:
                    # 1 linha por tag disparada — sem concatenação
                    for tag, sev, obs in items:
                        linhas.append(_row(
                            d, cat, "INCORRETO", sev,
                            f"{tag}: {obs}",   # observacao = apenas esta tag
                            billing, verdade
                        ))
                else:
                    # Item sem erro nesta categoria → 1 linha CORRETO/OK
                    linhas.append(_row(d, cat, "CORRETO", "OK", "Validacoes aprovadas", billing, verdade))

        if linhas:
            yield pd.DataFrame(linhas, columns=_OUT_COLS)
        else:
            yield pd.DataFrame(columns=_OUT_COLS)

df_exp = df_base.mapInPandas(explodir_pandas, schema=OUT_SCHEMA)

cnt_t = df_exp.count()
cnt_ok = df_exp.filter(F.col("status")=="CORRETO").count()
cnt_er = df_exp.filter(F.col("status")=="INCORRETO").count()
print(f"Secao 10 Explosao: {cnt_t:,} linhas | CORRETO={cnt_ok:,} | INCORRETO={cnt_er:,}")
if cnt_t == 0: raise Exception("STOP Secao 10: df_exp vazio apos mapInPandas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. PK, cast, hash

# COMMAND ----------

for c in ["icms_standing","icms_esperado","icms_verdade_uf","icms_confaz_uf",
          "pis_standing","cofins_standing","fust_standing","funttel_standing",
          "pis_esperado","cofins_esperado","fust_esperado","funttel_esperado","pis_nc","cofins_nc"]:
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

# ID_Lote na tabela usa formato YYYY-MM (ex: "2026-07")
ID_LOTE = f"{CICLO_REF[:4]}-{CICLO_REF[4:]}"

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
    F.col("segmento")                                               .alias("SEGMENTO"),
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
  SUM(CASE WHEN STATUS='CORRETO' THEN 1 ELSE 0 END) ok,
  SUM(CASE WHEN REGRA='VALIDACAO_IMPOSTOS' THEN 1 ELSE 0 END) impostos,
  SUM(CASE WHEN REGRA='VALIDACAO_NFCOM' THEN 1 ELSE 0 END) nfcom,
  SUM(CASE WHEN SUBSTATUS='BLOQUEANTE' THEN 1 ELSE 0 END) bloq,
  SUM(CASE WHEN SUBSTATUS='ALERTA' THEN 1 ELSE 0 END) alerta,
  ROUND(COUNT(DISTINCT CASE WHEN STATUS='CORRETO' THEN FATURA END)
    *100.0/NULLIF(COUNT(DISTINCT FATURA),0),2) pct_ok
FROM {TBL_RESULTADO} WHERE ID_Lote='{ID_LOTE}'
  AND REGRA IN ('VALIDACAO_IMPOSTOS','VALIDACAO_NFCOM')
""").show(truncate=False)