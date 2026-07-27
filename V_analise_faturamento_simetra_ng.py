# Databricks notebook source

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import Window

displayHTML = globals().get("displayHTML", lambda html: print(html))
spark = globals().get("spark")  # built-in Databricks SparkSession

# --------------------------------------------------------------------------
# Parâmetros
# --------------------------------------------------------------------------
MES_CORTE_SIMETRA = "2026-02"   # Último mês SIMETRA antes da migração
MES_INICIO_NG     = "2026-03"   # Primeiro mês NG após migração
MES_PAROU         = "2026-04"   # ULTIMO_MES <= esse valor → PAROU

# --------------------------------------------------------------------------
# Carga de dados — SIMETRA + NG (union)
# --------------------------------------------------------------------------
MESES = "('2025-12','2026-01','2026-02','2026-03','2026-04','2026-05','2026-06','2026-07')"

sql_query = f"""
SELECT
    'SIMETRA'                                                          AS SISTEMA,
    np.A1_CGC                                                          AS CPF_CNPJ,
    DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM')           AS ANO_MES,
    REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')                    AS FATURA,
    COUNT(DISTINCT np.C6_CONTRT)                                       AS QTD_CONTRATO,
    SUM(FT_VALCONT)                                                    AS VALOR_FATURADO
FROM NEGOCIO.TB_FATURAMENTO_PROTHEUS_COMPLETA np
WHERE DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM') IN {MESES}
GROUP BY
    np.A1_CGC,
    DATE_FORMAT(TO_DATE(FT_EMISSAO, 'yyyyMMdd'), 'yyyy-MM'),
    REGEXP_REPLACE(trim(C5_X_SIMET), '[^0-9]', '')

UNION ALL

SELECT
    'NG'                                                               AS SISTEMA,
    CPF_CNPJ                                                           AS CPF_CNPJ,
    try_cast(MESANO AS STRING)                                         AS ANO_MES,
    FATURA_NUMERO                                                      AS FATURA,
    COUNT(idcontrato)                                                  AS QTD_CONTRATO,
    SUM(try_cast(NULLIF(trim(ReceitaBruta), '') AS DOUBLE))            AS VALOR_FATURADO
FROM hive_metastore.gold.RELATORIOFATURAMENTO_22_PLUS
WHERE try_cast(MESANO AS STRING) IN {MESES}
and crm = 'NG'
GROUP BY
    CPF_CNPJ,
    try_cast(MESANO AS STRING),
    FATURA_NUMERO
"""

sdf = spark.sql(sql_query)
sdf.createOrReplaceTempView("vw_faturamento")
sdf = spark.table("vw_faturamento")
print(f"Total de registros carregados: {sdf.count():,}")

# COMMAND ----------

# --------------------------------------------------------------------------
# Tendência por CNPJ
# --------------------------------------------------------------------------
cnpj_mes = (
    sdf
    .filter(F.col("VALOR_FATURADO").isNotNull() & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ", "ANO_MES")
    .agg(
        F.sum("VALOR_FATURADO").alias("VALOR"),
        F.sum("QTD_CONTRATO").alias("QTD_CONTRATO"),
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
    .select("CPF_CNPJ", F.col("ANO_MES").alias("PRIMEIRO_MES"), F.col("VALOR").alias("PRIMEIRO_VALOR"))
)

last_vals = (
    cnpj_ranked.filter(F.col("rn_desc") == 1)
    .select("CPF_CNPJ", F.col("ANO_MES").alias("ULTIMO_MES"), F.col("VALOR").alias("ULTIMO_VALOR"))
)

cnpj_stats = (
    cnpj_mes
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR").alias("TOTAL_FATURADO"),
        F.countDistinct("ANO_MES").alias("MESES_ATIVOS"),
        F.avg("VALOR").alias("MEDIA_MENSAL"),
        F.sum("QTD_CONTRATO").alias("TOTAL_CONTRATOS"),
    )
    .join(first_vals, on="CPF_CNPJ", how="left")
    .join(last_vals,  on="CPF_CNPJ", how="left")
    .withColumn("TENDENCIA",
        F.when(F.col("MESES_ATIVOS") == 1, "DADOS INSUFICIENTES")
         .when(F.col("ULTIMO_MES") <= MES_PAROU, "PAROU")
         .when(F.col("ULTIMO_VALOR") > F.col("PRIMEIRO_VALOR") * 1.10, "CRESCIMENTO")
         .when(F.col("ULTIMO_VALOR") < F.col("PRIMEIRO_VALOR") * 0.90, "QUEDA")
         .otherwise("ESTÁVEL")
    )
)

# --------------------------------------------------------------------------
# Migração SIMETRA Fev → NG Mar
# --------------------------------------------------------------------------
cnpjs_simetra_fev = (
    sdf
    .filter((F.col("SISTEMA") == "SIMETRA") & (F.col("ANO_MES") == MES_CORTE_SIMETRA))
    .select("CPF_CNPJ").distinct()
    .withColumn("ERA_SIMETRA_FEV", F.lit(True))
)

cnpjs_ng_mar = (
    sdf
    .filter((F.col("SISTEMA") == "NG") & (F.col("ANO_MES") == MES_INICIO_NG))
    .select("CPF_CNPJ").distinct()
    .withColumn("FOI_NG_MAR", F.lit(True))
)

cnpj_stats = (
    cnpj_stats
    .join(cnpjs_simetra_fev, on="CPF_CNPJ", how="left")
    .join(cnpjs_ng_mar,      on="CPF_CNPJ", how="left")
    .withColumn("ERA_SIMETRA_FEV", F.coalesce(F.col("ERA_SIMETRA_FEV"), F.lit(False)))
    .withColumn("FOI_NG_MAR",      F.coalesce(F.col("FOI_NG_MAR"),      F.lit(False)))
    .withColumn("STATUS_MIGRACAO",
        F.when(F.col("ERA_SIMETRA_FEV") & F.col("FOI_NG_MAR"),  "MIGROU")
         .when(F.col("ERA_SIMETRA_FEV") & ~F.col("FOI_NG_MAR"), "SUMIU")
         .otherwise("N/A")
    )
)

# --------------------------------------------------------------------------
# Duplicidade: CNPJ em ambos os sistemas no mesmo mês
# --------------------------------------------------------------------------
sistemas_por_cnpj_mes = (
    sdf
    .groupBy("CPF_CNPJ", "ANO_MES")
    .agg(F.collect_set("SISTEMA").alias("SISTEMAS"))
)

duplicados = (
    sistemas_por_cnpj_mes
    .filter(F.size(F.col("SISTEMAS")) > 1)
    .groupBy("CPF_CNPJ")
    .agg(F.count("ANO_MES").alias("MESES_DUPLICADOS"))
)

cnpj_stats = (
    cnpj_stats
    .join(duplicados, on="CPF_CNPJ", how="left")
    .withColumn("MESES_DUPLICADOS", F.coalesce(F.col("MESES_DUPLICADOS"), F.lit(0)))
)

cnpj_stats.createOrReplaceTempView("vw_cnpj_stats")
cnpj_stats = spark.table("vw_cnpj_stats")

# COMMAND ----------

# --------------------------------------------------------------------------
# Impacto da migração: média mensal pré vs pós (somando todos os sistemas)
# --------------------------------------------------------------------------
MES_PRE  = ["2025-12", "2026-01", "2026-02"]
MES_POST = ["2026-03", "2026-04", "2026-05", "2026-06", "2026-07"]

pre_mig = (
    sdf
    .filter(F.col("ANO_MES").isin(MES_PRE) & (F.col("VALOR_FATURADO") > 0))
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
    sdf
    .filter(F.col("ANO_MES").isin(MES_POST) & (F.col("VALOR_FATURADO") > 0))
    .groupBy("CPF_CNPJ")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL_POS"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS_POS"),
        F.countDistinct("ANO_MES").alias("MESES_POS"),
    )
    .withColumn("MEDIA_POS",  F.when(F.col("MESES_POS")    > 0, F.col("TOTAL_POS") / F.col("MESES_POS")))
    .withColumn("TICKET_POS", F.when(F.col("CONTRATOS_POS") > 0, F.col("TOTAL_POS") / F.col("CONTRATOS_POS")))
)

impacto_migracao = (
    pre_mig.join(post_mig, on="CPF_CNPJ", how="inner")
    .withColumn("VAR_MEDIA_ABS", F.col("MEDIA_POS") - F.col("MEDIA_PRE"))
    .withColumn("VAR_MEDIA_PCT",
        F.when(F.col("MEDIA_PRE") > 0,
            (F.col("MEDIA_POS") - F.col("MEDIA_PRE")) / F.col("MEDIA_PRE") * 100)
    )
    .withColumn("MEDIA_CONTR_PRE", F.when(F.col("MESES_PRE") > 0, F.col("CONTRATOS_PRE") / F.col("MESES_PRE")))
    .withColumn("MEDIA_CONTR_POS", F.when(F.col("MESES_POS") > 0, F.col("CONTRATOS_POS") / F.col("MESES_POS")))
    .withColumn("VAR_CONTRATO_PCT",
        F.when(F.col("MEDIA_CONTR_PRE") > 0,
            (F.col("MEDIA_CONTR_POS") - F.col("MEDIA_CONTR_PRE")) / F.col("MEDIA_CONTR_PRE") * 100)
    )
    .withColumn("VAR_TICKET_PCT",
        F.when(F.col("TICKET_PRE") > 0,
            (F.col("TICKET_POS") - F.col("TICKET_PRE")) / F.col("TICKET_PRE") * 100)
    )
    .withColumn("CAUSA",
        F.when(F.col("VAR_MEDIA_ABS") >= 0, "Crescimento")
         .when(
            F.abs(F.col("VAR_CONTRATO_PCT")) >= F.abs(F.col("VAR_TICKET_PCT")),
            "Redução de contratos"
         )
         .when(
            F.abs(F.col("VAR_TICKET_PCT")) > F.abs(F.col("VAR_CONTRATO_PCT")),
            "Redução de ticket"
         )
         .otherwise("Mista")
    )
    .filter(F.col("VAR_MEDIA_ABS") < 0)
    .orderBy("VAR_MEDIA_ABS")
)

impacto_rows = impacto_migracao.collect()
total_perda_estimada = sum((r["VAR_MEDIA_ABS"] or 0) for r in impacto_rows)

# COMMAND ----------

# --------------------------------------------------------------------------
# ML — Regressão linear mensal: faturamento, contratos e ticket médio
# --------------------------------------------------------------------------
monthly_agg = (
    sdf
    .filter(F.col("VALOR_FATURADO").isNotNull() & (F.col("VALOR_FATURADO") > 0))
    .groupBy("ANO_MES")
    .agg(
        F.sum("VALOR_FATURADO").alias("TOTAL"),
        F.sum("QTD_CONTRATO").alias("CONTRATOS"),
        F.countDistinct("FATURA").alias("FATURAS"),
        F.countDistinct("CPF_CNPJ").alias("CNPJS_ATIVOS"),
    )
    .withColumn("TICKET_MEDIO", F.when(F.col("CONTRATOS") > 0, F.col("TOTAL") / F.col("CONTRATOS")))
    .orderBy("ANO_MES")
)

monthly_rows = monthly_agg.collect()

def regressao(valores):
    y = np.array(valores, dtype=float)
    x = np.arange(len(y), dtype=float)
    coef = np.polyfit(x, y, 1)
    slope = float(coef[0])
    y_pred = np.polyval(coef, x)
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return slope, round(r2, 3)

totais_list    = [float(r["TOTAL"]       or 0) for r in monthly_rows]
contratos_list = [float(r["CONTRATOS"]   or 0) for r in monthly_rows]
tickets_list   = [float(r["TICKET_MEDIO"]or 0) for r in monthly_rows]
cnpjs_list     = [float(r["CNPJS_ATIVOS"]or 0) for r in monthly_rows]

slope_total,     r2_total    = regressao(totais_list)
slope_contratos, r2_contr    = regressao(contratos_list)
slope_ticket,    r2_ticket   = regressao(tickets_list)
slope_cnpjs,     r2_cnpjs   = regressao(cnpjs_list)

# Diagnóstico automático
if slope_total < 0:
    ml_direcao = "QUEDA"
    ml_cor = "#c0392b"
elif slope_total > 0:
    ml_direcao = "CRESCIMENTO"
    ml_cor = "#1a7a3c"
else:
    ml_direcao = "ESTÁVEL"
    ml_cor = "#7f8c8d"

if slope_contratos < 0 and slope_ticket >= 0:
    ml_causa = "Principalmente redução na quantidade de contratos/faturas"
elif slope_ticket < 0 and slope_contratos >= 0:
    ml_causa = "Principalmente redução no valor médio por contrato (ticket médio)"
elif slope_contratos < 0 and slope_ticket < 0:
    ml_causa = "Mista: menos contratos E menor ticket médio por contrato"
else:
    ml_causa = "Crescimento em quantidade e/ou ticket médio"

# COMMAND ----------

# --------------------------------------------------------------------------
# Coleta dos dados para exibição
# --------------------------------------------------------------------------
def fmt_brl(valor):
    if valor is None:
        return "R$ 0,00"
    return "R$ {:,.2f}".format(valor).replace(",", "X").replace(".", ",").replace("X", ".")

tendencia_resumo = (
    cnpj_stats
    .groupBy("TENDENCIA")
    .agg(
        F.count("CPF_CNPJ").alias("QTD_CNPJ"),
        F.sum("TOTAL_FATURADO").alias("FATURAMENTO_TOTAL"),
    )
    .orderBy(F.desc("QTD_CNPJ"))
    .collect()
)

migracao_resumo = (
    cnpj_stats
    .filter(F.col("ERA_SIMETRA_FEV"))
    .groupBy("STATUS_MIGRACAO")
    .agg(
        F.count("CPF_CNPJ").alias("QTD"),
        F.sum("TOTAL_FATURADO").alias("FATURAMENTO_TOTAL"),
    )
    .collect()
)

total_duplicados = cnpj_stats.filter(F.col("MESES_DUPLICADOS") > 0).count()
fat_duplicado = (
    sdf
    .join(
        sistemas_por_cnpj_mes.filter(F.size(F.col("SISTEMAS")) > 1).select("CPF_CNPJ", "ANO_MES"),
        on=["CPF_CNPJ", "ANO_MES"],
        how="inner",
    )
    .agg(F.sum("VALOR_FATURADO").alias("TOTAL"))
    .collect()[0]["TOTAL"] or 0
)

top100 = (
    cnpj_stats
    .orderBy(F.desc("TOTAL_FATURADO"))
    .limit(100)
    .collect()
)

top20_problemas = [
    r for r in top100
    if r["TENDENCIA"] in ("PAROU", "QUEDA")
    or r["STATUS_MIGRACAO"] == "SUMIU"
    or r["MESES_DUPLICADOS"] > 0
][:20]

# Mês com maior queda mês a mês por CNPJ
w_lag = Window.partitionBy("CPF_CNPJ").orderBy("ANO_MES")

pior_mes_queda = (
    cnpj_mes
    .withColumn("VALOR_ANTERIOR", F.lag("VALOR").over(w_lag))
    .filter(F.col("VALOR_ANTERIOR").isNotNull())
    .withColumn("VARIACAO_MES", F.col("VALOR") - F.col("VALOR_ANTERIOR"))
    .filter(F.col("VARIACAO_MES") < 0)
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("CPF_CNPJ").orderBy("VARIACAO_MES")  # menor = maior queda
    ))
    .filter(F.col("rn") == 1)
    .select("CPF_CNPJ",
            F.col("ANO_MES").alias("MES_MAIOR_QUEDA"),
            F.col("VARIACAO_MES").alias("QUEDA_NO_MES"))
)

# Coleta só para os CNPJs do top100 (para não trazer tudo)
cnpjs_top100 = [r["CPF_CNPJ"] for r in top100]
pior_mes_map = {
    r["CPF_CNPJ"]: r
    for r in pior_mes_queda.filter(F.col("CPF_CNPJ").isin(cnpjs_top100)).collect()
}

top100_queda = (
    cnpj_stats
    .filter(F.col("MESES_ATIVOS") >= 2)
    .withColumn("QUEDA_ABSOLUTA", F.col("PRIMEIRO_VALOR") - F.col("ULTIMO_VALOR"))
    .withColumn("QUEDA_PERCENTUAL",
        F.when(F.col("PRIMEIRO_VALOR") > 0,
            (F.col("PRIMEIRO_VALOR") - F.col("ULTIMO_VALOR")) / F.col("PRIMEIRO_VALOR") * 100)
    )
    .filter(F.col("QUEDA_ABSOLUTA") > 0)
    .orderBy(F.desc("QUEDA_ABSOLUTA"))
    .limit(100)
    .collect()
)

faturamento_por_mes = (
    sdf
    .groupBy("ANO_MES", "SISTEMA")
    .agg(F.sum("VALOR_FATURADO").alias("TOTAL"))
    .orderBy("ANO_MES", "SISTEMA")
    .collect()
)

# COMMAND ----------

# --------------------------------------------------------------------------
# Exibição em HTML
# --------------------------------------------------------------------------
CORES = {
    "CRESCIMENTO":        "#1a7a3c",
    "ESTÁVEL":            "#1a4fa0",
    "QUEDA":              "#c0392b",
    "PAROU":              "#7d3c98",
    "DADOS INSUFICIENTES":"#7f8c8d",
    "MIGROU":             "#1a7a3c",
    "SUMIU":              "#c0392b",
    "N/A":                "#7f8c8d",
}

def badge(texto):
    cor = CORES.get(texto, "#555")
    return f'<span style="background:{cor};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px">{texto}</span>'

# --- Faturamento por mês e sistema ---
meses = sorted(set(r["ANO_MES"] for r in faturamento_por_mes))
sistemas = ["SIMETRA", "NG"]
fat_map = {(r["ANO_MES"], r["SISTEMA"]): r["TOTAL"] for r in faturamento_por_mes}

rows_fat = ""
for mes in meses:
    vals = [fat_map.get((mes, s), 0) for s in sistemas]
    total = sum(v for v in vals if v)
    rows_fat += f"<tr><td>{mes}</td>"
    for v in vals:
        rows_fat += f"<td>{fmt_brl(v) if v else '-'}</td>"
    rows_fat += f"<td><b>{fmt_brl(total)}</b></td></tr>"

# --- Tendências ---
rows_tend = ""
for r in tendencia_resumo:
    rows_tend += (
        f"<tr>"
        f"<td>{badge(r['TENDENCIA'])}</td>"
        f"<td style='text-align:right'>{r['QTD_CNPJ']:,}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['FATURAMENTO_TOTAL'])}</td>"
        f"</tr>"
    )

# --- Migração ---
rows_mig = ""
for r in migracao_resumo:
    rows_mig += (
        f"<tr>"
        f"<td>{badge(r['STATUS_MIGRACAO'])}</td>"
        f"<td style='text-align:right'>{r['QTD']:,}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['FATURAMENTO_TOTAL'])}</td>"
        f"</tr>"
    )

# --- Top 100 maiores quedas ---
rows_queda = ""
for i, r in enumerate(top100_queda, 1):
    pct = r["QUEDA_PERCENTUAL"] or 0
    rows_queda += (
        f"<tr>"
        f"<td style='text-align:center'>{i}</td>"
        f"<td>{r['CPF_CNPJ']}</td>"
        f"<td>{badge(r['TENDENCIA'])}</td>"
        f"<td style='text-align:center'>{r['PRIMEIRO_MES'] or '-'}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['PRIMEIRO_VALOR'])}</td>"
        f"<td style='text-align:center'>{r['ULTIMO_MES'] or '-'}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['ULTIMO_VALOR'])}</td>"
        f"<td style='text-align:right;color:#c0392b'>{fmt_brl(r['QUEDA_ABSOLUTA'])}</td>"
        f"<td style='text-align:right;color:#c0392b'>{pct:.1f}%</td>"
        f"<td style='text-align:center'>{r['MESES_ATIVOS']}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['TOTAL_FATURADO'])}</td>"
        f"</tr>"
    )

# --- ML: tabela mensal com variação M/M ---
rows_mensal = ""
prev = {}
for r in monthly_rows:
    total     = float(r["TOTAL"]        or 0)
    contratos = float(r["CONTRATOS"]    or 0)
    ticket    = float(r["TICKET_MEDIO"] or 0)
    cnpjs     = int(r["CNPJS_ATIVOS"]   or 0)

    def var_td(curr, ant):
        if not ant:
            return "<td>-</td>"
        pct = (curr - ant) / ant * 100
        cor = "#1a7a3c" if pct >= 0 else "#c0392b"
        return f"<td style='text-align:right;color:{cor}'>{pct:+.1f}%</td>"

    rows_mensal += (
        f"<tr>"
        f"<td>{r['ANO_MES']}</td>"
        f"<td style='text-align:right'>{fmt_brl(total)}</td>"
        f"{var_td(total,     prev.get('total'))}"
        f"<td style='text-align:right'>{int(contratos):,}</td>"
        f"{var_td(contratos, prev.get('contratos'))}"
        f"<td style='text-align:right'>{fmt_brl(ticket)}</td>"
        f"{var_td(ticket,    prev.get('ticket'))}"
        f"<td style='text-align:center'>{cnpjs:,}</td>"
        f"</tr>"
    )
    prev = {"total": total, "contratos": contratos, "ticket": ticket}

# --- Impacto migração por CNPJ ---
rows_impacto = ""
for i, r in enumerate(impacto_rows, 1):
    cor_v = "#c0392b"
    cor_c = "#c0392b" if (r["VAR_CONTRATO_PCT"] or 0) < 0 else "#1a7a3c"
    cor_t = "#c0392b" if (r["VAR_TICKET_PCT"]   or 0) < 0 else "#1a7a3c"
    rows_impacto += (
        f"<tr>"
        f"<td style='text-align:center'>{i}</td>"
        f"<td>{r['CPF_CNPJ']}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['MEDIA_PRE'])}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['MEDIA_POS'])}</td>"
        f"<td style='text-align:right;color:{cor_v}'>{fmt_brl(r['VAR_MEDIA_ABS'])}</td>"
        f"<td style='text-align:right;color:{cor_v}'>{(r['VAR_MEDIA_PCT'] or 0):.1f}%</td>"
        f"<td style='text-align:right;color:{cor_c}'>{(r['VAR_CONTRATO_PCT'] or 0):.1f}%</td>"
        f"<td style='text-align:right;color:{cor_t}'>{(r['VAR_TICKET_PCT']   or 0):.1f}%</td>"
        f"<td>{r['CAUSA']}</td>"
        f"</tr>"
    )

# --- Top 20 problema ---
rows_top = ""
for i, r in enumerate(top20_problemas, 1):
    alertas = []
    if r["TENDENCIA"] in ("PAROU", "QUEDA"):
        alertas.append(badge(r["TENDENCIA"]))
    if r["STATUS_MIGRACAO"] == "SUMIU":
        alertas.append(badge("SUMIU"))
    if r["MESES_DUPLICADOS"] > 0:
        alertas.append(badge(f"DUPLICADO {r['MESES_DUPLICADOS']}x"))
    variacao = ""
    if r["PRIMEIRO_VALOR"] and r["ULTIMO_VALOR"] and r["MESES_ATIVOS"] > 1:
        pct = (r["ULTIMO_VALOR"] - r["PRIMEIRO_VALOR"]) / r["PRIMEIRO_VALOR"] * 100
        cor_pct = "#1a7a3c" if pct >= 0 else "#c0392b"
        variacao = f'<span style="color:{cor_pct}">{pct:+.1f}%</span>'
    pm = pior_mes_map.get(r["CPF_CNPJ"])
    if pm:
        mes_queda  = pm["MES_MAIOR_QUEDA"]
        val_queda  = pm["QUEDA_NO_MES"] or 0
        cell_queda = (
            f"<span style='color:#c0392b'>{mes_queda}: "
            f"{fmt_brl(abs(val_queda))}</span>"
        )
    else:
        cell_queda = "-"
    rows_top += (
        f"<tr>"
        f"<td style='text-align:center'>{i}</td>"
        f"<td>{r['CPF_CNPJ']}</td>"
        f"<td style='text-align:right'>{fmt_brl(r['TOTAL_FATURADO'])}</td>"
        f"<td style='text-align:center'>{r['MESES_ATIVOS']}</td>"
        f"<td style='text-align:center'>{int(r['TOTAL_CONTRATOS'] or 0):,}</td>"
        f"<td style='text-align:right'>{variacao}</td>"
        f"<td style='text-align:right'>{cell_queda}</td>"
        f"<td>{'  '.join(alertas)}</td>"
        f"</tr>"
    )

# --------------------------------------------------------------------------
# Gráfico Físico × Financeiro + Insights de migração
# --------------------------------------------------------------------------
import json

chart_meses  = [r["ANO_MES"]         for r in monthly_rows]
chart_totais = [round(float(r["TOTAL"]        or 0), 2) for r in monthly_rows]
chart_contr  = [int  (r["CONTRATOS"]  or 0)             for r in monthly_rows]
chart_ticket = [round(float(r["TICKET_MEDIO"] or 0), 2) for r in monthly_rows]

mig_idx  = chart_meses.index("2026-02") if "2026-02" in chart_meses else -1
mig_idx2 = chart_meses.index("2026-03") if "2026-03" in chart_meses else -1

# Variações M/M
def mm_pct(lst):
    return [
        round((lst[i] - lst[i-1]) / lst[i-1] * 100, 1) if lst[i-1] else 0
        for i in range(1, len(lst))
    ]

mm_fat  = mm_pct(chart_totais)
mm_contr = mm_pct(chart_contr)

# Média pré e pós migração
if 0 <= mig_idx < len(chart_meses) - 1:
    split = mig_idx + 1
    pre_fat  = sum(chart_totais[:split]) / split
    post_fat = sum(chart_totais[split:]) / max(len(chart_totais[split:]), 1)
    pre_con  = sum(chart_contr[:split])  / split
    post_con = sum(chart_contr[split:])  / max(len(chart_contr[split:]), 1)
    fat_mig_pct = (post_fat - pre_fat) / pre_fat * 100 if pre_fat else 0
    con_mig_pct = (post_con - pre_con) / pre_con * 100 if pre_con else 0
else:
    pre_fat = post_fat = pre_con = post_con = 0
    fat_mig_pct = con_mig_pct = 0

# Piores meses M/M
def pior(lst, meses):
    if not lst:
        return "-", 0
    idx = lst.index(min(lst))
    return meses[idx + 1], lst[idx]

pior_mes_fat,  pior_fat_pct  = pior(mm_fat,   chart_meses)
pior_mes_contr, pior_contr_pct = pior(mm_contr, chart_meses)

# Geração dos insights
def sinal(v, positivo="verde", negativo="vermelho"):
    return negativo if v < 0 else positivo

insights_html = ""

def insight(cor, texto):
    cores = {"red": "#c0392b", "green": "#1a7a3c", "orange": "#d35400", "blue": "#1a4fa0"}
    c = cores.get(cor, "#555")
    return f'<li style="margin:6px 0"><span style="color:{c};font-weight:bold">▶</span> {texto}</li>'

if fat_mig_pct < -5:
    insights_html += insight("red",
        f"Faturamento médio caiu <b>{fat_mig_pct:.1f}%</b> após a migração "
        f"(pré: {fmt_brl(pre_fat)}/mês → pós: {fmt_brl(post_fat)}/mês)."
    )
elif fat_mig_pct > 5:
    insights_html += insight("green",
        f"Faturamento médio cresceu <b>{fat_mig_pct:.1f}%</b> após a migração "
        f"(pré: {fmt_brl(pre_fat)}/mês → pós: {fmt_brl(post_fat)}/mês)."
    )
else:
    insights_html += insight("blue",
        f"Faturamento médio estável após migração ({fat_mig_pct:+.1f}%)."
    )

if con_mig_pct < -5:
    insights_html += insight("red",
        f"Quantidade de contratos caiu <b>{con_mig_pct:.1f}%</b> após a migração "
        f"(pré: {pre_con:,.0f}/mês → pós: {post_con:,.0f}/mês)."
    )
elif con_mig_pct > 5:
    insights_html += insight("green",
        f"Quantidade de contratos cresceu <b>{con_mig_pct:.1f}%</b> após a migração."
    )

if pior_fat_pct < -5:
    insights_html += insight("orange",
        f"Maior queda financeira M/M em <b>{pior_mes_fat}</b>: {pior_fat_pct:+.1f}%."
    )
if pior_contr_pct < -5:
    insights_html += insight("orange",
        f"Maior queda física M/M em <b>{pior_mes_contr}</b>: {pior_contr_pct:+.1f}%."
    )

if abs(fat_mig_pct) > 2 and abs(con_mig_pct) > 2:
    if fat_mig_pct < con_mig_pct - 3:
        insights_html += insight("red",
            "Faturamento caiu proporcionalmente mais do que os contratos — "
            "indica <b>redução no ticket médio</b> pós-migração."
        )
    elif con_mig_pct < fat_mig_pct - 3:
        insights_html += insight("orange",
            "Contratos caíram mais do que o faturamento — "
            "indica <b>concentração em contratos de maior valor</b> pós-migração, "
            "mas com perda de volume."
        )
    else:
        insights_html += insight("orange",
            "Queda proporcional em faturamento e contratos — "
            "indica <b>saída real de clientes</b>, não apenas redução de ticket."
        )

# Dados para o Chart.js (injetados como JS vars)
chart_data_js = f"""
const meses   = {json.dumps(chart_meses)};
const totais  = {json.dumps(chart_totais)};
const contrs  = {json.dumps(chart_contr)};
const tickets = {json.dumps(chart_ticket)};
const migIdx  = {mig_idx};
"""

css = """
<style>
  .vero-report { font-family: Arial, sans-serif; color: #222; max-width: 1100px; }
  .vero-report h1 { font-size: 22px; border-bottom: 3px solid #1a4fa0; padding-bottom: 6px; }
  .vero-report h2 { font-size: 16px; color: #1a4fa0; margin-top: 28px; border-left: 4px solid #1a4fa0; padding-left: 8px; }
  .vero-report table { border-collapse: collapse; width: 100%; margin-top: 10px; }
  .vero-report th { background: #1a4fa0; color: #fff; padding: 8px 12px; text-align: left; font-size: 13px; }
  .vero-report td { padding: 7px 12px; border-bottom: 1px solid #e0e0e0; font-size: 13px; }
  .vero-report tr:hover td { background: #f5f7ff; }
  .vero-report .kpi-box { display:inline-block; background:#f0f4ff; border:1px solid #c0d0f0;
    border-radius:8px; padding:12px 24px; margin:8px; text-align:center; min-width:140px; }
  .vero-report .kpi-val { font-size:22px; font-weight:bold; color:#1a4fa0; }
  .vero-report .kpi-lbl { font-size:12px; color:#555; margin-top:4px; }
</style>
"""

# KPIs gerais
total_cnpjs      = sum(r["QTD_CNPJ"] for r in tendencia_resumo)
fat_total_geral  = sum(r["FATURAMENTO_TOTAL"] or 0 for r in tendencia_resumo)
qtd_crescimento  = next((r["QTD_CNPJ"] for r in tendencia_resumo if r["TENDENCIA"] == "CRESCIMENTO"), 0)
qtd_queda        = next((r["QTD_CNPJ"] for r in tendencia_resumo if r["TENDENCIA"] == "QUEDA"), 0)
qtd_parou        = next((r["QTD_CNPJ"] for r in tendencia_resumo if r["TENDENCIA"] == "PAROU"), 0)
qtd_migraram     = next((r["QTD"] for r in migracao_resumo if r["STATUS_MIGRACAO"] == "MIGROU"), 0)
qtd_sumiram      = next((r["QTD"] for r in migracao_resumo if r["STATUS_MIGRACAO"] == "SUMIU"), 0)

html = f"""
{css}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>

<div class="vero-report">

<h1>Análise de Faturamento — SIMETRA + NG (Jan/2026 em diante)</h1>

<div>
  <div class="kpi-box"><div class="kpi-val">{total_cnpjs:,}</div><div class="kpi-lbl">CNPJs únicos</div></div>
  <div class="kpi-box"><div class="kpi-val">{fmt_brl(fat_total_geral)}</div><div class="kpi-lbl">Faturamento total</div></div>
  <div class="kpi-box"><div class="kpi-val" style="color:#1a7a3c">{qtd_crescimento:,}</div><div class="kpi-lbl">Em crescimento</div></div>
  <div class="kpi-box"><div class="kpi-val" style="color:#c0392b">{qtd_queda:,}</div><div class="kpi-lbl">Em queda</div></div>
  <div class="kpi-box"><div class="kpi-val" style="color:#7d3c98">{qtd_parou:,}</div><div class="kpi-lbl">Pararam de faturar</div></div>
  <div class="kpi-box"><div class="kpi-val" style="color:#c0392b">{total_duplicados:,}</div><div class="kpi-lbl">CNPJs duplicados</div></div>
</div>

<h2>ML — Diagnóstico de Tendência Geral (Regressão Linear Mensal)</h2>
<div>
  <div class="kpi-box">
    <div class="kpi-val" style="color:{ml_cor}">{ml_direcao}</div>
    <div class="kpi-lbl">Direção do faturamento</div>
  </div>
  <div class="kpi-box">
    <div class="kpi-val" style="color:{ml_cor}">{fmt_brl(slope_total)}/mês</div>
    <div class="kpi-lbl">Slope faturamento (R²={r2_total:.2f})</div>
  </div>
  <div class="kpi-box">
    <div class="kpi-val" style="color:{'#c0392b' if slope_contratos < 0 else '#1a7a3c'}">{slope_contratos:+.0f}/mês</div>
    <div class="kpi-lbl">Slope contratos (R²={r2_contr:.2f})</div>
  </div>
  <div class="kpi-box">
    <div class="kpi-val" style="color:{'#c0392b' if slope_ticket < 0 else '#1a7a3c'}">{fmt_brl(slope_ticket)}/mês</div>
    <div class="kpi-lbl">Slope ticket médio (R²={r2_ticket:.2f})</div>
  </div>
</div>
<p style="font-size:13px;margin-top:10px"><b>Diagnóstico:</b> {ml_causa}</p>

<h2>Gráfico Mensal — Físico × Financeiro</h2>
<div style="position:relative;height:380px;margin:16px 0">
  <canvas id="chartFisFin"></canvas>
</div>
<div style="background:#f8f9fa;border-left:4px solid #1a4fa0;padding:12px 16px;margin:16px 0;border-radius:0 6px 6px 0">
  <b style="font-size:14px;color:#1a4fa0">Insights — Impacto da Migração</b>
  <ul style="margin:8px 0 0 0;padding-left:0;list-style:none">
    {insights_html}
  </ul>
</div>
<script>
{chart_data_js}
(function() {{
  const ctx = document.getElementById('chartFisFin').getContext('2d');
  const annots = {{}};
  if (migIdx >= 0) {{
    annots.migLine = {{
      type: 'line',
      xMin: migIdx + 0.5,
      xMax: migIdx + 0.5,
      borderColor: '#c0392b',
      borderWidth: 2,
      borderDash: [6, 4],
      label: {{
        display: true,
        content: 'Migração SIMETRA→NG',
        position: 'start',
        backgroundColor: 'rgba(192,57,43,0.85)',
        color: '#fff',
        font: {{ size: 11 }}
      }}
    }};
  }}
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: meses,
      datasets: [
        {{
          label: 'Faturamento (R$)',
          data: totais,
          borderColor: '#1a4fa0',
          backgroundColor: 'rgba(26,79,160,0.08)',
          yAxisID: 'yFat',
          tension: 0.35,
          fill: true,
          pointRadius: 5,
          pointHoverRadius: 7,
        }},
        {{
          label: 'Qtd Contratos',
          data: contrs,
          borderColor: '#1a7a3c',
          backgroundColor: 'rgba(26,122,60,0.06)',
          yAxisID: 'yContr',
          tension: 0.35,
          fill: false,
          pointRadius: 5,
          pointHoverRadius: 7,
          borderDash: [5, 3],
        }},
        {{
          label: 'Ticket Médio (R$)',
          data: tickets,
          borderColor: '#d68910',
          backgroundColor: 'rgba(214,137,16,0.06)',
          yAxisID: 'yFat',
          tension: 0.35,
          fill: false,
          pointRadius: 4,
          pointHoverRadius: 6,
          borderDash: [2, 2],
        }}
      ]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ position: 'top' }},
        tooltip: {{
          callbacks: {{
            label: function(ctx) {{
              const v = ctx.parsed.y;
              if (ctx.datasetIndex === 1) return ' ' + ctx.dataset.label + ': ' + Math.round(v).toLocaleString('pt-BR');
              return ' ' + ctx.dataset.label + ': R$ ' + v.toLocaleString('pt-BR', {{minimumFractionDigits:2, maximumFractionDigits:2}});
            }}
          }}
        }},
        annotation: {{ annotations: annots }}
      }},
      scales: {{
        yFat: {{
          type: 'linear',
          position: 'left',
          title: {{ display: true, text: 'R$ (Faturamento / Ticket)' }},
          ticks: {{ callback: v => 'R$ ' + v.toLocaleString('pt-BR', {{maximumFractionDigits:0}}) }}
        }},
        yContr: {{
          type: 'linear',
          position: 'right',
          title: {{ display: true, text: 'Qtd Contratos' }},
          grid: {{ drawOnChartArea: false }},
          ticks: {{ callback: v => v.toLocaleString('pt-BR') }}
        }}
      }}
    }}
  }});
}})();
</script>

<table>
  <tr>
    <th>Mês</th>
    <th style="text-align:right">Faturamento</th><th style="text-align:right">Var%</th>
    <th style="text-align:right">Contratos</th><th style="text-align:right">Var%</th>
    <th style="text-align:right">Ticket Médio</th><th style="text-align:right">Var%</th>
    <th style="text-align:center">CNPJs Ativos</th>
  </tr>
  {rows_mensal}
</table>

<h2>Faturamento por Mês e Sistema</h2>
<table>
  <tr><th>Mês</th><th>SIMETRA</th><th>NG</th><th>Total</th></tr>
  {rows_fat}
</table>

<h2>Tendência de Faturamento por CNPJ</h2>
<table>
  <tr><th>Tendência</th><th style="text-align:right">Qtd CNPJs</th><th style="text-align:right">Faturamento Total</th></tr>
  {rows_tend}
</table>

<h2>Análise de Migração — SIMETRA Fev/2026 → NG Mar/2026</h2>
<table>
  <tr><th>Status</th><th style="text-align:right">Qtd CNPJs</th><th style="text-align:right">Faturamento Total</th></tr>
  {rows_mig}
</table>
<p style="font-size:13px;color:#555;margin-top:6px">
  CNPJs faturados nos dois sistemas no mesmo mês: <b>{total_duplicados:,}</b> |
  Valor duplicado estimado: <b>{fmt_brl(fat_duplicado)}</b>
</p>

<h2>Top 100 CNPJs com Maior Queda de Faturamento</h2>
<p style="font-size:13px;color:#555;margin-top:4px">
  Ordenado pela queda absoluta (1º mês ativo vs. último mês ativo). Apenas CNPJs com 2+ meses de histórico.
</p>
<table>
  <tr>
    <th style="text-align:center">#</th>
    <th>CNPJ</th>
    <th>Tendência</th>
    <th style="text-align:center">1º Mês</th>
    <th style="text-align:right">Valor 1º Mês</th>
    <th style="text-align:center">Último Mês</th>
    <th style="text-align:right">Valor Último Mês</th>
    <th style="text-align:right">Queda R$</th>
    <th style="text-align:right">Queda %</th>
    <th style="text-align:center">Meses Ativos</th>
    <th style="text-align:right">Total Faturado</th>
  </tr>
  {rows_queda}
</table>

<h2>Impacto da Migração por CNPJ — Média Mensal Pré (Dez/25–Fev/26) vs Pós (Mar–Jul/26)</h2>
<p style="font-size:13px;color:#555;margin-top:4px">
  CNPJs com queda na média mensal de faturamento após a migração, somando todos os sistemas.
  Queda acumulada estimada (soma das diferenças de média): <b style="color:#c0392b">{fmt_brl(total_perda_estimada)}</b>
</p>
<table>
  <tr>
    <th style="text-align:center">#</th>
    <th>CNPJ</th>
    <th style="text-align:right">Média Mensal Pré</th>
    <th style="text-align:right">Média Mensal Pós</th>
    <th style="text-align:right">Variação R$</th>
    <th style="text-align:right">Variação %</th>
    <th style="text-align:right">Var. Contratos %</th>
    <th style="text-align:right">Var. Ticket %</th>
    <th>Causa Principal</th>
  </tr>
  {rows_impacto}
</table>

<h2>Top 20 CNPJs com Alertas (entre os maiores faturadores)</h2>
<table>
  <tr>
    <th style="text-align:center">#</th>
    <th>CNPJ</th>
    <th style="text-align:right">Total Faturado</th>
    <th style="text-align:center">Meses Ativos</th>
    <th style="text-align:center">Contratos</th>
    <th style="text-align:right">Variação Total</th>
    <th style="text-align:right">Mês / Queda Mais Alta</th>
    <th>Alertas</th>
  </tr>
  {rows_top}
</table>

</div>
"""

displayHTML(html)
