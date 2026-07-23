# ============================================================
# SISTEMA ML DE IDENTIFICAÇÃO DE PADRÕES DE PROBLEMAS
# Databricks | Python | Único Notebook
# ============================================================

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from IPython.display import display

# ─────────────────────────────────────────────
# 1. CARREGAMENTO
# ─────────────────────────────────────────────
# Caminho local do CSV na pasta data do projeto
FILE_PATH = "data/base.csv"

try:
    df = pd.read_csv(FILE_PATH, sep=";", decimal=".", low_memory=False)
    print(f"✅ Dataset carregado: {df.shape[0]:,} linhas | {df.shape[1]} colunas")
except Exception as e:
    raise RuntimeError(f"Erro ao carregar arquivo: {e}\nVerifique o caminho: {FILE_PATH}")

# ─────────────────────────────────────────────
# 2. PRÉ-PROCESSAMENTO
# ─────────────────────────────────────────────
NUM_COLS = [
    'faturamento_atual', 'faturamento_mes_anterior', 'media_faturamento_6m',
    'desvio_padrao_6m', 'qtde_meses_historico',
    'dispersao_abs_mes_anterior', 'dispersao_pct_mes_anterior',
    'dispersao_abs_media_6m', 'dispersao_pct_media_6m',
    'score_anomalia', 'valor_contrato'
]

CAT_COLS = [
    'SEGMENTO', 'PRODUTO', 'ITEM_FATURAMENTO', 'UF', 'MUNICIPIO',
    'TIPO_CLIENTE', 'STATUS_CLIENTE', 'tipo_dispersao_mes_anterior',
    'tipo_dispersao_media_6m', 'severidade', 'classificacao_final',
    'flag_anomalia', 'status_contrato', 'crm', 'nome_produto'
]

# Converte numéricos
for col in [c for c in NUM_COLS if c in df.columns]:
    df[col] = pd.to_numeric(
        df[col].astype(str).str.replace(',', '.').str.strip(), errors='coerce'
    ).fillna(0)

# Limpa categóricos
for col in [c for c in CAT_COLS if c in df.columns]:
    df[col] = df[col].fillna('N/A').astype(str).str.strip().str.upper()

print("✅ Pré-processamento concluído\n")

# ─────────────────────────────────────────────
# 3. FEATURES PARA ML
# ─────────────────────────────────────────────
FEAT = [c for c in [
    'faturamento_atual', 'faturamento_mes_anterior', 'media_faturamento_6m',
    'desvio_padrao_6m', 'dispersao_pct_mes_anterior', 'dispersao_pct_media_6m',
    'score_anomalia', 'qtde_meses_historico'
] if c in df.columns]

X_raw = df[FEAT].fillna(0)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_raw)

# ─────────────────────────────────────────────
# 4. CLUSTERIZAÇÃO: GRUPOS DE COMPORTAMENTO
# ─────────────────────────────────────────────
N_CLUSTERS = 6
kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
df['cluster'] = kmeans.fit_predict(X_scaled)

# Calcular score médio de anomalia por cluster
cluster_summary = df.groupby('cluster').agg(
    registros        = ('faturamento_atual', 'count'),
    media_faturamento= ('faturamento_atual', 'mean'),
    score_medio      = ('score_anomalia',    'mean')   if 'score_anomalia'    in df.columns else ('faturamento_atual','count'),
    pct_anomalia     = ('flag_anomalia',     lambda x: round((x=='S').mean()*100, 1)) if 'flag_anomalia' in df.columns else ('faturamento_atual','count'),
    pct_critico      = ('severidade',        lambda x: round((x=='CRITICA').mean()*100,1)) if 'severidade' in df.columns else ('faturamento_atual','count'),
    disp_neg_pct     = ('tipo_dispersao_mes_anterior', lambda x: round((x=='DISPERSAO_NEGATIVA').mean()*100,1)) if 'tipo_dispersao_mes_anterior' in df.columns else ('faturamento_atual','count'),
).round(2)

# Rotular nível de risco
def rotulo_risco(row):
    if row.get('pct_critico', 0) > 30 or row.get('score_medio', 0) > 2:
        return '🔴 ALTO'
    elif row.get('pct_anomalia', 0) > 20 or row.get('disp_neg_pct', 0) > 40:
        return '🟡 MÉDIO'
    return '🟢 BAIXO'

cluster_summary['risco'] = cluster_summary.apply(rotulo_risco, axis=1)

print("=" * 65)
print("📊 GRUPOS DE COMPORTAMENTO DA CARTEIRA")
print("=" * 65)

cluster_painel = cluster_summary.reset_index()[[
    'cluster', 'registros', 'media_faturamento', 'pct_critico', 'risco'
]].rename(columns={
    'cluster': 'grupo',
    'registros': 'volume_contratos',
    'media_faturamento': 'faturamento_medio',
    'pct_critico': 'pct_criticidade'
})
display(cluster_painel)

df['risco_cluster'] = df['cluster'].map(cluster_summary['risco'])

# ─────────────────────────────────────────────
# 5. ISOLATION FOREST: SINAL COMPLEMENTAR DE RISCO
# ─────────────────────────────────────────────
iso = IsolationForest(contamination=0.08, random_state=42)
df['anomalia_iso'] = iso.fit_predict(X_scaled)
df['anomalia_iso'] = df['anomalia_iso'].map({1: 'NORMAL', -1: '⚠️ ANOMALIA'})

n_anom = (df['anomalia_iso'] == '⚠️ ANOMALIA').sum()

# ─────────────────────────────────────────────
# 6. MODELO PREDITIVO: SEVERIDADE
# ─────────────────────────────────────────────
ALVOS_VALIDOS = ['BAIXA', 'MEDIA', 'ALTA', 'CRITICA']

if 'severidade' in df.columns:
    df_ml = df[df['severidade'].isin(ALVOS_VALIDOS)].copy()

    if len(df_ml) >= 200:
        le = LabelEncoder()
        y  = le.fit_transform(df_ml['severidade'])
        X_ml = df_ml[FEAT].fillna(0)

        X_tr, X_te, y_tr, y_te = train_test_split(
            X_ml, y, test_size=0.2, random_state=42, stratify=y
        )

        rf = RandomForestClassifier(
            n_estimators=150, random_state=42,
            class_weight='balanced', n_jobs=-1
        )
        rf.fit(X_tr, y_tr)
        y_pred = rf.predict(X_te)

        print("\n" + "=" * 65)
        print("🤖 MOTOR DE PRIORIZAÇÃO OPERACIONAL")
        print("=" * 65)
        print("Modelo de priorização atualizado com histórico de severidade.")

        # Drivers de priorização para contexto de negócio
        fi = pd.DataFrame({'variavel': FEAT, 'importancia': rf.feature_importances_})
        fi = fi.sort_values('importancia', ascending=False)
        print("Principais drivers para tomada de decisão:")
        display(fi.reset_index(drop=True))

        # Predição no dataset completo
        df_all_feat = df[FEAT].fillna(0)
        df['severidade_prevista'] = le.inverse_transform(rf.predict(df_all_feat))
    else:
        print("⚠️ Poucos registros rotulados para treinar o modelo.")

# ─────────────────────────────────────────────
# 7. ANÁLISE DE MACRO PROBLEMAS POR DIMENSÃO
# ─────────────────────────────────────────────
def analise_por(df, col, topn=10):
    """Agrupa e ranqueia frentes de ataque por dimensão."""
    if col not in df.columns:
        return pd.DataFrame()
    agg = {
        'volume_contratos'   : ('faturamento_atual', 'count'),
        'impacto_financeiro' : ('faturamento_atual', 'sum'),
        'ticket_medio'       : ('faturamento_atual', 'mean'),
    }
    if 'flag_anomalia' in df.columns:
        agg['ocorrencias_atencao'] = ('flag_anomalia', lambda x: (x == 'S').sum())
    if 'severidade' in df.columns:
        agg['ocorrencias_criticas']  = ('severidade', lambda x: (x == 'CRITICA').sum())
    if 'tipo_dispersao_mes_anterior' in df.columns:
        agg['disp_negativa'] = ('tipo_dispersao_mes_anterior', lambda x: (x == 'DISPERSAO_NEGATIVA').sum())

    out = df.groupby(col).agg(**agg).reset_index()

    if 'ocorrencias_atencao' in out.columns:
        out['pct_atencao'] = (out['ocorrencias_atencao'] / out['volume_contratos'] * 100).round(1)
    if 'ocorrencias_criticas' in out.columns:
        out['pct_critica'] = (out['ocorrencias_criticas'] / out['volume_contratos'] * 100).round(1)

    out['indice_prioridade'] = (
        out.get('pct_critica', 0) * 0.5 +
        out.get('pct_atencao', 0) * 0.3 +
        (out['impacto_financeiro'] / max(out['impacto_financeiro'].max(), 1) * 100) * 0.2
    ).round(1)

    out = out.sort_values('indice_prioridade', ascending=False)
    return out.head(topn)

dims = {
    'SEGMENTO'         : '📂 Segmento',
    'PRODUTO'          : '📦 Produto',
    'ITEM_FATURAMENTO' : '🧾 Item de Faturamento',
    'UF'               : '🗺️  UF',
    'MUNICIPIO'        : '📍 Município (Top 15)',
    'TIPO_CLIENTE'     : '👤 Tipo de Cliente',
    'crm'              : '🏢 CRM',
    'nome_produto'     : '🔖 Nome do Produto',
}

print("\n" + "=" * 65)
print("📌 PAINEL DE FRENTES DE ATAQUE POR DIMENSÃO")
print("=" * 65)

for col, label in dims.items():
    result = analise_por(df, col, topn=15 if col == 'MUNICIPIO' else 10)
    if not result.empty:
        print(f"\n{'─'*60}")
        print(f"  {label}")
        print(f"{'─'*60}")
        display(result.reset_index(drop=True))

# ─────────────────────────────────────────────
# 8. RELATÓRIO RESUMIDO
# ─────────────────────────────────────────────
total = len(df)

print("\n" + "=" * 65)
print("📋 RESUMO EXECUTIVO")
print("=" * 65)

print(f"\n  Carteira analisada             : {total:,} contratos")

if 'severidade' in df.columns:
    carteira_prioritaria = df['severidade'].isin(['CRITICA', 'ALTA']).sum()
    print(f"  Carteira prioritária imediata  : {carteira_prioritaria:,}  ({carteira_prioritaria/total*100:.1f}%)")

if 'severidade' in df.columns:
    print(f"\n  Distribuição de criticidade:")
    for s in ['CRITICA','ALTA','MEDIA','BAIXA','SEM_HISTORICO']:
        n = (df['severidade'] == s).sum()
        barra = '█' * int(n/total*40)
        print(f"    {s:<15} {n:>7,}  {n/total*100:>5.1f}%  {barra}")

if 'classificacao_final' in df.columns:
    print(f"\n  Principais motivos de atenção:")
    top_class = (
        df[df['classificacao_final'].str.contains('CRITICO|ANOMALIA|ALERTA', na=False)]
        ['classificacao_final'].value_counts().head(8)
    )

    rotulos_gerenciais = {
        'ANOMALIA_ML': 'SINAL FORA DO PADRÃO'
    }

    for k, v in top_class.items():
        k_view = rotulos_gerenciais.get(k, k)
        print(f"    {k_view:<35} {v:>6,}")

if 'tipo_dispersao_mes_anterior' in df.columns:
    disp_neg = (df['tipo_dispersao_mes_anterior'] == 'DISPERSAO_NEGATIVA').sum()
    disp_pos = (df['tipo_dispersao_mes_anterior'] == 'DISPERSAO_POSITIVA').sum()
    print(f"\n  Movimentação recente da carteira:")
    print(f"    Queda de faturamento         : {disp_neg:,}  ({disp_neg/total*100:.1f}%)")
    print(f"    Alta de faturamento          : {disp_pos:,}  ({disp_pos/total*100:.1f}%)")

if 'faturamento_atual' in df.columns:
    sem_fat = (df['faturamento_atual'] == 0).sum()
    print(f"\n  Contratos sem faturamento atual: {sem_fat:,}  ({sem_fat/total*100:.1f}%)")

print("\n  Recomendação de gestão rápida:")
print("    1) Atuar primeiro nos grupos com risco alto e médio")
print("    2) Priorizar dimensões com maior índice de prioridade")
print("    3) Direcionar plano comercial para queda de faturamento")

# ─────────────────────────────────────────────
# 9. EXPORTAR RESULTADO ENRIQUECIDO
# ─────────────────────────────────────────────
OUTPUT = "data/processed/output_ml_padroes.csv"
cols_extra = [c for c in ['cluster','risco_cluster','anomalia_iso','severidade_prevista'] if c in df.columns]

df_out = df.copy()
try:
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    df_out.to_csv(OUTPUT, index=False, sep=";")
    print(f"\n💾 Arquivo enriquecido exportado → {OUTPUT}")
    print(f"   Novas colunas adicionadas: {cols_extra}")
except Exception as e:
    print(f"\n⚠️ Exportação falhou: {e}")

print("\n✅ ANÁLISE CONCLUÍDA")