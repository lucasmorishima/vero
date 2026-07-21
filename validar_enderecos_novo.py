"""
validar_enderecos_novo.py
-------------------------
Fonte de dados:
  · Padrão  → query Databricks (hive_metastore.accenture.*)
  · Fallback → arquivo CSV passado como argumento CLI

Saída: arquivos .sql com INSERTs para:
  · validacao_enderecos
  · validacao_dados_cadastrais
  · validacao_status_fatura
"""

from __future__ import annotations

import math
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Query Databricks — fonte principal
# ---------------------------------------------------------------------------

_QUERY_DATABRICKS = """
SELECT
    ROW_NUMBER() OVER (ORDER BY bc.codigocliente) + 1          AS FATURA,
    COALESCE(ca.CONTRATO, ca.ID_CLIENTE)                       AS ID_CLIENTE_CONTRATO,
    'DADOS CADASTRAIS'                                         AS REGRA,
    ca.segmento                                                AS SEGMENTO,
    bc.cidade                                                  AS cidade,
    bc.bairro                                                  AS bairro,
    bc.cep                                                     AS cep,
    bc.uf                                                      AS uf,
    ca.CPF_CNPJ                                                AS CPF_CNPJ,
    ca.NOME_CLIENTE                                            AS NOME_CLIENTE,
    ''                                                         AS INSCRICAO_ESTADUAL,
    bc.nome_produto                                            AS PRODUTO,
    ''                                                         AS TIPO_SERVICO,
    ''                                                         AS DESCRICAO_SERVICO,
    ''                                                         AS TIPO_IMPOSTO,
    ''                                                         AS PROMOCAO,
    ''                                                         AS GRUPO_LOCALIDADE,
    date_format(current_date(), 'yyyy_MM')                     AS ID_LOTE,
    ca.sistema_origem                                          AS CRM
FROM hive_metastore.accenture.base_clientes_centralizada bc
LEFT JOIN hive_metastore.accenture.tb_dispersao_competencia_analitica ca
    ON (
        (bc.crm = 'NG' AND ca.ID_CLIENTE = bc.codigocliente)
        OR
        (bc.crm <> 'NG' AND ca.CONTRATO = bc.idcontrato)
    )
WHERE bc.crm = 'NG'
  AND ca.CPF_CNPJ IS NOT NULL
LIMIT 100
"""


def _carregar_dados_databricks() -> pd.DataFrame:
    """Executa a query no Databricks e retorna um DataFrame."""
    try:
        from databricks import sql as dbsql
    except ImportError:
        raise ImportError(
            "Pacote 'databricks-sql-connector' não instalado. "
            "Execute: pip install databricks-sql-connector"
        )

    host       = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    http_path  = os.getenv("DATABRICKS_HTTP_PATH", "").strip()
    token      = os.getenv("DATABRICKS_TOKEN", "").strip()

    if not host or not http_path or not token:
        raise ValueError(
            "Variáveis DATABRICKS_HOST, DATABRICKS_HTTP_PATH e DATABRICKS_TOKEN "
            "precisam estar definidas no .env para usar a query Databricks."
        )

    print(f"Conectando ao Databricks: {host}")
    with dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            print("Executando query...")
            cursor.execute(_QUERY_DATABRICKS)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=cols).astype(str)
    print(f"  {len(df)} registros carregados do Databricks.\n")
    return df

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
TOKEN = os.getenv("CORREIOS_TOKEN", "")
DNE_DIR = Path(os.getenv("DNE_DIR", Path(__file__).parent / "dne_basico"))

_DNE_COLS_LOG = [
    "LOG_NU", "UFE_SG", "LOC_NU", "BAI_NU_INI", "BAI_NU_FIM",
    "LOG_NO", "LOG_COMPLEMENTO", "CEP", "TLO_TX",
    "LOG_STA_EDI", "LOG_NO_ABREV",
]
_DNE_COLS_LOC = [
    "LOC_NU", "UFE_SG", "LOC_NO", "CEP", "LOC_IN_SIT",
    "LOC_IN_TIPO_LOC", "LOC_NU_SUB", "LOC_NO_ABREV", "MUN_NU",
]
_DNE_COLS_BAI = ["BAI_NU", "UFE_SG", "LOC_NU", "BAI_NO", "BAI_NO_ABREV"]
_DNE_COLS_GRU = [
    "GRU_NU", "UFE_SG", "LOC_NU", "BAI_NU", "LOG_NU",
    "GRU_NO", "GRU_ENDERECO", "CEP", "GRU_NO_ABREV",
]
_DNE_COLS_CPC = ["CPC_NU", "UFE_SG", "LOC_NU", "CPC_NO", "CPC_ENDERECO", "CEP"]

_DNE_ARQUIVOS = [
    "LOG_LOGRADOURO.TXT", "LOG_LOCALIDADE.TXT",
    "LOG_BAIRRO.TXT", "LOG_GRANDE_USUARIO.TXT", "LOG_CPC.TXT",
]
_dne_disponivel = all((DNE_DIR / f).exists() for f in _DNE_ARQUIVOS)

# ---------------------------------------------------------------------------
# e-DNE Básico
# ---------------------------------------------------------------------------

def _ler_dne(nome_arquivo: str, colunas: list) -> pd.DataFrame:
    return pd.read_csv(
        DNE_DIR / nome_arquivo, sep="|", header=None,
        names=colunas, dtype=str, encoding="latin-1",
    )


_dne_index: dict | None = None


def _carregar_dne() -> dict:
    global _dne_index
    if _dne_index is not None:
        return _dne_index

    print("  Carregando e-DNE Básico (primeira consulta)...")
    df_loc = _ler_dne("LOG_LOCALIDADE.TXT", _DNE_COLS_LOC)[["LOC_NU", "LOC_NO", "UFE_SG"]]
    df_bai = _ler_dne("LOG_BAIRRO.TXT",     _DNE_COLS_BAI)[["BAI_NU", "BAI_NO"]]
    df_log = _ler_dne("LOG_LOGRADOURO.TXT", _DNE_COLS_LOG)
    df_gru = _ler_dne("LOG_GRANDE_USUARIO.TXT", _DNE_COLS_GRU)
    df_cpc = _ler_dne("LOG_CPC.TXT",        _DNE_COLS_CPC)

    df_log = df_log.merge(df_loc, on="LOC_NU", how="left")
    df_log = df_log.merge(df_bai, left_on="BAI_NU_INI", right_on="BAI_NU", how="left")

    indice: dict = {}
    for _, r in df_log.iterrows():
        indice[r["CEP"].strip()] = {
            "Logradouro":  f"{r.get('TLO_TX','').strip()} {r.get('LOG_NO','').strip()}".strip(),
            "Bairro":      str(r.get("BAI_NO", "") or ""),
            "Cidade":      str(r.get("LOC_NO_x", r.get("LOC_NO", "")) or ""),
            "UF":          str(r.get("UFE_SG_x", r.get("UFE_SG", "")) or ""),
            "Complemento": str(r.get("LOG_COMPLEMENTO", "") or ""),
        }

    df_gru = df_gru.merge(df_loc, on="LOC_NU", how="left")
    df_gru = df_gru.merge(df_bai, on="BAI_NU", how="left")
    for _, r in df_gru.iterrows():
        indice[r["CEP"].strip()] = {
            "Logradouro":  str(r.get("GRU_ENDERECO", "") or ""),
            "Bairro":      str(r.get("BAI_NO", "") or ""),
            "Cidade":      str(r.get("LOC_NO", "") or ""),
            "UF":          str(r.get("UFE_SG_x", r.get("UFE_SG", "")) or ""),
            "Complemento": str(r.get("GRU_NO", "") or ""),
        }

    df_cpc = df_cpc.merge(df_loc, on="LOC_NU", how="left")
    for _, r in df_cpc.iterrows():
        indice[r["CEP"].strip()] = {
            "Logradouro":  str(r.get("CPC_ENDERECO", "") or ""),
            "Bairro":      "",
            "Cidade":      str(r.get("LOC_NO", "") or ""),
            "UF":          str(r.get("UFE_SG_x", r.get("UFE_SG", "")) or ""),
            "Complemento": str(r.get("CPC_NO", "") or ""),
        }

    _dne_index = indice
    print(f"  e-DNE carregado: {len(indice):,} CEPs indexados")
    return indice


# ---------------------------------------------------------------------------
# Consulta de CEP (e-DNE / Correios API / ViaCEP)
# ---------------------------------------------------------------------------

def _consultar_dne(cep: str) -> dict:
    dados = _carregar_dne().get(cep)
    if not dados:
        raise ValueError("CEP não encontrado no e-DNE")
    return {"CEP": cep, **dados, "Fonte_CEP": "e-DNE", "Status_CEP": "OK"}


def _consultar_correios(cep: str) -> dict:
    url = f"https://api.correios.com.br/cep/v2/{cep}"
    r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=10)
    r.raise_for_status()
    d = r.json()
    return {
        "CEP":         cep,
        "Logradouro":  d.get("logradouro", ""),
        "Bairro":      d.get("bairro", ""),
        "Cidade":      d.get("localidade", ""),
        "UF":          d.get("uf", ""),
        "Complemento": d.get("complemento", ""),
        "Fonte_CEP":   "Correios API",
        "Status_CEP":  "OK",
    }


def _consultar_viacep(cep: str) -> dict:
    r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
    r.raise_for_status()
    d = r.json()
    if d.get("erro"):
        raise ValueError("CEP não encontrado no ViaCEP")
    return {
        "CEP":         cep,
        "Logradouro":  d.get("logradouro", ""),
        "Bairro":      d.get("bairro", ""),
        "Cidade":      d.get("localidade", ""),
        "UF":          d.get("uf", ""),
        "Complemento": d.get("complemento", ""),
        "Fonte_CEP":   "ViaCEP",
        "Status_CEP":  "OK",
    }


def consultar_cep(cep: str) -> dict:
    """Prioridade: e-DNE → Correios API → ViaCEP."""
    if _dne_disponivel:
        return _consultar_dne(cep)
    if TOKEN:
        try:
            return _consultar_correios(cep)
        except Exception as e:
            print(f"    Correios API falhou ({e}), usando ViaCEP...")
    return _consultar_viacep(cep)


# ---------------------------------------------------------------------------
# Tipo de documento
# ---------------------------------------------------------------------------

def _limpar_doc(doc: str) -> str:
    return re.sub(r"\D", "", str(doc).strip())


def _cep_generico(cep: str) -> bool:
    """CEPs terminados em 000 são genéricos (sede do município) e não identificam endereço específico."""
    return len(cep) == 8 and cep.endswith("000")


def tipo_documento(doc: str) -> str:
    d = _limpar_doc(doc)
    if len(d) == 11:
        return "CPF"
    if len(d) == 14:
        return "CNPJ"
    return "INVALIDO"


# ---------------------------------------------------------------------------
# Consulta Receita Federal via BrasilAPI
# ---------------------------------------------------------------------------

def _nova_sessao_com_retry() -> requests.Session:
    retry = Retry(
        total=4, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "VeroValidacaoEnderecos/1.0"})
    return s


def consultar_receita(cnpj: str) -> dict[str, Any]:
    """Retorna todos os campos da BrasilAPI para o CNPJ informado."""
    normalizado = _limpar_doc(cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(normalizado)}"
    session = _nova_sessao_com_retry()
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 429:
            time.sleep(3)
            resp = session.get(url, timeout=20)

        if resp.status_code != 200:
            return {"receita_status": f"erro_http_{resp.status_code}"}

        p = resp.json()
        return {
            "receita_status":                   "ok",
            "razao_social":                     p.get("razao_social"),
            "nome_fantasia":                    p.get("nome_fantasia"),
            "situacao_cadastral":               p.get("descricao_situacao_cadastral"),
            "data_situacao_cadastral":          p.get("data_situacao_cadastral"),
            "motivo_situacao_cadastral":        p.get("descricao_motivo_situacao_cadastral"),
            "natureza_juridica":                p.get("descricao_natureza_juridica"),
            "data_inicio_atividade":            p.get("data_inicio_atividade"),
            "cnae_principal_codigo":            p.get("cnae_fiscal"),
            "cnae_principal_descricao":         p.get("cnae_fiscal_descricao"),
            "porte":                            p.get("descricao_porte"),
            "capital_social":                   p.get("capital_social"),
            "opcao_simples":                    p.get("opcao_pelo_simples"),
            "opcao_mei":                        p.get("opcao_pelo_mei"),
            "email":                            p.get("email"),
            "telefone":                         p.get("ddd_telefone_1"),
            "receita_cep":                      (p.get("cep") or "").replace("-", "").replace(".", "").strip().zfill(8),
            "receita_logradouro":               p.get("logradouro"),
            "receita_numero":                   p.get("numero"),
            "receita_complemento":              p.get("complemento"),
            "receita_bairro":                   p.get("bairro"),
            "receita_municipio":                p.get("municipio"),
            "receita_uf":                       p.get("uf"),
        }
    except requests.RequestException as exc:
        return {"receita_status": f"erro_rede: {exc}"}
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Comparação de endereços
# ---------------------------------------------------------------------------

_TIPOS_LOGRADOURO = re.compile(
    r"^(AVENIDA|AVENUE|AV|RUA|RODOVIA|ROD|ESTRADA|EST|ALAMEDA|AL|"
    r"TRAVESSA|TR|TV|PRACA|PCA|PC|LARGO|LGO|LADEIRA|VIELA|BECO|"
    r"SETOR|QUADRA|QD|CONJUNTO|CJ|LOTE|LT|VILA|VL|PARQUE|LINHA|"
    r"CORREDOR|GALERIA|RAMAL|TRECHO|TREVO|VIA|VIADUTO|ACESSO)\b\s*",
    re.IGNORECASE,
)


def _normalizar(texto: str | None) -> str:
    if not texto:
        return ""
    s = str(texto).upper().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalizar_logradouro(texto: str | None) -> str:
    """Normaliza e remove o tipo de logradouro do início para comparação."""
    return _TIPOS_LOGRADOURO.sub("", _normalizar(texto)).strip()


def comparar_enderecos(receita: dict, correios: dict) -> str:
    if correios.get("Status_CEP") != "OK":
        return "CEP não encontrado nos Correios"

    uf_ok  = _normalizar(receita.get("receita_uf")) == _normalizar(correios.get("UF"))
    mun_ok = _normalizar(receita.get("receita_municipio")) == _normalizar(correios.get("Cidade"))
    log_r  = _normalizar_logradouro(receita.get("receita_logradouro"))
    log_c  = _normalizar_logradouro(correios.get("Logradouro"))
    log_ok = (not log_r or not log_c) or (log_r == log_c)

    return "Confere" if (uf_ok and mun_ok and log_ok) else "Divergente"


# ---------------------------------------------------------------------------
# Geração de SQL INSERT
# ---------------------------------------------------------------------------

def _sql_val(v) -> str:
    """Converte um valor Python em literal SQL."""
    if v is None:
        return "NULL"
    if isinstance(v, float) and math.isnan(v):
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return "'" + str(v).replace("'", "''") + "'"


def _gerar_inserts(tabela: str, linhas: list[dict], arquivo_sql: Path) -> None:
    """Grava um arquivo .sql com um INSERT por linha."""
    if not linhas:
        arquivo_sql.write_text(f"-- Sem registros para {tabela}\n", encoding="utf-8")
        return

    # União ordenada de todas as colunas encontradas nas linhas
    cols = list(dict.fromkeys(k for row in linhas for k in row.keys()))
    col_str = ", ".join(cols)

    with arquivo_sql.open("w", encoding="utf-8") as f:
        for row in linhas:
            val_str = ", ".join(_sql_val(row.get(c)) for c in cols)
            f.write(f"INSERT INTO {tabela} ({col_str}) VALUES ({val_str});\n")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    upper = {c.strip().upper(): c for c in df.columns}
    for nome in candidatos:
        if nome.upper() in upper:
            return upper[nome.upper()]
    return None


def processar_planilha(arquivo_entrada: str | Path | None = None) -> None:
    if arquivo_entrada is None:
        df = _carregar_dados_databricks()
    else:
        arquivo_entrada = Path(arquivo_entrada)
        print(f"Lendo arquivo: {arquivo_entrada}\n")
        df = pd.read_csv(arquivo_entrada, dtype=str, encoding="utf-8-sig", sep=None, engine="python")

    col_doc     = _encontrar_coluna(df, ["CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ", "CPF", "DOC"])
    col_cep     = _encontrar_coluna(df, ["CEP"])
    col_fatura  = _encontrar_coluna(df, ["FATURA"])
    col_id      = _encontrar_coluna(df, ["ID_CLIENTE_CONTRATO", "ID_CLIENTE", "ID"])
    col_regra   = _encontrar_coluna(df, ["REGRA"])
    col_cidade  = _encontrar_coluna(df, ["CIDADE", "CITY"])
    col_bairro  = _encontrar_coluna(df, ["BAIRRO"])
    col_uf      = _encontrar_coluna(df, ["UF", "ESTADO", "STATE"])
    col_nome    = _encontrar_coluna(df, ["NOME_CLIENTE", "NOME", "CLIENTE"])
    col_ie      = _encontrar_coluna(df, ["INSCRICAO_ESTADUAL", "IE"])
    col_produto = _encontrar_coluna(df, ["PRODUTO"])
    col_tpsvc   = _encontrar_coluna(df, ["TIPO_SERVICO"])
    col_dssvc   = _encontrar_coluna(df, ["DESCRICAO_SERVICO"])
    col_imposto = _encontrar_coluna(df, ["TIPO_IMPOSTO"])
    col_promo   = _encontrar_coluna(df, ["PROMOCAO"])
    col_grupo   = _encontrar_coluna(df, ["GRUPO_LOCALIDADE"])
    col_lote    = _encontrar_coluna(df, ["ID_LOTE"])
    col_seg     = _encontrar_coluna(df, ["SEGMENTO"])

    if col_doc is None:
        raise ValueError(
            "Coluna de documento não encontrada. "
            "Esperado: DOCUMENTO, CPF_CNPJ, CPF/CNPJ, CNPJ ou CPF."
        )
    if col_cep is None:
        raise ValueError("Coluna CEP não encontrada na planilha.")

    pasta_saida = Path(__file__).parent / "output_endereco"
    pasta_saida.mkdir(exist_ok=True)

    if _dne_disponivel:
        fonte_ativa = "e-DNE Básico (local)"
        delay = 0.0
    elif TOKEN:
        fonte_ativa = "API Correios (autenticada)"
        delay = 0.1
    else:
        fonte_ativa = "ViaCEP (gratuito)"
        delay = 0.3

    total = len(df)
    print(f"\nProcessando {total} registros | CEP via {fonte_ativa}\n")

    linhas_validacao: list[dict] = []
    linhas_cadastrais: list[dict] = []
    linhas_relatorio: list[dict] = []

    for i, row in df.iterrows():
        doc_raw     = str(row[col_doc]).strip()
        cep_raw     = str(row[col_cep]).replace("-", "").replace(".", "").strip().zfill(8)
        tipo        = tipo_documento(doc_raw)
        doc_num     = _limpar_doc(doc_raw)

        def _val(col):
            v = str(row[col]).strip() if col and col in row.index else ""
            return "" if v in ("nan", "None") else v

        fatura      = _val(col_fatura)
        id_cli      = _val(col_id)
        regra       = _val(col_regra)
        cidade_base = _val(col_cidade)
        bairro_base = _val(col_bairro)
        uf_base     = _val(col_uf)
        nome_base   = _val(col_nome)
        ie_base     = _val(col_ie)
        produto     = _val(col_produto)
        tipo_svc    = _val(col_tpsvc)
        desc_svc    = _val(col_dssvc)
        imposto     = _val(col_imposto)
        promo       = _val(col_promo)
        grupo       = _val(col_grupo)
        lote        = _val(col_lote)
        segmento    = _val(col_seg)

        cidade_base_limpa = re.sub(r"\s*-\s*\w+$", "", cidade_base).strip()

        prefixo = {
            "Fatura":               fatura,
            "ID_Cliente":           id_cli,
            "Regra":                regra,
            "Segmento":             segmento,
            "Cidade_Base":          cidade_base,
            "Bairro_Base":          bairro_base,
            "UF_Base":              uf_base,
            "Nome_Cliente_Base":    nome_base,
            "IE_Base":              ie_base,
            "Produto":              produto,
            "Tipo_Servico":         tipo_svc,
            "Descricao_Servico":    desc_svc,
            "Tipo_Imposto":         imposto,
            "Promocao":             promo,
            "Grupo_Localidade":     grupo,
            "ID_Lote":              lote,
        }

        print(f"  [{int(i)+1:4d}/{total}] {doc_raw[:18]:<20} ({tipo}) ", end="", flush=True)

        # CEP genérico para CPF: valida município via ViaCEP e encerra
        # Para CNPJ: deixa passar — município será validado com receita_municipio adiante
        if _cep_generico(cep_raw) and tipo == "CPF":
            try:
                end_gen    = consultar_cep(cep_raw)
                cidade_gen = _normalizar(end_gen.get("Cidade", ""))
                loc_str    = f"{end_gen.get('Cidade','')}/{end_gen.get('UF','')}"
            except Exception:
                cidade_gen = ""
                loc_str    = "não identificada"

            cidade_ok_gen = bool(cidade_gen) and _normalizar(cidade_base_limpa) == cidade_gen
            nota_gen = f"[CEP] CEP genérico: {cep_raw[:5]}-{cep_raw[5:]} (representa sede do município)"
            if cidade_ok_gen:
                obs_gen        = f"{nota_gen} | Município confirmado: {loc_str}"
                status_gen     = "CORRETO"
                substatus_gen  = "ALERTA"
                status_val_gen = "CEP genérico - município confirmado"
            else:
                obs_gen        = f"{nota_gen} | Município divergente: base '{cidade_base_limpa}' x CEP '{loc_str}'"
                status_gen     = "INCORRETO"
                substatus_gen  = "ERRO"
                status_val_gen = "CEP genérico - município divergente"

            linhas_validacao.append({
                **prefixo,
                "Documento":        doc_num,
                "Tipo":             "CPF",
                "CEP_Informado":    cep_raw,
                "Status_Validacao": status_val_gen,
                "Observacao":       obs_gen,
            })
            dados_billing_gen = (
                f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw}"
                f" | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}"
            )
            linhas_relatorio.append({
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": status_gen, "SUBSTATUS": substatus_gen,
                "OBSERVACAO": obs_gen,
                "DADOS_BILLING": dados_billing_gen, "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
            })
            print(f"— CEP GENÉRICO | {status_gen} | {loc_str}")
            if delay:
                time.sleep(delay)
            continue

        if tipo == "INVALIDO":
            linhas_validacao.append({
                **prefixo,
                "Documento":        doc_raw,
                "Tipo":             "INVALIDO",
                "CEP_Informado":    cep_raw,
                "Status_Validacao": "Documento inválido",
            })
            dados_billing = f"DOC: {doc_raw} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base}"
            linhas_relatorio.append({
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
                "OBSERVACAO": "[DOC] Documento inválido",
                "DADOS_BILLING": dados_billing, "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
            })
            print("— DOCUMENTO INVÁLIDO")
            continue

        # ------------------------------------------------------------------
        # CPF: busca CEP nos Correios e compara com a base interna
        # ------------------------------------------------------------------
        if tipo == "CPF":
            try:
                end = consultar_cep(cep_raw)

                # Compara cidade e UF da base interna com o retorno dos Correios
                cidade_cor = _normalizar(end.get("Cidade", ""))
                uf_cor     = _normalizar(end.get("UF", ""))
                cidade_ok  = not cidade_base_limpa or not cidade_cor or _normalizar(cidade_base_limpa) == cidade_cor
                uf_ok      = not uf_base or not uf_cor or _normalizar(uf_base) == uf_cor

                divs_cpf = []
                if not cidade_ok:
                    divs_cpf.append(f"Cidade divergente: base '{cidade_base_limpa}' x Correios '{end.get('Cidade','')}'")
                if not uf_ok:
                    divs_cpf.append(f"UF divergente: base '{uf_base}' x Correios '{end.get('UF','')}'")

                obs_cpf        = " | ".join(divs_cpf)
                status_cpf     = "INCORRETO" if divs_cpf else "CORRETO"
                substatus_cpf  = "ERRO" if divs_cpf else "OK"
                status_val_cpf = "Divergente" if divs_cpf else "Confere"

                dados_tabela_verdade_cpf = (
                    f"CEP: {cep_raw} | LOGRADOURO: {end.get('Logradouro','')} | BAIRRO: {end.get('Bairro','')} | CIDADE: {end.get('Cidade','')} | UF: {end.get('UF','')}"
                )

                linhas_validacao.append({
                    **prefixo,
                    "Documento":        doc_num,
                    "Tipo":             "CPF",
                    "CEP_Informado":    cep_raw,
                    "Logradouro":       end.get("Logradouro", ""),
                    "Bairro":           end.get("Bairro", ""),
                    "Cidade":           end.get("Cidade", ""),
                    "UF":               end.get("UF", ""),
                    "Complemento":      end.get("Complemento", ""),
                    "Cidade_Confere":   "Sim" if cidade_ok else "Não",
                    "UF_Confere":       "Sim" if uf_ok else "Não",
                    "Fonte_CEP":        end.get("Fonte_CEP", ""),
                    "Status_Validacao": status_val_cpf,
                    "Observacao":       obs_cpf,
                })
                dados_billing_cpf = (
                    f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw}"
                    f" | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}"
                )
                linhas_relatorio.append({
                    "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                    "STATUS": status_cpf, "SUBSTATUS": substatus_cpf, "OBSERVACAO": obs_cpf,
                    "DADOS_BILLING": dados_billing_cpf, "DADOS_CONTRATO": None,
                    "DADOS_TABELA_VERDADE": dados_tabela_verdade_cpf,
                    "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                    "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                    "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
                })
                print(f"— {status_val_cpf} | {end.get('Logradouro','')[:30]}, {end.get('Cidade','')}/{end.get('UF','')}")
            except Exception as exc:
                linhas_validacao.append({
                    **prefixo,
                    "Documento":        doc_num,
                    "Tipo":             "CPF",
                    "CEP_Informado":    cep_raw,
                    "Status_Validacao": f"Erro CEP: {exc}",
                })
                dados_billing_cpf = (
                    f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw}"
                    f" | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}"
                )
                linhas_relatorio.append({
                    "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                    "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
                    "OBSERVACAO": f"[CEP] CEP não encontrado: {cep_raw}",
                    "DADOS_BILLING": dados_billing_cpf, "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                    "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                    "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                    "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
                })
                print(f"— ERRO CEP: {exc}")
            if delay:
                time.sleep(delay)
            continue

        # ------------------------------------------------------------------
        # CNPJ: consulta Receita + valida CEP nos Correios
        # ------------------------------------------------------------------
        receita = consultar_receita(doc_num)
        time.sleep(0.5)

        if receita.get("receita_status") != "ok":
            linhas_validacao.append({
                **prefixo,
                "Documento":        doc_num,
                "Tipo":             "CNPJ",
                "CEP_Informado":    cep_raw,
                "Status_Validacao": f"Erro Receita: {receita.get('receita_status')}",
            })
            dados_billing_cnpj_err = (
                f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw}"
                f" | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}"
            )
            linhas_relatorio.append({
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
                "OBSERVACAO": f"[API] Erro na consulta Receita: {receita.get('receita_status')}",
                "DADOS_BILLING": dados_billing_cnpj_err, "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
            })
            print(f"— ERRO RECEITA: {receita.get('receita_status')}")
            continue

        cep_receita = receita["receita_cep"]

        try:
            end_correios = consultar_cep(cep_receita)
        except Exception as exc:
            end_correios = {"Status_CEP": str(exc)}
        if delay:
            time.sleep(delay)

        status_end = comparar_enderecos(receita, end_correios)

        cidade_rec = _normalizar(receita.get("receita_municipio", ""))
        uf_rec     = _normalizar(receita.get("receita_uf", ""))
        cidade_ok  = not cidade_base_limpa or not cidade_rec or _normalizar(cidade_base_limpa) == cidade_rec
        uf_ok      = not uf_base or not uf_rec or _normalizar(uf_base) == uf_rec
        cep_ok     = cep_raw == cep_receita

        mun_receita = _normalizar(receita.get("receita_municipio", ""))
        mun_base    = _normalizar(cidade_base_limpa)

        divergencias = []
        # CEP base genérico para CNPJ: compara município da base com receita_municipio
        if _cep_generico(cep_raw):
            nota_gen_b = f"[CEP] CEP da base é genérico: {cep_raw[:5]}-{cep_raw[5:]} (representa sede do município)"
            if mun_receita and mun_receita == mun_base:
                divergencias.append(f"{nota_gen_b} | Município confirmado: {receita.get('receita_municipio','')}")
            else:
                divergencias.append(
                    f"{nota_gen_b} | Município divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'"
                )
        # CEP da Receita genérico: compara município da Receita com município da base
        if _cep_generico(cep_receita):
            nota_gen_r = f"[CEP] CEP da Receita é genérico: {cep_receita[:5]}-{cep_receita[5:]} (sede do município)"
            if mun_receita and mun_receita == mun_base:
                divergencias.append(f"{nota_gen_r} | Município confirmado: {receita.get('receita_municipio','')}")
            else:
                divergencias.append(
                    f"{nota_gen_r} | Município divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'"
                )
        if not cep_ok:
            divergencias.append(f"CEP divergente: base '{cep_raw}' x Receita '{cep_receita}'")
        if not cidade_ok:
            divergencias.append(
                f"Cidade divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'"
            )
        if not uf_ok:
            divergencias.append(
                f"UF divergente: base '{uf_base}' x Receita '{receita.get('receita_uf','')}'"
            )
        if status_end == "Divergente":
            divergencias.append("Endereço Receita x Correios divergente")
        elif status_end == "CEP não encontrado nos Correios":
            divergencias.append("CEP da Receita não encontrado nos Correios")

        status_validacao = "Divergente" if divergencias else "Confere"
        observacao       = " | ".join(divergencias)

        linhas_validacao.append({
            **prefixo,
            "Documento":                 doc_num,
            "Tipo":                      "CNPJ",
            "CEP_Informado":             cep_raw,
            "CEP_Receita":               cep_receita,
            "CEP_Confere_com_Informado": "Sim" if cep_ok else "Não",
            "Cidade_Confere":            "Sim" if cidade_ok else "Não",
            "UF_Confere":                "Sim" if uf_ok else "Não",
            "Logradouro_Receita":        receita.get("receita_logradouro", ""),
            "Numero_Receita":            receita.get("receita_numero", ""),
            "Complemento_Receita":       receita.get("receita_complemento", ""),
            "Bairro_Receita":            receita.get("receita_bairro", ""),
            "Cidade_Receita":            receita.get("receita_municipio", ""),
            "UF_Receita":                receita.get("receita_uf", ""),
            "Logradouro_Correios":       end_correios.get("Logradouro", ""),
            "Bairro_Correios":           end_correios.get("Bairro", ""),
            "Cidade_Correios":           end_correios.get("Cidade", ""),
            "UF_Correios":               end_correios.get("UF", ""),
            "Fonte_CEP":                 end_correios.get("Fonte_CEP", ""),
            "Status_Validacao":          status_validacao,
            "Observacao":                observacao,
            "Razao_Social":              receita.get("razao_social", ""),
            "Situacao_Cadastral":        receita.get("situacao_cadastral", ""),
        })

        _end_rec = (
            f"{receita.get('receita_logradouro','')}, {receita.get('receita_numero','')}"
            f" - {receita.get('receita_bairro','')} - {receita.get('receita_municipio','')}/{receita.get('receita_uf','')}"
        ).strip(", -/")
        dados_billing_ok = (
            f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw}"
            f" | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}"
        )
        dados_contrato_ok = (
            f"RAZAO: {receita.get('razao_social','')} | SITUACAO: {receita.get('situacao_cadastral','')}"
            f" | CEP: {cep_receita} | END: {_end_rec}"
        )
        linhas_relatorio.append({
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "INCORRETO" if divergencias else "CORRETO",
            "SUBSTATUS": "ERRO" if divergencias else "OK",
            "OBSERVACAO": observacao,
            "DADOS_BILLING": dados_billing_ok,
            "DADOS_CONTRATO": dados_contrato_ok,
            "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
        })

        linhas_cadastrais.append({
            **prefixo,
            "CNPJ":                     doc_num,
            "Razao_Social":             receita.get("razao_social"),
            "Nome_Fantasia":            receita.get("nome_fantasia"),
            "Situacao_Cadastral":       receita.get("situacao_cadastral"),
            "Data_Situacao_Cadastral":  receita.get("data_situacao_cadastral"),
            "Motivo_Situacao":          receita.get("motivo_situacao_cadastral"),
            "Natureza_Juridica":        receita.get("natureza_juridica"),
            "Data_Inicio_Atividade":    receita.get("data_inicio_atividade"),
            "CNAE_Principal_Codigo":    receita.get("cnae_principal_codigo"),
            "CNAE_Principal_Descricao": receita.get("cnae_principal_descricao"),
            "Porte":                    receita.get("porte"),
            "Capital_Social":           receita.get("capital_social"),
            "Simples_Nacional":         receita.get("opcao_simples"),
            "MEI":                      receita.get("opcao_mei"),
            "Email":                    receita.get("email"),
            "Telefone":                 receita.get("telefone"),
            "CEP_Receita":              receita.get("receita_cep"),
            "Logradouro_Receita":       receita.get("receita_logradouro"),
            "Numero_Receita":           receita.get("receita_numero"),
            "Complemento_Receita":      receita.get("receita_complemento"),
            "Bairro_Receita":           receita.get("receita_bairro"),
            "Municipio_Receita":        receita.get("receita_municipio"),
            "UF_Receita":               receita.get("receita_uf"),
        })

        print(
            f"— {status_validacao} | "
            f"{receita.get('razao_social','')[:30]} | "
            f"CEP Receita: {cep_receita}"
        )

    # -----------------------------------------------------------------------
    # Gera arquivos SQL com INSERTs
    # -----------------------------------------------------------------------
    arq_val_sql = pasta_saida / "validacao_enderecos.sql"
    _gerar_inserts("validacao_enderecos", linhas_validacao, arq_val_sql)

    arq_cad_sql = pasta_saida / "validacao_dados_cadastrais.sql"
    _gerar_inserts("validacao_dados_cadastrais", linhas_cadastrais, arq_cad_sql)

    arq_rel_sql = pasta_saida / "validacao_status_fatura.sql"
    _gerar_inserts("validacao_status_fatura", linhas_relatorio, arq_rel_sql)

    ok   = sum(1 for r in linhas_validacao if r.get("Status_Validacao") in ("Confere", "CEP encontrado"))
    div  = sum(1 for r in linhas_validacao if r.get("Status_Validacao") == "Divergente")
    erro = sum(1 for r in linhas_validacao if r.get("Status_Validacao") not in ("Confere", "CEP encontrado", "Divergente"))

    print(f"\nConcluído: {ok} OK | {div} divergente(s) | {erro} erro(s)")
    print(f"  → {arq_val_sql}  ({len(linhas_validacao)} INSERTs → validacao_enderecos)")
    print(f"  → {arq_cad_sql}  ({len(linhas_cadastrais)} INSERTs → validacao_dados_cadastrais)")
    print(f"  → {arq_rel_sql}  ({len(linhas_relatorio)} INSERTs → validacao_status_fatura)")


if __name__ == "__main__":
    import sys
    # Sem argumento → Databricks; com argumento → CSV local
    arquivo = sys.argv[1] if len(sys.argv) > 1 else None
    processar_planilha(arquivo)
