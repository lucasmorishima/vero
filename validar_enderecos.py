"""
validar_enderecos.py
--------------------
Lê uma planilha com colunas DOCUMENTO (CPF ou CNPJ) e CEP.

CNPJ → consulta Receita Federal (BrasilAPI), compara endereço da Receita
       com o endereço retornado pelos Correios para o mesmo CEP e grava
       dois arquivos de saída:
         · validacao_enderecos.xlsx   — todos os clientes + status de validação
         · dados_cadastrais_cnpj.xlsx — somente CNPJs com dados da Receita

CPF  → consulta apenas o CEP nos Correios/e-DNE e devolve o endereço com status.
"""

from __future__ import annotations

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
# Configuração — mesma lógica do script CEP original
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
            # Endereço na Receita
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

# Tipos de logradouro para remover antes de comparar o nome da via.
# A Receita armazena só o nome (ex: "REPUBLICA DO CHILE"); o ViaCEP inclui
# o tipo junto (ex: "Avenida República do Chile"). Removemos o tipo de ambos.
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
    """
    Compara UF, cidade e logradouro (sem tipo de via) entre Receita e Correios.
    Retorna 'Confere', 'Divergente' ou 'CEP não encontrado nos Correios'.
    """
    if correios.get("Status_CEP") != "OK":
        return "CEP não encontrado nos Correios"

    uf_ok  = _normalizar(receita.get("receita_uf")) == _normalizar(correios.get("UF"))
    mun_ok = _normalizar(receita.get("receita_municipio")) == _normalizar(correios.get("Cidade"))
    log_r  = _normalizar_logradouro(receita.get("receita_logradouro"))
    log_c  = _normalizar_logradouro(correios.get("Logradouro"))
    log_ok = (not log_r or not log_c) or (log_r == log_c)

    return "Confere" if (uf_ok and mun_ok and log_ok) else "Divergente"


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    upper = {c.strip().upper(): c for c in df.columns}
    for nome in candidatos:
        if nome.upper() in upper:
            return upper[nome.upper()]
    return None


def processar_planilha(arquivo_entrada: str | Path = "base teste.csv") -> None:
    arquivo_entrada = Path(arquivo_entrada)
    df = pd.read_csv(arquivo_entrada, dtype=str, encoding="utf-8-sig", sep=None, engine="python")

    col_doc = _encontrar_coluna(df, ["CPF_CNPJ", "CPF/CNPJ", "DOCUMENTO", "CNPJ", "CPF", "DOC"])
    col_cep = _encontrar_coluna(df, ["CEP"])
    col_fatura = _encontrar_coluna(df, ["FATURA"])
    col_id     = _encontrar_coluna(df, ["ID_CLIENTE_CONTRATO", "ID_CLIENTE", "ID"])

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
        delay = 0.3          # ViaCEP pede cortesia

    total = len(df)
    print(f"\nProcessando {total} registros | CEP via {fonte_ativa}\n")

    linhas_validacao: list[dict] = []
    linhas_cadastrais: list[dict] = []

    for i, row in df.iterrows():
        doc_raw  = str(row[col_doc]).strip()
        cep_raw  = str(row[col_cep]).replace("-", "").replace(".", "").strip().zfill(8)
        tipo     = tipo_documento(doc_raw)
        doc_num  = _limpar_doc(doc_raw)
        fatura   = str(row[col_fatura]).strip() if col_fatura else ""
        id_cli   = str(row[col_id]).strip()     if col_id     else ""

        # Prefixo de rastreabilidade presente em todas as linhas de saída
        prefixo = {"Fatura": fatura, "ID_Cliente": id_cli}

        print(f"  [{int(i)+1:4d}/{total}] {doc_raw[:18]:<20} ({tipo}) ", end="", flush=True)

        if tipo == "INVALIDO":
            linhas_validacao.append({
                **prefixo,
                "Documento":         doc_raw,
                "Tipo":              "INVALIDO",
                "CEP_Informado":     cep_raw,
                "Status_Validacao":  "Documento inválido",
            })
            print("— DOCUMENTO INVÁLIDO")
            continue

        # ------------------------------------------------------------------
        # CPF: só busca endereço pelo CEP
        # ------------------------------------------------------------------
        if tipo == "CPF":
            try:
                end = consultar_cep(cep_raw)
                linhas_validacao.append({
                    **prefixo,
                    "Documento":         doc_num,
                    "Tipo":              "CPF",
                    "CEP_Informado":     cep_raw,
                    "Logradouro":        end.get("Logradouro", ""),
                    "Bairro":            end.get("Bairro", ""),
                    "Cidade":            end.get("Cidade", ""),
                    "UF":                end.get("UF", ""),
                    "Complemento":       end.get("Complemento", ""),
                    "Fonte_CEP":         end.get("Fonte_CEP", ""),
                    "Status_Validacao":  "CEP encontrado",
                })
                print(f"— {end.get('Logradouro','')[:35]}, {end.get('Cidade','')}/{end.get('UF','')}")
            except Exception as exc:
                linhas_validacao.append({
                    **prefixo,
                    "Documento":        doc_num,
                    "Tipo":             "CPF",
                    "CEP_Informado":    cep_raw,
                    "Status_Validacao": f"Erro CEP: {exc}",
                })
                print(f"— ERRO CEP: {exc}")
            if delay:
                time.sleep(delay)
            continue

        # ------------------------------------------------------------------
        # CNPJ: consulta Receita + valida CEP nos Correios
        # ------------------------------------------------------------------
        receita = consultar_receita(doc_num)
        time.sleep(0.5)        # cortesia BrasilAPI

        if receita.get("receita_status") != "ok":
            linhas_validacao.append({
                **prefixo,
                "Documento":        doc_num,
                "Tipo":             "CNPJ",
                "CEP_Informado":    cep_raw,
                "Status_Validacao": f"Erro Receita: {receita.get('receita_status')}",
            })
            print(f"— ERRO RECEITA: {receita.get('receita_status')}")
            continue

        cep_receita = receita["receita_cep"]

        # Busca endereço do CEP da Receita nos Correios
        try:
            end_correios = consultar_cep(cep_receita)
        except Exception as exc:
            end_correios = {"Status_CEP": str(exc)}
        if delay:
            time.sleep(delay)

        status_validacao = comparar_enderecos(receita, end_correios)

        linhas_validacao.append({
            **prefixo,
            "Documento":                doc_num,
            "Tipo":                     "CNPJ",
            "CEP_Informado":            cep_raw,
            "CEP_Receita":              cep_receita,
            "CEP_Confere_com_Informado": "Sim" if cep_raw == cep_receita else "Não",
            # Endereço da Receita
            "Logradouro_Receita":       receita.get("receita_logradouro", ""),
            "Numero_Receita":           receita.get("receita_numero", ""),
            "Complemento_Receita":      receita.get("receita_complemento", ""),
            "Bairro_Receita":           receita.get("receita_bairro", ""),
            "Cidade_Receita":           receita.get("receita_municipio", ""),
            "UF_Receita":               receita.get("receita_uf", ""),
            # Endereço dos Correios para o CEP da Receita
            "Logradouro_Correios":      end_correios.get("Logradouro", ""),
            "Bairro_Correios":          end_correios.get("Bairro", ""),
            "Cidade_Correios":          end_correios.get("Cidade", ""),
            "UF_Correios":              end_correios.get("UF", ""),
            "Fonte_CEP":                end_correios.get("Fonte_CEP", ""),
            # Resultado
            "Status_Validacao":         status_validacao,
            "Razao_Social":             receita.get("razao_social", ""),
            "Situacao_Cadastral":       receita.get("situacao_cadastral", ""),
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

    # Salva arquivos de saída
    arq_validacao = pasta_saida / "validacao_enderecos.csv"
    pd.DataFrame(linhas_validacao).to_csv(arq_validacao, index=False, encoding="utf-8-sig")

    arq_cadastral = pasta_saida / "dados_cadastrais_cnpj.csv"
    if linhas_cadastrais:
        pd.DataFrame(linhas_cadastrais).to_csv(arq_cadastral, index=False, encoding="utf-8-sig")

    ok   = sum(1 for r in linhas_validacao if r.get("Status_Validacao") in ("Confere", "CEP encontrado"))
    div  = sum(1 for r in linhas_validacao if r.get("Status_Validacao") == "Divergente")
    erro = total - ok - div

    print(f"\nConcluído: {ok} OK | {div} divergente(s) | {erro} erro(s)")
    print(f"  → {arq_validacao}")
    if linhas_cadastrais:
        print(f"  → {arq_cadastral}  ({len(linhas_cadastrais)} CNPJs)")


if __name__ == "__main__":
    import sys
    arquivo = sys.argv[1] if len(sys.argv) > 1 else "base teste.csv"
    processar_planilha(arquivo)
