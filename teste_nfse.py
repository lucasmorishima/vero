"""
teste_nfse.py
-------------
Teste local da API NFS-e Via (Nota Fiscal de Exploração de Via) — Serpro/RFB.
Retorna alíquotas de ISS por município associadas a trechos de concessão.

Endpoints (Anexo V v1.1 — julho/2026):
  Homologação : https://hom-cert-api-nfsevia.np.estaleiro.serpro.gov.br/municipio
  Prod. Restrita: https://producaorestrita.certificado.api.via.nfse.gov.br/municipio
  Produção    : https://certificado.api.via.nfse.gov.br/municipio

ATENÇÃO: A API exige autenticação mTLS (certificado ICP-Brasil A1/A3).
  Sem certificado o servidor retorna HTTP 496 (No Client Certificate).
  Configure CERT_PATH e KEY_PATH com os arquivos do certificado digital.

Saída: CSV em output_endereco/teste_nfse.csv

Uso:
    python teste_nfse.py
"""

import csv
import json
import time
import concurrent.futures
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

# Ambiente — troque para produção quando tiver certificado válido
URL_BASE = "https://hom-cert-api-nfsevia.np.estaleiro.serpro.gov.br/municipio"

# Certificado mTLS ICP-Brasil (obrigatório para retorno 200)
# Formato: tupla (cert.pem, key.pem) ou caminho para .pfx convertido
CERT_PATH = None   # ex: ("caminho/cert.pem", "caminho/key.pem")
VERIFY_SSL = False  # False apenas para teste de conectividade; True em produção

HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json",
}

MAX_WORKERS = 5
MAX_RETRIES = 2
TIMEOUT_SEG = 20

ARQUIVO_SAIDA = Path(__file__).parent / "output_endereco" / "teste_nfse.csv"

# ---------------------------------------------------------------------------
# Municípios para teste
# ---------------------------------------------------------------------------
MUNICIPIOS_TESTE = [
    {"codigo_ibge": "3550308", "municipio": "São Paulo",      "uf": "SP"},
    {"codigo_ibge": "3304557", "municipio": "Rio de Janeiro", "uf": "RJ"},
    {"codigo_ibge": "3106200", "municipio": "Belo Horizonte", "uf": "MG"},
    {"codigo_ibge": "4106902", "municipio": "Curitiba",       "uf": "PR"},
    {"codigo_ibge": "4314902", "municipio": "Porto Alegre",   "uf": "RS"},
    {"codigo_ibge": "2927408", "municipio": "Salvador",       "uf": "BA"},
    {"codigo_ibge": "2304400", "municipio": "Fortaleza",      "uf": "CE"},
    {"codigo_ibge": "2611606", "municipio": "Recife",         "uf": "PE"},
    {"codigo_ibge": "1302603", "municipio": "Manaus",         "uf": "AM"},
    {"codigo_ibge": "5300108", "municipio": "Brasília",       "uf": "DF"},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nova_sessao() -> requests.Session:
    retry = Retry(
        total=MAX_RETRIES, backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    if CERT_PATH:
        s.cert = CERT_PATH
    return s


def _registro_erro(mun: dict, data: str, status: str, detalhe: str = "") -> dict:
    return {
        "codigo_ibge":      mun["codigo_ibge"],
        "municipio":        mun["municipio"],
        "uf":               mun["uf"],
        "codigo_trecho":    None,
        "nome_trecho":      None,
        "nome_contrato":    None,
        "aliquota":         None,
        "aliquota_efetiva": None,
        "percentual":       None,
        "extensao":         None,
        "inicio_vigencia":  None,
        "fim_vigencia":     None,
        "situacao":         None,
        "fonte":            "NFS-e Via",
        "data_consulta":    data,
        "status":           status,
        "detalhe":          detalhe[:300] if detalhe else None,
        "json_retorno":     None,
    }


def consultar(mun: dict) -> list[dict]:
    """
    GET /Aliquotas/municipio/{codigoMunicipio}
    Retorna todos os trechos e alíquotas de ISS associados ao município.
    """
    data_consulta = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    session = _nova_sessao()
    url = f"{URL_BASE}/Aliquotas/municipio/{mun['codigo_ibge']}"

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT_SEG, verify=VERIFY_SSL)

            if resp.status_code == 200:
                try:
                    dados = resp.json()
                except json.JSONDecodeError:
                    return [_registro_erro(mun, data_consulta, "json_invalido", resp.text[:200])]

                # Estrutura esperada: {"sucesso": true, "dados": [...], "notificacoes": [...]}
                itens = dados.get("dados") if isinstance(dados, dict) else dados
                if not itens:
                    return [_registro_erro(mun, data_consulta, "sem_dados", resp.text[:200])]

                registros = []
                for contrato in (itens if isinstance(itens, list) else [itens]):
                    nome_contrato = contrato.get("nomeContrato") or contrato.get("codigoContrato", "")
                    for trecho in contrato.get("aliquotas", [contrato]):
                        try:
                            aliq = float(trecho.get("aliquota") or 0)
                        except (TypeError, ValueError):
                            aliq = None
                        try:
                            aliq_ef = float(trecho.get("aliquotaEfetiva") or 0)
                        except (TypeError, ValueError):
                            aliq_ef = None
                        try:
                            pct = float(trecho.get("percentual") or 0)
                        except (TypeError, ValueError):
                            pct = None
                        try:
                            ext = float(trecho.get("extensao") or 0)
                        except (TypeError, ValueError):
                            ext = None

                        registros.append({
                            "codigo_ibge":      str(mun["codigo_ibge"]),
                            "municipio":        str(mun["municipio"]),
                            "uf":               str(mun["uf"]),
                            "codigo_trecho":    trecho.get("codigoTrecho"),
                            "nome_trecho":      contrato.get("nomeTrecho"),
                            "nome_contrato":    nome_contrato,
                            "aliquota":         aliq,
                            "aliquota_efetiva": aliq_ef,
                            "percentual":       pct,
                            "extensao":         ext,
                            "inicio_vigencia":  trecho.get("dhInicioVigencia"),
                            "fim_vigencia":     trecho.get("dhFimVigencia"),
                            "situacao":         trecho.get("situacao"),
                            "fonte":            "NFS-e Via",
                            "data_consulta":    data_consulta,
                            "status":           "ok",
                            "detalhe":          None,
                            "json_retorno":     json.dumps(trecho, ensure_ascii=False),
                        })
                return registros if registros else [_registro_erro(mun, data_consulta, "sem_trechos")]

            elif resp.status_code == 496:
                return [_registro_erro(mun, data_consulta, "sem_certificado",
                                       "Certificado mTLS ICP-Brasil obrigatório (HTTP 496)")]
            elif resp.status_code == 401:
                return [_registro_erro(mun, data_consulta, "nao_autorizado", resp.text[:200])]
            elif resp.status_code == 404:
                return [_registro_erro(mun, data_consulta, "nao_encontrado", f"URL: {url}")]
            elif resp.status_code == 429:
                time.sleep(2 ** tentativa)
                continue
            else:
                if tentativa == MAX_RETRIES:
                    return [_registro_erro(mun, data_consulta,
                                           f"erro_http_{resp.status_code}", resp.text[:200])]
                time.sleep(tentativa)

        except requests.Timeout:
            if tentativa == MAX_RETRIES:
                return [_registro_erro(mun, data_consulta, "timeout")]
            time.sleep(tentativa)
        except requests.RequestException as exc:
            if tentativa == MAX_RETRIES:
                return [_registro_erro(mun, data_consulta, "erro_rede", str(exc))]
            time.sleep(tentativa)
        finally:
            session.close()

    return [_registro_erro(mun, data_consulta, "max_tentativas")]


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def main():
    total = len(MUNICIPIOS_TESTE)

    print(f"\nTestando API NFS-e Via — Serpro/RFB")
    print(f"Ambiente   : {URL_BASE}")
    print(f"Certificado: {'configurado' if CERT_PATH else 'NÃO configurado (esperado HTTP 496)'}")
    print(f"Municípios : {total} | Workers: {MAX_WORKERS}\n")

    t0 = time.monotonic()
    todos: list[dict] = []
    sucessos = erros = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(consultar, mun): mun for mun in MUNICIPIOS_TESTE}
        for future in concurrent.futures.as_completed(futures):
            mun = futures[future]
            try:
                registros = future.result()
                todos.extend(registros)
                if registros[0]["status"] == "ok":
                    sucessos += 1
                    print(f"  [OK]   {mun['municipio']}/{mun['uf']} — {len(registros)} trecho(s)")
                else:
                    erros += 1
                    print(f"  [ERRO] {mun['municipio']}/{mun['uf']} — {registros[0]['status']}: {registros[0]['detalhe'] or ''}")
            except Exception as exc:
                erros += 1
                print(f"  [EXCEÇÃO] {mun['municipio']}/{mun['uf']} — {exc}")

    tempo = time.monotonic() - t0

    # Salva CSV
    ARQUIVO_SAIDA.parent.mkdir(exist_ok=True)
    colunas = [
        "codigo_ibge", "municipio", "uf", "codigo_trecho", "nome_trecho",
        "nome_contrato", "aliquota", "aliquota_efetiva", "percentual", "extensao",
        "inicio_vigencia", "fim_vigencia", "situacao",
        "fonte", "data_consulta", "status", "detalhe", "json_retorno",
    ]
    with ARQUIVO_SAIDA.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for r in todos:
            writer.writerow({c: r.get(c) for c in colunas})

    contagem: dict[str, int] = {}
    for r in todos:
        contagem[r["status"]] = contagem.get(r["status"], 0) + 1

    print(f"\n{'='*55}")
    print(f"Municípios processados : {total}")
    print(f"Sucessos               : {sucessos}")
    print(f"Erros                  : {erros}")
    print(f"Total de registros     : {len(todos)}")
    print(f"Tempo total            : {tempo:.1f}s")
    print(f"\nDistribuição por status:")
    for status, qtd in sorted(contagem.items()):
        print(f"  {status:<35} {qtd}")
    print(f"\nArquivo salvo: {ARQUIVO_SAIDA}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
