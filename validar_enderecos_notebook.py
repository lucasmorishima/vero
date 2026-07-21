# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação de Endereços e Dados Cadastrais
# MAGIC
# MAGIC **Fonte:** `hive_metastore.accenture.base_clientes_centralizada` + `tb_dispersao_competencia_analitica`
# MAGIC
# MAGIC **Saída (tabelas Delta):**
# MAGIC - `hive_metastore.accenture.validacao_enderecos`
# MAGIC - `hive_metastore.accenture.validacao_dados_cadastrais`
# MAGIC - `hive_metastore.accenture.validacao_status_fatura`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e configuração

# COMMAND ----------

from __future__ import annotations

import math
import re
import time
import unicodedata
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.getActiveSession()

# Token Correios (opcional — sem token usa ViaCEP)
# Configure via Databricks Secrets: dbutils.secrets.get(scope="vero", key="correios_token")
try:
    TOKEN = dbutils.secrets.get(scope="vero", key="correios_token")
except Exception:
    TOKEN = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query — carrega base de clientes

# COMMAND ----------

_QUERY = """
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
LIMIT 100
"""

df = spark.sql(_QUERY).toPandas().astype(str)
print(f"{len(df)} registros carregados.")
display(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções auxiliares — CEP, normalização, Receita

# COMMAND ----------

# --- CEP ViaCEP / Correios API ---

def _consultar_correios(cep: str) -> dict:
    url = f"https://api.correios.com.br/cep/v2/{cep}"
    r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=10)
    r.raise_for_status()
    d = r.json()
    return {
        "CEP": cep, "Logradouro": d.get("logradouro", ""),
        "Bairro": d.get("bairro", ""), "Cidade": d.get("localidade", ""),
        "UF": d.get("uf", ""), "Complemento": d.get("complemento", ""),
        "Fonte_CEP": "Correios API", "Status_CEP": "OK",
    }

def _consultar_viacep(cep: str) -> dict:
    r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
    r.raise_for_status()
    d = r.json()
    if d.get("erro"):
        raise ValueError("CEP não encontrado no ViaCEP")
    return {
        "CEP": cep, "Logradouro": d.get("logradouro", ""),
        "Bairro": d.get("bairro", ""), "Cidade": d.get("localidade", ""),
        "UF": d.get("uf", ""), "Complemento": d.get("complemento", ""),
        "Fonte_CEP": "ViaCEP", "Status_CEP": "OK",
    }

def consultar_cep(cep: str) -> dict:
    """Tenta Correios API; fallback ViaCEP."""
    if TOKEN:
        try:
            return _consultar_correios(cep)
        except Exception:
            pass
    return _consultar_viacep(cep)

# --- Documento ---

def _limpar_doc(doc: str) -> str:
    return re.sub(r"\D", "", str(doc).strip())

def _cep_generico(cep: str) -> bool:
    """CEPs terminados em 000 são genéricos (sede do município)."""
    return len(cep) == 8 and cep.endswith("000")

def tipo_documento(doc: str) -> str:
    d = _limpar_doc(doc)
    if len(d) == 11: return "CPF"
    if len(d) == 14: return "CNPJ"
    return "INVALIDO"

# --- Receita Federal (BrasilAPI) ---

def _nova_sessao() -> requests.Session:
    retry = Retry(total=4, backoff_factor=1.0,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "VeroValidacaoEnderecos/1.0"})
    return s

def consultar_receita(cnpj: str) -> dict[str, Any]:
    normalizado = _limpar_doc(cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(normalizado)}"
    session = _nova_sessao()
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 429:
            time.sleep(3)
            resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return {"receita_status": f"erro_http_{resp.status_code}"}
        p = resp.json()
        return {
            "receita_status":            "ok",
            "razao_social":              p.get("razao_social"),
            "nome_fantasia":             p.get("nome_fantasia"),
            "situacao_cadastral":        p.get("descricao_situacao_cadastral"),
            "data_situacao_cadastral":   p.get("data_situacao_cadastral"),
            "motivo_situacao_cadastral": p.get("descricao_motivo_situacao_cadastral"),
            "natureza_juridica":         p.get("descricao_natureza_juridica"),
            "data_inicio_atividade":     p.get("data_inicio_atividade"),
            "cnae_principal_codigo":     p.get("cnae_fiscal"),
            "cnae_principal_descricao":  p.get("cnae_fiscal_descricao"),
            "porte":                     p.get("descricao_porte"),
            "capital_social":            p.get("capital_social"),
            "opcao_simples":             p.get("opcao_pelo_simples"),
            "opcao_mei":                 p.get("opcao_pelo_mei"),
            "email":                     p.get("email"),
            "telefone":                  p.get("ddd_telefone_1"),
            "receita_cep":               (p.get("cep") or "").replace("-","").replace(".","").strip().zfill(8),
            "receita_logradouro":        p.get("logradouro"),
            "receita_numero":            p.get("numero"),
            "receita_complemento":       p.get("complemento"),
            "receita_bairro":            p.get("bairro"),
            "receita_municipio":         p.get("municipio"),
            "receita_uf":                p.get("uf"),
        }
    except requests.RequestException as exc:
        return {"receita_status": f"erro_rede: {exc}"}
    finally:
        session.close()

# --- Normalização e comparação de endereços ---

_TIPOS_LOGRADOURO = re.compile(
    r"^(AVENIDA|AVENUE|AV|RUA|RODOVIA|ROD|ESTRADA|EST|ALAMEDA|AL|"
    r"TRAVESSA|TR|TV|PRACA|PCA|PC|LARGO|LGO|LADEIRA|VIELA|BECO|"
    r"SETOR|QUADRA|QD|CONJUNTO|CJ|LOTE|LT|VILA|VL|PARQUE|LINHA|"
    r"CORREDOR|GALERIA|RAMAL|TRECHO|TREVO|VIA|VIADUTO|ACESSO)\b\s*",
    re.IGNORECASE,
)

def _normalizar(texto: str | None) -> str:
    if not texto: return ""
    s = str(texto).upper().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _normalizar_logradouro(texto: str | None) -> str:
    return _TIPOS_LOGRADOURO.sub("", _normalizar(texto)).strip()

def comparar_enderecos(receita: dict, correios: dict) -> str:
    if correios.get("Status_CEP") != "OK":
        return "CEP não encontrado nos Correios"
    uf_ok  = _normalizar(receita.get("receita_uf"))  == _normalizar(correios.get("UF"))
    mun_ok = _normalizar(receita.get("receita_municipio")) == _normalizar(correios.get("Cidade"))
    log_r  = _normalizar_logradouro(receita.get("receita_logradouro"))
    log_c  = _normalizar_logradouro(correios.get("Logradouro"))
    log_ok = (not log_r or not log_c) or (log_r == log_c)
    return "Confere" if (uf_ok and mun_ok and log_ok) else "Divergente"

def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    upper = {c.strip().upper(): c for c in df.columns}
    for nome in candidatos:
        if nome.upper() in upper:
            return upper[nome.upper()]
    return None

print("Funções carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Processamento principal

# COMMAND ----------

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
col_crm     = _encontrar_coluna(df, ["CRM", "SISTEMA_ORIGEM"])

total = len(df)
fonte_cep = "Correios API" if TOKEN else "ViaCEP"
delay = 0.1 if TOKEN else 0.3
print(f"Processando {total} registros | CEP via {fonte_cep}\n")

linhas_validacao:  list[dict] = []
linhas_cadastrais: list[dict] = []
linhas_relatorio:  list[dict] = []

for i, row in df.iterrows():
    doc_raw = str(row[col_doc]).strip()
    cep_raw = str(row[col_cep]).replace("-","").replace(".","").strip().zfill(8)
    tipo    = tipo_documento(doc_raw)
    doc_num = _limpar_doc(doc_raw)

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
    crm         = _val(col_crm)

    cidade_base_limpa = re.sub(r"\s*-\s*\w+$", "", cidade_base).strip()

    prefixo = {
        "Fatura": fatura, "ID_Cliente": id_cli, "Regra": regra, "Segmento": segmento,
        "Cidade_Base": cidade_base, "Bairro_Base": bairro_base, "UF_Base": uf_base,
        "Nome_Cliente_Base": nome_base, "IE_Base": ie_base,
        "Produto": produto, "Tipo_Servico": tipo_svc, "Descricao_Servico": desc_svc,
        "Tipo_Imposto": imposto, "Promocao": promo, "Grupo_Localidade": grupo, "ID_Lote": lote,
        "CRM": crm,
    }

    print(f"  [{int(i)+1:4d}/{total}] {doc_raw[:18]:<20} ({tipo}) ", end="", flush=True)

    # --- CEP genérico ---
    if _cep_generico(cep_raw):
        obs = (f"[CEP] CEP genérico não aceito: {cep_raw[:5]}-{cep_raw[5:]}"
               f" (representa sede do município, não um endereço específico)")
        linhas_validacao.append({**prefixo, "Documento": doc_num or doc_raw,
                                  "Tipo": tipo, "CEP_Informado": cep_raw,
                                  "Status_Validacao": "CEP genérico", "Observacao": obs})
        linhas_relatorio.append({
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "INCORRETO", "SUBSTATUS": "ERRO", "OBSERVACAO": obs,
            "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num or doc_raw} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
        })
        print(f"— CEP GENÉRICO REJEITADO ({cep_raw[:5]}-{cep_raw[5:]})")
        continue

    # --- Documento inválido ---
    if tipo == "INVALIDO":
        linhas_validacao.append({**prefixo, "Documento": doc_raw, "Tipo": "INVALIDO",
                                  "CEP_Informado": cep_raw, "Status_Validacao": "Documento inválido"})
        linhas_relatorio.append({
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "INCORRETO", "SUBSTATUS": "ERRO", "OBSERVACAO": "[DOC] Documento inválido",
            "DADOS_BILLING": f"DOC: {doc_raw} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
            "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
            "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
            "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
        })
        print("— DOCUMENTO INVÁLIDO")
        continue

    # --- CPF ---
    if tipo == "CPF":
        try:
            end = consultar_cep(cep_raw)
            linhas_validacao.append({**prefixo, "Documento": doc_num, "Tipo": "CPF",
                                      "CEP_Informado": cep_raw,
                                      "Logradouro": end.get("Logradouro",""), "Bairro": end.get("Bairro",""),
                                      "Cidade": end.get("Cidade",""), "UF": end.get("UF",""),
                                      "Complemento": end.get("Complemento",""),
                                      "Fonte_CEP": end.get("Fonte_CEP",""), "Status_Validacao": "CEP encontrado"})
            linhas_relatorio.append({
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": "CORRETO", "SUBSTATUS": "OK", "OBSERVACAO": "",
                "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
                "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
            })
            print(f"— {end.get('Logradouro','')[:35]}, {end.get('Cidade','')}/{end.get('UF','')}")
        except Exception as exc:
            linhas_validacao.append({**prefixo, "Documento": doc_num, "Tipo": "CPF",
                                      "CEP_Informado": cep_raw, "Status_Validacao": f"Erro CEP: {exc}"})
            linhas_relatorio.append({
                "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
                "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
                "OBSERVACAO": f"[CEP] CEP não encontrado: {cep_raw}",
                "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
                "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
                "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
                "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
                "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo, "CRM": crm,
            })
            print(f"— ERRO CEP: {exc}")
        if delay: time.sleep(delay)
        continue

    # --- CNPJ ---
    receita = consultar_receita(doc_num)
    time.sleep(0.5)

    if receita.get("receita_status") != "ok":
        linhas_validacao.append({**prefixo, "Documento": doc_num, "Tipo": "CNPJ",
                                  "CEP_Informado": cep_raw,
                                  "Status_Validacao": f"Erro Receita: {receita.get('receita_status')}"})
        linhas_relatorio.append({
            "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
            "STATUS": "INCORRETO", "SUBSTATUS": "ERRO",
            "OBSERVACAO": f"[API] Erro na consulta Receita: {receita.get('receita_status')}",
            "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
            "DADOS_CONTRATO": None, "DADOS_TABELA_VERDADE": None,
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
    if delay: time.sleep(delay)

    status_end    = comparar_enderecos(receita, end_correios)
    cidade_rec    = _normalizar(receita.get("receita_municipio", ""))
    uf_rec        = _normalizar(receita.get("receita_uf", ""))
    cidade_ok     = not cidade_base_limpa or not cidade_rec or _normalizar(cidade_base_limpa) == cidade_rec
    uf_ok         = not uf_base or not uf_rec or _normalizar(uf_base) == uf_rec
    cep_ok        = cep_raw == cep_receita

    divergencias = []
    if _cep_generico(cep_receita):
        divergencias.append(f"[CEP] CEP da Receita é genérico: {cep_receita[:5]}-{cep_receita[5:]} (sede do município)")
    if not cep_ok:
        divergencias.append(f"CEP divergente: base '{cep_raw}' x Receita '{cep_receita}'")
    if not cidade_ok:
        divergencias.append(f"Cidade divergente: base '{cidade_base_limpa}' x Receita '{receita.get('receita_municipio','')}'")
    if not uf_ok:
        divergencias.append(f"UF divergente: base '{uf_base}' x Receita '{receita.get('receita_uf','')}'")
    if status_end == "Divergente":
        divergencias.append("Endereço Receita x Correios divergente")
    elif status_end == "CEP não encontrado nos Correios":
        divergencias.append("CEP da Receita não encontrado nos Correios")

    status_validacao = "Divergente" if divergencias else "Confere"
    observacao       = " | ".join(divergencias)

    linhas_validacao.append({
        **prefixo,
        "Documento": doc_num, "Tipo": "CNPJ", "CEP_Informado": cep_raw,
        "CEP_Receita": cep_receita, "CEP_Confere_com_Informado": "Sim" if cep_ok else "Não",
        "Cidade_Confere": "Sim" if cidade_ok else "Não", "UF_Confere": "Sim" if uf_ok else "Não",
        "Logradouro_Receita": receita.get("receita_logradouro",""),
        "Numero_Receita": receita.get("receita_numero",""),
        "Complemento_Receita": receita.get("receita_complemento",""),
        "Bairro_Receita": receita.get("receita_bairro",""),
        "Cidade_Receita": receita.get("receita_municipio",""),
        "UF_Receita": receita.get("receita_uf",""),
        "Logradouro_Correios": end_correios.get("Logradouro",""),
        "Bairro_Correios": end_correios.get("Bairro",""),
        "Cidade_Correios": end_correios.get("Cidade",""),
        "UF_Correios": end_correios.get("UF",""),
        "Fonte_CEP": end_correios.get("Fonte_CEP",""),
        "Status_Validacao": status_validacao, "Observacao": observacao,
        "Razao_Social": receita.get("razao_social",""),
        "Situacao_Cadastral": receita.get("situacao_cadastral",""),
    })

    _end_rec = (
        f"{receita.get('receita_logradouro','')}, {receita.get('receita_numero','')}"
        f" - {receita.get('receita_bairro','')} - {receita.get('receita_municipio','')}/{receita.get('receita_uf','')}"
    ).strip(", -/")
    linhas_relatorio.append({
        "FATURA": fatura, "ID_CONTA_CONTRATO": id_cli, "REGRA": regra, "SEGMENTO": segmento,
        "STATUS": "INCORRETO" if divergencias else "CORRETO",
        "SUBSTATUS": "ERRO" if divergencias else "OK",
        "OBSERVACAO": observacao,
        "DADOS_BILLING": f"NOME: {nome_base} | DOC: {doc_num} | CEP: {cep_raw} | CIDADE: {cidade_base} | UF: {uf_base} | IE: {ie_base or '-'}",
        "DADOS_CONTRATO": f"RAZAO: {receita.get('razao_social','')} | SITUACAO: {receita.get('situacao_cadastral','')} | CEP: {cep_receita} | END: {_end_rec}",
        "DADOS_TABELA_VERDADE": None,
        "ID_LOTE": lote, "PRODUTO": produto, "TIPO_SERVICO": tipo_svc,
        "DESCRICAO_SERVICO": desc_svc, "TIPO_IMPOSTO": imposto,
        "PROMOCAO": promo or None, "GRUPO_LOCALIDADE": grupo,
    })

    linhas_cadastrais.append({
        **prefixo,
        "CNPJ": doc_num,
        "Razao_Social": receita.get("razao_social"),
        "Nome_Fantasia": receita.get("nome_fantasia"),
        "Situacao_Cadastral": receita.get("situacao_cadastral"),
        "Data_Situacao_Cadastral": receita.get("data_situacao_cadastral"),
        "Motivo_Situacao": receita.get("motivo_situacao_cadastral"),
        "Natureza_Juridica": receita.get("natureza_juridica"),
        "Data_Inicio_Atividade": receita.get("data_inicio_atividade"),
        "CNAE_Principal_Codigo": receita.get("cnae_principal_codigo"),
        "CNAE_Principal_Descricao": receita.get("cnae_principal_descricao"),
        "Porte": receita.get("porte"),
        "Capital_Social": receita.get("capital_social"),
        "Simples_Nacional": receita.get("opcao_simples"),
        "MEI": receita.get("opcao_mei"),
        "Email": receita.get("email"),
        "Telefone": receita.get("telefone"),
        "CEP_Receita": receita.get("receita_cep"),
        "Logradouro_Receita": receita.get("receita_logradouro"),
        "Numero_Receita": receita.get("receita_numero"),
        "Complemento_Receita": receita.get("receita_complemento"),
        "Bairro_Receita": receita.get("receita_bairro"),
        "Municipio_Receita": receita.get("receita_municipio"),
        "UF_Receita": receita.get("receita_uf"),
    })

    print(f"— {status_validacao} | {receita.get('razao_social','')[:30]} | CEP Receita: {cep_receita}")

ok   = sum(1 for r in linhas_validacao if r.get("Status_Validacao") in ("Confere", "CEP encontrado"))
div  = sum(1 for r in linhas_validacao if r.get("Status_Validacao") == "Divergente")
erro = sum(1 for r in linhas_validacao if r.get("Status_Validacao") not in ("Confere", "CEP encontrado", "Divergente"))
print(f"\nConcluído: {ok} OK | {div} divergente(s) | {erro} erro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grava resultados nas tabelas Delta

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

CATALOG = "hive_metastore.accenture"

# Schema explícito para validacao_dados_cadastrais
_SCHEMA_CADASTRAIS = {
    "CNAE_Principal_Codigo": LongType(),
    "Capital_Social":        DoubleType(),
    "Simples_Nacional":      BooleanType(),
    "MEI":                   BooleanType(),
}

_VAZIOS = {"", "nan", "NaN", "None", "none", "null", "NULL", "NaT", "na", "NA"}

def _to_spark(linhas: list[dict], tipos: dict | None = None):
    """Converte lista de dicts para Spark DataFrame com schema explícito.
    Qualquer valor vazio/nulo (None, NaN, '', 'null', 'nan', etc.) vira NULL no Spark."""
    df_pd = pd.DataFrame(linhas)
    tipos = tipos or {}

    for col in df_pd.columns:
        spark_type = tipos.get(col)

        if isinstance(spark_type, LongType):
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce") \
                           .apply(lambda x: int(x) if pd.notna(x) else None)

        elif isinstance(spark_type, DoubleType):
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce") \
                           .apply(lambda x: float(x) if pd.notna(x) else None)

        elif isinstance(spark_type, BooleanType):
            df_pd[col] = df_pd[col].apply(
                lambda x: None if (x is None or str(x).lower() in _VAZIOS)
                          else str(x).lower() not in ("false", "0")
            )

        else:  # StringType — normaliza todos os vazios para None → NULL
            df_pd[col] = df_pd[col].astype(str).apply(
                lambda x: None if x in _VAZIOS else x
            )

    schema = StructType([
        StructField(c, tipos.get(c, StringType()), True)
        for c in df_pd.columns
    ])
    return spark.createDataFrame(df_pd, schema=schema)

# validacao_enderecos
if linhas_validacao:
    _to_spark(linhas_validacao) \
        .write.mode("append").saveAsTable(f"{CATALOG}.validacao_enderecos")
    print(f"✓ {len(linhas_validacao)} registros → {CATALOG}.validacao_enderecos")

# validacao_dados_cadastrais
if linhas_cadastrais:
    _to_spark(linhas_cadastrais, tipos=_SCHEMA_CADASTRAIS) \
        .write.mode("append").saveAsTable(f"{CATALOG}.validacao_dados_cadastrais")
    print(f"✓ {len(linhas_cadastrais)} registros → {CATALOG}.validacao_dados_cadastrais")

# validacao_status_fatura
if linhas_relatorio:
    _to_spark(linhas_relatorio) \
        .write.mode("append").saveAsTable(f"{CATALOG}.validacao_status_fatura")
    print(f"✓ {len(linhas_relatorio)} registros → {CATALOG}.validacao_status_fatura")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificação rápida dos resultados

# COMMAND ----------

display(spark.sql(f"SELECT STATUS, SUBSTATUS, COUNT(*) AS QTD FROM {CATALOG}.validacao_status_fatura GROUP BY 1, 2 ORDER BY 1, 2"))
