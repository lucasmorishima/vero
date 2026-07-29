# Databricks notebook source
# MAGIC %md
# MAGIC # BILLING ASSURANCE — Standing `dados_nfcom_cliente` — V3 Silver (Colunas Reais)
# MAGIC **Vero Internet | Accenture | v3.0**
# MAGIC
# MAGIC Mapeamento 100% validado contra os dados reais de cada Silver.
# MAGIC Sem campos de fatura. Sem validação cadastral/endereço.
# MAGIC
# MAGIC | Fonte   | Tabela Silver                                  | Filtro de ciclo               |
# MAGIC |---------|------------------------------------------------|-------------------------------|
# MAGIC | NG      | `silver.NG_RELATORIO_OFICIAL_FATURAMENTO`      | FATURA_DATA_EMISSAO dd/MM/yyyy|
# MAGIC | ADAPTER | `silver.Adapter_RELATORIO_OFICIAL_FATURAMENTO` | DATA_EMISSAO_NOTA (Timestamp) |
# MAGIC | SIMETRA | `silver.SIMETRA_RELATORIO_OFICIAL_FATURAMENTO` | FT_EMISSAO (AAAAMMDD int)     |
# MAGIC
# MAGIC ## Mapa de campos por sistema
# MAGIC | Campo destino         | NG                       | ADAPTER                        | SIMETRA         |
# MAGIC |-----------------------|--------------------------|--------------------------------|-----------------|
# MAGIC | nf_numero             | NF_NUMERO                | NUMERO_NF                      | FT_NFISCAL      |
# MAGIC | nf_serie              | —                        | —                              | FT_SERIE        |
# MAGIC | nf_valor              | NF_VALOR                 | ValorNotaFiltro                | FT_VALCONT      |
# MAGIC | data_emissao          | NF_DATA_EMISSAO          | DATA_EMISSAO_NOTA              | FT_EMISSAO      |
# MAGIC | chave_acesso_nfcom    | CHAVE_ACESSO_NFCOM       | —                              | FT_CHVNFE       |
# MAGIC | status_integracao     | STATUS_INTEGRACAO_NFCOM  | —                              | F3_DESCRET      |
# MAGIC | tipo_emissao_nfcom    | TIPO_EMISSAO_NFCOM       | Tipo_Emissao                   | —               |
# MAGIC | regime_especial       | REGIME_ESPECIAL          | REGIME_ESPECIAL                | FT_CLASFIS      |
# MAGIC | cancelada             | CANCELADA                | NotaCancelada                  | FT_DTCANC       |
# MAGIC | status_nfcom          | STATUS_NFCOM             | —                              | —               |
# MAGIC | nota_substituta       | NOTA_SUBSTITUTA          | —                              | IS_NFSUBS       |
# MAGIC | empresa_prestadora    | EMPRESA_PRESTADORA       | Empresa                        | FT_FILIAL       |
# MAGIC | pessoa_emissora       | PESSOA_EMISSORA          | —                              | —               |
# MAGIC | operadora             | OPERADORA                | OPERACAO                       | —               |
# MAGIC | id_contrato           | CONTA_NUMERO             | CONTRATO                       | COD_CNTR        |
# MAGIC | id_cliente            | COD_CLIENTE_SAP          | IDCliente                      | FT_CLIEFOR      |
# MAGIC | cpf_cnpj              | —                        | CPF_CNPJ                       | —               |
# MAGIC | nome_cliente          | NOME_ASSINANTE           | Cliente                        | —               |
# MAGIC | tipo_assinante        | TIPO_ASSINANTE           | —                              | —               |
# MAGIC | status_contrato       | —                        | Status_Contrato                | —               |
# MAGIC | uf_dest               | NF_UF                    | —                              | FT_ESTADO       |
# MAGIC | cidade_dest           | NF_CIDADE                | Cidade                         | —               |
# MAGIC | cod_produto           | NF_ITEM_COD_SAP          | ITEM_CODE_SAP                  | B1_COD          |
# MAGIC | posicao_item          | POSICAO_ITEM             | Ordem                          | FT_ITEM         |
# MAGIC | descricao_item        | NF_ITEM_DESCRICAO        | DescricaoFiscal                | B1_DESC         |
# MAGIC | cclass                | CCLASS                   | codigoClassificacao            | B1_XCCLASS      |
# MAGIC | tipo_receita          | —                        | TipoReceita                    | TIPO_PROD       |
# MAGIC | data_inicio_cobranca  | DATA_INICIO_COBRANCA     | —                              | —               |
# MAGIC | data_fim_cobranca     | DATA_FIM_COBRANCA        | —                              | —               |
# MAGIC | nf_item_valor         | NF_ITEM_VALOR            | ValorItemFiltro                | FT_TOTAL        |
# MAGIC | desconto              | —                        | Desconto                       | FT_DESCONT      |
# MAGIC | cfop                  | CFOP                     | NumeroCFOP                     | FT_CFOP         |
# MAGIC | icms_cst              | ICMS_CST                 | CST                            | —               |
# MAGIC | icms_base             | ICMS_BASE_CALCULO        | BaseICMS                       | FT_BASEICM      |
# MAGIC | icms_aliquota         | ICMS_ALIQUOTA            | —                              | FT_ALIQICM      |
# MAGIC | icms_valor            | ICMS_VALOR_IMPOSTO       | ValorICMS                      | FT_VALICM       |
# MAGIC | icms_isento           | —                        | IsentoICMS                     | FT_ISENICM      |
# MAGIC | iss_cst               | ISS_CST                  | —                              | F3_CSTISS       |
# MAGIC | iss_base              | ISS_BASE_CALCULO         | BaseISS                        | —               |
# MAGIC | iss_aliquota          | ISS_ALIQUOTA             | —                              | —               |
# MAGIC | iss_valor             | ISS_VALOR_IMPOSTO        | ValorISS                       | —               |
# MAGIC | pis_cst               | PIS_CST                  | —                              | FT_CSTPIS       |
# MAGIC | pis_base              | PIS_BASE_CALCULO         | BasePIS                        | FT_BASEPIS      |
# MAGIC | pis_aliquota          | PIS_ALIQUOTA             | PIS (%)                        | FT_ALIQPIS      |
# MAGIC | pis_valor             | PIS_VALOR_IMPOSTO        | ValorPIS                       | FT_VALPIS       |
# MAGIC | cofins_cst            | COFINS_CST               | —                              | FT_CSTCOF       |
# MAGIC | cofins_base           | COFINS_BASE_CALCULO      | BaseCOFINS                     | FT_BASECOF      |
# MAGIC | cofins_aliquota       | COFINS_ALIQUOTA          | COFINS (%)                     | FT_ALIQCOF      |
# MAGIC | cofins_valor          | COFINS_VALOR_IMPOSTO     | ValorCOFINS                    | FT_VALCOF       |
# MAGIC | fust_cst              | FUST_CST                 | —                              | —               |
# MAGIC | fust_base             | FUST_BASE_CALCULO        | —                              | FT_BASIMP5      |
# MAGIC | fust_aliquota         | FUST_ALIQUOTA            | Fust                           | FT_ALQIMP5      |
# MAGIC | fust_valor            | FUST_VALOR_IMPOSTO       | —                              | FT_VALIMP5      |
# MAGIC | funttel_cst           | FUNTTEL_CST              | —                              | —               |
# MAGIC | funttel_base          | FUNTTEL_BASE_CALCULO     | —                              | FT_BASIMP6      |
# MAGIC | funttel_aliquota      | FUNTTEL_ALIQUOTA         | AliquotaFunttel                | FT_ALQIMP6      |
# MAGIC | funttel_valor         | FUNTTEL_VALOR_IMPOSTO    | ValorFuntel                    | FT_VALIMP6      |
# MAGIC | ir_retido_aliquota    | ALIQUOTA_IR_RETIDO       | —                              | FT_ALIQIRR      |
# MAGIC | ir_retido_valor       | VALOR_IR_RETIDO          | —                              | FT_VALIRR       |
# MAGIC | pis_retido_aliquota   | ALIQUOTA_PIS_RETIDO      | —                              | FT_ARETPIS      |
# MAGIC | pis_retido_valor      | VALOR_PIS_RETIDO         | —                              | FT_VRETPIS      |
# MAGIC | cofins_retido_aliq    | ALIQUOTA_COFINS_RETIDO   | —                              | FT_ARETCOF      |
# MAGIC | cofins_retido_valor   | VALOR_COFINS_RETIDO      | —                              | FT_VRETCOF      |
# MAGIC | csll_retido_aliq      | ALIQUOTA_CSLL_RETIDO     | —                              | FT_ARETCSL      |
# MAGIC | csll_retido_valor     | VALOR_CSLL_RETIDO        | —                              | FT_VRETCSL      |
# MAGIC | iss_retido_valor      | VALOR_ISS_RETIDO         | —                              | FT_VALINS       |
# MAGIC | conta_debito_rec      | CONTA_DEBITO_REC         | ContrapartidaDebito            | —               |
# MAGIC | descr_conta_debito_rec| DESCR_CONTA_DEBITO_REC   | —                              | —               |
# MAGIC | conta_debito_adiant   | CONTA_DEBITO_ADIANT      | —                              | —               |
# MAGIC | conta_credito_rec     | CONTA_CREDITO_REC        | CreditoReceita                 | —               |
# MAGIC | descr_conta_credito   | DESCR_CONTA_CREDITO_REC  | —                              | —               |
# MAGIC | conta_debito_icms     | CONTA_DEBITO_ICMS        | ClassificadorContaDebitoICMS   | —               |
# MAGIC | descr_conta_deb_icms  | CONTA_DEBITO_ICMS_DESCR  | DescricaoContaDebitoICMS       | —               |
# MAGIC | conta_credito_icms    | CONTA_CREDITO_ICMS       | ClassificadorContaCreditoICMS  | —               |
# MAGIC | descr_conta_cred_icms | CONTA_CREDITO_ICMS_DESCR | DescricaoContaCreditoICMS      | —               |
# MAGIC | conta_debito_pis      | CONTA_DEBITO_PIS         | ClassificadorContaDebitoPis    | —               |
# MAGIC | descr_conta_deb_pis   | CONTA_DEBITO_PIS_DESCR   | DescricaoContaDebitoPis        | —               |
# MAGIC | conta_credito_pis     | CONTA_CREDITO_PIS        | ClassificadorContaCreditoPis   | —               |
# MAGIC | descr_conta_cred_pis  | CONTA_CREDITO_PIS_DESCR  | DescricaoContaCreditoPis       | —               |
# MAGIC | conta_debito_cofins   | CONTA_DEBITO_COFINS      | ClassificadorContaDebitoCofins | —               |
# MAGIC | descr_conta_deb_cof   | CONTA_DEBITO_COFINS_DESCR| DescricaoContaDebitoCofins     | —               |
# MAGIC | conta_credito_cofins  | CONTA_CREDITO_COFINS     | ClassificadorContaCreditoCofins| —               |
# MAGIC | descr_conta_cred_cof  | CONTA_CREDITO_COFINS_DESCR|DescricaoContaCreditoCofins    | —               |
# MAGIC | conta_debito_iss      | CONTA_DEBITO_ISS         | ClassificadorContaDebitoISS    | —               |
# MAGIC | descr_conta_deb_iss   | CONTA_DEBITO_ISS_DESCR   | DescricaoContaDebitoISS        | —               |
# MAGIC | conta_credito_iss     | CONTA_CREDITO_ISS        | ClassificadorContaCreditoISS   | —               |
# MAGIC | descr_conta_cred_iss  | CONTA_CREDITO_ISS_DESCR  | DescricaoContaCreditoISS       | —               |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

from datetime import datetime as _dt

dbutils.widgets.removeAll()
dbutils.widgets.text("ciclo_ref",         _dt.now().strftime("%Y%m"), "Ciclo (AAAAMM)")
dbutils.widgets.text("schema_dest",       "accenture",               "Schema Destino")
dbutils.widgets.text("executar_optimize", "true",                    "Executar OPTIMIZE?")

CICLO_REF         = dbutils.widgets.get("ciclo_ref")
SCHEMA_DEST       = dbutils.widgets.get("schema_dest")
EXECUTAR_OPTIMIZE = dbutils.widgets.get("executar_optimize").lower() == "true"

TABELA_DEST = f"{SCHEMA_DEST}.dados_nfcom_cliente_v3"
TBL_NG      = "silver.NG_RELATORIO_OFICIAL_FATURAMENTO"
TBL_ADAPTER = "silver.Adapter_RELATORIO_OFICIAL_FATURAMENTO"
TBL_SIMETRA = "silver.SIMETRA_RELATORIO_OFICIAL_FATURAMENTO"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import logging
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DecimalType, IntegerType, StringType

log = logging.getLogger("ba.standing.v3")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

spark.conf.set("spark.sql.shuffle.partitions",                 "200")
spark.conf.set("spark.sql.legacy.timeParserPolicy",            "LEGACY")

# ── Helpers ──────────────────────────────────────────────────────────────────
def _s(c):   return F.trim(c.cast(StringType()))
def _up(c):  return F.upper(F.trim(c.cast(StringType())))
def _d2(c):  return c.cast(DecimalType(18, 2))
def _d4(c):  return c.cast(DecimalType(8,  4))
def _int(c): return c.cast(IntegerType())

NUL_STR  = F.lit(None).cast(StringType())
NUL_D2   = F.lit(None).cast(DecimalType(18, 2))
NUL_D4   = F.lit(None).cast(DecimalType(8,  4))
NUL_DATE = F.lit(None).cast(DateType())
ZERO2    = F.lit(0).cast(DecimalType(18, 2))

# ── Parsers de ciclo ─────────────────────────────────────────────────────────
def _ciclo_dd_mm_yyyy(c):
    """'dd/MM/yyyy' → 'AAAAMM'  (NG: FATURA_DATA_EMISSAO)"""
    return F.date_format(F.to_date(c, "dd/MM/yyyy"), "yyyyMM")

def _ciclo_ts(c):
    """Timestamp → 'AAAAMM'  (ADAPTER: DATA_EMISSAO_NOTA)"""
    return F.date_format(c, "yyyyMM")

def _ciclo_yyyymmdd(c):
    """Int AAAAMMDD → 'AAAAMM'  (SIMETRA: FT_EMISSAO)"""
    return F.substring(c.cast(StringType()), 1, 6)

def _to_date_safe(c):
    """
    Converte para DATE tolerando três formatos que coexistem nas Silver:
      1. Timestamp nativo  (DATA_INICIO_COBRANCA no NG)
      2. String dd/MM/yyyy (DATA_FIM_COBRANCA no NG, FATURA_DATA_EMISSAO)
      3. String yyyy-MM-dd (fallback ISO)
    Usa coalesce com try_to_timestamp para não quebrar em dados sujos.
    """
    col_str = c.cast(StringType())
    return F.coalesce(
        # Formato Timestamp nativo → cast direto para DATE
        F.try_to_timestamp(c).cast(DateType()),
        # dd/MM/yyyy
        F.when(col_str.rlike(r"^\d{2}/\d{2}/\d{4}$"),
               F.to_date(col_str, "dd/MM/yyyy")).otherwise(F.lit(None).cast(DateType())),
        # yyyy-MM-dd ISO
        F.when(col_str.rlike(r"^\d{4}-\d{2}-\d{2}"),
               F.to_date(col_str, "yyyy-MM-dd")).otherwise(F.lit(None).cast(DateType())),
    )

log.info("Setup V3 | ciclo=%s | destino=%s", CICLO_REF, TABELA_DEST)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL — Tabela destino

# COMMAND ----------

# ── Gestão de schema: detecta divergência e recria a tabela se necessário ────
# O MERGE falha quando a tabela destino tem colunas diferentes do DataFrame.
# Estratégia: comparar colunas do schema atual com as colunas do novo código.
# Se divergirem → DROP + CREATE (recarga completa do ciclo informado).

COLUNAS_ESPERADAS = {
    "id_registro","sistema_origem","ciclo_faturamento","dt_carga","dt_processamento",
    "hash_registro","categoria_fiscal","modelo_nf","tipo_emissao_nfcom","nf_numero",
    "nf_serie","nf_valor","data_emissao","chave_acesso_nfcom","status_integracao_nfcom",
    "regime_especial","cancelada","status_nfcom","nota_substituta","empresa_prestadora",
    "pessoa_emissora","operadora","id_contrato","id_cliente","cpf_cnpj","nome_cliente",
    "tipo_assinante","status_contrato","uf_dest","cidade_dest","cod_produto","posicao_item",
    "descricao_item","cclass","tipo_receita","data_inicio_cobranca","data_fim_cobranca",
    "nf_item_valor","desconto","cfop","icms_cst","icms_base","icms_aliquota","icms_valor",
    "icms_isento","iss_cst","iss_base","iss_aliquota","iss_valor","pis_cst","pis_base",
    "pis_aliquota","pis_valor","cofins_cst","cofins_base","cofins_aliquota","cofins_valor",
    "fust_cst","fust_base","fust_aliquota","fust_valor","funttel_cst","funttel_base",
    "funttel_aliquota","funttel_valor","ir_retido_aliquota","ir_retido_valor",
    "pis_retido_aliquota","pis_retido_valor","cofins_retido_aliquota","cofins_retido_valor",
    "csll_retido_aliquota","csll_retido_valor","iss_retido_valor","conta_debito_rec",
    "descr_conta_debito_rec","conta_debito_adiant","conta_credito_rec",
    "descr_conta_credito_rec","conta_debito_icms","descr_conta_debito_icms",
    "conta_credito_icms","descr_conta_credito_icms","conta_debito_pis","descr_conta_debito_pis",
    "conta_credito_pis","descr_conta_credito_pis","conta_debito_cofins",
    "descr_conta_debito_cofins","conta_credito_cofins","descr_conta_credito_cofins",
    "conta_debito_iss","descr_conta_debito_iss","conta_credito_iss","descr_conta_credito_iss",
    "ind_sem_cst","faturamento_zerado","fatura_sem_numero","ind_cancelada",
}

tabela_existe = spark.catalog.tableExists(TABELA_DEST)

if tabela_existe:
    colunas_atuais = {f.name for f in spark.table(TABELA_DEST).schema.fields}
    adicionadas    = COLUNAS_ESPERADAS - colunas_atuais
    removidas      = colunas_atuais - COLUNAS_ESPERADAS
    schema_ok      = not adicionadas and not removidas
    if not schema_ok:
        log.warning("Schema divergente! Adicionadas=%s | Removidas=%s", adicionadas, removidas)
        log.warning("Executando DROP TABLE + recriação. Dados do ciclo %s serão recarregados.", CICLO_REF)
        spark.sql(f"DROP TABLE IF EXISTS {TABELA_DEST}")
        tabela_existe = False
    else:
        log.info("Schema OK — %d colunas compatíveis.", len(colunas_atuais))
else:
    log.info("Tabela não existe — será criada.")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_DEST} (

    -- ── Metadados ──────────────────────────────────────────────────────────
    id_registro              STRING        COMMENT 'PK MD5',
    sistema_origem           STRING        COMMENT 'NG | ADAPTER | SIMETRA',
    ciclo_faturamento        STRING        COMMENT 'AAAAMM',
    dt_carga                 TIMESTAMP,
    dt_processamento         TIMESTAMP,
    hash_registro            STRING        COMMENT 'SHA256 para CDC',

    -- ── Classificação da nota ──────────────────────────────────────────────
    categoria_fiscal         STRING        COMMENT 'NG: ISS|ICMS|DEBITO / ADAPTER: TipoNF / SIMETRA: TIPO_PROD',
    modelo_nf                STRING        COMMENT 'NG: NF_COM|ISS|DEBITOS / SIMETRA: C5_TIPOF',
    tipo_emissao_nfcom       STRING        COMMENT 'NG: TIPO_EMISSAO_NFCOM / ADAPTER: Tipo_Emissao',

    -- ── Nota Fiscal ────────────────────────────────────────────────────────
    nf_numero                STRING        COMMENT 'NG: NF_NUMERO / ADAPTER: NUMERO_NF / SIMETRA: FT_NFISCAL',
    nf_serie                 STRING        COMMENT 'SIMETRA: FT_SERIE',
    nf_valor                 DECIMAL(18,2) COMMENT 'NG: NF_VALOR / ADAPTER: ValorNotaFiltro / SIMETRA: FT_VALCONT',
    data_emissao             DATE          COMMENT 'NG: NF_DATA_EMISSAO / ADAPTER: DATA_EMISSAO_NOTA / SIMETRA: FT_EMISSAO',
    chave_acesso_nfcom       STRING        COMMENT 'NG: CHAVE_ACESSO_NFCOM / SIMETRA: FT_CHVNFE',
    status_integracao_nfcom  STRING        COMMENT 'NG: STATUS_INTEGRACAO_NFCOM / SIMETRA: F3_DESCRET',
    regime_especial          STRING        COMMENT 'NG+ADAPTER: REGIME_ESPECIAL / SIMETRA: FT_CLASFIS',
    cancelada                STRING        COMMENT 'NG: CANCELADA / ADAPTER: NotaCancelada / SIMETRA: FT_DTCANC',
    status_nfcom             STRING        COMMENT 'NG: STATUS_NFCOM',
    nota_substituta          STRING        COMMENT 'NG: NOTA_SUBSTITUTA / SIMETRA: IS_NFSUBS',

    -- ── Emitente ───────────────────────────────────────────────────────────
    empresa_prestadora       STRING        COMMENT 'NG: EMPRESA_PRESTADORA / ADAPTER: Empresa / SIMETRA: FT_FILIAL',
    pessoa_emissora          STRING        COMMENT 'NG: PESSOA_EMISSORA (CNPJ)',
    operadora                STRING        COMMENT 'NG: OPERADORA / ADAPTER: OPERACAO',

    -- ── Cliente ────────────────────────────────────────────────────────────
    id_contrato              STRING        COMMENT 'NG: CONTA_NUMERO / ADAPTER: CONTRATO / SIMETRA: COD_CNTR',
    id_cliente               STRING        COMMENT 'NG: COD_CLIENTE_SAP / ADAPTER: IDCliente / SIMETRA: FT_CLIEFOR',
    cpf_cnpj                 STRING        COMMENT 'ADAPTER: CPF_CNPJ',
    nome_cliente             STRING        COMMENT 'NG: NOME_ASSINANTE / ADAPTER: Cliente',
    tipo_assinante           STRING        COMMENT 'NG: TIPO_ASSINANTE',
    status_contrato          STRING        COMMENT 'ADAPTER: Status_Contrato',

    -- ── Localização ────────────────────────────────────────────────────────
    uf_dest                  STRING        COMMENT 'NG: NF_UF / SIMETRA: FT_ESTADO',
    cidade_dest              STRING        COMMENT 'NG: NF_CIDADE / ADAPTER: Cidade',

    -- ── Produto / Item ─────────────────────────────────────────────────────
    cod_produto              STRING        COMMENT 'NG: NF_ITEM_COD_SAP / ADAPTER: ITEM_CODE_SAP / SIMETRA: B1_COD',
    posicao_item             INTEGER       COMMENT 'NG: POSICAO_ITEM / ADAPTER: Ordem / SIMETRA: FT_ITEM',
    descricao_item           STRING        COMMENT 'NG: NF_ITEM_DESCRICAO / ADAPTER: DescricaoFiscal / SIMETRA: B1_DESC',
    cclass                   STRING        COMMENT 'NG: CCLASS / ADAPTER: codigoClassificacao / SIMETRA: B1_XCCLASS',
    tipo_receita             STRING        COMMENT 'ADAPTER: TipoReceita / SIMETRA: TIPO_PROD',
    data_inicio_cobranca     DATE          COMMENT 'NG: DATA_INICIO_COBRANCA',
    data_fim_cobranca        DATE          COMMENT 'NG: DATA_FIM_COBRANCA',
    nf_item_valor            DECIMAL(18,2) COMMENT 'NG: NF_ITEM_VALOR / ADAPTER: ValorItemFiltro / SIMETRA: FT_TOTAL',
    desconto                 DECIMAL(18,2) COMMENT 'ADAPTER: Desconto / SIMETRA: FT_DESCONT',

    -- ── CFOP ───────────────────────────────────────────────────────────────
    cfop                     STRING        COMMENT 'NG: CFOP / ADAPTER: NumeroCFOP / SIMETRA: FT_CFOP',

    -- ── ICMS ───────────────────────────────────────────────────────────────
    icms_cst                 STRING        COMMENT 'NG: ICMS_CST / ADAPTER: CST',
    icms_base                DECIMAL(18,2) COMMENT 'NG: ICMS_BASE_CALCULO / ADAPTER: BaseICMS / SIMETRA: FT_BASEICM',
    icms_aliquota            DECIMAL(8,4)  COMMENT 'NG: ICMS_ALIQUOTA / SIMETRA: FT_ALIQICM',
    icms_valor               DECIMAL(18,2) COMMENT 'NG: ICMS_VALOR_IMPOSTO / ADAPTER: ValorICMS / SIMETRA: FT_VALICM',
    icms_isento              DECIMAL(18,2) COMMENT 'ADAPTER: IsentoICMS / SIMETRA: FT_ISENICM',

    -- ── ISS ────────────────────────────────────────────────────────────────
    iss_cst                  STRING        COMMENT 'NG: ISS_CST / SIMETRA: F3_CSTISS',
    iss_base                 DECIMAL(18,2) COMMENT 'NG: ISS_BASE_CALCULO / ADAPTER: BaseISS',
    iss_aliquota             DECIMAL(8,4)  COMMENT 'NG: ISS_ALIQUOTA',
    iss_valor                DECIMAL(18,2) COMMENT 'NG: ISS_VALOR_IMPOSTO / ADAPTER: ValorISS',

    -- ── PIS ────────────────────────────────────────────────────────────────
    pis_cst                  STRING        COMMENT 'NG: PIS_CST / SIMETRA: FT_CSTPIS',
    pis_base                 DECIMAL(18,2) COMMENT 'NG: PIS_BASE_CALCULO / ADAPTER: BasePIS / SIMETRA: FT_BASEPIS',
    pis_aliquota             DECIMAL(8,4)  COMMENT 'NG: PIS_ALIQUOTA / ADAPTER: PIS / SIMETRA: FT_ALIQPIS',
    pis_valor                DECIMAL(18,2) COMMENT 'NG: PIS_VALOR_IMPOSTO / ADAPTER: ValorPIS / SIMETRA: FT_VALPIS',

    -- ── COFINS ─────────────────────────────────────────────────────────────
    cofins_cst               STRING        COMMENT 'NG: COFINS_CST / SIMETRA: FT_CSTCOF',
    cofins_base              DECIMAL(18,2) COMMENT 'NG: COFINS_BASE_CALCULO / ADAPTER: BaseCOFINS / SIMETRA: FT_BASECOF',
    cofins_aliquota          DECIMAL(8,4)  COMMENT 'NG: COFINS_ALIQUOTA / ADAPTER: COFINS / SIMETRA: FT_ALIQCOF',
    cofins_valor             DECIMAL(18,2) COMMENT 'NG: COFINS_VALOR_IMPOSTO / ADAPTER: ValorCOFINS / SIMETRA: FT_VALCOF',

    -- ── FUST ───────────────────────────────────────────────────────────────
    fust_cst                 STRING        COMMENT 'NG: FUST_CST',
    fust_base                DECIMAL(18,2) COMMENT 'NG: FUST_BASE_CALCULO / SIMETRA: FT_BASIMP5',
    fust_aliquota            DECIMAL(8,4)  COMMENT 'NG: FUST_ALIQUOTA / ADAPTER: Fust / SIMETRA: FT_ALQIMP5',
    fust_valor               DECIMAL(18,2) COMMENT 'NG: FUST_VALOR_IMPOSTO / SIMETRA: FT_VALIMP5',

    -- ── FUNTTEL ────────────────────────────────────────────────────────────
    funttel_cst              STRING        COMMENT 'NG: FUNTTEL_CST',
    funttel_base             DECIMAL(18,2) COMMENT 'NG: FUNTTEL_BASE_CALCULO / SIMETRA: FT_BASIMP6',
    funttel_aliquota         DECIMAL(8,4)  COMMENT 'NG: FUNTTEL_ALIQUOTA / ADAPTER: AliquotaFunttel / SIMETRA: FT_ALQIMP6',
    funttel_valor            DECIMAL(18,2) COMMENT 'NG: FUNTTEL_VALOR_IMPOSTO / ADAPTER: ValorFuntel / SIMETRA: FT_VALIMP6',

    -- ── Retenções (NG + SIMETRA) ───────────────────────────────────────────
    ir_retido_aliquota       DECIMAL(8,4)  COMMENT 'NG: ALIQUOTA_IR_RETIDO / SIMETRA: FT_ALIQIRR',
    ir_retido_valor          DECIMAL(18,2) COMMENT 'NG: VALOR_IR_RETIDO / SIMETRA: FT_VALIRR',
    pis_retido_aliquota      DECIMAL(8,4)  COMMENT 'NG: ALIQUOTA_PIS_RETIDO / SIMETRA: FT_ARETPIS',
    pis_retido_valor         DECIMAL(18,2) COMMENT 'NG: VALOR_PIS_RETIDO / SIMETRA: FT_VRETPIS',
    cofins_retido_aliquota   DECIMAL(8,4)  COMMENT 'NG: ALIQUOTA_COFINS_RETIDO / SIMETRA: FT_ARETCOF',
    cofins_retido_valor      DECIMAL(18,2) COMMENT 'NG: VALOR_COFINS_RETIDO / SIMETRA: FT_VRETCOF',
    csll_retido_aliquota     DECIMAL(8,4)  COMMENT 'NG: ALIQUOTA_CSLL_RETIDO / SIMETRA: FT_ARETCSL',
    csll_retido_valor        DECIMAL(18,2) COMMENT 'NG: VALOR_CSLL_RETIDO / SIMETRA: FT_VRETCSL',
    iss_retido_valor         DECIMAL(18,2) COMMENT 'NG: VALOR_ISS_RETIDO / SIMETRA: FT_VALINS',

    -- ── Contas Contábeis (NG + ADAPTER) ────────────────────────────────────
    conta_debito_rec         STRING        COMMENT 'NG: CONTA_DEBITO_REC / ADAPTER: ContrapartidaDebito',
    descr_conta_debito_rec   STRING        COMMENT 'NG: DESCR_CONTA_DEBITO_REC',
    conta_debito_adiant      STRING        COMMENT 'NG: CONTA_DEBITO_ADIANT',
    conta_credito_rec        STRING        COMMENT 'NG: CONTA_CREDITO_REC / ADAPTER: CreditoReceita',
    descr_conta_credito_rec  STRING        COMMENT 'NG: DESCR_CONTA_CREDITO_REC',
    conta_debito_icms        STRING        COMMENT 'NG: CONTA_DEBITO_ICMS / ADAPTER: ClassificadorContaDebitoICMS',
    descr_conta_debito_icms  STRING        COMMENT 'NG: CONTA_DEBITO_ICMS_DESCRICAO / ADAPTER: DescricaoContaDebitoICMS',
    conta_credito_icms       STRING        COMMENT 'NG: CONTA_CREDITO_ICMS / ADAPTER: ClassificadorContaCreditoICMS',
    descr_conta_credito_icms STRING        COMMENT 'NG: CONTA_CREDITO_ICMS_DESCRICAO / ADAPTER: DescricaoContaCreditoICMS',
    conta_debito_pis         STRING        COMMENT 'NG: CONTA_DEBITO_PIS / ADAPTER: ClassificadorContaDebitoPis',
    descr_conta_debito_pis   STRING        COMMENT 'NG: CONTA_DEBITO_PIS_DESCRICAO / ADAPTER: DescricaoContaDebitoPis',
    conta_credito_pis        STRING        COMMENT 'NG: CONTA_CREDITO_PIS / ADAPTER: ClassificadorContaCreditoPis',
    descr_conta_credito_pis  STRING        COMMENT 'NG: CONTA_CREDITO_PIS_DESCRICAO / ADAPTER: DescricaoContaCreditoPis',
    conta_debito_cofins      STRING        COMMENT 'NG: CONTA_DEBITO_COFINS / ADAPTER: ClassificadorContaDebitoCofins',
    descr_conta_debito_cofins STRING       COMMENT 'NG: CONTA_DEBITO_COFINS_DESCRICAO / ADAPTER: DescricaoContaDebitoCofins',
    conta_credito_cofins     STRING        COMMENT 'NG: CONTA_CREDITO_COFINS / ADAPTER: ClassificadorContaCreditoCofins',
    descr_conta_credito_cofins STRING      COMMENT 'NG: CONTA_CREDITO_COFINS_DESCRICAO / ADAPTER: DescricaoContaCreditoCofins',
    conta_debito_iss         STRING        COMMENT 'NG: CONTA_DEBITO_ISS / ADAPTER: ClassificadorContaDebitoISS',
    descr_conta_debito_iss   STRING        COMMENT 'NG: CONTA_DEBITO_ISS_DESCRICAO / ADAPTER: DescricaoContaDebitoISS',
    conta_credito_iss        STRING        COMMENT 'NG: CONTA_CREDITO_ISS / ADAPTER: ClassificadorContaCreditoISS',
    descr_conta_credito_iss  STRING        COMMENT 'NG: CONTA_CREDITO_ISS_DESCRICAO / ADAPTER: DescricaoContaCreditoISS',

    -- ── Derivados ──────────────────────────────────────────────────────────
    ind_sem_cst              BOOLEAN       COMMENT 'iss_valor > 0 → item ISS/indSemCST',
    faturamento_zerado       BOOLEAN       COMMENT 'nf_item_valor = 0 → PB-000',
    fatura_sem_numero        BOOLEAN       COMMENT 'nf_numero nulo/vazio',
    ind_cancelada            BOOLEAN       COMMENT 'cancelada = SIM derivado'
)
USING DELTA
PARTITIONED BY (ciclo_faturamento, sistema_origem)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Standing NFCom V3 — colunas 100% validadas contra Silver real. Sem campos de fatura.'
""")
print(f"✅ DDL OK → {TABELA_DEST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — Silver NG
# MAGIC
# MAGIC Filtro de ciclo: `FATURA_DATA_EMISSAO` (dd/MM/yyyy)

# COMMAND ----------

df_ng = (
    spark.table(TBL_NG)
    .filter(_ciclo_dd_mm_yyyy(F.col("FATURA_DATA_EMISSAO")) == CICLO_REF)
    .select(
        F.lit("NG").alias("sistema_origem"),
        _ciclo_dd_mm_yyyy(F.col("FATURA_DATA_EMISSAO")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.current_timestamp().alias("dt_processamento"),
        # Classificação
        _s(F.col("CATEGORIA_FISCAL")).alias("categoria_fiscal"),
        _s(F.col("MODELO")).alias("modelo_nf"),
        _s(F.col("TIPO_EMISSAO_NFCOM")).alias("tipo_emissao_nfcom"),
        # Nota Fiscal
        _s(F.col("NF_NUMERO").cast(StringType())).alias("nf_numero"),
        NUL_STR.alias("nf_serie"),
        _d2(F.col("NF_VALOR")).alias("nf_valor"),
        F.to_date(F.col("NF_DATA_EMISSAO"), "dd/MM/yyyy").alias("data_emissao"),
        _s(F.col("CHAVE_ACESSO_NFCOM").cast(StringType())).alias("chave_acesso_nfcom"),
        _s(F.col("STATUS_INTEGRACAO_NFCOM")).alias("status_integracao_nfcom"),
        _s(F.col("REGIME_ESPECIAL")).alias("regime_especial"),
        _s(F.col("CANCELADA")).alias("cancelada"),
        _s(F.col("STATUS_NFCOM")).alias("status_nfcom"),
        _s(F.col("NOTA_SUBSTITUTA").cast(StringType())).alias("nota_substituta"),
        # Emitente
        _s(F.col("EMPRESA_PRESTADORA")).alias("empresa_prestadora"),
        _s(F.col("PESSOA_EMISSORA").cast(StringType())).alias("pessoa_emissora"),
        _s(F.col("OPERADORA")).alias("operadora"),
        # Cliente
        _s(F.col("CONTA_NUMERO").cast(StringType())).alias("id_contrato"),
        _s(F.col("COD_CLIENTE_SAP")).alias("id_cliente"),
        NUL_STR.alias("cpf_cnpj"),
        _s(F.col("NOME_ASSINANTE")).alias("nome_cliente"),
        _s(F.col("TIPO_ASSINANTE")).alias("tipo_assinante"),
        NUL_STR.alias("status_contrato"),
        # Localização
        _up(F.col("NF_UF")).alias("uf_dest"),
        _s(F.col("NF_CIDADE")).alias("cidade_dest"),
        # Produto / Item
        _s(F.col("NF_ITEM_COD_SAP")).alias("cod_produto"),
        _int(F.col("POSICAO_ITEM")).alias("posicao_item"),
        _s(F.col("NF_ITEM_DESCRICAO")).alias("descricao_item"),
        _s(F.col("CCLASS").cast(StringType())).alias("cclass"),
        NUL_STR.alias("tipo_receita"),
        _to_date_safe(F.col("DATA_INICIO_COBRANCA")).alias("data_inicio_cobranca"),
        _to_date_safe(F.col("DATA_FIM_COBRANCA")).alias("data_fim_cobranca"),
        _d2(F.col("NF_ITEM_VALOR")).alias("nf_item_valor"),
        NUL_D2.alias("desconto"),
        # CFOP
        _s(F.col("CFOP").cast(StringType())).alias("cfop"),
        # ICMS
        _s(F.col("ICMS_CST").cast(StringType())).alias("icms_cst"),
        _d2(F.col("ICMS_BASE_CALCULO")).alias("icms_base"),
        _d4(F.col("ICMS_ALIQUOTA")).alias("icms_aliquota"),
        _d2(F.col("ICMS_VALOR_IMPOSTO")).alias("icms_valor"),
        NUL_D2.alias("icms_isento"),
        # ISS
        _s(F.col("ISS_CST").cast(StringType())).alias("iss_cst"),
        _d2(F.col("ISS_BASE_CALCULO")).alias("iss_base"),
        _d4(F.col("ISS_ALIQUOTA")).alias("iss_aliquota"),
        _d2(F.col("ISS_VALOR_IMPOSTO")).alias("iss_valor"),
        # PIS
        _s(F.col("PIS_CST").cast(StringType())).alias("pis_cst"),
        _d2(F.col("PIS_BASE_CALCULO")).alias("pis_base"),
        _d4(F.col("PIS_ALIQUOTA")).alias("pis_aliquota"),
        _d2(F.col("PIS_VALOR_IMPOSTO")).alias("pis_valor"),
        # COFINS
        _s(F.col("COFINS_CST").cast(StringType())).alias("cofins_cst"),
        _d2(F.col("COFINS_BASE_CALCULO")).alias("cofins_base"),
        _d4(F.col("COFINS_ALIQUOTA")).alias("cofins_aliquota"),
        _d2(F.col("COFINS_VALOR_IMPOSTO")).alias("cofins_valor"),
        # FUST
        _s(F.col("FUST_CST").cast(StringType())).alias("fust_cst"),
        _d2(F.col("FUST_BASE_CALCULO")).alias("fust_base"),
        _d4(F.col("FUST_ALIQUOTA")).alias("fust_aliquota"),
        _d2(F.col("FUST_VALOR_IMPOSTO")).alias("fust_valor"),
        # FUNTTEL
        _s(F.col("FUNTTEL_CST").cast(StringType())).alias("funttel_cst"),
        _d2(F.col("FUNTTEL_BASE_CALCULO")).alias("funttel_base"),
        _d4(F.col("FUNTTEL_ALIQUOTA")).alias("funttel_aliquota"),
        _d2(F.col("FUNTTEL_VALOR_IMPOSTO")).alias("funttel_valor"),
        # Retenções
        _d4(F.col("ALIQUOTA_IR_RETIDO")).alias("ir_retido_aliquota"),
        _d2(F.col("VALOR_IR_RETIDO")).alias("ir_retido_valor"),
        _d4(F.col("ALIQUOTA_PIS_RETIDO")).alias("pis_retido_aliquota"),
        _d2(F.col("VALOR_PIS_RETIDO")).alias("pis_retido_valor"),
        _d4(F.col("ALIQUOTA_COFINS_RETIDO")).alias("cofins_retido_aliquota"),
        _d2(F.col("VALOR_COFINS_RETIDO")).alias("cofins_retido_valor"),
        _d4(F.col("ALIQUOTA_CSLL_RETIDO")).alias("csll_retido_aliquota"),
        _d2(F.col("VALOR_CSLL_RETIDO")).alias("csll_retido_valor"),
        _d2(F.col("VALOR_ISS_RETIDO")).alias("iss_retido_valor"),
        # Contas Contábeis
        _s(F.col("CONTA_DEBITO_REC")).alias("conta_debito_rec"),
        _s(F.col("DESCR_CONTA_DEBITO_REC")).alias("descr_conta_debito_rec"),
        _s(F.col("CONTA_DEBITO_ADIANT").cast(StringType())).alias("conta_debito_adiant"),
        _s(F.col("CONTA_CREDITO_REC")).alias("conta_credito_rec"),
        _s(F.col("DESCR_CONTA_CREDITO_REC")).alias("descr_conta_credito_rec"),
        _s(F.col("CONTA_DEBITO_ICMS").cast(StringType())).alias("conta_debito_icms"),
        _s(F.col("CONTA_DEBITO_ICMS_DESCRICAO").cast(StringType())).alias("descr_conta_debito_icms"),
        _s(F.col("CONTA_CREDITO_ICMS").cast(StringType())).alias("conta_credito_icms"),
        _s(F.col("CONTA_CREDITO_ICMS_DESCRICAO").cast(StringType())).alias("descr_conta_credito_icms"),
        _s(F.col("CONTA_DEBITO_PIS")).alias("conta_debito_pis"),
        _s(F.col("CONTA_DEBITO_PIS_DESCRICAO")).alias("descr_conta_debito_pis"),
        _s(F.col("CONTA_CREDITO_PIS")).alias("conta_credito_pis"),
        _s(F.col("CONTA_CREDITO_PIS_DESCRICAO")).alias("descr_conta_credito_pis"),
        _s(F.col("CONTA_DEBITO_COFINS")).alias("conta_debito_cofins"),
        _s(F.col("CONTA_DEBITO_COFINS_DESCRICAO")).alias("descr_conta_debito_cofins"),
        _s(F.col("CONTA_CREDITO_COFINS")).alias("conta_credito_cofins"),
        _s(F.col("CONTA_CREDITO_COFINS_DESCRICAO")).alias("descr_conta_credito_cofins"),
        _s(F.col("CONTA_DEBITO_ISS").cast(StringType())).alias("conta_debito_iss"),
        _s(F.col("CONTA_DEBITO_ISS_DESCRICAO").cast(StringType())).alias("descr_conta_debito_iss"),
        _s(F.col("CONTA_CREDITO_ISS").cast(StringType())).alias("conta_credito_iss"),
        _s(F.col("CONTA_CREDITO_ISS_DESCRICAO").cast(StringType())).alias("descr_conta_credito_iss"),
    )
)
print(f"✅ NG | {df_ng.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — Silver ADAPTER
# MAGIC
# MAGIC Filtro de ciclo: `DATA_EMISSAO_NOTA` (Timestamp)
# MAGIC
# MAGIC Campos ausentes/nulos no ADAPTER: nf_serie, pessoa_emissora, tipo_assinante,
# MAGIC data_inicio/fim_cobranca, iss_cst, iss_aliquota, fust_cst, fust_base, fust_valor,
# MAGIC funttel_cst, funttel_base, todas as retenções (ImpRet* = NaN nos dados reais),
# MAGIC conta_debito_adiant, contas de ISS.

# COMMAND ----------

df_adapter = (
    spark.table(TBL_ADAPTER)
    .filter(_ciclo_ts(F.col("DATA_EMISSAO_NOTA")) == CICLO_REF)
    .select(
        F.lit("ADAPTER").alias("sistema_origem"),
        _ciclo_ts(F.col("DATA_EMISSAO_NOTA")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.current_timestamp().alias("dt_processamento"),
        # Classificação
        _s(F.col("TipoNF")).alias("categoria_fiscal"),
        NUL_STR.alias("modelo_nf"),
        _s(F.col("Tipo_Emissao")).alias("tipo_emissao_nfcom"),
        # Nota Fiscal
        _s(F.col("NUMERO_NF").cast(StringType())).alias("nf_numero"),
        NUL_STR.alias("nf_serie"),
        _d2(F.col("ValorNotaFiltro")).alias("nf_valor"),
        F.to_date(F.col("DATA_EMISSAO_NOTA")).alias("data_emissao"),
        NUL_STR.alias("chave_acesso_nfcom"),             # ChaveAcesso = NaN nos dados reais
        _s(F.col("StatusNotaIntegracao").cast(StringType())).alias("status_integracao_nfcom"),
        _s(F.col("REGIME_ESPECIAL")).alias("regime_especial"),
        _s(F.col("NotaCancelada")).alias("cancelada"),
        NUL_STR.alias("status_nfcom"),
        NUL_STR.alias("nota_substituta"),
        # Emitente
        _s(F.col("Empresa")).alias("empresa_prestadora"),
        NUL_STR.alias("pessoa_emissora"),
        _s(F.col("OPERACAO")).alias("operadora"),
        # Cliente
        _s(F.col("CONTRATO").cast(StringType())).alias("id_contrato"),
        _s(F.col("IDCliente").cast(StringType())).alias("id_cliente"),
        _s(F.col("CPF_CNPJ").cast(StringType())).alias("cpf_cnpj"),
        _s(F.col("Cliente")).alias("nome_cliente"),
        NUL_STR.alias("tipo_assinante"),
        _s(F.col("Status_Contrato")).alias("status_contrato"),
        # Localização
        NUL_STR.alias("uf_dest"),                        # ADAPTER não tem UF
        _s(F.col("Cidade")).alias("cidade_dest"),
        # Produto / Item
        _s(F.col("ITEM_CODE_SAP")).alias("cod_produto"),
        _int(F.col("Ordem")).alias("posicao_item"),
        _s(F.col("DescricaoFiscal")).alias("descricao_item"),
        _s(F.col("codigoClassificacao").cast(StringType())).alias("cclass"),
        _s(F.col("TipoReceita")).alias("tipo_receita"),
        NUL_DATE.alias("data_inicio_cobranca"),
        NUL_DATE.alias("data_fim_cobranca"),
        _d2(F.col("ValorItemFiltro")).alias("nf_item_valor"),
        _d2(F.col("Desconto")).alias("desconto"),
        # CFOP
        _s(F.col("NumeroCFOP").cast(StringType())).alias("cfop"),
        # ICMS
        _s(F.col("CST").cast(StringType())).alias("icms_cst"),
        _d2(F.col("BaseICMS")).alias("icms_base"),
        NUL_D4.alias("icms_aliquota"),                   # ADAPTER não tem alíquota ICMS separada
        _d2(F.col("ValorICMS")).alias("icms_valor"),
        _d2(F.col("IsentoICMS")).alias("icms_isento"),
        # ISS
        NUL_STR.alias("iss_cst"),
        _d2(F.col("BaseISS")).alias("iss_base"),
        NUL_D4.alias("iss_aliquota"),
        _d2(F.col("ValorISS")).alias("iss_valor"),
        # PIS — campo PIS = alíquota %, ValorPIS = valor calculado
        NUL_STR.alias("pis_cst"),
        _d2(F.col("BasePIS")).alias("pis_base"),
        _d4(F.col("PIS")).alias("pis_aliquota"),
        _d2(F.col("ValorPIS")).alias("pis_valor"),
        # COFINS — campo COFINS = alíquota %, ValorCOFINS = valor calculado
        NUL_STR.alias("cofins_cst"),
        _d2(F.col("BaseCOFINS")).alias("cofins_base"),
        _d4(F.col("COFINS")).alias("cofins_aliquota"),
        _d2(F.col("ValorCOFINS")).alias("cofins_valor"),
        # FUST — apenas alíquota (Fust); base e valor não existem no ADAPTER
        NUL_STR.alias("fust_cst"),
        NUL_D2.alias("fust_base"),
        _d4(F.col("Fust")).alias("fust_aliquota"),
        NUL_D2.alias("fust_valor"),
        # FUNTTEL
        NUL_STR.alias("funttel_cst"),
        NUL_D2.alias("funttel_base"),
        _d4(F.col("AliquotaFunttel")).alias("funttel_aliquota"),
        _d2(F.col("ValorFuntel")).alias("funttel_valor"),
        # Retenções — ImpRet* são NaN nos dados reais
        NUL_D4.alias("ir_retido_aliquota"),
        NUL_D2.alias("ir_retido_valor"),
        NUL_D4.alias("pis_retido_aliquota"),
        NUL_D2.alias("pis_retido_valor"),
        NUL_D4.alias("cofins_retido_aliquota"),
        NUL_D2.alias("cofins_retido_valor"),
        NUL_D4.alias("csll_retido_aliquota"),
        NUL_D2.alias("csll_retido_valor"),
        NUL_D2.alias("iss_retido_valor"),
        # Contas Contábeis
        _s(F.col("ContrapartidaDebito")).alias("conta_debito_rec"),
        NUL_STR.alias("descr_conta_debito_rec"),
        NUL_STR.alias("conta_debito_adiant"),
        _s(F.col("CreditoReceita")).alias("conta_credito_rec"),
        NUL_STR.alias("descr_conta_credito_rec"),
        _s(F.col("ClassificadorContaDebitoICMS").cast(StringType())).alias("conta_debito_icms"),
        _s(F.col("DescricaoContaDebitoICMS").cast(StringType())).alias("descr_conta_debito_icms"),
        _s(F.col("ClassificadorContaCreditoICMS").cast(StringType())).alias("conta_credito_icms"),
        _s(F.col("DescricaoContaCreditoICMS").cast(StringType())).alias("descr_conta_credito_icms"),
        _s(F.col("ClassificadorContaDebitoPis").cast(StringType())).alias("conta_debito_pis"),
        _s(F.col("DescricaoContaDebitoPis").cast(StringType())).alias("descr_conta_debito_pis"),
        _s(F.col("ClassificadorContaCreditoPis").cast(StringType())).alias("conta_credito_pis"),
        _s(F.col("DescricaoContaCreditoPis").cast(StringType())).alias("descr_conta_credito_pis"),
        _s(F.col("ClassificadorContaDebitoCofins").cast(StringType())).alias("conta_debito_cofins"),
        _s(F.col("DescricaoContaDebitoCofins").cast(StringType())).alias("descr_conta_debito_cofins"),
        _s(F.col("ClassificadorContaCreditoCofins").cast(StringType())).alias("conta_credito_cofins"),
        _s(F.col("DescricaoContaCreditoCofins").cast(StringType())).alias("descr_conta_credito_cofins"),
        _s(F.col("ClassificadorContaDebitoISS").cast(StringType())).alias("conta_debito_iss"),
        _s(F.col("DescricaoContaDebitoISS").cast(StringType())).alias("descr_conta_debito_iss"),
        _s(F.col("ClassificadorContaCreditoISS").cast(StringType())).alias("conta_credito_iss"),
        _s(F.col("DescricaoContaCreditoISS").cast(StringType())).alias("descr_conta_credito_iss"),
    )
)
print(f"✅ ADAPTER | {df_adapter.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração — Silver SIMETRA
# MAGIC
# MAGIC Filtro de ciclo: `FT_EMISSAO` (int AAAAMMDD → substring 1-6)
# MAGIC
# MAGIC Campos ausentes no SIMETRA: operadora, cpf_cnpj, nome_cliente, tipo_assinante,
# MAGIC status_contrato, cidade_dest, tipo_emissao_nfcom, status_integracao_nfcom,
# MAGIC status_nfcom, iss_base, iss_aliquota, iss_valor, fust_cst, funttel_cst,
# MAGIC data_inicio/fim_cobranca, todas as contas contábeis.

# COMMAND ----------

df_simetra = (
    spark.table(TBL_SIMETRA)
    .filter(_ciclo_yyyymmdd(F.col("FT_EMISSAO")) == CICLO_REF)
    .select(
        F.lit("SIMETRA").alias("sistema_origem"),
        _ciclo_yyyymmdd(F.col("FT_EMISSAO")).alias("ciclo_faturamento"),
        F.current_timestamp().alias("dt_carga"),
        F.current_timestamp().alias("dt_processamento"),
        # Classificação
        _s(F.col("TIPO_PROD")).alias("categoria_fiscal"),
        _s(F.col("C5_TIPOF")).alias("modelo_nf"),
        NUL_STR.alias("tipo_emissao_nfcom"),
        # Nota Fiscal
        _s(F.col("FT_NFISCAL").cast(StringType())).alias("nf_numero"),
        _s(F.col("FT_SERIE").cast(StringType())).alias("nf_serie"),
        _d2(F.col("FT_VALCONT")).alias("nf_valor"),
        F.to_date(F.col("FT_EMISSAO").cast(StringType()), "yyyyMMdd").alias("data_emissao"),
        _s(F.col("FT_CHVNFE").cast(StringType())).alias("chave_acesso_nfcom"),
        _s(F.col("F3_DESCRET")).alias("status_integracao_nfcom"),  # "Autorizado o uso da NFCom"
        _s(F.col("FT_CLASFIS").cast(StringType())).alias("regime_especial"),
        F.when(
            F.trim(F.col("FT_DTCANC").cast(StringType())) != "", F.lit("SIM")
        ).otherwise(F.lit("NAO")).alias("cancelada"),
        NUL_STR.alias("status_nfcom"),
        _s(F.col("IS_NFSUBS")).alias("nota_substituta"),
        # Emitente
        _s(F.col("FT_FILIAL").cast(StringType())).alias("empresa_prestadora"),
        NUL_STR.alias("pessoa_emissora"),
        NUL_STR.alias("operadora"),
        # Cliente
        _s(F.col("COD_CNTR").cast(StringType())).alias("id_contrato"),
        _s(F.col("FT_CLIEFOR").cast(StringType())).alias("id_cliente"),
        NUL_STR.alias("cpf_cnpj"),
        NUL_STR.alias("nome_cliente"),
        NUL_STR.alias("tipo_assinante"),
        NUL_STR.alias("status_contrato"),
        # Localização
        _up(F.col("FT_ESTADO")).alias("uf_dest"),
        NUL_STR.alias("cidade_dest"),
        # Produto / Item
        _s(F.col("B1_COD").cast(StringType())).alias("cod_produto"),
        _int(F.col("FT_ITEM")).alias("posicao_item"),
        _s(F.col("B1_DESC")).alias("descricao_item"),
        _s(F.col("B1_XCCLASS").cast(StringType())).alias("cclass"),
        _s(F.col("TIPO_PROD")).alias("tipo_receita"),
        NUL_DATE.alias("data_inicio_cobranca"),
        NUL_DATE.alias("data_fim_cobranca"),
        _d2(F.col("FT_TOTAL")).alias("nf_item_valor"),
        _d2(F.col("FT_DESCONT")).alias("desconto"),
        # CFOP
        _s(F.col("FT_CFOP").cast(StringType())).alias("cfop"),
        # ICMS
        NUL_STR.alias("icms_cst"),
        _d2(F.col("FT_BASEICM")).alias("icms_base"),
        _d4(F.col("FT_ALIQICM")).alias("icms_aliquota"),
        _d2(F.col("FT_VALICM")).alias("icms_valor"),
        _d2(F.col("FT_ISENICM")).alias("icms_isento"),
        # ISS — SIMETRA tem código CST (F3_CSTISS) mas sem base/aliquota/valor separados
        _s(F.col("F3_CSTISS").cast(StringType())).alias("iss_cst"),
        NUL_D2.alias("iss_base"),
        NUL_D4.alias("iss_aliquota"),
        NUL_D2.alias("iss_valor"),
        # PIS
        _s(F.col("FT_CSTPIS").cast(StringType())).alias("pis_cst"),
        _d2(F.col("FT_BASEPIS")).alias("pis_base"),
        _d4(F.col("FT_ALIQPIS")).alias("pis_aliquota"),
        _d2(F.col("FT_VALPIS")).alias("pis_valor"),
        # COFINS
        _s(F.col("FT_CSTCOF").cast(StringType())).alias("cofins_cst"),
        _d2(F.col("FT_BASECOF")).alias("cofins_base"),
        _d4(F.col("FT_ALIQCOF")).alias("cofins_aliquota"),
        _d2(F.col("FT_VALCOF")).alias("cofins_valor"),
        # FUST — FT_BASIMP5 / FT_ALQIMP5 / FT_VALIMP5
        NUL_STR.alias("fust_cst"),
        _d2(F.col("FT_BASIMP5")).alias("fust_base"),
        _d4(F.col("FT_ALQIMP5")).alias("fust_aliquota"),
        _d2(F.col("FT_VALIMP5")).alias("fust_valor"),
        # FUNTTEL — FT_BASIMP6 / FT_ALQIMP6 / FT_VALIMP6
        NUL_STR.alias("funttel_cst"),
        _d2(F.col("FT_BASIMP6")).alias("funttel_base"),
        _d4(F.col("FT_ALQIMP6")).alias("funttel_aliquota"),
        _d2(F.col("FT_VALIMP6")).alias("funttel_valor"),
        # Retenções
        _d4(F.col("FT_ALIQIRR")).alias("ir_retido_aliquota"),
        _d2(F.col("FT_VALIRR")).alias("ir_retido_valor"),
        _d4(F.col("FT_ARETPIS")).alias("pis_retido_aliquota"),
        _d2(F.col("FT_VRETPIS")).alias("pis_retido_valor"),
        _d4(F.col("FT_ARETCOF")).alias("cofins_retido_aliquota"),
        _d2(F.col("FT_VRETCOF")).alias("cofins_retido_valor"),
        _d4(F.col("FT_ARETCSL")).alias("csll_retido_aliquota"),
        _d2(F.col("FT_VRETCSL")).alias("csll_retido_valor"),
        _d2(F.col("FT_VALINS")).alias("iss_retido_valor"),
        # Contas Contábeis — não existem no SIMETRA/Protheus
        NUL_STR.alias("conta_debito_rec"),
        NUL_STR.alias("descr_conta_debito_rec"),
        NUL_STR.alias("conta_debito_adiant"),
        NUL_STR.alias("conta_credito_rec"),
        NUL_STR.alias("descr_conta_credito_rec"),
        NUL_STR.alias("conta_debito_icms"),
        NUL_STR.alias("descr_conta_debito_icms"),
        NUL_STR.alias("conta_credito_icms"),
        NUL_STR.alias("descr_conta_credito_icms"),
        NUL_STR.alias("conta_debito_pis"),
        NUL_STR.alias("descr_conta_debito_pis"),
        NUL_STR.alias("conta_credito_pis"),
        NUL_STR.alias("descr_conta_credito_pis"),
        NUL_STR.alias("conta_debito_cofins"),
        NUL_STR.alias("descr_conta_debito_cofins"),
        NUL_STR.alias("conta_credito_cofins"),
        NUL_STR.alias("descr_conta_credito_cofins"),
        NUL_STR.alias("conta_debito_iss"),
        NUL_STR.alias("descr_conta_debito_iss"),
        NUL_STR.alias("conta_credito_iss"),
        NUL_STR.alias("descr_conta_credito_iss"),
    )
)
print(f"✅ SIMETRA | {df_simetra.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION ALL + Derivados + PK + Hash CDC

# COMMAND ----------

df_final = (
    df_ng
    .unionByName(df_adapter, allowMissingColumns=False)
    .unionByName(df_simetra, allowMissingColumns=False)
    # ind_sem_cst: ISS > 0 → item sem ICMS (indSemCST)
    .withColumn("ind_sem_cst",
        F.coalesce((F.col("iss_valor") > ZERO2).cast("boolean"), F.lit(False))
    )
    # faturamento_zerado: PB-000
    .withColumn("faturamento_zerado",
        F.coalesce((F.col("nf_item_valor") == ZERO2).cast("boolean"), F.lit(True))
    )
    # fatura_sem_numero: SIMETRA legado
    .withColumn("fatura_sem_numero",
        (
            F.col("nf_numero").isNull() |
            F.trim(F.col("nf_numero").cast(StringType())).isin("", "-", "NaN", "nan", "0")
        ).cast("boolean")
    )
    # ind_cancelada
    .withColumn("ind_cancelada",
        F.coalesce(
            (F.upper(F.trim(F.col("cancelada"))) == F.lit("SIM")).cast("boolean"),
            F.lit(False)
        )
    )
    # PK
    .withColumn("id_registro",
        F.md5(F.concat_ws("|",
            F.col("sistema_origem"),
            F.col("ciclo_faturamento"),
            F.coalesce(_s(F.col("nf_numero")),     F.lit("")),
            F.coalesce(F.col("posicao_item").cast(StringType()), F.lit("")),
            F.coalesce(_s(F.col("id_contrato")),   F.lit("")),
            F.coalesce(_s(F.col("cod_produto")),   F.lit("")),
        ))
    )
    # Hash CDC
    .withColumn("hash_registro",
        F.sha2(F.concat_ws("|",
            F.coalesce(_s(F.col("cfop")),                           F.lit("")),
            F.coalesce(_s(F.col("icms_cst")),                       F.lit("")),
            F.coalesce(F.col("icms_aliquota").cast(StringType()),    F.lit("")),
            F.coalesce(F.col("icms_valor").cast(StringType()),       F.lit("")),
            F.coalesce(F.col("iss_valor").cast(StringType()),        F.lit("")),
            F.coalesce(F.col("pis_aliquota").cast(StringType()),     F.lit("")),
            F.coalesce(F.col("pis_valor").cast(StringType()),        F.lit("")),
            F.coalesce(F.col("cofins_aliquota").cast(StringType()),  F.lit("")),
            F.coalesce(F.col("cofins_valor").cast(StringType()),     F.lit("")),
            F.coalesce(F.col("fust_aliquota").cast(StringType()),    F.lit("")),
            F.coalesce(F.col("fust_valor").cast(StringType()),       F.lit("")),
            F.coalesce(F.col("funttel_aliquota").cast(StringType()), F.lit("")),
            F.coalesce(F.col("funttel_valor").cast(StringType()),    F.lit("")),
            F.coalesce(F.col("nf_item_valor").cast(StringType()),    F.lit("")),
            F.coalesce(_s(F.col("uf_dest")),                         F.lit("")),
            F.coalesce(_s(F.col("cancelada")),                       F.lit("")),
        ), 256)
    )
)
print(f"✅ UNION ALL | {df_final.count():,} itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO — Upsert incremental

# COMMAND ----------

if not tabela_existe:
    # Tabela foi recriada (DROP por schema divergente) ou nunca existiu
    # → INSERT direto, sem MERGE (tabela vazia, sem conflito possível)
    df_final.write.format("delta").mode("append").saveAsTable(TABELA_DEST)
    h = DeltaTable.forName(spark, TABELA_DEST).history(1).select("operationMetrics").collect()
    m = h[0]["operationMetrics"] if h else {}
    inserted = int(m.get("numOutputRows", df_final.count()))
    print(f"✅ INSERT (tabela recriada) | inseridos={inserted:,}")
else:
    # Tabela existe com schema compatível → MERGE incremental normal
    campos_update = {c: f"src.{c}" for c in df_final.columns if c != "id_registro"}
    (
        DeltaTable.forName(spark, TABELA_DEST).alias("tgt")
        .merge(
            df_final.alias("src"),
            """tgt.id_registro       = src.id_registro AND
               tgt.ciclo_faturamento = src.ciclo_faturamento AND
               tgt.sistema_origem    = src.sistema_origem"""
        )
        .whenMatchedUpdate(condition="tgt.hash_registro <> src.hash_registro", set=campos_update)
        .whenNotMatchedInsertAll()
        .execute()
    )
    h = DeltaTable.forName(spark, TABELA_DEST).history(1).select("operationMetrics").collect()
    m = h[0]["operationMetrics"] if h else {}
    print(f"✅ MERGE | inseridos={int(m.get('numTargetRowsInserted',0)):,} | atualizados={int(m.get('numTargetRowsUpdated',0)):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Resumo por sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sistema_origem,
# MAGIC     COUNT(*)                                                          AS total_itens,
# MAGIC     COUNT(DISTINCT id_contrato)                                      AS contratos,
# MAGIC     COUNT(DISTINCT nf_numero)                                        AS notas,
# MAGIC     SUM(CASE WHEN faturamento_zerado  THEN 1 ELSE 0 END)             AS zerados_pb000,
# MAGIC     SUM(CASE WHEN fatura_sem_numero   THEN 1 ELSE 0 END)             AS sem_nf,
# MAGIC     SUM(CASE WHEN ind_cancelada       THEN 1 ELSE 0 END)             AS canceladas,
# MAGIC     SUM(CASE WHEN ind_sem_cst         THEN 1 ELSE 0 END)             AS itens_iss,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst     THEN 1 ELSE 0 END)             AS itens_icms,
# MAGIC     ROUND(SUM(COALESCE(nf_item_valor, 0)), 2)                        AS valor_r,
# MAGIC     ROUND(SUM(COALESCE(icms_valor,    0)), 2)                        AS icms_r,
# MAGIC     ROUND(SUM(COALESCE(iss_valor,     0)), 2)                        AS iss_r,
# MAGIC     ROUND(SUM(COALESCE(pis_valor,     0)), 2)                        AS pis_r,
# MAGIC     ROUND(SUM(COALESCE(cofins_valor,  0)), 2)                        AS cofins_r,
# MAGIC     ROUND(SUM(COALESCE(fust_valor,    0)), 2)                        AS fust_r,
# MAGIC     ROUND(SUM(COALESCE(funttel_valor, 0)), 2)                        AS funttel_r
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem ORDER BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — CFOP por UF (Dimensão 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH validos AS (
# MAGIC     SELECT explode(array(
# MAGIC         '5301','5302','5303','5304','5305','5306','5307','5933',
# MAGIC         '6301','6302','6303','6304','6305','6306','6307','6933',
# MAGIC         '1205','7301'
# MAGIC     )) AS cfop_ok
# MAGIC )
# MAGIC SELECT
# MAGIC     s.sistema_origem, s.uf_dest, s.cfop, s.icms_cst, s.ind_sem_cst,
# MAGIC     COUNT(*) AS qtd, ROUND(SUM(s.nf_item_valor), 2) AS valor_r,
# MAGIC     CASE
# MAGIC         WHEN s.cfop IS NULL                                      THEN '⚪ sem CFOP'
# MAGIC         WHEN s.ind_sem_cst                                       THEN '🟡 ISS/indSemCST'
# MAGIC         WHEN v.cfop_ok IS NULL                                   THEN '🔴 CFOP_INVALIDO'
# MAGIC         WHEN s.uf_dest = 'SP' AND LEFT(s.cfop,1) = '6'         THEN '🔴 deveria 5xxx (intra)'
# MAGIC         WHEN s.uf_dest <> 'SP'
# MAGIC              AND s.uf_dest IS NOT NULL
# MAGIC              AND LEFT(s.cfop,1) = '5'                           THEN '🔴 deveria 6xxx (inter)'
# MAGIC         ELSE '✅ OK'
# MAGIC     END AS status
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3 s
# MAGIC LEFT JOIN validos v ON v.cfop_ok = s.cfop
# MAGIC WHERE s.ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY s.sistema_origem, s.uf_dest, s.cfop, s.icms_cst, s.ind_sem_cst, v.cfop_ok
# MAGIC ORDER BY qtd DESC LIMIT 200

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Tributação: ICMS × ISS × PIS × COFINS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sistema_origem,
# MAGIC     SUM(CASE WHEN icms_valor > 0 AND iss_valor > 0                     THEN 1 ELSE 0 END) AS icms_iss_simult,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND icms_aliquota IS NOT NULL
# MAGIC                  AND icms_aliquota = 0                                  THEN 1 ELSE 0 END) AS icms_aliq_zero,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND icms_cst IS NULL                 THEN 1 ELSE 0 END) AS icms_sem_cst,
# MAGIC     SUM(CASE WHEN ind_sem_cst AND (pis_valor > 0 OR cofins_valor > 0)  THEN 1 ELSE 0 END) AS pis_cofins_indev_iss,
# MAGIC     COUNT(*) AS total
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — FUST / FUNTTEL (regra determinística)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sistema_origem,
# MAGIC     CASE WHEN ind_sem_cst THEN 'ISS/SVA' ELSE 'ICMS' END AS tipo,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND fust_valor IS NOT NULL
# MAGIC                  AND fust_valor = 0                                 THEN 1 ELSE 0 END) AS fust_zero_icms,
# MAGIC     SUM(CASE WHEN ind_sem_cst     AND fust_valor IS NOT NULL
# MAGIC                  AND fust_valor > 0                                 THEN 1 ELSE 0 END) AS fust_indevido_iss,
# MAGIC     SUM(CASE WHEN NOT ind_sem_cst AND funttel_valor IS NOT NULL
# MAGIC                  AND funttel_valor = 0                              THEN 1 ELSE 0 END) AS funttel_zero_icms,
# MAGIC     SUM(CASE WHEN ind_sem_cst     AND funttel_valor IS NOT NULL
# MAGIC                  AND funttel_valor > 0                              THEN 1 ELSE 0 END) AS funttel_indevido_iss,
# MAGIC     ROUND(SUM(COALESCE(fust_valor,   0)),2) AS total_fust_r,
# MAGIC     ROUND(SUM(COALESCE(funttel_valor,0)),2) AS total_funttel_r,
# MAGIC     COUNT(*) AS total
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem, ind_sem_cst ORDER BY sistema_origem, ind_sem_cst

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — PB-000 Faturamento Zerado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sistema_origem,
# MAGIC        COUNT(DISTINCT id_contrato)                            AS contratos,
# MAGIC        SUM(CASE WHEN faturamento_zerado THEN 1 ELSE 0 END)   AS itens_zerados,
# MAGIC        SUM(CASE WHEN fatura_sem_numero  THEN 1 ELSE 0 END)   AS sem_nf_numero
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC   AND (faturamento_zerado OR fatura_sem_numero)
# MAGIC GROUP BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA — Cobertura de campos por sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sistema_origem, COUNT(*) AS total,
# MAGIC     ROUND(SUM(CASE WHEN cfop          IS NOT NULL AND cfop<>''    THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cfop,
# MAGIC     ROUND(SUM(CASE WHEN cclass        IS NOT NULL AND cclass<>''  THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cclass,
# MAGIC     ROUND(SUM(CASE WHEN uf_dest       IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_uf,
# MAGIC     ROUND(SUM(CASE WHEN cpf_cnpj      IS NOT NULL AND cpf_cnpj<>''THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_cpf_cnpj,
# MAGIC     ROUND(SUM(CASE WHEN icms_aliquota IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_icms_aliq,
# MAGIC     ROUND(SUM(CASE WHEN icms_cst      IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_icms_cst,
# MAGIC     ROUND(SUM(CASE WHEN pis_aliquota  IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_pis,
# MAGIC     ROUND(SUM(CASE WHEN fust_valor    IS NOT NULL                 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_fust,
# MAGIC     ROUND(SUM(CASE WHEN chave_acesso_nfcom IS NOT NULL
# MAGIC                        AND chave_acesso_nfcom NOT IN ('','-','0') THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_chave_nfcom
# MAGIC FROM ${schema_dest}.dados_nfcom_cliente_v3
# MAGIC WHERE ciclo_faturamento = '${ciclo_ref}'
# MAGIC GROUP BY sistema_origem ORDER BY sistema_origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE

# COMMAND ----------

if EXECUTAR_OPTIMIZE:
    spark.sql(f"""
        OPTIMIZE {TABELA_DEST}
        WHERE ciclo_faturamento = '{CICLO_REF}'
        ZORDER BY (sistema_origem, uf_dest, cfop, cclass)
    """)
    print(f"✅ OPTIMIZE | ciclo={CICLO_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final

# COMMAND ----------

r = spark.sql(f"""
SELECT COUNT(*) AS itens, COUNT(DISTINCT id_contrato) AS contratos,
       ROUND(SUM(nf_item_valor),2) AS valor_r,
       SUM(CASE WHEN faturamento_zerado THEN 1 ELSE 0 END) AS zerados,
       SUM(CASE WHEN fatura_sem_numero  THEN 1 ELSE 0 END) AS sem_nf
FROM {TABELA_DEST} WHERE ciclo_faturamento = '{CICLO_REF}'
""").collect()[0]

print(f"""
╔══════════════════════════════════════════════╗
║  BILLING ASSURANCE — Standing V3 Concluído  ║
╠══════════════════════════════════════════════╣
║  Ciclo         : {CICLO_REF}
║  Total Itens   : {r['itens']:,}
║  Contratos     : {r['contratos']:,}
║  Valor Total   : R$ {r['valor_r']:,.2f}
║  PB-000 Zerados: {r['zerados']:,}
║  Sem NF Número : {r['sem_nf']:,}
╚══════════════════════════════════════════════╝
""")