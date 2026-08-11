# Mapeamento Funcional — Billing Assurance NFCom
**Vero Internet | Accenture**
Notebook: `validacao_nfcom.py` — versão 8

---

## 1. Visão Geral

O pipeline valida os dados de faturamento NFCom (modelo 62 SEFAZ) emitidos pela Vero Internet, cruzando o standing extraído dos sistemas de billing contra tabelas de referência fiscal. O resultado é gravado na tabela de saída para consumo analítico e acompanhamento de qualidade fiscal.

```
tab_validacoes_NFCOM_v4   ──┐
tab_mestre_nfcom_nova      ──┼──► Motor de Validação ──► validacao_status_fatura
tab_impostos_verdade_nova  ──┘
```

---

## 2. Tabelas — Mapeamento e Finalidade

### 2.1 Tabelas de Entrada

| Tabela | Variável no código | Papel | Chave de join |
|--------|--------------------|-------|---------------|
| `accenture.tab_validacoes_NFCOM_v4` | `TBL_STANDING` | **Standing / fonte primária** — extração dos sistemas de billing com todos os campos do item faturado (CCLASS, CFOP, UF, alíquotas, CST, fatura, cliente, etc.) | Filtro por `CICLO` + normalização de `CCLASS` |
| `accenture.tab_mestre_nfcom_nova` | `TBL_MESTRE` | **Tabela mestre fiscal** — mapeia cada `CCLASS_NFCON` ao seu tipo de tributação (`TAX_UF_MUNICIPIO`) e CST esperado (`CST`). Define se o item é ICMS, ISS/SVA, sem incidência, cofaturamento, etc. | `CCLASS` e `CCLASS_7D` do standing |
| `accenture.tab_impostos_verdade_nova` | `TBL_IMPOSTOS` | **Tabela verdade de alíquotas** — armazena as alíquotas fiscais corretas por tipo de imposto e UF (`ICMS`, `ICMS_CONFAZ`, `ICMS_ESTADUAL`, `PIS_COFINS`). Fonte de verdade para comparação com o que o billing declarou. | `ESTADO` × `TIPO_IMPOSTO` |

### 2.2 Tabela de Saída

| Tabela | Variável no código | Papel |
|--------|--------------------|-------|
| `accenture.validacao_status_fatura` | `TBL_RESULTADO` | **Resultado das validações** — 1 linha por item × categoria de regra disparada. Alimenta dashboards de Billing Assurance. Regravada por ciclo via DELETE + INSERT. |

---

## 3. Campos Principais por Tabela

### `tab_validacoes_NFCOM_v4` — Standing
| Campo | Uso no pipeline |
|-------|----------------|
| `CICLO` | Filtro do ciclo de faturamento (parâmetro `ciclo_ref`) |
| `CCLASS` | Código de classificação do item — JOIN com tabela mestre |
| `CCLASS_7D` | Código interno de 7 dígitos — fallback do JOIN mestre |
| `CFOP` | Código Fiscal de Operações — validado contra lista oficial SEFAZ |
| `UF_DEST` | UF do destinatário — valida direção geográfica do CFOP e alíquota ICMS |
| `UF_EMIT_PARAMETRIZADA` | UF da emissora declarada no item — comparada com UF_DEST |
| `CST_ICMS` | Código de Situação Tributária — valida compatibilidade com CCLASS e alíquota |
| `ICMS_STANDING` | Alíquota ICMS informada pelo billing — comparada com tabela verdade |
| `PIS_STANDING` | Alíquota PIS informada pelo billing |
| `COFINS_STANDING` | Alíquota COFINS informada pelo billing |
| `FUST_STANDING` | Alíquota FUST informada pelo billing |
| `FUNTTEL_STANDING` | Alíquota FUNTTEL informada pelo billing |
| `REGIME_TRIB` | Regime tributário (Simples Nacional, Lucro Presumido, etc.) |
| `GRUPO_CCLASS` | Grupo do item (ICMS=10-40-70, ISS=60-80, Financeiro=100-110, Cofaturamento=130) |
| `FATURA_NUMERO` | Número da fatura NFCom |
| `ID_CLIENTE` | Identificador do cliente/contrato |
| `sistem_origem` | Sistema de origem do billing (renomeado para `SISTEMA_ORIGEM` no pipeline → gravado como `CRM` na saída) |
| `TIPO_FAT` | Tipo da fatura (Normal / Substituição) |
| `MUNICIPIO_PRESTACAO` | Município de prestação do serviço — obrigatório pela SEFAZ |
| `MUNICIPIO_DEST` | Município do destinatário |

### `tab_mestre_nfcom_nova` — Mestre Fiscal
| Campo | Uso no pipeline |
|-------|----------------|
| `CCLASS_NFCON` | Chave de join com o CCLASS do standing (normalizado sem zeros à esquerda) |
| `TAX_UF_MUNICIPIO` | Tipo de tributação do item (`ICMS_CST_0`, `ICMS_CST_51`, `ICMS_CST_40`, `SEM_IMPOSTO_CST_NULO`, etc.) — define a TAG de join |
| `CST` | CST esperado para o CCLASS — comparado com o CST declarado no standing |

### `tab_impostos_verdade_nova` — Alíquotas
| Campo | Uso no pipeline |
|-------|----------------|
| `TIPO_IMPOSTO` | Tag do imposto (`ICMS`, `ICMS_CST_0`, `ICMS_CONFAZ`, `ICMS_CST_51`, `ICMS_ESTADUAL`, `ICMS_CST_40`, `PIS_COFINS`) |
| `ESTADO` | UF de aplicação da alíquota |
| `ALIQUOTA` | Alíquota de referência (%) |
| `PIS` | Alíquota PIS de referência |
| `COFINS` | Alíquota COFINS de referência |

### `validacao_status_fatura` — Saída
| Campo destino | Origem no pipeline | Descrição |
|---------------|--------------------|-----------|
| `FATURA` | `fatura_numero` | Número da fatura NFCom |
| `ID_CONTA_CONTRATO` | `id_cliente` | ID do cliente/contrato |
| `REGRA` | `regra` | Categoria da validação (`VALIDACAO_IMPOSTOS` / `VALIDACAO_NFCOM`) |
| `STATUS` | `status` | Resultado (`CORRETO` / `INCORRETO` / `ALERTA`) |
| `SUBSTATUS` | `substatus` | Severidade (`OK` / `BLOQUEANTE` / `ALERTA`) |
| `OBSERVACAO` | `observacao` | Tag da regra disparada + descrição detalhada |
| `DADOS_BILLING` | `dados_billing` | Snapshot dos valores declarados pelo billing |
| `DADOS_TABELA_VERDADE` | `dados_tabela_verdade` | Valores esperados conforme tabelas de referência + regras disparadas |
| `Tipo_Servico` | `tipo_servico` | Tipo de serviço esperado pelo CCLASS |
| `Tipo_Imposto` | `tipo_imposto_mestre` | Tipo de imposto conforme tabela mestre |
| `ID_Lote` | `ID_LOTE` (parâmetro `ciclo_ref`) | Ciclo de faturamento no formato `AAAA-MM` |
| `CRM` | `sistema_origem` (`sistem_origem` na fonte) | Sistema de billing de origem |
| `DT_CARGA` / `DT_ATUALIZACAO` | `current_timestamp()` | Timestamps de processamento |
| `NUMERO_FATURA` | `fatura_numero` | Número da fatura (redundante — mantido por compatibilidade) |

---

## 4. Fluxo de Processamento

```
[1] Parâmetros
    ciclo_ref (AAAA-MM) → ID_LOTE, UF_EMISSORA

[2] Leitura e filtro
    tab_validacoes_NFCOM_v4 filtrado por CICLO
    sistem_origem → renomeado para SISTEMA_ORIGEM

[3] JOIN Mestre (tab_mestre_nfcom_nova)
    Tentativa 1: CCLASS   → CCLASS_NFCON  (direto)
    Tentativa 2: CCLASS_7D → CCLASS_NFCON (fallback)
    Resultado: TAX_UF_MUNICIPIO (TAG) e CST_MESTRE

[4] Derivação da TAG de tributação
    Prioridade: CST declarado no standing → TAG da mestre como fallback
    TAGs válidas: ICMS_CST_0, ICMS_CST_51, ICMS_CST_40, SEM_IMPOSTO_CST_NULO ...

[5] JOIN Impostos (tab_impostos_verdade_nova)
    ICMS por UF (CST 0), CONFAZ (CST 51), Estadual (CST 40)
    PIS/COFINS não-cumulativo (crossJoin — valores fixos)

[6] Cálculo das validações (seção 8)
    9 regras de IMPOSTOS + 17 regras de NFCOM = 26 validações booleanas

[7] Explosão por categoria (mapInPandas)
    1 linha por item × categoria disparada (IMPOSTOS e/ou NFCOM)
    Item sem erro → 1 linha CORRETO/OK por categoria

[8] PK e hash
    id_validacao: MD5(sistema_origem|fatura|cliente|cclass|cfop|regra|ciclo)
    hash_registro: SHA256(status|substatus|observacao)

[9] DELETE + INSERT em validacao_status_fatura
    DELETE WHERE ID_Lote = ciclo + regra IMPOSTOS/NFCOM
    INSERT df_final
```

---

## 5. Catálogo de Validações

### VALIDACAO_IMPOSTOS

| Tag | Severidade | Regra |
|-----|------------|-------|
| `CST_INCOMPATIVEL_TRIBUTO` | ALERTA | CST declarado não corresponde ao esperado para o CCLASS na tabela mestre |
| `CST_ICMS_NULO` | **BLOQUEANTE** | CST ausente em item que deveria ter ICMS (exceto CST 40/41) — Rejeição 539 SEFAZ |
| `ICMS_DIVERGENTE` | ALERTA | Alíquota ICMS ≠ alíquota esperada para a UF (±0,005 pp de tolerância) |
| `PIS_DIVERGENTE` | ALERTA | Alíquota PIS ≠ 0,65% (cumulativo) nem 1,65% (não-cumulativo) |
| `COFINS_DIVERGENTE` | ALERTA | Alíquota COFINS ≠ 3,0% (cumulativo) nem 7,6% (não-cumulativo) |
| `PIS_COFINS_SIMPLES_ALERTA` | ALERTA | PIS/COFINS divergente em empresa do Simples Nacional |
| `FUST_INCORRETO` | ALERTA | FUST ≠ 1,0% (item ICMS) ou ≠ 0% (sem ICMS) — RN004 |
| `FUNTTEL_INCORRETO` | ALERTA | FUNTTEL ≠ 0,5% (item ICMS) ou ≠ 0% (sem ICMS) — RN004 |
| `ICMS_SEM_ALIQUOTA` | ALERTA | Item ICMS (CST 0/10/20/70/90) com alíquota zero informada |

### VALIDACAO_NFCOM

| Tag | Severidade | Regra |
|-----|------------|-------|
| `CCLASS_NAO_MAPEADO` | ALERTA | CCLASS não encontrado na tabela mestre — motor não consegue validar tributação |
| `CFOP_INVALIDO` | **BLOQUEANTE** | CFOP fora dos 17 CFOPs válidos para NFCom (MOC SEFAZ) |
| `CFOP_INCOMPATIVEL_UF` | ALERTA | 5xxx usado interestadual ou 6xxx usado intra-estadual |
| `CFOP_ICMS_COM_ISS` | ALERTA | Item ICMS com CFOP de ISS (5933/6933) |
| `CFOP_SVA_COM_ICMS` | ALERTA | Item ISS/SVA com CFOP de serviço de comunicação ICMS |
| `CFOP_933_COM_CST` | ALERTA | CFOP 5933/6933 com CST ICMS preenchido |
| `ITEM_FINANCEIRO_COM_CFOP` | **BLOQUEANTE** | Item financeiro (grupo 100/110) com CFOP — Rejeição 541 SEFAZ |
| `UF_DEST_INVALIDA` | **BLOQUEANTE** | UF destino ausente ou não reconhecida |
| `FATURA_SEM_NUMERO` | ALERTA | Número de fatura ausente ou inválido |
| `CFOP_AUSENTE` | **BLOQUEANTE** | Item com CST informado sem CFOP — Rejeição 540 SEFAZ |
| `INDSEMCST_COM_CFOP` | ALERTA | Item indSemCST com CFOP preenchido (exceto 5933/6933 e grupo financeiro) |
| `COFATURAMENTO_COM_ICMS` | **BLOQUEANTE** | Grupo 130 (cofaturamento) com ICMS destacado — Rejeição 266 SEFAZ |
| `FAT_CENTRALIZADO_COM_ICMS` | **BLOQUEANTE** | Grupo 120 (fat. centralizado) com ICMS destacado — Rejeição 269 SEFAZ |
| `MUNICIPIO_PRESTACAO_AUSENTE` | ALERTA | Município de prestação ausente (obrigatório SEFAZ) |
| `TIPO_FAT_SUBSTITUICAO` | ALERTA | NFCom de substituição identificada no ciclo |
| `MUNICIPIO_DEST_AUSENTE` | ALERTA | UF destino preenchida mas município do destinatário ausente |
| `CCLASS_7D_DIVERGENTE` | ALERTA | Código interno CCLASS_7D difere do CCLASS oficial SEFAZ |

---

## 6. Regra de Saída na Tabela Destino

| Situação | REGRA | STATUS | SUBSTATUS |
|----------|-------|--------|-----------|
| Erro objetivo (deve corrigir) | `VALIDACAO_IMPOSTOS` ou `VALIDACAO_NFCOM` | `INCORRETO` | `BLOQUEANTE` |
| Anomalia (pode ter exceção) | `VALIDACAO_IMPOSTOS` ou `VALIDACAO_NFCOM` | `ALERTA` | `ALERTA` |
| Nenhuma divergência | `VALIDACAO_IMPOSTOS` ou `VALIDACAO_NFCOM` | `CORRETO` | `OK` |

> **Modelo de linhas:** cada item gera 1 linha por categoria por regra disparada.
> Item com erro de imposto E de NFCom → 2 linhas (uma por categoria).
> Item sem erro → 2 linhas CORRETO (uma por categoria).

---

## 7. Parâmetros de Execução

| Parâmetro | Widget | Padrão | Descrição |
|-----------|--------|--------|-----------|
| `ciclo_ref` | `Ciclo (AAAA-MM)` | mês corrente | Ciclo de faturamento a processar |
| `uf_emissora` | `UF Emissora NFCom` | `RS` | UF sede da Vero Internet (emissora da NFCom) |

---

*Documento gerado em 2026-08-10 a partir do notebook `validacao_nfcom.py` v8.*
