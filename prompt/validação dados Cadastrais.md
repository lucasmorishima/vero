# PROMPT — Validação de Dados Cadastrais CNPJ/CPF para NFCOM

## Contexto

Você é um especialista sênior em Billing Assurance e Validação Fiscal para empresas de Telecomunicações.

**Stack implementada:**
- Python 3.x + pandas
- Execução local / servidor (sem Databricks ou PySpark)
- **BrasilAPI** (`brasilapi.com.br/api/cnpj/v1/{cnpj}`) como fonte da Receita Federal
- **e-DNE Básico** (arquivos locais dos Correios) ou **ViaCEP** como fallback para validação de CEP
- **CSV** (`utf-8-sig`, separador `;`) como formato de entrada e saída
- Script principal: `validar_enderecos.py`
- Cache local de consultas: `cache_cnpj.json` (JSON, validade configurável)

---

## Objetivo

Expandir o módulo `validar_enderecos.py` para aplicar as regras de validação RV001–RV005 sobre clientes PJ (CNPJ) e PF (CPF), gerando dois CSVs de saída com resultado completo da validação e dados cadastrais da Receita.

Fluxo:
1. Ler `base_teste.csv` (separador `;`, encoding `utf-8-sig`)
2. Detectar CPF vs CNPJ automaticamente pelo comprimento do documento (11 = CPF, 14 = CNPJ)
3. Validar dígito verificador do CNPJ antes de qualquer chamada externa
4. Deduplicar CNPJs; reutilizar cache JSON para evitar consultas repetidas
5. Consultar BrasilAPI para cada CNPJ único (sequencial, com retry e delay)
6. Consultar CEP via e-DNE (prioridade) ou ViaCEP (fallback)
7. Aplicar RV001–RV005 e montar STATUS / SUBSTATUS / OBSERVACAO
8. Salvar `output_endereco/validacao_enderecos.csv` e `output_endereco/dados_cadastrais_cnpj.csv`

---

## Input — base_teste.csv

Separador `;`, encoding `utf-8-sig`. Colunas obrigatórias:

| Coluna | Tipo | Descrição |
|---|---|---|
| FATURA | str | Número da fatura |
| ID_CLIENTE_CONTRATO | str | Identificador do contrato |
| REGRA | str | Regra de negócio aplicada |
| NOME_CLIENTE | str | Nome/razão social conforme base interna (para RV002) |
| INSCRICAO_ESTADUAL | str | IE cadastrada internamente (para RV003) |
| PRODUTO | str | Produto contratado |
| TIPO_SERVICO | str | SCM, STFC, SMP ou SVA |
| DESCRICAO_SERVICO | str | Descrição do serviço |
| TIPO_IMPOSTO | str | ICMS ou ISS |
| PROMOCAO | str | Promoção vigente (pode ser vazio) |
| GRUPO_LOCALIDADE | str | CAPITAL ou INTERIOR |
| ID_LOTE | str | Identificador do lote de processamento |
| cidade | str | Cidade conforme base interna |
| bairro | str | Bairro conforme base interna |
| cep | str | CEP conforme base interna (sem formatação) |
| uf | str | UF conforme base interna |
| CPF_CNPJ | str | Documento do cliente (CPF ou CNPJ, só dígitos) |

---

## Regras de Validação

### RV001 — Situação Cadastral *(somente CNPJ)*
Fonte: campo `descricao_situacao_cadastral` retornado pela BrasilAPI.

Se situação ≠ `"ATIVA"`:
- STATUS = INCORRETO | SUBSTATUS = ERRO
- OBSERVACAO = `"[RV001] Situação cadastral irregular: {situacao_retornada}"`

---

### RV002 — Razão Social / Nome do Cliente *(somente CNPJ)*
Comparar `NOME_CLIENTE` (base interna) com `razao_social` (BrasilAPI) após normalização (sem acentos, maiúsculas, sem caracteres especiais).

Usar `difflib.SequenceMatcher` para calcular similaridade (0–100%).

Threshold configurável via parâmetro (padrão: `80`).

Se similaridade < threshold:
- STATUS = INCORRETO | SUBSTATUS = ERRO
- OBSERVACAO = `"[RV002] Razão Social divergente ({sim}%): base '{nome_cliente}' x Receita '{razao_social}'"`

---

### RV003 — Inscrição Estadual *(somente CNPJ)*
**Limitação**: A Receita Federal não disponibiliza IE via BrasilAPI (dado estadual, não federal).
A validação verifica apenas a consistência do dado interno:

- Se `INSCRICAO_ESTADUAL` está vazia e UF exige IE para CNPJ ativo → INCORRETO
- Se `INSCRICAO_ESTADUAL` contém valor não numérico diferente de "ISENTO" → INCORRETO
- Se `INSCRICAO_ESTADUAL` = "ISENTO" ou contém dígitos válidos → OK

Se inválida:
- STATUS = INCORRETO | SUBSTATUS = ERRO
- OBSERVACAO = `"[RV003] Inscrição Estadual inválida ou ausente: '{ie_informada}'"`

---

### RV004 — Múltiplos Erros
Quando mais de uma regra falhar no mesmo documento, concatenar todas as observações separadas por `" | "`.

Exemplo:
> `"[RV001] Situação cadastral irregular: SUSPENSA | [RV002] Razão Social divergente (62%): base 'EMPRESA TESTE' x Receita 'COMERCIO GERAL LTDA'"`

---

### RV005 — Registro Válido
Todas as regras aplicáveis passaram:
- STATUS = CORRETO | SUBSTATUS = OK | OBSERVACAO = `""`

---

### CPF — Comportamento Especial
Para documentos CPF, RV001/RV002/RV003 **não se aplicam** (sem dados na Receita Federal para PF).
Aplicar apenas:
- Consultar o CEP informado via e-DNE/ViaCEP
- Se encontrado: STATUS = CORRETO, SUBSTATUS = OK
- Se não encontrado: STATUS = INCORRETO, SUBSTATUS = ERRO, OBSERVACAO = `"[CEP] CEP não encontrado: {cep}"`

---

## Validações de Endereço *(complementares, sempre aplicadas para CNPJ)*

Além das regras RV001–RV003, para CNPJ comparar:

| Comparação | Campo base | Campo Receita | Flag de saída |
|---|---|---|---|
| CEP base vs CEP Receita | `cep` | `receita_cep` | `CEP_Confere_com_Informado` |
| Cidade base vs Cidade Receita | `cidade` (sem sufixo " - AN") | `receita_municipio` | `Cidade_Confere` |
| UF base vs UF Receita | `uf` | `receita_uf` | `UF_Confere` |
| Endereço Receita vs Correios | logradouro+cidade+UF | CEP lookup | `Status_Endereco` |

Divergências de endereço são adicionadas ao campo OBSERVACAO com prefixo `[END]`.

---

## Campos de Saída — validacao_enderecos.csv

| Coluna | Origem |
|---|---|
| Fatura, ID_Cliente, Regra | Do input |
| Produto, Tipo_Servico, Descricao_Servico | Do input |
| Tipo_Imposto, Promocao, Grupo_Localidade, ID_Lote | Do input |
| Cidade_Base, Bairro_Base, UF_Base | Do input (`cidade`, `bairro`, `uf`) |
| Nome_Cliente_Base | Do input (`NOME_CLIENTE`) |
| IE_Base | Do input (`INSCRICAO_ESTADUAL`) |
| Documento | CPF/CNPJ normalizado (só dígitos) |
| Tipo | `CPF` ou `CNPJ` |
| **STATUS** | `CORRETO` ou `INCORRETO` |
| **SUBSTATUS** | `OK` ou `ERRO` |
| **OBSERVACAO** | Motivos concatenados com ` | ` |
| Regras_Aplicadas | Ex: `RV001,RV002,RV003` |
| Similaridade_Razao_Social | % numérico (somente CNPJ, RV002) |
| CEP_Informado, CEP_Receita | CEPs normalizados |
| CEP_Confere_com_Informado | `Sim` ou `Não` |
| Cidade_Confere, UF_Confere | `Sim` ou `Não` |
| Logradouro_Receita, Numero_Receita, Complemento_Receita | Da BrasilAPI |
| Bairro_Receita, Cidade_Receita, UF_Receita | Da BrasilAPI |
| Logradouro_Correios, Bairro_Correios, Cidade_Correios, UF_Correios | Do e-DNE/ViaCEP |
| Fonte_CEP | `e-DNE`, `Correios API` ou `ViaCEP` |
| Razao_Social | Da BrasilAPI |
| Situacao_Cadastral | Da BrasilAPI |

---

## Campos de Saída — relatorio_validacao.csv *(planilha consolidada — nova)*

Gerada para **todos os registros** (CPF e CNPJ). Colunas com informação ausente devem ficar **nulas**.

| Coluna | Tipo | Descrição |
|---|---|---|
| FATURA | str | Número da fatura (do input) |
| ID_CONTA_CONTRATO | str | Identificador do contrato (do input `ID_CLIENTE_CONTRATO`) |
| REGRA | str | Regra de negócio aplicada (do input) |
| STATUS | str | `CORRETO` ou `INCORRETO` |
| SUBSTATUS | str | `OK` ou `ERRO` |
| OBSERVACAO | str | Motivos concatenados com ` \| ` (vazio se tudo OK) |
| DADOS_BILLING | str | Dados conforme base interna, formato: `NOME: {nome} \| DOC: {doc} \| CEP: {cep} \| CIDADE: {cidade} \| UF: {uf} \| IE: {ie}` |
| DADOS_CONTRATO | str | Dados retornados pela Receita Federal, formato: `RAZAO: {razao} \| SITUACAO: {sit} \| CEP: {cep} \| END: {logradouro}, {numero} - {bairro} - {cidade}/{uf}` (nulo para CPF) |
| DADOS_TABELA_VERDADE | str | Dados da Tabela Verdade cadastral quando disponível (nulo — reservado para implementação futura) |
| ID_LOTE | str | Identificador do lote (do input) |
| PRODUTO | str | Produto contratado (do input) |
| TIPO_SERVICO | str | SCM, STFC, SMP ou SVA (do input) |
| DESCRICAO_SERVICO | str | Descrição do serviço (do input) |
| TIPO_IMPOSTO | str | ICMS ou ISS (do input) |
| PROMOCAO | str | Promoção vigente (do input, pode ser nulo) |
| GRUPO_LOCALIDADE | str | CAPITAL ou INTERIOR (do input) |

**Regras de preenchimento:**
- `DADOS_BILLING`: sempre preenchido com os dados da base interna
- `DADOS_CONTRATO`: preenchido somente para CNPJ consultado com sucesso na BrasilAPI; nulo para CPF e CNPJ com erro
- `DADOS_TABELA_VERDADE`: sempre nulo nesta versão (reservado)
- Campos do input ausentes no arquivo de entrada → nulo na saída

---

## Campos de Saída — dados_cadastrais_cnpj.csv

Somente CNPJs consultados com sucesso na BrasilAPI:

| Coluna | Descrição |
|---|---|
| Fatura, ID_Cliente, ID_Lote | Rastreabilidade |
| CNPJ | CNPJ normalizado |
| Razao_Social, Nome_Fantasia | Identificação |
| Situacao_Cadastral, Data_Situacao_Cadastral, Motivo_Situacao | Situação na Receita |
| Natureza_Juridica | Natureza jurídica |
| Data_Inicio_Atividade | Data de abertura |
| CNAE_Principal_Codigo, CNAE_Principal_Descricao | Atividade econômica principal |
| Porte, Capital_Social | Porte e capital |
| Simples_Nacional, MEI | Regime tributário |
| Email, Telefone | Contato da Receita |
| CEP_Receita, Logradouro_Receita, Numero_Receita | Endereço da Receita |
| Complemento_Receita, Bairro_Receita, Municipio_Receita, UF_Receita | Endereço da Receita |

---

## Regras Técnicas

### Cache Local de CNPJs
- Arquivo: `cache_cnpj.json` na raiz do projeto
- Estrutura: `{ "12345678000100": { "data_consulta": "2026-07-20", "payload": {...} } }`
- Validade configurável (padrão: 30 dias); entradas expiradas são reconsultadas
- Salvar após cada consulta bem-sucedida para sobreviver a interrupções

### Deduplicação
- Antes do loop principal, extrair CNPJs únicos
- Consultar BrasilAPI apenas uma vez por CNPJ único (mesmo que apareça em N faturas)
- Reutilizar resultado do cache/consulta para todas as linhas do mesmo CNPJ

### Consulta à BrasilAPI
- Endpoint: `https://brasilapi.com.br/api/cnpj/v1/{cnpj}`
- Retry com backoff exponencial: 4 tentativas, fator 1.0, status `[429, 500, 502, 503, 504]`
- Em HTTP 429 persistente: sleep 3 s + 1 tentativa final
- Delay de 0,5 s entre consultas (cortesia — API pública gratuita)
- CNPJ inválido (falha no dígito verificador): não consultar, marcar como INCORRETO diretamente

### Consulta de CEP — Prioridade
1. **e-DNE Básico** — arquivos `LOG_*.TXT` em `dne_basico/` (local, sem limite, sem internet)
2. **API Correios** — autenticada via `CORREIOS_TOKEN` no `.env` (quando configurado)
3. **ViaCEP** — fallback gratuito, delay 0,3 s por consulta

### Normalização para Comparação de Strings
1. Converter para maiúsculas
2. Remover acentos (NFD → strip categoria Mn)
3. Remover caracteres não alfanuméricos (substituir por espaço)
4. Colapsar múltiplos espaços
5. Para logradouro: remover prefixo do tipo de via (AVENIDA, RUA, ESTRADA, RODOVIA, ALAMEDA, TRAVESSA, PRACA, LARGO, SETOR, QUADRA, CONJUNTO, VILA, PARQUE, VIA, VIADUTO) — a Receita grava o tipo separado do nome; ViaCEP os une
6. Para cidade base: remover sufixo ` - AN` e similares antes de comparar

### Similaridade de Texto (RV002)
- Usar `difflib.SequenceMatcher(None, str_a, str_b).ratio() * 100`
- Ambos os lados normalizados antes do cálculo
- Threshold padrão: 80 (configurável via parâmetro ou `.env`)
- Registrar o percentual calculado no campo `Similaridade_Razao_Social`

### Validação de CNPJ
- Calcular e verificar os dois dígitos verificadores antes de qualquer chamada
- CNPJs com dígito inválido:
  - STATUS = INCORRETO | SUBSTATUS = ERRO
  - OBSERVACAO = `"[DOC] CNPJ inválido — falha no dígito verificador"`
  - Não gera chamada à BrasilAPI nem ao CEP

### Configuração via .env
```
CORREIOS_TOKEN=            # Token API Correios (opcional)
DNE_DIR=./dne_basico       # Caminho dos arquivos e-DNE (opcional)
SIMILARIDADE_THRESHOLD=80  # Threshold RV002 (opcional, padrão 80)
CACHE_VALIDADE_DIAS=30     # Validade do cache CNPJ (opcional, padrão 30)
```

### Uso
```bash
# Arquivo padrão (base_teste.csv)
python validar_enderecos.py

# Arquivo específico
python validar_enderecos.py outro_arquivo.csv
```

---

## Arquitetura de Arquivos

```
Vero_prd/
├── validar_enderecos.py        # Script principal
├── base_teste.csv              # Input (separador ;)
├── cache_cnpj.json             # Cache local de consultas BrasilAPI
├── .env                        # Credenciais e configurações
├── dne_basico/                 # Arquivos e-DNE Básico (opcional)
│   ├── LOG_LOGRADOURO.TXT
│   ├── LOG_LOCALIDADE.TXT
│   ├── LOG_BAIRRO.TXT
│   ├── LOG_GRANDE_USUARIO.TXT
│   └── LOG_CPC.TXT
├── output_endereco/
│   ├── validacao_enderecos.csv      # Todos os clientes + status
│   └── dados_cadastrais_cnpj.csv   # Somente CNPJs com dados da Receita
└── prompt/
    └── validação dados Cadastrais.md
```

---

## Exemplo de Saída — validacao_enderecos.csv (CNPJ divergente)

```
Fatura,ID_Cliente,Regra,Produto,...,STATUS,SUBSTATUS,OBSERVACAO,Regras_Aplicadas,...
1,2248075,DADOS CADASTRAIS,BANDA LARGA EMPRESARIAL,...,INCORRETO,ERRO,"[RV001] Situação cadastral irregular: BAIXADA | [END] CEP divergente: base '79610230' x Receita '09842000'",RV001,RV003,...
```

## Exemplo de Saída — validacao_enderecos.csv (CNPJ OK)

```
2,2321987,DADOS CADASTRAIS,SOLUCAO CORPORATIVA,...,CORRETO,OK,,RV001,RV002,RV003,...
```
