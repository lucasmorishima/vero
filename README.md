# Pipeline de Dados Completo

Projeto com script unico `pipeline_completo.py` para executar um pipeline de dados ponta a ponta, com boas praticas:
- configuracao centralizada em YAML
- logging em arquivo e console
- validacao de colunas obrigatorias
- limpeza de dados (nulos e duplicados)
- padronizacao de variaveis numericas
- separacao treino e teste

## Estrutura

```
Vero_prd/
  config/
    pipeline.yaml
  data/
    raw/
    processed/
  logs/
  output/
  src/
    pipeline/
  .gitignore
  pipeline_completo.py
  requirements.txt
```

## Como configurar o ambiente

No Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Como executar

```powershell
python pipeline_completo.py
```

Opcionalmente, com outro config:

```powershell
python pipeline_completo.py --config config/pipeline.yaml
```

## Saidas

- Dados brutos: `data/raw/`
- Dados processados: `data/processed/`
- Treino e teste: `output/`
- Logs: `logs/pipeline.log`
