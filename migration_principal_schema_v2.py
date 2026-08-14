# Databricks notebook source
# MAGIC %md
# MAGIC # MIGRATION — accenture.faturas_principal_vero — Schema v2
# MAGIC
# MAGIC **Alteracoes:**
# MAGIC - `ASSET` → renomear para `SISTEMA`
# MAGIC - Remover: `STATUS_RETORNO`, `CHAMADO`
# MAGIC - Adicionar: `SEGMENTO` (nova coluna)
# MAGIC
# MAGIC **Schema resultante: 19 colunas (era 20 → 18 apos drops → 19 com SEGMENTO)**
# MAGIC
# MAGIC > ⚠️ Executar UMA UNICA VEZ. A tabela e full-delete-reload, entao nao ha risco de perda de dados historicos.
# MAGIC > Mas verifique se nenhuma carga esta rodando em paralelo antes de executar.

# COMMAND ----------

TBL = "accenture.faturas_principal_vero"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Habilitar Column Mapping (necessario para RENAME e DROP)

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {TBL}
    SET TBLPROPERTIES (
        'delta.columnMapping.mode'  = 'name',
        'delta.minReaderVersion'    = '2',
        'delta.minWriterVersion'    = '5'
    )
""")
print("Column mapping habilitado ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Renomear coluna ASSET → SISTEMA

# COMMAND ----------

spark.sql(f"ALTER TABLE {TBL} RENAME COLUMN ASSET TO SISTEMA")
print("ASSET → SISTEMA ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remover colunas STATUS_RETORNO e CHAMADO

# COMMAND ----------

for col in ["STATUS_RETORNO", "CHAMADO"]:
    try:
        spark.sql(f"ALTER TABLE {TBL} DROP COLUMN {col}")
        print(f"DROP {col} ✅")
    except Exception as e:
        print(f"DROP {col} — ignorado (coluna pode nao existir): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Adicionar coluna SEGMENTO (nova)

# COMMAND ----------

for _nova_col in ["SEGMENTO STRING", "POSSUI_PREBILLING STRING"]:
    try:
        spark.sql(f"ALTER TABLE {TBL} ADD COLUMNS ({_nova_col})")
        print(f"ADD COLUMN {_nova_col} ✅")
    except Exception as e:
        print(f"ADD COLUMN {_nova_col} — ignorado (coluna pode ja existir): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificacao do schema final

# COMMAND ----------

schema_final = [f.name for f in spark.table(TBL).schema.fields]
esperado = [
    "FATURA", "ID_CONTA", "NOME_CLIENTE", "CPF_CNPJ", "SISTEMA", "SEGMENTO", "POSSUI_PREBILLING",
    "STATUS", "STATUS_VALIDACAO", "ANALISTA", "OBSERVACAO", "PROBLEMA",
    "VALOR", "CRIADO_EM", "RESUMO", "Ordem_Status",
    "DATA_ABERTURA_CHAMADO", "DT_EMISSAO", "Valor_Positive", "ID_LOTE",
]

print(f"Colunas na tabela ({len(schema_final)}): {schema_final}")
print(f"Colunas esperadas ({len(esperado)}): {esperado}")

faltando = set(esperado) - set(schema_final)
extras   = set(schema_final) - set(esperado)
ok = not faltando and not extras

print(f"\nSchema correto: {'✅' if ok else '❌ — verificar diferenca abaixo'}")
if faltando: print(f"  Faltando na tabela: {faltando}")
if extras:   print(f"  Extras na tabela:   {extras}")
