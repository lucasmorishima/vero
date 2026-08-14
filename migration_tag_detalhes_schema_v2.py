# Databricks notebook source
# MAGIC %md
# MAGIC # MIGRATION — accenture.tag_detalhes_vero — Schema v2
# MAGIC
# MAGIC **Alteracoes:**
# MAGIC - `ASSET` → renomear para `SISTEMA`
# MAGIC - `PRODUTO` → renomear para `SEGMENTO`
# MAGIC - Remover: `COMPONENT_ID`, `ID_SERVICO`, `PROMOCAO`, `GRUPO`, `STATUS_RETORNO`, `CHAMADO`
# MAGIC
# MAGIC **Schema resultante: 21 colunas (era 27)**
# MAGIC
# MAGIC > ⚠️ Executar UMA UNICA VEZ. Verificar se nenhuma carga esta rodando antes de executar.

# COMMAND ----------

TBL = "accenture.tag_detalhes_vero"

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
# MAGIC ## 2. Renomear colunas

# COMMAND ----------

spark.sql(f"ALTER TABLE {TBL} RENAME COLUMN ASSET   TO SISTEMA")
print("ASSET  → SISTEMA  ✅")

spark.sql(f"ALTER TABLE {TBL} RENAME COLUMN PRODUTO TO SEGMENTO")
print("PRODUTO → SEGMENTO ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remover colunas desnecessarias

# COMMAND ----------

for col in ["COMPONENT_ID", "ID_SERVICO", "PROMOCAO", "GRUPO", "STATUS_RETORNO", "CHAMADO"]:
    try:
        spark.sql(f"ALTER TABLE {TBL} DROP COLUMN {col}")
        print(f"DROP {col} ✅")
    except Exception as e:
        print(f"DROP {col} — ignorado (coluna pode nao existir): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Adicionar novas colunas (POSSUI_PREBILLING)

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {TBL} ADD COLUMNS (POSSUI_PREBILLING STRING)")
    print("ADD COLUMN POSSUI_PREBILLING ✅")
except Exception as e:
    print(f"ADD COLUMN POSSUI_PREBILLING — ignorado: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificacao do schema final

# COMMAND ----------

schema_final = [f.name for f in spark.table(TBL).schema.fields]
esperado = [
    "FATURA", "ID_CONTA", "SISTEMA", "REGRA", "STATUS", "SUBSTATUS", "OBSERVACAO",
    "DADOS_KENAN", "DADOS_TABELA_VERDADE", "ID_LOTE", "SEGMENTO", "POSSUI_PREBILLING",
    "TIPO_SERVICO", "DESCRICAO_SERVICO", "TIPO_IMPOSTO",
    "STATUS_VALIDACAO", "TAG", "ANALISTA",
    "RESUMO", "_FILTRA_PAGE_TAG", "DATA_ABERTURA_CHAMADO", "DT_EMISSAO",
]

print(f"Colunas na tabela ({len(schema_final)}): {schema_final}")
print(f"Colunas esperadas ({len(esperado)}): {esperado}")

faltando = set(esperado) - set(schema_final)
extras   = set(schema_final) - set(esperado)
ok = not faltando and not extras

print(f"\nSchema correto: {'✅' if ok else '❌ — verificar diferenca abaixo'}")
if faltando: print(f"  Faltando na tabela: {faltando}")
if extras:   print(f"  Extras na tabela:   {extras}")
