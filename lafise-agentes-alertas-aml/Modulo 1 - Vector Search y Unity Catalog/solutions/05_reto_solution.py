# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Reto — Mini-retriever para una alerta — SOLUCIÓN

# COMMAND ----------

import re, json

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.chunks_index"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------

alerta = spark.sql(f"""
  SELECT * FROM {CATALOG}.{SCHEMA}.alertas
  WHERE estado IN ('Abierta','En análisis') ORDER BY score_riesgo DESC LIMIT 1
""").collect()[0]
print(dict(alerta.asDict()))

# COMMAND ----------

# SOLUCIÓN: mini-retriever con búsqueda híbrida + filtros por metadata.
def buscar(pregunta, k=2, filters_json=None):
    r = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME, columns=["source_doc", "articulo", "seccion", "chunk_to_retrieve"],
        query_text=pregunta, query_type="HYBRID", num_results=k, filters_json=filters_json)
    if not r.result:
        return []
    # Devolvemos cada resultado con su cita.
    return [{"cita": f"{f[0]} · {f[1]}", "seccion": f[2], "texto": f[3]} for f in r.result.data_array]

consulta = f"{alerta['tipologia']} monto {alerta['monto_involucrado_usd']}"

contexto = {
    "alerta_id": alerta["alerta_id"],
    "politica":   buscar(consulta, k=1, filters_json=json.dumps({"source_doc": "politica_pld_lafise.pdf"})),
    "tipologia":  buscar(consulta, k=1, filters_json=json.dumps({"source_doc": "manual_tipologias.pdf"})),
    "precedente": buscar(consulta, k=1, filters_json=json.dumps({"source_doc": "narrativas_casos"})),
}
print(json.dumps(contexto, ensure_ascii=False, indent=2, default=str)[:1500])

# COMMAND ----------

assert contexto["politica"] and contexto["tipologia"] and contexto["precedente"]
print("✅ Reto completo: contexto citado para la alerta", alerta["alerta_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enfoques alternativos
# MAGIC - Podrías **combinar las tres consultas en una sola** (sin filtro) y luego agrupar por `source_doc` en
# MAGIC   Python — menos llamadas, pero menos control sobre cuántos chunks tomas de cada fuente.
# MAGIC - En la Sesión 2 este `buscar(...)` se registra como **UC Function / VectorSearchRetrieverTool** para que
# MAGIC   el agente lo invoque como herramienta gobernada.
