# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Búsqueda híbrida y filtros por metadata — SOLUCIÓN

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

def buscar(pregunta, k=3, query_type="ANN", filters_json=None):
    r = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME, columns=["source_doc","articulo","seccion","chunk_to_retrieve"],
        query_text=pregunta, query_type=query_type, num_results=k, filters_json=filters_json)
    return r.result.data_array if r.result else []

# COMMAND ----------

# SOLUCIÓN: la segunda llamada usa query_type="HYBRID".
consulta = "precedente del caso K-2024-018 sobre estructuración"
print("=== Solo SEMÁNTICA (ANN) ===")
for f in buscar(consulta, k=3, query_type="ANN"):
    print(f"  [{f[0]} · {f[1]}] score={f[-1]:.3f}  {f[3][:90]}...")
print("\n=== HÍBRIDA ===")
for f in buscar(consulta, k=3, query_type="HYBRID"):
    print(f"  [{f[0]} · {f[1]}] score={f[-1]:.3f}  {f[3][:90]}...")

# COMMAND ----------

for tipo in ["ANN", "HYBRID"]:
    print(f"=== {tipo} ===")
    for f in buscar("qué hacer ante una coincidencia OFAC", k=2, query_type=tipo):
        print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")
    print()

# COMMAND ----------

# SOLUCIÓN: filtro por metadata a narrativas_casos.
print("=== Filtrado a la política PLD ===")
for f in buscar("conservación de registros", k=2, query_type="HYBRID",
                filters_json=json.dumps({"source_doc": "politica_pld_lafise.pdf"})):
    print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")

print("\n=== Filtrado a precedentes ===")
filtro = json.dumps({"source_doc": "narrativas_casos"})
for f in buscar("movimiento rápido de fondos", k=2, query_type="HYBRID", filters_json=filtro):
    print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")

# COMMAND ----------

hits = buscar("caso K-2024-018", k=5, query_type="HYBRID")
assert any("K-2024-018" in str(f[3]) for f in hits)
solo = buscar("testaferros", k=3, query_type="HYBRID", filters_json=json.dumps({"source_doc": "narrativas_casos"}))
assert all(f[0] == "narrativas_casos" for f in solo)
print("✅ Híbrida + filtro por metadata OK.")
