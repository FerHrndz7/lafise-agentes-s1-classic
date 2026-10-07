# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Búsqueda híbrida (keyword + semántica) y filtros por metadata
# MAGIC
# MAGIC **Objetivo:** combinar búsqueda **semántica** con **coincidencia por palabra clave** para no perder
# MAGIC términos exactos — un requisito en AML, donde la consulta trae **nombres, números de cuenta, códigos**
# MAGIC (ROS, RTE, tipologías, IDs de caso) y **nombres de listas de sanciones** (OFAC, ONU). ⏱️ ~25 min.
# MAGIC
# MAGIC ### ¿Por qué híbrida?
# MAGIC - **Semántica (ANN)** entiende el *significado* ("cuándo reportar efectivo" ≈ "umbral de RTE") pero puede
# MAGIC   **pasar por alto un término literal** poco frecuente (un ID `K-2024-018`, "OFAC", "Artículo 5").
# MAGIC - **Keyword (BM25)** acierta el término exacto pero **no entiende sinónimos ni paráfrasis**.
# MAGIC - **Híbrida** fusiona ambos rankings → recall alto sin perder exactitud. Es el default recomendado para RAG.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — Configuración

# COMMAND ----------

import re

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
        index_name=INDEX_NAME,
        columns=["source_doc", "articulo", "seccion", "chunk_to_retrieve"],
        query_text=pregunta, query_type=query_type, num_results=k, filters_json=filters_json,
    )
    return r.result.data_array if r.result else []

print(f"Índice: {INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Semántica vs. Híbrida sobre una consulta con término exacto
# MAGIC Buscamos un **precedente por su ID de caso** (`K-2024-018`) — un token literal que la búsqueda semántica
# MAGIC pura suele diluir.
# MAGIC
# MAGIC **TODO:** en la segunda llamada, activa la búsqueda híbrida con `query_type="HYBRID"`.

# COMMAND ----------

consulta = "precedente del caso K-2024-018 sobre estructuración"

print("=== Solo SEMÁNTICA (ANN) ===")
for f in buscar(consulta, k=3, query_type="ANN"):
    print(f"  [{f[0]} · {f[1]}] score={f[-1]:.3f}  {f[3][:90]}...")

print("\n=== HÍBRIDA (keyword + semántica) ===")
for f in buscar(consulta, k=3, query_type=<FILL_IN>):     # TODO: "HYBRID"
    print(f"  [{f[0]} · {f[1]}] score={f[-1]:.3f}  {f[3][:90]}...")

print("\n👉 La híbrida sube el chunk que contiene el token exacto 'K-2024-018'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Otro caso: nombre de lista de sanciones
# MAGIC "OFAC" es un acrónimo literal. Compara cómo la híbrida asegura recuperar el artículo de listas de sanciones.

# COMMAND ----------

for tipo in ["ANN", "HYBRID"]:
    print(f"=== {tipo} ===")
    for f in buscar("qué hacer ante una coincidencia OFAC", k=2, query_type=tipo):
        print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Filtros por metadata
# MAGIC Restringimos la búsqueda a un documento o sección usando la metadata que guardamos en el Módulo 02.
# MAGIC Útil para acotar el retrieval del agente ("solo el manual de tipologías", "solo precedentes").
# MAGIC
# MAGIC **TODO:** completa el filtro para buscar únicamente en `narrativas_casos` (precedentes).

# COMMAND ----------

import json

# Solo dentro de la política PLD:
print("=== Filtrado a la política PLD ===")
for f in buscar("conservación de registros", k=2, query_type="HYBRID",
                filters_json=json.dumps({"source_doc": "politica_pld_lafise.pdf"})):
    print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")

# Solo precedentes (narrativas de casos):
print("\n=== Filtrado a precedentes ===")
filtro = json.dumps({"source_doc": <FILL_IN>})     # TODO: "narrativas_casos"
for f in buscar("movimiento rápido de fondos", k=2, query_type="HYBRID", filters_json=filtro):
    print(f"  [{f[0]} · {f[1]}] {f[3][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nota avanzada — Reranking nativo
# MAGIC Para exprimir aún más el recall, Vector Search ofrece un **reranker** nativo (`DatabricksReranker`) que
# MAGIC reordena los candidatos recuperados por relevancia fina. Se usa típicamente al conectar el índice como
# MAGIC herramienta del agente (Sesión 2), vía `VectorSearchRetrieverTool(..., reranker=DatabricksReranker(...))`.
# MAGIC Lo mencionamos aquí como el siguiente nivel; hoy nos quedamos con híbrida + filtros.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

try:
    hits = buscar("caso K-2024-018", k=5, query_type="HYBRID")
    assert any("K-2024-018" in str(f[3]) for f in hits), "La híbrida debería recuperar el caso por su ID exacto"
    solo_casos = buscar("testaferros", k=3, query_type="HYBRID",
                        filters_json=json.dumps({"source_doc": "narrativas_casos"}))
    assert all(f[0] == "narrativas_casos" for f in solo_casos), "El filtro debería limitar a precedentes"
    print("✅ Híbrida recupera el token exacto y el filtro por metadata acota bien.")
except AssertionError as e:
    print(f"❌ {e}\nRevisa los TODO (query_type='HYBRID' y el filtro de precedentes).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Comparaste **semántica vs. híbrida**: la híbrida recupera términos literales (IDs, OFAC, artículos) sin
# MAGIC   perder la comprensión semántica.
# MAGIC - Aplicaste **filtros por metadata** para acotar el retrieval por documento/precedente.
# MAGIC - Conociste el **reranking nativo** como siguiente nivel.
# MAGIC
# MAGIC ➡️ Sigue en **`05_reto`**: arma un mini-retriever que, dada una alerta, reúna el contexto que necesitaría
# MAGIC el Agente de Alertas.
