# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Vector Search + gobierno en UC — SOLUCIÓN

# COMMAND ----------

import re, time

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"

VS_ENDPOINT = "lafise_vs_endpoint"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.chunks"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.chunks_index"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType, EndpointType)
w = WorkspaceClient()
print(f"Fuente: {SOURCE_TABLE}\nÍndice: {INDEX_NAME}\nEndpoint: {VS_ENDPOINT}")

# COMMAND ----------

# SOLUCIÓN: preferimos el modelo multilingüe; caemos a gte/bge si no está.
disponibles = {e.name for e in w.serving_endpoints.list()}
for cand in ["databricks-qwen3-embedding-0-6b", "databricks-gte-large-en", "databricks-bge-large-en"]:
    if cand in disponibles:
        EMBEDDING_MODEL = cand
        break
else:
    raise RuntimeError("No hay modelo de embeddings servido.")
print(f"Modelo de embeddings: {EMBEDDING_MODEL}")

# COMMAND ----------

existentes = {e.name for e in w.vector_search_endpoints.list_endpoints()}
if VS_ENDPOINT not in existentes:
    print(f"Creando endpoint {VS_ENDPOINT} (5–15 min)...")
    w.vector_search_endpoints.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type=EndpointType.STANDARD)
else:
    print(f"♻️  Reutilizando {VS_ENDPOINT}")
print("Estado:", w.vector_search_endpoints.get_endpoint(VS_ENDPOINT).endpoint_status.state)

# COMMAND ----------

# SOLUCIÓN: primary_key='chunk_id', columna a embeber='chunk_to_embed' (la enriquecida).
indices = {i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=VS_ENDPOINT)}
if INDEX_NAME not in indices:
    # SOLUCIÓN: primary_key='chunk_id', columna a embeber='chunk_to_embed'. El SDK requiere
    # objetos tipados (DeltaSyncVectorIndexSpecRequest/EmbeddingSourceColumn), no un dict.
    w.vector_search_indexes.create_index(
        name=INDEX_NAME, endpoint_name=VS_ENDPOINT, primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=SOURCE_TABLE, pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="chunk_to_embed", embedding_model_endpoint_name=EMBEDDING_MODEL)]),
    )
    print("Índice en creación...")
else:
    print("♻️  Índice ya existe; sincronizando.")
    w.vector_search_indexes.sync_index(index_name=INDEX_NAME)

# COMMAND ----------

# Expected output: ready pasa de False a True en ~5–15 min (corpus pequeño; más si el endpoint es nuevo).
deadline = time.time() + 30 * 60
while time.time() < deadline:
    try:
        status = w.vector_search_indexes.get_index(INDEX_NAME).status
    except Exception as e:
        # Tras crearlo, get_index puede dar 404 unos segundos (consistencia eventual).
        print(f"  (índice aún no visible: {type(e).__name__}) ..."); time.sleep(30); continue
    ready = getattr(status, "ready", False)
    print(f"  ready={ready}  filas_indexadas={getattr(status,'indexed_row_count',None)}")
    if ready:
        print("✅ Índice listo."); break
    time.sleep(30)

# COMMAND ----------

def buscar(pregunta, k=3, query_type="ANN", filters_json=None):
    r = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME, columns=["source_doc","articulo","seccion","chunk_to_retrieve"],
        query_text=pregunta, query_type=query_type, num_results=k, filters_json=filters_json)
    return r.result.data_array if r.result else []

for fila in buscar("¿cuándo debo reportar una transacción en efectivo?"):
    print(f"[{fila[0]} · {fila[1]}] score={fila[-1]:.3f}\n  {fila[3][:160]}...\n")

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {SOURCE_TABLE}"))
print("UI: Catalog →", CATALOG, "→", SCHEMA, "→ chunks_index → 'Lineage'.")

# COMMAND ----------

res = buscar("plazo para presentar un ROS a la UAF", k=3)
assert len(res) > 0
assert any(("3 días" in str(f[3])) or ("ROS" in str(f[3])) for f in res)
print("✅ Vector Search recupera el fundamento correcto.")
