# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Vector Search + gobierno en Unity Catalog
# MAGIC
# MAGIC **Objetivo:** indexar los `chunks` en **Databricks Vector Search** y consultarlos semánticamente, con el
# MAGIC índice **gobernado en Unity Catalog** (permisos + linaje). ⏱️ ~30 min.
# MAGIC
# MAGIC ### Conceptos
# MAGIC - **Endpoint**: el recurso de cómputo que aloja los índices (lo compartimos entre participantes).
# MAGIC - **Delta Sync Index (embeddings gestionados)**: Databricks calcula los embeddings por ti y **mantiene el
# MAGIC   índice sincronizado** con la tabla Delta fuente (`chunks`). Es el patrón recomendado.
# MAGIC - **Gobierno**: el índice es un objeto de UC (`catalog.schema.índice`) → hereda permisos, aparece en el
# MAGIC   **linaje** y se controla como cualquier tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — Configuración y convenciones

# COMMAND ----------

import re, time

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"

VS_ENDPOINT = "lafise_vs_endpoint"                 # compartido en el workspace
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.chunks"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.chunks_index"    # por participante

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType, EndpointType)
w = WorkspaceClient()
print(f"Fuente: {SOURCE_TABLE}\nÍndice: {INDEX_NAME}\nEndpoint: {VS_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Elegir el modelo de embeddings (multilingüe para español)
# MAGIC Preferimos **`databricks-qwen3-embedding-0-6b`** (multilingüe, ideal para documentos en español); si no
# MAGIC está disponible en el workspace, caemos a **`databricks-gte-large-en`** (GA).

# COMMAND ----------

disponibles = {e.name for e in w.serving_endpoints.list()}
for cand in ["databricks-qwen3-embedding-0-6b", "databricks-gte-large-en", "databricks-bge-large-en"]:
    if cand in disponibles:
        EMBEDDING_MODEL = cand
        break
else:
    raise RuntimeError("No hay un modelo de embeddings servido. Revisa Foundation Model APIs.")
print(f"Modelo de embeddings: {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear (o reutilizar) el endpoint de Vector Search
# MAGIC El endpoint es compartido: si ya existe, lo reutilizamos. Crearlo por primera vez puede tardar varios
# MAGIC minutos.

# COMMAND ----------

existentes = {e.name for e in w.vector_search_endpoints.list_endpoints()}
if VS_ENDPOINT not in existentes:
    print(f"Creando endpoint {VS_ENDPOINT} (puede tardar 5–15 min)...")
    w.vector_search_endpoints.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type=EndpointType.STANDARD)
else:
    print(f"♻️  Reutilizando endpoint existente {VS_ENDPOINT}")
print("Estado:", w.vector_search_endpoints.get_endpoint(VS_ENDPOINT).endpoint_status.state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Crear el Delta Sync Index (embeddings gestionados)
# MAGIC
# MAGIC Le decimos a Vector Search: **qué tabla** (`chunks`), **qué llave primaria** (`chunk_id`), **qué columna
# MAGIC embeber** (`chunk_to_embed`, la enriquecida) y **con qué modelo**. El resto de columnas (metadata para
# MAGIC citar) se sincronizan también.
# MAGIC
# MAGIC **TODO:** completa el nombre de la columna de texto a embeber y la llave primaria.

# COMMAND ----------

indices = {i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=VS_ENDPOINT)}
if INDEX_NAME not in indices:
    # El SDK espera objetos tipados (no un dict) para el spec del índice.
    w.vector_search_indexes.create_index(
        name=INDEX_NAME,
        endpoint_name=VS_ENDPOINT,
        primary_key="<FILL_IN>",                 # TODO: la llave primaria de la tabla chunks
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=SOURCE_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="<FILL_IN>",               # TODO: la columna de texto a embeber
                embedding_model_endpoint_name=EMBEDDING_MODEL,
            )],
        ),
    )
    print(f"Índice {INDEX_NAME} en creación...")
else:
    print(f"♻️  El índice {INDEX_NAME} ya existe; disparando sincronización.")
    w.vector_search_indexes.sync_index(index_name=INDEX_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Esperar a que el índice esté listo
# MAGIC La primera indexación de un corpus pequeño suele tardar **~5–15 min**. Buen momento para un descanso.

# COMMAND ----------

deadline = time.time() + 30 * 60   # un endpoint nuevo + primera indexación puede tardar; damos margen
while time.time() < deadline:
    try:
        status = w.vector_search_indexes.get_index(INDEX_NAME).status
    except Exception as e:
        # Justo tras crearlo, get_index puede dar 404 unos segundos (consistencia eventual).
        print(f"  (índice aún no visible: {type(e).__name__}) ...")
        time.sleep(30)
        continue
    ready = getattr(status, "ready", False)
    print(f"  ready={ready}  filas_indexadas={getattr(status,'indexed_row_count',None)}")
    if ready:
        print("✅ Índice listo.")
        break
    time.sleep(30)
else:
    print("⏳ Aún indexando. Puedes continuar cuando 'ready=True' (revisa la UI: Catalog → tu índice).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Primera consulta semántica
# MAGIC Preguntamos en lenguaje natural; Vector Search devuelve los chunks más parecidos por significado (aún
# MAGIC sin coincidir palabra por palabra).

# COMMAND ----------

def buscar(pregunta, k=3, query_type="ANN", filters_json=None):
    r = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["source_doc", "articulo", "seccion", "chunk_to_retrieve"],
        query_text=pregunta,
        query_type=query_type,
        num_results=k,
        filters_json=filters_json,
    )
    return r.result.data_array if r.result else []

for fila in buscar("¿cuándo debo reportar una transacción en efectivo?"):
    print(f"[{fila[0]} · {fila[1]}] score={fila[-1]:.3f}\n  {fila[3][:160]}...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Gobierno en Unity Catalog
# MAGIC El índice es un objeto de UC. Se controla con `GRANT`/`REVOKE` y aparece en el **linaje** (tabla fuente →
# MAGIC índice). Así, el retrieval del agente hereda el control de acceso del banco.

# COMMAND ----------

# El índice y su tabla fuente son objetos gobernados. Ejemplo de control de acceso (heredado por el agente):
#   GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.chunks TO `analistas_pld`;
# Revisa metadata y linaje del índice:
display(spark.sql(f"DESCRIBE EXTENDED {SOURCE_TABLE}"))
print("En la UI: Catalog →", CATALOG, "→", SCHEMA, "→ chunks_index → pestaña 'Lineage' para ver fuente→índice.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

try:
    res = buscar("plazo para presentar un ROS a la UAF", k=3)
    assert len(res) > 0, "La consulta no devolvió resultados (¿índice aún indexando?)"
    textos = " ".join(str(f[3]) for f in res)
    assert "3 días" in textos or "ROS" in textos, "Se esperaba recuperar el artículo del ROS"
    print("✅ Vector Search responde y recupera el fundamento correcto.")
except AssertionError as e:
    print(f"❌ {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Creaste un **endpoint** y un **Delta Sync Index** con **embeddings gestionados** (multilingüe).
# MAGIC - Consultaste el índice en lenguaje natural y recuperaste el fundamento correcto.
# MAGIC - Viste que el índice vive en **Unity Catalog** (permisos + linaje).
# MAGIC
# MAGIC ➡️ Sigue en **`04_hybrid_search`**: combinar búsqueda **semántica + por palabra clave** — clave cuando la
# MAGIC consulta trae nombres, números de cuenta o códigos (ROS, tipologías, listas de sanciones).
