# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Demo: ¿por qué tarda la indexación y cómo hacerla rápida?
# MAGIC
# MAGIC **Objetivo:** medir, paso a paso, cuánto tarda cada etapa de **Databricks AI Search** (antes *Vector Search*)
# MAGIC y mostrar las tres palancas que la hacen rápida. ⏱️ ~5 min con el endpoint caliente; ~25 min si el endpoint es nuevo.
# MAGIC
# MAGIC Este notebook es **autocontenido**: genera su propio corpus PLD sintético, no depende de los módulos 00–05.
# MAGIC
# MAGIC ### Lo que vas a ver
# MAGIC | Etapa | Endpoint nuevo (frío) | Endpoint caliente |
# MAGIC |---|---|---|
# MAGIC | Calcular embeddings de 300 chunks (`ai_query`, qwen3) | ~13 s | ~13 s |
# MAGIC | Crear un **Delta Sync Index** nuevo | ~19 min | **~50 s** |
# MAGIC | Modificar un documento (`MERGE`) + sync incremental | — | **~45–50 s** |
# MAGIC | Cambio vía **Direct Access** (upsert) | — | **~1,5 s** |
# MAGIC
# MAGIC *Valores medidos en un smoke test (2026-10-14) con 300 chunks de ~1.100 caracteres. Los tiempos varían por región y carga.*
# MAGIC
# MAGIC ### Las tres ideas clave
# MAGIC 1. **El cuello de botella no son los embeddings ni el sync**: es el **aprovisionamiento inicial del endpoint**. El endpoint
# MAGIC    dice `ONLINE` en segundos, pero su cómputo se aprovisiona cuando llega el **primer índice** (~18 min, una sola vez).
# MAGIC    → **Mantén el endpoint caliente** (con al menos un índice vivo).
# MAGIC 2. **La actualización incremental solo reprocesa lo que cambió**, si el `chunk_id` es **determinístico** y se escribe
# MAGIC    con `MERGE` (no con `overwrite` + `uuid()`).
# MAGIC 3. **Para frescura en segundos** existe el **índice de acceso directo** (Direct Vector Access).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — Configuración
# MAGIC - `catalog`: un catálogo donde puedas crear esquemas (REQUERIDO).
# MAGIC - `endpoint`: un endpoint **STANDARD** de AI Search. **Si usas uno que ya tiene índices (caliente), la demo tarda ~5 min.**
# MAGIC   Si no existe, se crea (y la primera indexación tardará ~20 min: es justamente lo que queremos mostrar).
# MAGIC - `schema` (opcional): por defecto `demo_indexacion_<usuario>`. Cámbialo para empezar con nombres nuevos.
# MAGIC - `cleanup`: `si` para borrar índices y esquema al final.

# COMMAND ----------

import re, time, json

dbutils.widgets.text("catalog", "", "Catálogo (REQUERIDO)")
dbutils.widgets.text("endpoint", "demo_indexacion_endpoint", "Endpoint de AI Search")
dbutils.widgets.text("schema", "", "Esquema (opcional)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "si"], "Borrar al final")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe un catálogo en el widget 'catalog'."
SCHEMA = dbutils.widgets.get("schema").strip() or f"demo_indexacion_{user_prefix}"
VS_ENDPOINT = dbutils.widgets.get("endpoint").strip()
EMBEDDING_MODEL = "databricks-qwen3-embedding-0-6b"   # multilingüe, ideal para español

Q = lambda name: f"`{CATALOG}`.`{SCHEMA}`.`{name}`"    # nombre SQL con backticks (soporta catálogos con guiones)
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.chunks"
INDEX_DELTA = f"{CATALOG}.{SCHEMA}.chunks_index"
INDEX_DIRECT = f"{CATALOG}.{SCHEMA}.chunks_direct_index"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, DirectAccessVectorIndexSpec, EmbeddingSourceColumn,
    EmbeddingVectorColumn, PipelineType, VectorIndexType, EndpointType)
w = WorkspaceClient()

TIEMPOS = []   # (etapa, segundos, nota) — se muestran al final
def registrar(etapa, segundos, nota=""):
    TIEMPOS.append((etapa, round(segundos, 1), nota))
    print(f"⏱️  {etapa}: {segundos:.1f} s {('— ' + nota) if nota else ''}")

print(f"Esquema: {CATALOG}.{SCHEMA}\nEndpoint: {VS_ENDPOINT}\nModelo: {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Corpus sintético con `chunk_id` determinístico
# MAGIC
# MAGIC 300 chunks de una política PLD ficticia (~1.100 caracteres c/u). La clave del diseño:
# MAGIC
# MAGIC ```sql
# MAGIC chunk_id = sha2(concat_ws('|', source_doc, articulo), 256)
# MAGIC ```
# MAGIC
# MAGIC El **mismo artículo del mismo documento siempre produce el mismo ID**. Así, cuando se recarga un documento,
# MAGIC el índice puede **actualizar** solo los chunks que cambiaron. Con `uuid()`, cada recarga genera IDs nuevos y
# MAGIC el índice tiene que **borrar y re-embeber todo**.
# MAGIC
# MAGIC La tabla tiene **Change Data Feed** habilitado: así el índice sabe qué filas cambiaron.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")

t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {Q('chunks')} TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
SELECT
  sha2(concat_ws('|', source_doc, articulo), 256) AS chunk_id, source_doc, articulo, chunk_text
FROM (
  SELECT
    CASE WHEN id <= 150 THEN 'politica_pld' WHEN id <= 250 THEN 'manual_tipologias' ELSE 'instructivo_alertas' END AS source_doc,
    concat('Artículo ', cast(id AS STRING)) AS articulo,
    concat('Artículo ', cast(id AS STRING), '. ',
      repeat(concat('La institución deberá aplicar debida diligencia reforzada a clientes de alto riesgo, incluyendo ',
        'personas expuestas políticamente, y reportar a la UAF toda operación inusual mediante un ROS dentro del plazo ',
        'establecido. Las transacciones en efectivo superiores a USD 10,000 se registran en el RTE. Tipología ',
        cast(id % 17 AS STRING), ': fraccionamiento de depósitos y uso de testaferros. '), 3)) AS chunk_text
  FROM range(1, 301)
)""")
registrar("Crear tabla de chunks (300 filas)", time.time() - t0)
display(spark.sql(f"SELECT source_doc, count(*) AS chunks, round(avg(length(chunk_text))) AS caracteres_prom FROM {Q('chunks')} GROUP BY ALL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — ¿Los embeddings son el cuello de botella? (spoiler: no)
# MAGIC
# MAGIC Calculamos los embeddings de los 300 chunks con `ai_query` sobre el mismo modelo que usará el índice.
# MAGIC Esto tarda segundos, así que **no** explica los ~10 minutos que vimos en la Sesión 1.
# MAGIC (Guardamos el resultado: lo reutilizamos en el Paso 6 para el índice de acceso directo.)

# COMMAND ----------

t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {Q('chunks_emb')} AS
SELECT *, ai_query('{EMBEDDING_MODEL}', chunk_text) AS embedding FROM {Q('chunks')}""")
n_emb = spark.table(f"`{CATALOG}`.`{SCHEMA}`.`chunks_emb`").count()
registrar(f"Embeddings de {n_emb} chunks con ai_query", time.time() - t0, EMBEDDING_MODEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — El endpoint: `ONLINE` no significa "caliente"
# MAGIC
# MAGIC Un endpoint recién creado reporta `ONLINE` en segundos, pero **su cómputo se aprovisiona cuando llega su primer
# MAGIC índice**. Por eso el primer índice de un endpoint nuevo tarda ~18–20 min y los siguientes menos de un minuto.
# MAGIC
# MAGIC 👉 **Regla práctica:** si el endpoint ya tiene índices, está caliente.

# COMMAND ----------

existentes = {e.name: e for e in w.vector_search_endpoints.list_endpoints()}
if VS_ENDPOINT not in existentes:
    t0 = time.time()
    w.vector_search_endpoints.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type=EndpointType.STANDARD)
    registrar("Crear endpoint (hasta ONLINE)", time.time() - t0, "nuevo")
    ENDPOINT_CALIENTE = False
else:
    # Caliente = aloja al menos un índice YA LISTO (distinto de los de esta demo). Un índice que aún se está
    # aprovisionando no cuenta: el endpoint sigue frío hasta que el primero termina.
    def _listo(nombre):
        try: return w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{nombre}").get("status", {}).get("ready", False)
        except Exception: return False
    otros = [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=VS_ENDPOINT)
             if i.name not in (INDEX_DELTA, INDEX_DIRECT)]
    ENDPOINT_CALIENTE = any(_listo(n) for n in otros)

ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT)
print(f"Endpoint {VS_ENDPOINT}: estado={ep.endpoint_status.state.value}, índices={ep.num_indexes or 0}")
print("🔥 Endpoint caliente: el índice debería estar listo en ~1 min." if ENDPOINT_CALIENTE
      else "🧊 Endpoint frío: el primer índice tardará ~20 min (aprovisionamiento único). Observa el estado PROVISIONING_ENDPOINT.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Crear el Delta Sync Index y ver en qué se va el tiempo
# MAGIC
# MAGIC Imprimimos cada **cambio de estado** del índice con el segundo en que ocurre:
# MAGIC
# MAGIC | Estado | Qué significa |
# MAGIC |---|---|
# MAGIC | `PROVISIONING_ENDPOINT` | El endpoint está obteniendo su cómputo (solo con endpoint frío; es lo lento) |
# MAGIC | `PROVISIONING_INDEX` / `PROVISIONING_PIPELINE_RESOURCES` | Se crea el índice y el pipeline de sincronización |
# MAGIC | `PROVISIONING_INITIAL_SNAPSHOT` | Primera carga: lee la tabla, calcula embeddings, indexa |
# MAGIC | `ONLINE_*` | Listo para consultar |

# COMMAND ----------

def borrar_indice_si_existe(nombre, limite_min=10):
    """Re-ejecución: borra el índice y espera a que desaparezca (el borrado es asíncrono)."""
    try: w.vector_search_indexes.delete_index(nombre)
    except Exception: return                      # no existía
    t0 = time.time()
    while time.time() - t0 < limite_min * 60:
        try: w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{nombre}")
        except Exception: print(f"  índice previo borrado en {time.time() - t0:.0f} s"); return
        time.sleep(5)
    raise TimeoutError(f"{nombre} sigue pendiente de borrado tras {limite_min} min")

def crear_indice(limite_min=10, **spec):
    """create_index con reintento: si el nombre sigue 'pending deletion' (borrado asíncrono), espera y reintenta."""
    t0 = time.time()
    while True:
        try: return w.vector_search_indexes.create_index(**spec)
        except Exception as e:
            if "pending deletion" not in str(e) or time.time() - t0 > limite_min * 60:
                if "pending deletion" in str(e):
                    raise RuntimeError(f"{spec['name']} sigue 'pending deletion' tras {limite_min} min. "
                                       "Cambia el widget 'schema' para usar nombres nuevos y re-ejecuta.") from e
                raise
            print("  el nombre aún está pendiente de borrado; reintentando en 15 s..."); time.sleep(15)

def esperar_indice(nombre, filas_esperadas, limite_min=40):
    """Espera hasta que el índice esté listo con N filas; imprime cada cambio de estado."""
    t0, ultimo = time.time(), None
    while time.time() - t0 < limite_min * 60:
        try:   # vía REST: el estado detallado (detailed_state) no está en el objeto tipado de todas las versiones del SDK
            st = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{nombre}").get("status", {})
        except Exception:                            # justo después de crear puede no ser visible aún
            time.sleep(5); continue
        estado = st.get("detailed_state")
        if estado != ultimo:
            print(f"  [{time.time() - t0:6.0f} s] {estado}"); ultimo = estado
        if st.get("ready") and (st.get("indexed_row_count") or 0) >= filas_esperadas:
            return time.time() - t0
        time.sleep(5)
    raise TimeoutError(f"{nombre} no quedó listo en {limite_min} min (último estado: {ultimo})")

borrar_indice_si_existe(INDEX_DELTA)   # re-ejecución: empezamos limpio

crear_indice(
    name=INDEX_DELTA, endpoint_name=VS_ENDPOINT, primary_key="chunk_id",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table=SOURCE_TABLE, pipeline_type=PipelineType.TRIGGERED,
        embedding_source_columns=[EmbeddingSourceColumn(name="chunk_text", embedding_model_endpoint_name=EMBEDDING_MODEL)]))
seg = esperar_indice(INDEX_DELTA, 300)
registrar("Crear Delta Sync Index (300 filas)", seg, "endpoint caliente" if ENDPOINT_CALIENTE else "endpoint frío")

# COMMAND ----------

def buscar(indice, texto, k=3, columnas=("chunk_id", "source_doc", "articulo", "chunk_text"), **kw):
    r = w.vector_search_indexes.query_index(index_name=indice, columns=list(columnas),
                                            query_text=texto, query_type="HYBRID", num_results=k, **kw)
    return r.result.data_array or []

for fila in buscar(INDEX_DELTA, "reporte de operación inusual a la UAF"):
    print(f"{fila[-1]:.3f} | {fila[1]} · {fila[2]} | {fila[3][:90]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Modificar un documento: actualización incremental
# MAGIC
# MAGIC Simulamos que llega una **nueva versión del `instructivo_alertas`**:
# MAGIC - el **Artículo 260** cambia de texto (actualización),
# MAGIC - se agrega el **Artículo 301** (nuevo),
# MAGIC - se elimina el **Artículo 300** (ya no existe en la nueva versión).
# MAGIC
# MAGIC Escribimos con **`MERGE`** acotado a ese documento. Gracias al `chunk_id` determinístico, el índice solo
# MAGIC reprocesa **3 filas** de 300.
# MAGIC
# MAGIC > ⚠️ No usar `CREATE OR REPLACE` / `mode("overwrite")` sobre la tabla fuente de un índice: reemplazar la tabla
# MAGIC > obliga a reconstruir y puede romper el sync. Usa `MERGE` / `INSERT` / `UPDATE` / `DELETE`.

# COMMAND ----------

MARCA = f"VERSION-{int(time.time())}"
nueva_version = f"""
SELECT sha2(concat_ws('|', source_doc, articulo), 256) AS chunk_id, source_doc, articulo, chunk_text FROM VALUES
  ('instructivo_alertas', 'Artículo 260', 'Artículo 260 ({MARCA}). Toda alerta de monitoreo transaccional debe resolverse en un máximo de 30 días calendario, documentando la justificación del cierre.'),
  ('instructivo_alertas', 'Artículo 301', 'Artículo 301 ({MARCA}). Nuevo: las alertas por coincidencia en listas OFAC se escalan al Oficial de Cumplimiento en menos de 24 horas.')
  AS v(source_doc, articulo, chunk_text)
UNION ALL
SELECT chunk_id, source_doc, articulo, chunk_text FROM {Q('chunks')}
WHERE source_doc = 'instructivo_alertas' AND articulo NOT IN ('Artículo 260', 'Artículo 300')
"""
t0 = time.time()
spark.sql(f"""
MERGE INTO {Q('chunks')} t
USING ({nueva_version}) s ON t.chunk_id = s.chunk_id
WHEN MATCHED AND t.chunk_text <> s.chunk_text THEN UPDATE SET t.chunk_text = s.chunk_text
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.source_doc = 'instructivo_alertas' THEN DELETE
""")
registrar("MERGE de la nueva versión del documento", time.time() - t0)
# Change Data Feed de la última versión: exactamente lo que el índice va a procesar
ultima = spark.sql(f"DESCRIBE HISTORY {Q('chunks')} LIMIT 1").collect()[0]["version"]
display(spark.sql(f"""SELECT _change_type, articulo FROM table_changes('{Q('chunks')}', {ultima})
                      WHERE _change_type <> 'update_preimage' ORDER BY articulo"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora disparamos el **sync** y medimos hasta que los cambios son **consultables** (no solo "sync terminado"):
# MAGIC el texto nuevo aparece en las búsquedas y el Artículo 300 ya no existe en el índice.

# COMMAND ----------

def cambios_visibles():
    hits = buscar(INDEX_DELTA, MARCA, k=5, columnas=("chunk_id", "articulo", "chunk_text"))
    nuevos = {h[1] for h in hits if MARCA in (h[2] or "")}
    borrado = buscar(INDEX_DELTA, "Artículo 300", k=1, columnas=("chunk_id", "articulo"),
                     filters_json=json.dumps({"articulo": "Artículo 300"}))
    return nuevos >= {"Artículo 260", "Artículo 301"} and not borrado

def sincronizar(nombre, limite_min=10):
    """sync_index con reintento: justo después de crear, el pipeline de la carga inicial puede seguir en RUNNING
    aunque el índice ya responda consultas; el sync solo se acepta cuando ese pipeline termina."""
    t0 = time.time()
    while True:
        try: w.vector_search_indexes.sync_index(nombre); return
        except Exception as e:
            if "not ready to sync" not in str(e) or time.time() - t0 > limite_min * 60: raise
            print("  el pipeline anterior sigue corriendo; reintentando el sync en 10 s..."); time.sleep(10)

t0 = time.time()
sincronizar(INDEX_DELTA)
while not cambios_visibles():
    if time.time() - t0 > 1800: raise TimeoutError("Los cambios no se hicieron visibles en 30 min")
    time.sleep(3)
registrar("Sync incremental hasta cambios consultables (1 upd, 1 ins, 1 del)", time.time() - t0)
print("filas en el índice:", w.vector_search_indexes.get_index(INDEX_DELTA).status.indexed_row_count, "(esperado: 300)")
for fila in buscar(INDEX_DELTA, "alertas OFAC Oficial de Cumplimiento", k=2):
    print(f"{fila[-1]:.3f} | {fila[2]} | {fila[3][:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Contraste: índice de acceso directo (frescura en segundos)
# MAGIC
# MAGIC Un **Direct Vector Access Index** no sincroniza desde una tabla: la aplicación le escribe directamente
# MAGIC (`upsert`) con los embeddings ya calculados. No hay pipeline de por medio, así que el cambio es visible en segundos.
# MAGIC
# MAGIC **Trade-off:** pierdes el sync automático y el linaje desde la tabla Delta, y tú calculas los embeddings.
# MAGIC Útil para "documentos recién subidos" o narrativas de alertas que deben estar disponibles al instante.

# COMMAND ----------

borrar_indice_si_existe(INDEX_DIRECT)

t0 = time.time()
crear_indice(
    name=INDEX_DIRECT, endpoint_name=VS_ENDPOINT, primary_key="chunk_id",
    index_type=VectorIndexType.DIRECT_ACCESS,
    direct_access_index_spec=DirectAccessVectorIndexSpec(
        embedding_vector_columns=[EmbeddingVectorColumn(name="embedding", embedding_dimension=1024)],
        schema_json=json.dumps({"chunk_id": "string", "source_doc": "string", "articulo": "string",
                                "chunk_text": "string", "embedding": "array<float>"})))
seg = esperar_indice(INDEX_DIRECT, 0)
registrar("Crear Direct Access Index (vacío)", seg)

def upsert(filas):
    for b in range(0, len(filas), 100):
        w.vector_search_indexes.upsert_data_vector_index(index_name=INDEX_DIRECT, inputs_json=json.dumps(filas[b:b+100]))

filas = [r.asDict() for r in spark.table(f"`{CATALOG}`.`{SCHEMA}`.`chunks_emb`").collect()]
for f in filas: f["embedding"] = [float(x) for x in f["embedding"]]
t0 = time.time(); upsert(filas)
registrar(f"Upsert de {len(filas)} filas al Direct Access Index", time.time() - t0)

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora el mismo cambio del Paso 5, pero vía `upsert`: calculamos el embedding del texto nuevo y lo escribimos.

# COMMAND ----------

nuevo = spark.sql(f"""
SELECT chunk_id, source_doc, articulo, chunk_text, ai_query('{EMBEDDING_MODEL}', chunk_text) AS embedding
FROM {Q('chunks')} WHERE chunk_text LIKE '%{MARCA}%'""").collect()
nuevo = [dict(r.asDict(), embedding=[float(x) for x in r["embedding"]]) for r in nuevo]

t0 = time.time()
upsert(nuevo)
while True:
    r = w.vector_search_indexes.query_index(index_name=INDEX_DIRECT, columns=["articulo", "chunk_text"],
                                            query_vector=nuevo[0]["embedding"], num_results=3)
    if any(MARCA in (x[1] or "") for x in (r.result.data_array or [])): break
    time.sleep(0.5)
registrar(f"Direct Access: upsert de {len(nuevo)} filas hasta consultable", time.time() - t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Resumen de tiempos de esta ejecución

# COMMAND ----------

display(spark.createDataFrame(TIEMPOS, "etapa STRING, segundos DOUBLE, nota STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusiones
# MAGIC | Necesidad | Recomendación |
# MAGIC |---|---|
# MAGIC | Que crear un índice no tarde ~20 min | **Mantener el endpoint caliente** (al menos un índice vivo). El aprovisionamiento ocurre una sola vez. |
# MAGIC | Actualizar documentos sin reconstruir | `chunk_id` determinístico + `MERGE` + sync **triggered** (~45–50 s a esta escala). |
# MAGIC | Frescura en segundos | **Direct Vector Access** (~1,5 s), o una ruta paralela para documentos recién subidos. |
# MAGIC | Corpus de millones de filas | Evaluar **Storage-Optimized** y Provisioned Throughput para embeddings. ⚠️ Con corpus pequeños SO es **más lento**: cada sync reconstruye parcialmente (~14 min medidos con 300 filas). |
# MAGIC
# MAGIC Recuerda: un endpoint de AI Search está **siempre encendido** y factura aunque no se consulte (un endpoint vacío no factura).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Limpieza (opcional)
# MAGIC Con `cleanup = si` se borran los dos índices y el esquema de la demo. **El endpoint no se borra**
# MAGIC (borrarlo lo vuelve a dejar frío). Bórralo manualmente si ya no lo necesitas.

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "si":
    for idx in (INDEX_DELTA, INDEX_DIRECT):
        try: w.vector_search_indexes.delete_index(idx); print("🗑️ índice borrado:", idx)
        except Exception as e: print("  (no se pudo borrar)", idx, str(e)[:120])
    spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{SCHEMA}` CASCADE"); print("🗑️ esquema borrado:", SCHEMA)
else:
    print("Limpieza omitida. Cambia el widget 'cleanup' a 'si' y re-ejecuta esta celda para borrar.")
