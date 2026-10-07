# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 1 · Preparación — Vector Search + Unity Catalog (base de conocimiento PLD/AML)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Duración** | ~120 min (esta preparación: ~10 min) |
# MAGIC | **Nivel** | Principiante–Intermedio |
# MAGIC | **Lenguaje** | Python / SQL |
# MAGIC | **Cómputo** | Clúster **clásico** (DBR 17.3+ recomendado) |
# MAGIC | **Catalog.Schema.Volumen** | `<catálogo_asignado>.pld_aml_<tu_usuario>.documentos` |
# MAGIC
# MAGIC ## Qué construirás hoy 🕵️
# MAGIC La **base de conocimiento** del *Agente de Alertas AML*: que las **políticas PLD, tipologías de lavado y
# MAGIC narrativas de casos** se vuelvan **consultables semánticamente** y **gobernadas por Unity Catalog**.
# MAGIC
# MAGIC Recorrido de la sesión:
# MAGIC 1. `00_setup` — catalog/schema/volumen personal + corpus PLD sintético (**estás aquí**).
# MAGIC 2. `01_ingesta_y_parseo` — cargar PDFs a un Volumen UC y parsearlos con `ai_parse_document`.
# MAGIC 3. `02_chunking_avanzado` — chunking *estructura-consciente* para documentos legales/AML (`ai_prep_search`).
# MAGIC 4. `03_vector_search_y_gobierno` — endpoint + Delta Sync Index + gobierno en UC.
# MAGIC 5. `04_hybrid_search` — búsqueda **híbrida** (keyword + semántica) y filtros por metadata.
# MAGIC 6. `05_reto` — arma un mini-retriever para un escenario de alerta.
# MAGIC
# MAGIC ## El escenario de negocio
# MAGIC Un LLM por sí solo **no conoce** las políticas PLD del banco ni el historial de casos. Para que el futuro
# MAGIC *Agente de Alertas* razone sobre una alerta necesita **recuperar** el fundamento correcto (el artículo de
# MAGIC la política, la tipología, el precedente). Eso es lo que construimos hoy con **Vector Search**.
# MAGIC
# MAGIC > ⚠️ **Datos sintéticos.** Todo el corpus es inventado (Faker + hechos regulatorios plausibles pero
# MAGIC > ficticios). Nunca uses PII real de clientes en un workspace de laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 1 — Dependencias
# MAGIC Instalamos las librerías del laboratorio: SDK de Databricks, generadores de documentos (`reportlab`,
# MAGIC `python-docx`), `faker` para datos tabulares, `langchain-text-splitters` (chunking del Módulo 02) y
# MAGIC `pypdf` (parseo de respaldo del Módulo 01 si `ai_parse_document` no está disponible en tu runtime).
# MAGIC Ejecuta esta celda **primero**; reinicia el intérprete de Python. En un clúster clásico `%pip` instala en
# MAGIC el clúster para tu sesión; vuelve a ejecutarla si reinicias el clúster.

# COMMAND ----------

# MAGIC %pip install --retries 5 --timeout 300 --quiet "databricks-sdk>=0.65.0" reportlab python-docx "faker>=25.0.0" langchain-text-splitters pypdf
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2 — Catalog, Schema y Volumen (aislamiento por participante)
# MAGIC
# MAGIC En el widget **catalog** (arriba) escribe el **catálogo que te asignó el facilitador**. El laboratorio
# MAGIC admite los dos modelos de aislamiento:
# MAGIC - **Un catálogo por asistente** — escribe el nombre de **tu** catálogo.
# MAGIC - **Un catálogo compartido con un esquema por asistente** — escribe el catálogo **compartido**.
# MAGIC
# MAGIC En ambos casos tu schema lleva automáticamente tu usuario (`pld_aml_<tu_usuario>`), así que **nunca
# MAGIC colisionas** con otros participantes aunque compartan catálogo. El índice de Vector Search también queda
# MAGIC aislado (`<catálogo>.pld_aml_<tu_usuario>.chunks_index`).
# MAGIC
# MAGIC > 🔧 Usa un catálogo **sin puntos** en el nombre (los puntos rompen `CREATE SCHEMA`). Tu usuario se
# MAGIC > saneó para el schema, así que un correo con punto (`nombre.apellido`) no es problema.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("volume", "documentos", "Volume")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())

CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, ("Escribe en el widget 'catalog' el catálogo que te asignó el facilitador "
                 "(tu catálogo personal, o el catálogo compartido del curso).")
# El schema lleva tu usuario para aislarte aun si compartes catálogo con otros asistentes.
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
VOLUME = dbutils.widgets.get("volume")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Convenciones que reutilizarán los notebooks 01–05:
VS_ENDPOINT = "lafise_vs_endpoint"                       # endpoint compartido del workspace
INDEX_NAME = f"{CATALOG}.{SCHEMA}.chunks_index"          # índice por participante

print(f"Usuario:      {user_email}")
print(f"user_prefix:  {user_prefix}")
print(f"Catalog:      {CATALOG}")
print(f"Destino:      {CATALOG}.{SCHEMA}")
print(f"Volumen:      {VOLUME_PATH}")
print(f"VS endpoint:  {VS_ENDPOINT}  (compartido)")
print(f"Índice:       {INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 3 — Verificación del entorno

# COMMAND ----------

import sys
from databricks.sdk import WorkspaceClient
import databricks.sdk

print(f"Python: {sys.version.split()[0]}")
assert sys.version_info >= (3, 10), "Se requiere Python 3.10+"
print(f"databricks-sdk: {databricks.sdk.version.__version__}")

# Versión del runtime del clúster clásico. ai_parse_document requiere DBR 17.3+; ai_prep_search 18.2+.
# Si tu DBR es menor, el Módulo 01 usa un parseo de respaldo (pypdf) y el 02 el chunker con LangChain.
try:
    dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    major_minor = tuple(int(x) for x in dbr.split(".")[:2] if x.isdigit() or x[:2].isdigit()) if dbr else ()
    print(f"DBR del clúster: {dbr}")
    try:
        mm = tuple(int(p) for p in dbr.split("-")[0].split(".")[:2])
        if mm < (17, 3):
            print("  ⚠️ DBR < 17.3: 'ai_parse_document' podría no estar disponible → se usará el parseo de respaldo (pypdf).")
        elif mm < (18, 2):
            print("  ℹ️ DBR 17.3–18.1: 'ai_parse_document' disponible; 'ai_prep_search' (18.2+) quizá no → el 02 usa LangChain.")
        else:
            print("  ✅ DBR 18.2+: 'ai_parse_document' y 'ai_prep_search' disponibles.")
    except Exception:
        pass
except Exception:
    print("DBR del clúster: no legible aquí (confirma 17.3+ en la config del clúster).")

w = WorkspaceClient()
# Confirmamos que existe al menos un modelo de embeddings servido (para el índice gestionado).
emb = [e.name for e in w.serving_endpoints.list()
       if e.name in ("databricks-qwen3-embedding-0-6b", "databricks-gte-large-en", "databricks-bge-large-en")]
print(f"Modelos de embedding disponibles: {emb or '⚠️ ninguno detectado (revisa Foundation Model APIs)'}")
print("✅ El entorno se ve bien.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 4 — Validación de permisos
# MAGIC Fallamos rápido y con un mensaje claro si no puedes crear objetos en tu catalog personal.

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{SCHEMA}.{VOLUME}")
    print(f"✅ Puedes crear schema y volumen en {CATALOG}.{SCHEMA}")
except Exception as e:
    raise PermissionError(
        f"No se pueden crear objetos en {CATALOG}.{SCHEMA}. Confirma que `{CATALOG}` es tu catalog personal "
        f"(USE CATALOG / CREATE SCHEMA / CREATE VOLUME), o escribe un catalog sin puntos en el widget "
        f"'catalog'. Error subyacente: {e}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 5 — Generar el corpus PLD sintético
# MAGIC Llamamos al generador en `./data/sample_data_gen`, que escribe los PDFs/DOCX en tu Volumen y crea las
# MAGIC tablas `clientes`, `transacciones`, `alertas` y `narrativas_casos` en tu schema.

# COMMAND ----------

# MAGIC %run ./data/sample_data_gen $catalog=$catalog $schema=$schema $volume=$volume $num_clientes=500

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 6 — Resumen de verificación

# COMMAND ----------

files = [f for f in dbutils.fs.ls(VOLUME_PATH) if not f.name.endswith("/")]
print("=" * 60)
print("PREPARACIÓN COMPLETA ✅")
print("=" * 60)
print(f"Catalog.Schema : {CATALOG}.{SCHEMA}")
print(f"Volumen        : {VOLUME_PATH}  ({len(files)} archivos)")
for f in files:
    print(f"   - {f.name}")
print("=" * 60)
print("➡️  Continúa con 01_ingesta_y_parseo")
