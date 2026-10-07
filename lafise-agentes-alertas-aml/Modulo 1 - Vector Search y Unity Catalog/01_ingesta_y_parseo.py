# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta y parseo de documentos con `ai_parse_document`
# MAGIC
# MAGIC **Objetivo:** convertir los PDFs de políticas PLD y tipologías (no estructurados) en **texto estructurado
# MAGIC en una tabla Delta**, listos para chunkear. ⏱️ ~20 min.
# MAGIC
# MAGIC ### Concepto
# MAGIC Un PDF es binario: el LLM no puede "leerlo" directamente. El primer paso de todo pipeline RAG es
# MAGIC **parsear** el documento a texto. En Databricks esto se hace con la AI Function **`ai_parse_document`**
# MAGIC (GA): recibe el contenido binario y devuelve un `VARIANT` con la estructura del documento
# MAGIC (`document.elements`, `document.pages`), respetando encabezados, párrafos y tablas.
# MAGIC
# MAGIC Lo bueno: es una función SQL nativa, gobernada y escalable — no montas librerías de parseo ni gestionas
# MAGIC endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — Recuperar configuración
# MAGIC Reconstruimos catalog/schema/volumen igual que en `00_setup` (mismo criterio de aislamiento).

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("volume", "documentos", "Volume")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
VOLUME = dbutils.widgets.get("volume")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Volumen fuente: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Ver los documentos del Volumen
# MAGIC Los Volúmenes de Unity Catalog gobiernan archivos no tabulares (PDF, DOCX, imágenes) con los mismos
# MAGIC permisos y linaje que las tablas.

# COMMAND ----------

display(spark.sql(f"LIST '{VOLUME_PATH}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Leer los binarios con `read_files`
# MAGIC `read_files(..., format => 'binaryFile')` carga cada archivo como una fila con su `path` y su `content`
# MAGIC binario — la entrada que espera `ai_parse_document`. Filtramos solo PDFs en este paso.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW docs_binarios AS
SELECT path, content
FROM read_files('{VOLUME_PATH}', format => 'binaryFile')
WHERE lower(path) LIKE '%.pdf'
""")
display(spark.sql("SELECT path, length(content) AS bytes FROM docs_binarios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Parsear con `ai_parse_document`
# MAGIC
# MAGIC **TODO:** completa la llamada a `ai_parse_document` sobre la columna `content`. Pasa la opción de versión
# MAGIC `map('version', '2.0')`. Guardamos el `VARIANT` crudo para inspeccionarlo antes de extraer texto.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.docs_parsed_raw AS
SELECT
  path,
  regexp_extract(path, '([^/]+)$', 1) AS doc_nombre,
  ai_parse_document(<FILL_IN>, map('version', '2.0')) AS parsed   -- TODO: pasa la columna binaria
FROM docs_binarios
""")
display(spark.sql(f"SELECT path, doc_nombre FROM {CATALOG}.{SCHEMA}.docs_parsed_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Inspeccionar la estructura devuelta
# MAGIC Antes de extraer texto, mira **cómo** viene el `VARIANT`. `ai_parse_document` devuelve
# MAGIC `document.elements` (bloques de contenido) y `document.pages`, más `error_status` (NULL si todo bien).

# COMMAND ----------

# Muestra el JSON de un documento para entender su forma real.
row = spark.sql(f"""
  SELECT to_json(parsed:document) AS doc_json, to_json(parsed:error_status) AS err
  FROM {CATALOG}.{SCHEMA}.docs_parsed_raw
  WHERE doc_nombre = 'politica_pld_lafise.pdf'
""").collect()[0]
import json
print("error_status:", row["err"])
print(json.dumps(json.loads(row["doc_json"]), ensure_ascii=False, indent=2)[:1500], "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Extraer el texto plano por documento
# MAGIC Concatenamos el `content` de cada elemento de `document.elements` para obtener el texto completo del
# MAGIC documento. (En el Módulo 02 dejaremos que `ai_prep_search` haga el chunking sobre esta base.)
# MAGIC
# MAGIC **TODO:** completa la ruta del `transform` para leer `e:content` de cada elemento.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.docs_parsed AS
SELECT
  path,
  doc_nombre,
  concat_ws('\\n\\n',
    transform(
      cast(parsed:document:elements AS ARRAY<VARIANT>),
      e -> <FILL_IN>::STRING            -- TODO: extrae el texto del elemento (p. ej. e:content)
    )
  ) AS texto,
  parsed
FROM {CATALOG}.{SCHEMA}.docs_parsed_raw
WHERE size(cast(parsed:document:elements AS ARRAY<VARIANT>)) > 0   -- quedarnos con los que sí parsearon
""")
display(spark.sql(f"SELECT doc_nombre, length(texto) AS caracteres FROM {CATALOG}.{SCHEMA}.docs_parsed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed")
try:
    assert df.count() >= 3, "Se esperaban al menos 3 PDFs parseados"
    muestra = df.filter("doc_nombre = 'politica_pld_lafise.pdf'").collect()[0]["texto"]
    assert "10,000" in muestra or "10.000" in muestra, "El texto de la política debería mencionar el umbral USD 10,000"
    print("✅ Parseo correcto. Ejemplo de texto extraído:\n")
    print(muestra[:400], "...")
except AssertionError as e:
    print(f"❌ {e}\nRevisa los TODO de los Pasos 3 y 5.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan B — Parseo de respaldo con `pypdf`
# MAGIC `ai_parse_document` requiere **DBR 17.3+**. Si tu clúster clásico usa un runtime anterior (o la función no
# MAGIC está habilitada), esta celda construye `docs_parsed` con `pypdf` para que el resto del laboratorio funcione
# MAGIC igual. **Solo actúa si `docs_parsed` no quedó listo** por la vía nativa; si todo salió bien, no hace nada.

# COMMAND ----------

def _docs_parsed_ok():
    try:
        return spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed").count() >= 3
    except Exception:
        return False

if _docs_parsed_ok():
    print("✅ docs_parsed ya está listo (vía ai_parse_document). No se necesita respaldo.")
else:
    print("⚠️ docs_parsed no está listo → usando el parseo de respaldo con pypdf...")
    import io
    from pypdf import PdfReader
    from pyspark.sql import Row

    filas = []
    for fi in dbutils.fs.ls(VOLUME_PATH):
        if fi.name.lower().endswith(".pdf"):
            with open(f"{VOLUME_PATH}/{fi.name}", "rb") as fh:
                reader = PdfReader(io.BytesIO(fh.read()))
            texto = "\n\n".join((pg.extract_text() or "") for pg in reader.pages)
            filas.append(Row(path=f"{VOLUME_PATH}/{fi.name}", doc_nombre=fi.name, texto=texto))
    (spark.createDataFrame(filas)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.docs_parsed"))
    n = spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed").count()
    print(f"✅ docs_parsed (respaldo pypdf): {n} documentos. El Módulo 02 continúa con el chunker de LangChain.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Cargaste PDFs desde un **Volumen de Unity Catalog** con `read_files(... binaryFile)`.
# MAGIC - Los parseaste con **`ai_parse_document`** (función SQL nativa, GA) a una tabla Delta — con un **respaldo
# MAGIC   `pypdf`** por si tu runtime clásico no la incluye.
# MAGIC - Inspeccionaste el `VARIANT` (`document.elements`) y extrajiste el texto plano.
# MAGIC
# MAGIC ➡️ Sigue en **`02_chunking_avanzado`**: partir estos documentos en *chunks* que preserven la estructura
# MAGIC legal (artículo/sección) para poder **citar** la fuente — clave en cumplimiento AML.
