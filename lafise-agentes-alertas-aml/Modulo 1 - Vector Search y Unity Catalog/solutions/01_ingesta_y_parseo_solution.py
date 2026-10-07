# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta y parseo con `ai_parse_document` — SOLUCIÓN

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

display(spark.sql(f"LIST '{VOLUME_PATH}'"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW docs_binarios AS
SELECT path, content
FROM read_files('{VOLUME_PATH}', format => 'binaryFile')
WHERE lower(path) LIKE '%.pdf'
""")
display(spark.sql("SELECT path, length(content) AS bytes FROM docs_binarios"))

# COMMAND ----------

# SOLUCIÓN: Paso 3 — pasamos la columna binaria `content` a ai_parse_document.
# Guardamos el VARIANT crudo para poder inspeccionarlo antes de extraer texto.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.docs_parsed_raw AS
SELECT
  path,
  regexp_extract(path, '([^/]+)$', 1) AS doc_nombre,
  ai_parse_document(content, map('version', '2.0')) AS parsed
FROM docs_binarios
""")
display(spark.sql(f"SELECT path, doc_nombre FROM {CATALOG}.{SCHEMA}.docs_parsed_raw"))

# COMMAND ----------

# Expected output: error_status None y un JSON con document.elements[] / document.pages[]
row = spark.sql(f"""
  SELECT to_json(parsed:document) AS doc_json, to_json(parsed:error_status) AS err
  FROM {CATALOG}.{SCHEMA}.docs_parsed_raw WHERE doc_nombre = 'politica_pld_lafise.pdf'
""").collect()[0]
import json
print("error_status:", row["err"])
print(json.dumps(json.loads(row["doc_json"]), ensure_ascii=False, indent=2)[:1500], "...")

# COMMAND ----------

# SOLUCIÓN: Paso 5 — extraemos e:content de cada elemento de document.elements.
# Nota: si tu versión de ai_parse_document expone el texto en otro campo (p. ej. e:text o e:markdown),
# ajusta la ruta aquí — por eso inspeccionamos el VARIANT en la celda anterior.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.docs_parsed AS
SELECT
  path,
  doc_nombre,
  concat_ws('\\n\\n',
    transform(cast(parsed:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)
  ) AS texto,
  parsed
FROM {CATALOG}.{SCHEMA}.docs_parsed_raw
WHERE size(cast(parsed:document:elements AS ARRAY<VARIANT>)) > 0
""")
display(spark.sql(f"SELECT doc_nombre, length(texto) AS caracteres FROM {CATALOG}.{SCHEMA}.docs_parsed"))

# COMMAND ----------

# SOLUCIÓN: Plan B — respaldo con pypdf si ai_parse_document no dejó docs_parsed (DBR < 17.3).
def _docs_parsed_ok():
    try:
        return spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed").count() >= 3
    except Exception:
        return False

if not _docs_parsed_ok():
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
    (spark.createDataFrame(filas).write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.docs_parsed"))
    print("✅ docs_parsed construido con respaldo pypdf.")

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed")
assert df.count() >= 3
muestra = df.filter("doc_nombre = 'politica_pld_lafise.pdf'").collect()[0]["texto"]
assert "10,000" in muestra or "10.000" in muestra
print("✅ Parseo correcto.\n", muestra[:400], "...")
