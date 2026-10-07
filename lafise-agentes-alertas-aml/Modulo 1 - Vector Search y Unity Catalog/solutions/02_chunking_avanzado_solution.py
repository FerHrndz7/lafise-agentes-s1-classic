# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Chunking avanzado para documentos legales / AML — SOLUCIÓN

# COMMAND ----------

# MAGIC %pip install --quiet langchain-text-splitters
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Trabajando en {CATALOG}.{SCHEMA}")

# COMMAND ----------

from langchain_text_splitters import RecursiveCharacterTextSplitter

texto_politica = spark.sql(f"""
  SELECT texto FROM {CATALOG}.{SCHEMA}.docs_parsed WHERE doc_nombre = 'politica_pld_lafise.pdf'
""").collect()[0]["texto"]

naive = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=0, separators=[" "])
chunks_naive = naive.split_text(texto_politica)
print(f"Chunks ingenuos: {len(chunks_naive)}")
for i, c in enumerate(chunks_naive[:3]):
    print(f"--- chunk {i} ---\n{c}\n")

# COMMAND ----------

# SOLUCIÓN: el regex detecta 'Artículo N.', 'Tipología N.' y encabezados numerados 'N.'
HEADING_RE = re.compile(r"^(Artículo\s+\d+\.|Tipología\s+\d+\.|\d+\.)\s", re.MULTILINE)

def split_estructurado(texto, doc_nombre, chunk_size=1200, overlap=150):
    matches = list(HEADING_RE.finditer(texto))
    sub = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=overlap)
    out = []
    for idx, m in enumerate(matches):
        ini = m.start()
        fin = matches[idx + 1].start() if idx + 1 < len(matches) else len(texto)
        bloque = texto[ini:fin].strip()
        encabezado = bloque.split("\n", 1)[0].strip()
        articulo = re.match(r"^(Artículo\s+\d+|Tipología\s+\d+|\d+)", encabezado)
        articulo = articulo.group(0) if articulo else encabezado[:40]
        for pieza in sub.split_text(bloque):
            out.append({
                "source_doc": doc_nombre, "seccion": encabezado, "articulo": articulo,
                "chunk_to_retrieve": pieza,
                "chunk_to_embed": f"[{doc_nombre} · {encabezado}] {pieza}",
            })
    return out

demo = split_estructurado(texto_politica, "politica_pld_lafise.pdf")
print(f"Chunks estructurados: {len(demo)}")
for c in demo[:2]:
    print(f"articulo={c['articulo']!r}  embed→ {c['chunk_to_embed'][:120]}...")

# COMMAND ----------

docs = spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed").select("doc_nombre", "texto").collect()
filas = []
for r in docs:
    filas += split_estructurado(r["texto"], r["doc_nombre"])

for c in spark.table(f"{CATALOG}.{SCHEMA}.narrativas_casos").collect():
    txt = f"Caso {c['caso_id']} — Tipología {c['tipologia']} — Disposición: {c['disposicion']}. {c['narrativa']}"
    filas.append({
        "source_doc": "narrativas_casos", "seccion": f"Caso {c['caso_id']}",
        "articulo": c["tipologia"], "chunk_to_retrieve": txt,
        "chunk_to_embed": f"[Precedente · {c['tipologia']}] {txt}",
    })

from pyspark.sql import functions as F
df_chunks = spark.createDataFrame(filas).withColumn("chunk_id", F.expr("uuid()"))
(df_chunks.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.chunks"))
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"✅ chunks: {spark.table(f'{CATALOG}.{SCHEMA}.chunks').count()}")
display(spark.sql(f"SELECT source_doc, count(*) n FROM {CATALOG}.{SCHEMA}.chunks GROUP BY source_doc ORDER BY source_doc"))

# COMMAND ----------

# ai_prep_search: inspección de la salida real (vía nativa/escalable para producción).
# Requiere docs_parsed_raw (ai_parse_document) + DBR 18.2+. Si no está, se salta sin afectar al lab.
import json
try:
    prep = spark.sql(f"""
      SELECT ai_prep_search(parsed) AS prep FROM {CATALOG}.{SCHEMA}.docs_parsed_raw
      WHERE doc_nombre = 'politica_pld_lafise.pdf'
    """)
    raw = prep.selectExpr("to_json(prep) AS j").collect()[0]["j"]
    print(json.dumps(json.loads(raw), ensure_ascii=False, indent=2)[:1800], "...")
except Exception as e:
    print("ℹ️ ai_prep_search no disponible aquí:", str(e)[:160])
    print("   Seguimos con la tabla `chunks` del Enfoque 2 (LangChain).")

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.chunks")
assert df.count() > 10
assert {"chunk_id","chunk_to_retrieve","chunk_to_embed","articulo","source_doc"} <= set(df.columns)
assert df.filter("source_doc='politica_pld_lafise.pdf' AND articulo='Artículo 5'").count() >= 1
print("✅ Chunks estructurados con metadata de citación.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enfoques alternativos
# MAGIC - **`ai_prep_search`** es la vía preferida en producción (una llamada SQL, distribuida, gobernada). Aquí
# MAGIC   usamos el splitter manual para *enseñar* el criterio de citación; una vez confirmado el esquema de
# MAGIC   salida de `ai_prep_search` en tu workspace, puedes construir la tabla `chunks` directamente desde él.
# MAGIC - **Retrieval jerárquico (parent-child):** indexar chunks hijos pero devolver el artículo padre completo
# MAGIC   al LLM — útil cuando la cita requiere el artículo entero.
