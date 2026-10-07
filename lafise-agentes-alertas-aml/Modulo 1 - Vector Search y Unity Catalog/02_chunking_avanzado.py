# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Chunking avanzado para documentos legales / AML
# MAGIC
# MAGIC **Objetivo:** partir los documentos en *chunks* que preserven la **estructura legal** (artículo / sección)
# MAGIC y lleven **metadata para citar la fuente** — un requisito de cumplimiento: toda disposición de una alerta
# MAGIC debe poder citar *qué artículo o tipología* la fundamenta. ⏱️ ~30 min.
# MAGIC
# MAGIC ### ¿Por qué el chunking importa (y más aún en AML)?
# MAGIC Vector Search recupera **chunks**, no documentos enteros. Si el chunk se corta mal:
# MAGIC - **Pierde el contexto legal:** un chunk que parte a la mitad del *Artículo 5* no dice a qué artículo
# MAGIC   pertenece → no se puede **citar** la fuente en el ROS.
# MAGIC - **Mezcla temas:** un chunk que junta el final de una tipología con el inicio de otra confunde al modelo.
# MAGIC - **Recupera de menos o de más:** chunks demasiado grandes diluyen la señal; demasiado pequeños la fragmentan.
# MAGIC
# MAGIC Veremos tres enfoques, de peor a mejor para cumplimiento:
# MAGIC 1. **Ingenuo** (tamaño fijo) — el problema.
# MAGIC 2. **Estructura-consciente** (por artículo/sección + metadata) — la solución de cumplimiento.
# MAGIC 3. **`ai_prep_search`** — el chunker nativo de Databricks (estructura-consciente y escalable).

# COMMAND ----------

# MAGIC %pip install --quiet langchain-text-splitters
# MAGIC dbutils.library.restartPython()

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
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Trabajando en {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enfoque 1 — Chunking ingenuo (tamaño fijo): el problema
# MAGIC Partimos el texto cada N caracteres, sin mirar la estructura. Rápido, pero rompe artículos y no sabe de
# MAGIC qué sección viene cada chunk.

# COMMAND ----------

from langchain_text_splitters import RecursiveCharacterTextSplitter

texto_politica = spark.sql(f"""
  SELECT texto FROM {CATALOG}.{SCHEMA}.docs_parsed WHERE doc_nombre = 'politica_pld_lafise.pdf'
""").collect()[0]["texto"]

naive = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=0, separators=[" "])
chunks_naive = naive.split_text(texto_politica)
print(f"Chunks ingenuos: {len(chunks_naive)}\n")
for i, c in enumerate(chunks_naive[:3]):
    print(f"--- chunk {i} ---\n{c}\n")
print("⚠️ Observa: los cortes caen a mitad de un artículo; ningún chunk 'sabe' su número de artículo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enfoque 2 — Chunking estructura-consciente (por artículo/sección) + metadata
# MAGIC
# MAGIC Partimos **en los límites de encabezado** (`Artículo N`, `Tipología N`, o `N.` numerado) y adjuntamos
# MAGIC metadata (`seccion`, `articulo`). Cada chunk queda auto-contenido y **citable**.
# MAGIC
# MAGIC Dos técnicas avanzadas que aplicamos:
# MAGIC - **`chunk_to_embed` enriquecido:** anteponemos el título del documento + el encabezado de la sección al
# MAGIC   texto que se *embebe* (mejora el retrieval), separado del `chunk_to_retrieve` que se le muestra al LLM.
# MAGIC - **Sub-división controlada:** si un artículo es muy largo, lo sub-partimos con solapamiento, pero
# MAGIC   **sin cruzar** el límite del artículo.
# MAGIC
# MAGIC **TODO:** completa el patrón regex que detecta los encabezados (`Artículo N`, `Tipología N`, `N. Título`).

# COMMAND ----------

# Encabezados típicos de documentos legales/AML.
# TODO: completa la alternativa para 'Tipología' (mira cómo está escrito 'Artículo').
HEADING_RE = re.compile(r"^(Artículo\s+\d+\.|<FILL_IN>|\d+\.)\s", re.MULTILINE)

def split_estructurado(texto, doc_nombre, chunk_size=1200, overlap=150):
    """Parte por encabezado; sub-divide artículos largos sin cruzar el límite de sección."""
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
        for j, pieza in enumerate(sub.split_text(bloque)):
            out.append({
                "source_doc": doc_nombre,
                "seccion": encabezado,
                "articulo": articulo,
                "chunk_to_retrieve": pieza,
                # Enriquecemos el texto a embeber con doc + sección (mejor recall semántico):
                "chunk_to_embed": f"[{doc_nombre} · {encabezado}] {pieza}",
            })
    return out

demo = split_estructurado(texto_politica, "politica_pld_lafise.pdf")
print(f"Chunks estructurados: {len(demo)}\n")
for c in demo[:2]:
    print(f"articulo={c['articulo']!r}  seccion={c['seccion']!r}")
    print(f"  embed→ {c['chunk_to_embed'][:120]}...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construir la tabla `chunks` (todos los documentos + narrativas de casos)
# MAGIC Aplicamos el splitter a los tres PDFs de política/tipologías/instructivo, y sumamos las **narrativas de
# MAGIC casos** (cada caso = un chunk, para recuperar precedentes). Habilitamos **Change Data Feed** — requisito
# MAGIC del Delta Sync Index del Módulo 03.

# COMMAND ----------

docs = spark.table(f"{CATALOG}.{SCHEMA}.docs_parsed").select("doc_nombre", "texto").collect()

filas = []
for r in docs:
    filas += split_estructurado(r["texto"], r["doc_nombre"])

# Narrativas de casos: precedentes históricos como chunks citables.
for c in spark.table(f"{CATALOG}.{SCHEMA}.narrativas_casos").collect():
    txt = f"Caso {c['caso_id']} — Tipología {c['tipologia']} — Disposición: {c['disposicion']}. {c['narrativa']}"
    filas.append({
        "source_doc": "narrativas_casos", "seccion": f"Caso {c['caso_id']}",
        "articulo": c["tipologia"], "chunk_to_retrieve": txt,
        "chunk_to_embed": f"[Precedente · {c['tipologia']}] {txt}",
    })

from pyspark.sql import functions as F
df_chunks = (spark.createDataFrame(filas)
             .withColumn("chunk_id", F.expr("uuid()")))

(df_chunks.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.chunks"))
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"✅ Tabla chunks creada: {spark.table(f'{CATALOG}.{SCHEMA}.chunks').count()} chunks")
display(spark.sql(f"SELECT source_doc, count(*) AS n FROM {CATALOG}.{SCHEMA}.chunks GROUP BY source_doc ORDER BY source_doc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enfoque 3 — `ai_prep_search`: el chunker nativo de Databricks
# MAGIC
# MAGIC `ai_prep_search` (GA) hace en **una sola llamada SQL** lo que armamos a mano: chunking
# MAGIC estructura-consciente + `chunk_to_retrieve` / `chunk_to_embed` + metadata. Es la vía **escalable** para
# MAGIC producción (corre distribuido sobre la tabla parseada, gobernado por UC).
# MAGIC
# MAGIC Como su esquema de salida evoluciona, primero lo **inspeccionamos en vivo** y luego mapeamos sus campos.

# COMMAND ----------

# Ejecuta ai_prep_search sobre un documento parseado e inspecciona su salida REAL.
# Requiere la tabla docs_parsed_raw (sólo existe si usaste ai_parse_document en el 01) y DBR 18.2+.
# Si no está disponible en tu clúster clásico, lo saltamos: el laboratorio sigue con la tabla `chunks`
# del Enfoque 2 (chunker con LangChain), que es el camino que usamos de aquí en adelante.
import json
try:
    prep = spark.sql(f"""
      SELECT ai_prep_search(parsed) AS prep
      FROM {CATALOG}.{SCHEMA}.docs_parsed_raw
      WHERE doc_nombre = 'politica_pld_lafise.pdf'
    """)
    raw = prep.selectExpr("to_json(prep) AS j").collect()[0]["j"]
    print("Forma REAL de la salida de ai_prep_search (primeros 1800 chars):\n")
    print(json.dumps(json.loads(raw), ensure_ascii=False, indent=2)[:1800], "...")
    print("\n👉 Confirma aquí la ruta de los chunks (p. ej. prep:response) y ajusta el mapeo si tu versión "
          "difiere. Para el laboratorio seguimos con la tabla `chunks` del Enfoque 2.")
except Exception as e:
    print("ℹ️ ai_prep_search no disponible aquí (requiere docs_parsed_raw + DBR 18.2+):", str(e)[:160])
    print("   No pasa nada: seguimos con la tabla `chunks` del Enfoque 2 (LangChain), ya construida arriba.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.chunks")
try:
    assert df.count() > 10, "Se esperaban múltiples chunks"
    cols = set(df.columns)
    assert {"chunk_id", "chunk_to_retrieve", "chunk_to_embed", "articulo", "source_doc"} <= cols, \
        f"Faltan columnas de metadata. Tengo: {cols}"
    art5 = df.filter("source_doc = 'politica_pld_lafise.pdf' AND articulo = 'Artículo 5'").count()
    assert art5 >= 1, "El Artículo 5 (umbral RTE) debería ser su propio chunk citable"
    print("✅ Chunks estructurados con metadata de citación. El Artículo 5 quedó como chunk propio.")
except AssertionError as e:
    print(f"❌ {e}\nRevisa el TODO del regex de encabezados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Viste por qué el chunking ingenuo **rompe** la citación legal.
# MAGIC - Construiste `chunks` **estructura-consciente**: un chunk por artículo/tipología, con metadata
# MAGIC   (`source_doc`, `seccion`, `articulo`) y un `chunk_to_embed` **enriquecido** para mejor recall.
# MAGIC - Conociste **`ai_prep_search`**, la vía nativa y escalable para producción.
# MAGIC
# MAGIC ➡️ Sigue en **`03_vector_search_y_gobierno`**: indexar estos chunks en Vector Search y gobernarlos en UC.
