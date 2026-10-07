# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Reto — Mini-retriever para una alerta
# MAGIC
# MAGIC **Objetivo (abierto):** dada una **alerta** del monitoreo transaccional, arma el **paquete de contexto**
# MAGIC que el futuro *Agente de Alertas* necesitará para razonar: la **política aplicable**, la **tipología**
# MAGIC disparada y un **precedente similar** — todo con su **cita** (documento + artículo). ⏱️ ~15 min.
# MAGIC
# MAGIC Esto conecta la Sesión 1 (retrieval) con la Sesión 2 (el agente que usa este retrieval como herramienta).
# MAGIC
# MAGIC Poca estructura a propósito: reutiliza lo de los notebooks 03 y 04.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Elige una alerta
# MAGIC Tomamos una alerta abierta de la tabla `alertas`. Fíjate en su `tipologia`, `monto` y `score`.

# COMMAND ----------

alerta = spark.sql(f"""
  SELECT * FROM {CATALOG}.{SCHEMA}.alertas
  WHERE estado IN ('Abierta','En análisis') ORDER BY score_riesgo DESC LIMIT 1
""").collect()[0]
print(dict(alerta.asDict()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Arma el paquete de contexto (TU CÓDIGO)
# MAGIC
# MAGIC Construye `contexto` con **tres piezas citadas**, usando búsqueda **híbrida** + **filtros por metadata**:
# MAGIC 1. **Política aplicable** — busca en `politica_pld_lafise.pdf` el fundamento relacionado con la tipología.
# MAGIC 2. **Tipología** — busca en `manual_tipologias.pdf` la definición y señales de alerta.
# MAGIC 3. **Precedente** — busca en `narrativas_casos` un caso similar.
# MAGIC
# MAGIC Cada pieza debe guardar su **cita** (`source_doc` + `articulo`). Pista: define una función `buscar(...)`
# MAGIC como la del notebook 04 y llámala con distintos `filters_json`.

# COMMAND ----------

# TODO: implementa el mini-retriever.
def buscar(pregunta, k=2, filters_json=None):
    <FILL_IN>   # usa w.vector_search_indexes.query_index con query_type="HYBRID"

consulta = f"{alerta['tipologia']} monto {alerta['monto_involucrado_usd']}"

contexto = {
    "alerta_id": alerta["alerta_id"],
    "politica":   <FILL_IN>,   # TODO: buscar filtrando source_doc = política
    "tipologia":  <FILL_IN>,   # TODO: buscar filtrando source_doc = manual de tipologías
    "precedente": <FILL_IN>,   # TODO: buscar filtrando source_doc = narrativas_casos
}
print(json.dumps(contexto, ensure_ascii=False, indent=2, default=str)[:1500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

try:
    assert contexto.get("politica") and contexto.get("tipologia") and contexto.get("precedente"), \
        "El contexto debe tener las tres piezas (política, tipología, precedente)"
    print("✅ Reto completo: reuniste contexto citado para la alerta", alerta["alerta_id"])
    print("   Este es exactamente el input que el Agente de Alertas usará en la Sesión 2.")
except AssertionError as e:
    print(f"❌ {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cierre del Módulo 1
# MAGIC Construiste la **base de conocimiento** del Agente de Alertas: parseo → chunking estructura-consciente →
# MAGIC Vector Search (híbrido + gobernado en UC) → retrieval citado para una alerta.
# MAGIC
# MAGIC **En la Sesión 2** envolveremos este retrieval como **herramienta** de un agente que razona sobre la
# MAGIC alerta y recomienda una disposición (cerrar / escalar / reportar) con su justificación citada.
