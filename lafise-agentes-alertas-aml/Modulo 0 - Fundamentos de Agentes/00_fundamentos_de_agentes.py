# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 0 · Fundamentos de Agentes (conceptual)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Duración** | ~40 min |
# MAGIC | **Formato** | Conceptual — sin ejercicios de código (una demo opcional al final) |
# MAGIC | **Meta** | Marco mental común antes de construir |
# MAGIC
# MAGIC Este módulo **encuadra** el bootcamp: qué es un agente, cómo se construye en Databricks, y el caso hilo
# MAGIC conductor que iremos armando pieza por pieza.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ¿Qué es un *agente*? (vs. un prompt directo)
# MAGIC
# MAGIC Un **LLM directo** recibe un prompt y responde con texto. Útil, pero **no actúa** ni **consulta datos** por
# MAGIC su cuenta.
# MAGIC
# MAGIC Un **agente** combina el LLM con:
# MAGIC - **Herramientas (tools):** funciones que puede invocar (buscar en una base de conocimiento, calcular
# MAGIC   features de una transacción, llamar a una API de sanciones…).
# MAGIC - **Razonamiento en bucle:** decide *qué herramienta usar*, observa el resultado y vuelve a decidir
# MAGIC   (patrón *ReAct* / tool-calling).
# MAGIC - **Memoria y estado:** mantiene contexto de la tarea de varios pasos.
# MAGIC - **Acción:** produce una salida accionable (p. ej. una **disposición** de alerta con su justificación).
# MAGIC
# MAGIC > **En una frase:** un agente *razona sobre un objetivo, usa herramientas para conseguir información y
# MAGIC > actúa* — no solo "responde".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Anatomía de un agente en Databricks
# MAGIC
# MAGIC | Capa | Qué aporta | Se ve en |
# MAGIC |------|------------|----------|
# MAGIC | **Modelo (FMAPI)** | El LLM que razona (Claude, Llama, GPT-OSS…) servido como endpoint | Sesión 2 |
# MAGIC | **Conocimiento (Vector Search + UC)** | Recuperar políticas, tipologías y precedentes citables | **Hoy (Módulo 1)** |
# MAGIC | **Herramientas (UC Functions)** | Funciones gobernadas que el agente invoca | Sesión 2 |
# MAGIC | **Orquestación (Agent Framework / Agent Bricks)** | El bucle de razonamiento; code-first o low-code | Sesiones 2–3 |
# MAGIC | **Serving** | Desplegar el agente como endpoint | Sesiones 2–3 |
# MAGIC | **Gobierno (Unity Catalog)** | Permisos y linaje sobre datos, índices, funciones y modelos | Transversal |
# MAGIC | **Evaluación (MLflow)** | Trazas + métricas anti-alucinación (groundedness) + guardrails | Sesión 4 |
# MAGIC | **Integración (MCP + AI Gateway)** | Conectar APIs internas/externas gobernadas | Sesión 5 |
# MAGIC
# MAGIC **Panorama Mosaic AI:** el **Agent Framework** es la vía *code-first* (Python + SDK); **Agent Bricks** es la
# MAGIC vía *low-code* encima del mismo framework; el **Playground** sirve para probar rápido. Elegiremos según el caso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. El caso hilo conductor: **Agente de Alertas AML**
# MAGIC
# MAGIC Todo el bootcamp construye **un mismo agente**, pieza por pieza. El *Agente de Alertas* recibe una alerta
# MAGIC del monitoreo transaccional y debe:
# MAGIC
# MAGIC 1. **Reunir contexto:** cliente + transacciones + la **política/tipología aplicable** y **precedentes** →
# MAGIC    *esto es lo que construimos hoy con Vector Search.*
# MAGIC 2. **Razonar** sobre el riesgo.
# MAGIC 3. **Recomendar una disposición:** cerrar sin acción · escalar a investigación · reportar (ROS a la UAF),
# MAGIC    **con justificación citada**.
# MAGIC
# MAGIC Es el **Agente #7 "Alertas"** del catálogo de *Agentes Maestro* de LAFISE — la prioridad de despliegue. El
# MAGIC patrón que aprendamos (retrieval + razonamiento + herramientas + evaluación) se reutiliza para los demás
# MAGIC agentes (Investigaciones, Sanciones, Noticias Adversas…).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapa del bootcamp
# MAGIC
# MAGIC | Sesión | Tema | Pieza que añade |
# MAGIC |--------|------|-----------------|
# MAGIC | **1 (hoy)** | **Vector Search + UC** | **Base de conocimiento consultable y gobernada** |
# MAGIC | 2 | Agent Framework (code-first) | El agente que razona y recomienda |
# MAGIC | 3 | Agent Bricks (low-code) | El mismo agente por la vía declarativa |
# MAGIC | 4 | MLflow Tracing & Eval | Confiabilidad: anti-alucinación + guardrails |
# MAGIC | 5 | MCP + AI Gateway | Conexión a APIs (sanciones, KYC, gestión de casos) |
# MAGIC | 6 | Modelos ML con Genie Code | Scoring de alertas que el agente invoca |
# MAGIC | 7 | Capstone | Presentación del primer agente |
# MAGIC
# MAGIC ### Por qué empezamos por Vector Search
# MAGIC Sin **conocimiento recuperable**, el agente alucina: inventaría el fundamento de una disposición. La base
# MAGIC de hoy es la **herramienta de conocimiento** que el agente usará en la Sesión 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. (Opcional) Un LLM directo, para tenerlo tangible
# MAGIC Antes de pasar al Módulo 1, una mini-demo: preguntarle a un LLM *sin* darle las políticas del banco. Verás
# MAGIC que responde de forma **genérica** (o inventa) — justo el problema que el retrieval resuelve.
# MAGIC
# MAGIC > Requiere Foundation Model APIs. Si no corre, no pasa nada: es solo ilustrativo.

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    w = WorkspaceClient()
    chat = next((e.name for e in w.serving_endpoints.list()
                 if any(k in e.name for k in ("claude", "llama", "gpt", "qwen3-next", "gemini"))), None)
    # El SDK espera objetos ChatMessage tipados (no un dict).
    resp = w.serving_endpoints.query(
        name=chat,
        messages=[ChatMessage(role=ChatMessageRole.USER,
                              content="¿Cuál es el umbral exacto para reportar una transacción en efectivo "
                                      "(RTE) en la política PLD de LAFISE?")],
    )
    print(f"[{chat}] respuesta genérica (sin conocer la política del banco):\n")
    print(resp.choices[0].message.content[:600])
    print("\n👉 El modelo NO conoce la política interna → por eso necesitamos Vector Search (Módulo 1).")
except Exception as e:
    print("Demo opcional no ejecutada (ok):", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ➡️ **Continúa en** `Modulo 1 - Vector Search y Unity Catalog/00_setup` para construir la base de conocimiento.
