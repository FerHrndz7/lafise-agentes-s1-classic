# Databricks notebook source
# MAGIC %md
# MAGIC # LAFISE — Bootcamp de Agentes AML · Sesión 1
# MAGIC ## Fundamentos de Agentes + Vector Search & Unity Catalog
# MAGIC
# MAGIC Bienvenidos al **primer encuentro** del bootcamp con el que el equipo de **PLD (Prevención de Lavado de
# MAGIC Dinero)** construirá, paso a paso, un **Agente de Alertas AML** sobre Databricks.
# MAGIC
# MAGIC Esta sesión (≈3 horas) cubre **dos módulos**:
# MAGIC - **Módulo 0 — Fundamentos de Agentes** (conceptual): qué es un agente, cómo se construye en Databricks,
# MAGIC   y el caso hilo conductor.
# MAGIC - **Módulo 1 — Vector Search + Unity Catalog** (práctico): construir la **base de conocimiento** del
# MAGIC   agente sobre políticas PLD, tipologías y precedentes — con **chunking avanzado** y **búsqueda híbrida**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Aviso
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Solo laboratorio · datos sintéticos</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Ejecuta esta práctica únicamente en el <strong>workspace de laboratorio</strong> proporcionado.</li>
# MAGIC <li>Todo el corpus PLD es <strong>sintético</strong> (Faker + hechos regulatorios ficticios). <strong>Nunca</strong> uses PII real de clientes de LAFISE.</li>
# MAGIC <li>Los umbrales/plazos regulatorios son plausibles para Centroamérica pero <strong>simplificados y didácticos</strong> — no son asesoría de cumplimiento.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Recorrido de la sesión (~3 h)
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <style>.agenda-table td, .agenda-table th { font-size: 14pt !important; }</style>
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC      <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 60px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 90px;">Tiempo</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Notebook</th>
# MAGIC      </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">M0</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">40 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Fundamentos de Agentes (conceptual) — <code>Modulo 0</code></td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">00</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">10 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Preparación — catalog/schema/volumen + corpus PLD</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">01</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">20 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingesta y parseo con <code>ai_parse_document</code></td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">02</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">30 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Chunking avanzado para documentos legales/AML</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">03</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">30 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Vector Search + gobierno en Unity Catalog</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">04</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">25 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Búsqueda híbrida (keyword + semántica) + filtros</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">05</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">15 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Reto — mini-retriever para una alerta</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC Empieza en `Modulo 0 - Fundamentos de Agentes/00_fundamentos_de_agentes`, luego
# MAGIC `Modulo 1 - Vector Search y Unity Catalog/00_setup`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Requisitos
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC - Conecta un **clúster clásico** (DBR **17.3+** recomendado; mínimo 16.4 LTS ML) para ejecutar los notebooks.
# MAGIC - En el widget **catalog** de cada notebook escribe el **catálogo que te asignó el facilitador**.
# MAGIC - Usa **Chrome o Firefox** (no Safari), sin modo Incógnito.
# MAGIC - El notebook `00_setup` instala sus dependencias con `%pip`; ejecútalo primero.
# MAGIC - El workspace debe tener **Vector Search** y **Foundation Model APIs** habilitados (servicios gestionados,
# MAGIC   independientes del cómputo del notebook). Se validan con el notebook de validación de entorno.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. Todos los derechos reservados.
