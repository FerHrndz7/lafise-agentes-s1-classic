# LAFISE — Agentes AML · Sesión 1 (Vector Search + Unity Catalog) — Runbook (cómputo clásico, workspace propio)

**Audiencia:** Grupo LAFISE (Nicaragua / Centroamérica, FSI) — equipo de **PLD (Prevención de Lavado de Dinero)**. Nivel principiante–intermedio, facilitación en **español**.
**Workshop:** primera sesión (~3 h) del bootcamp de Agentes AML. Cubre **Módulo 0 (Fundamentos de Agentes, conceptual)** + **Módulo 1 (Vector Search + Unity Catalog, práctico)** — parseo con `ai_parse_document` (con respaldo `pypdf`), **chunking avanzado estructura-consciente** para documentos legales/AML (LangChain + `ai_prep_search` cuando esté disponible), **Delta Sync Index** con embeddings gestionados, y **búsqueda híbrida** (keyword + semántica) + filtros por metadata. Todo sobre un **corpus PLD sintético**.

**Diferencia clave con la versión Vocareum:** los asistentes siguen el contenido en **el workspace de desarrollo de LAFISE**, con **cómputo clásico** (sin serverless), y cada uno trabaja en **su propio catálogo** (o en un esquema propio dentro de un catálogo compartido). El contenido se **importa** al workspace; no hay empaquetado Vocareum (`content.zip` / `config.json`).

> La versión Vocareum se conserva en `~/vocareum-lafise-agentes-s1/` por si se decide dictar en Vocareum.

## Artefactos en esta carpeta
- `lafise-agentes-alertas-aml/` — el árbol de notebooks a importar:
  - `00 - Course Overview`
  - `Modulo 0 - Fundamentos de Agentes/00_fundamentos_de_agentes` (conceptual)
  - `Modulo 1 - Vector Search y Unity Catalog/`: `00_setup`, `01_ingesta_y_parseo`, `02_chunking_avanzado`, `03_vector_search_y_gobierno`, `04_hybrid_search`, `05_reto`, `solutions/`, `data/sample_data_gen.py`
  - `requirements.txt` — dependencias para instalar como **librerías de clúster**.
- `es/` — `guia-rapida.md` (referencia del facilitador) + `invitacion.md` (invitación a los asistentes).

## Qué hace el laboratorio (por asistente)
1. `00_setup` — crea el schema `pld_aml_<usuario>` + volumen `documentos` en el catálogo asignado; genera el corpus PLD sintético (3 PDFs + 1 DOCX + `preguntas_eval.json`) y las tablas `clientes`/`transacciones`/`alertas`/`narrativas_casos`. Reporta la versión de DBR.
2. `01_ingesta_y_parseo` — `read_files(binaryFile)` + **`ai_parse_document`** → `docs_parsed`. **Respaldo `pypdf`** automático si el runtime no incluye `ai_parse_document` (DBR < 17.3).
3. `02_chunking_avanzado` — chunking **estructura-consciente** (por artículo/tipología + metadata de citación) con LangChain; introspección de **`ai_prep_search`** cuando está disponible (DBR 18.2+), tolerante si no.
4. `03_vector_search_y_gobierno` — endpoint compartido `lafise_vs_endpoint` + **Delta Sync Index** por asistente (`<catálogo>.pld_aml_<usuario>.chunks_index`, embeddings gestionados, auto-detección `qwen3` → `gte`); consulta semántica; linaje en UC.
5. `04_hybrid_search` — `query_type="HYBRID"` vs `ANN`; filtros por metadata (`filters_json`).
6. `05_reto` — mini-retriever que arma contexto citado para una alerta (puente a la Sesión 2).

### Modelo de aislamiento (soporta los dos escenarios)
El widget **catalog** de cada notebook recibe el catálogo asignado. El schema **siempre** lleva el usuario saneado (`pld_aml_<usuario>`), así que:
- **Un catálogo por asistente** → cada quien escribe su propio catálogo.
- **Un catálogo compartido con esquema por asistente** → todos escriben el catálogo compartido; el sufijo de usuario evita colisiones.
En ambos casos el índice de Vector Search queda aislado por asistente. El **endpoint** de Vector Search es **compartido** (`lafise_vs_endpoint`, uno por workspace).

---

## Requisitos del entorno (go/no-go)
El workspace de desarrollo de LAFISE **debe** soportar, en su región:
**Unity Catalog + Volúmenes · Foundation Model APIs (un endpoint de embeddings: qwen3 *o* gte/bge) · Vector Search (endpoint + Delta Sync Index).**
Deseable: **`ai_parse_document`** (DBR 17.3+) y **`ai_prep_search`** (DBR 18.2+); si no están, el lab usa respaldos (`pypdf` + LangChain).

> **Vector Search y Foundation Model APIs son servicios gestionados** que corren sobre infraestructura serverless de Databricks, **independientes del cómputo clásico del notebook**. Que "no haya serverless" para notebooks **no** implica que estos servicios estén deshabilitados — por eso se validan explícitamente. Usa el notebook `~/lafise-env-validation/validacion_entorno_lafise.py` (entregado por correo) para el go/no-go.

### Cómputo recomendado (≈20 asistentes)
Clúster(es) **de acceso compartido** (shared), autoescalado. Punto de partida: **driver `m5d.4xlarge`** (16 vCPU / 64 GB) para alojar las sesiones concurrentes, **workers `m5d.2xlarge`** (8 vCPU / 32 GB) con autoescalado de 2 a 6; o **dos clústeres de ~10 asistentes**. El trabajo pesado corre en los servicios gestionados (Vector Search / FMAPI), así que el clúster se dimensiona sobre todo por la **concurrencia en el driver**. (En Azure: equivalentes `Standard_D16ds_v5` / `Standard_D8ds_v5`.) DBR **17.3+** recomendado.

---

## Paso 1 — Instalar dependencias como librerías de clúster
En cada clúster del curso: **Compute → (clúster) → Libraries → Install new → PyPI**, instala los paquetes de `requirements.txt`:
`databricks-sdk>=0.65.0`, `reportlab>=4.0.0`, `python-docx>=1.1.0`, `faker>=25.0.0`, `langchain-text-splitters>=0.3.0`, `pypdf>=4.0.0`.
Así todos los notebooks tienen las dependencias sin depender de `%pip` por-notebook. (Los notebooks `00_setup` y `02` igual traen un `%pip` de respaldo por si alguien corre en un clúster sin las librerías.)

## Paso 2 — Importar el contenido al workspace
Opción A (recomendada, CLI):
```
databricks workspace import-dir \
  "lafise-agentes-alertas-aml" \
  "/Workspace/Shared/LAFISE-Agentes-S1" \
  --overwrite --profile <perfil-lafise>
```
Opción B (UI, un clic): usa **Workspace → (carpeta destino) → Import → File**, y sube `build/lafise-agentes-alertas-aml.dbc` (archivo DBC con todo el árbol). Colócalo en una ruta compartida (p. ej. `/Workspace/Shared/LAFISE-Agentes-S1`) para que todos los asistentes lo abran; quien quiera editar puede **Clone** a su carpeta.

## Paso 3 — Permisos por asistente (según el modelo que confirme LAFISE)
- **Catálogo por asistente** (requiere `CREATE CATALOG` a nivel de metastore): crea un catálogo por persona y otórgale `ALL PRIVILEGES` (o `USE CATALOG` + `CREATE SCHEMA`/`CREATE VOLUME`/`CREATE TABLE`).
- **Esquema por asistente en catálogo compartido**: `GRANT USE CATALOG` + `CREATE SCHEMA` sobre el catálogo compartido (el notebook crea `pld_aml_<usuario>`).
En ambos: permiso para **crear y consultar índices de Vector Search** y **llamar a los modelos** (Model Serving / FMAPI). El notebook de validación de entorno comprueba cuál de los dos modelos es viable.

## Paso 4 — Smoke test (antes de la sesión)
Como un asistente, en un clúster clásico (DBR 17.3+) con las librerías instaladas y el widget `catalog` apuntando a un catálogo de prueba:
1. `00_setup` → volumen + 4 docs + `preguntas_eval.json` + 4 tablas; imprime el DBR.
2. `01_ingesta_y_parseo` → `docs_parsed` con ≥3 filas (vía `ai_parse_document` o respaldo `pypdf`).
3. `02_chunking_avanzado` → tabla `chunks`; el Artículo 5 queda como chunk propio.
4. `03_vector_search_y_gobierno` → **núcleo go/no-go:** endpoint online, índice `ready`, consulta semántica recupera el artículo del ROS/RTE.
5. `04_hybrid_search` → HYBRID recupera el ID exacto `K-2024-018`; el filtro por metadata acota a `narrativas_casos`.
6. `05_reto` → arma el contexto citado.
Al terminar, revisa que no queden índices/endpoints/volúmenes/catálogos huérfanos de prueba.

## Paso 5 — Día de la sesión
- **Chrome/Firefox, no Safari; sin Incógnito.**
- **Pre-calentamiento:** todos adjuntan el clúster y corren `00_setup` y `01` temprano.
- **Secuencia para la espera del índice:** todos crean su índice (`03`) y toman un descanso corto mientras indexa (~5–15 min); hacen `04`/`05` después. El `03` hace polling y avisa cuando `ready=True`.
- El **endpoint** de Vector Search es **compartido** (`lafise_vs_endpoint`); solo el **índice** es por asistente. No lo renombren.
- Facilita en español con `es/guia-rapida.md`; el markdown de los notebooks ya está en español.

## Limpieza
A diferencia de Vocareum (que reapa al derribar el workspace), aquí **no hay teardown automático**. Al final del bootcamp:
- Borra los índices (`DROP` o vía UI) y, si procede, el endpoint `lafise_vs_endpoint`.
- `DROP SCHEMA ... CASCADE` de cada `pld_aml_<usuario>` (o `DROP CATALOG` si fue catálogo por asistente).

---

## Notas horneadas en el paquete
- **Datos:** 100% sintéticos (Faker + hechos AML centroamericanos ficticios pero plausibles: UAF, ROS/RTE, GAFI). Nunca PII real de LAFISE.
- **Respaldos de robustez (clásico):** `01` usa `pypdf` si `ai_parse_document` no está (DBR < 17.3); `02` usa el chunker de LangChain siempre y solo *introspecciona* `ai_prep_search` cuando está (DBR 18.2+).
- **VS `create_index` requiere objetos tipados** (`DeltaSyncVectorIndexSpecRequest`/`EmbeddingSourceColumn`/`VectorIndexType`/`PipelineType`), no un dict.
- **`filters_json` es solo para endpoints STANDARD** — `03` crea STANDARD.
- **`databricks-qwen3-embedding-0-6b`** (multilingüe) se auto-prefiere para el corpus en español; cae a `gte`/`bge`.

## Bitácora de smoke — latam-workspace (Azure), clúster clásico DBR 17.3, 2026-10-01

Se corrió el árbol completo como jobs sobre un **clúster clásico single-node `Standard_DS3_v2`, DBR 17.3**
(con `requirements.txt` instalado como librerías de clúster), catálogo `ferhrndz`, schema
`pld_aml_fernando_hernandez`. **Todo verde:**

- `00_setup` ✅ — `%pip`+`restartPython`+`%run` del generador funcionan en clásico; volumen + 4 docs +
  `preguntas_eval.json` + 4 tablas creados; imprime el DBR.
- `01_ingesta_y_parseo` (solución) ✅ — `ai_parse_document` corre nativo en DBR 17.3.
- **Respaldo `pypdf` validado por separado** ✅ — forzado sobre los PDFs del volumen: 3 PDFs, recupera el
  umbral "USD 10,000". (El camino de respaldo del `01` sirve en DBR < 17.3.)
- `02_chunking_avanzado` (solución) ✅ — tabla `chunks` con el Artículo 5 como chunk propio; la celda de
  `ai_prep_search` quedó tolerante.
- `03_vector_search_y_gobierno` (solución) ✅ — endpoint STANDARD `lafise_vs_endpoint` creado, índice `ready`,
  consulta semántica recupera el artículo del ROS.
- `04_hybrid_search` (solución) ✅ — HYBRID recupera el ID exacto `K-2024-018`; filtro `filters_json` acota a
  `narrativas_casos`.
- `05_reto` (solución) ✅ — arma el contexto citado.

**Bug encontrado y corregido en el smoke:** notebook `03` usaba
`w.vector_search_indexes.list_indexes(endpoint_name=...).vector_indexes`, pero el SDK devuelve un **iterador**
directamente (no un objeto con `.vector_indexes`) → `AttributeError: 'generator' object has no attribute
'vector_indexes'`. Corregido a iterar directo (`{i.name for i in w.vector_search_indexes.list_indexes(...)}`)
en el ejercicio y la solución. (Estaba latente también en la versión Vocareum: el smoke previo validó Vector
Search por la ruta `create_index`/`query_index` del notebook de validación, nunca por el `list_indexes` del `03`.)

**Limpieza:** índice, endpoint, schema borrados; árbol de notebooks y clúster de prueba eliminados; sin
huérfanos en latam.
