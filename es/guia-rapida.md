# Guía rápida del facilitador — Sesión 1 (Vector Search + UC)

**Duración:** ~3 h · **Idioma:** español · **Audiencia:** equipo PLD (ingeniería de datos/BI, nuevos en GenAI).

## Arco de la sesión
1. **Módulo 0 (40 min, conceptual):** encuadra qué es un agente y presenta el **Agente de Alertas AML** como
   hilo conductor. No hay ejercicios; la demo final (LLM sin contexto) motiva el "por qué" del retrieval.
2. **Módulo 1 (práctico):** parseo → chunking avanzado → Vector Search → híbrida → reto.

## Mensajes clave por notebook
- **01 · Parseo:** un PDF es binario; `ai_parse_document` lo vuelve texto gobernado en una tabla. Es una
  función SQL nativa — sin librerías de parseo.
- **02 · Chunking (el corazón):** el chunking ingenuo **rompe la citación legal**. La solución de cumplimiento
  es chunkear **por artículo/tipología** con metadata (`source_doc`, `articulo`) y un `chunk_to_embed`
  enriquecido. Menciona `ai_prep_search` como la vía nativa/escalable (inspeccionamos su salida en vivo).
- **03 · Vector Search + UC:** Delta Sync Index con **embeddings gestionados** (multilingüe para español). El
  índice vive en **Unity Catalog** → permisos + linaje. La primera indexación tarda ~5–15 min → **descanso**.
- **04 · Híbrida:** semántica sola pierde tokens exactos (IDs de caso, "OFAC", "Artículo 5"). `HYBRID` los
  recupera sin perder el significado. Muestra filtros por metadata.
- **05 · Reto:** arman el "paquete de contexto" citado para una alerta — exactamente el input del agente en la
  Sesión 2.

## Logística
- Antes de la sesión: instala `requirements.txt` como **librerías de clúster** en cada clúster del curso, e
  **importa** la carpeta al workspace (ver RUNBOOK, Pasos 1–2).
- Todos adjuntan el **clúster clásico** del curso y corren `00_setup` + `01` al inicio (pre-warm).
- Cada quien escribe su **catálogo asignado** en el widget `catalog`; el esquema se aísla solo
  (`pld_aml_<usuario>`), así que no colisionan aunque compartan catálogo.
- Antes del `03`, encuadra el tiempo de indexación (park en descanso).
- **Endpoint compartido** `lafise_vs_endpoint` (no renombrar); **índice por participante**.
- Usa catálogos **sin puntos** en el nombre (el usuario con punto en el correo no es problema: se sanea).

## Cierre / puente a la Sesión 2
"Hoy construimos la memoria del agente: conocimiento citable y gobernado. La próxima sesión el agente
**razona** sobre la alerta usando este retrieval como herramienta y **recomienda una disposición**."

## Watch-outs
- Vector Search / FMAPI son servicios **gestionados** (serverless gestionado), independientes del cómputo
  clásico del notebook — valida que estén habilitados con el notebook de validación de entorno el día previo.
- Corpus 100% sintético; recuérdalo al abrir (nadie debe pegar PII real).
- `ai_parse_document` requiere **DBR 17.3+**; si el clúster es anterior, `01` usa el respaldo `pypdf` y el lab
  igual corre. `ai_prep_search` (DBR 18.2+) es solo demostrativo: el índice se alimenta del chunking de
  LangChain (Enfoque 2), disponible en cualquier runtime.
- No hay teardown automático (no es Vocareum): limpia índices, endpoint y esquemas al cierre del bootcamp.
