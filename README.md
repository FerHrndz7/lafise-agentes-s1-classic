# LAFISE — Bootcamp de Agentes AML · Sesión 1 (cómputo clásico)

Contenido del taller de la Sesión 1 del bootcamp de Agentes AML para el equipo de
PLD (Prevención de Lavado de Dinero) de Grupo LAFISE, preparado para ejecutarse en el
**workspace de desarrollo de LAFISE con cómputo clásico** (sin serverless para el notebook),
con un **catálogo (o esquema) por asistente**.

> Todos los datos del laboratorio son **100% sintéticos** (Faker + hechos AML ficticios
> pero plausibles: UAF, ROS/RTE, GAFI). Nunca contiene PII real de clientes.

## Estructura

- `lafise-agentes-alertas-aml/` — árbol de notebooks a importar al workspace:
  - `00 - Course Overview`
  - `Modulo 0 - Fundamentos de Agentes/` — conceptual
  - `Modulo 1 - Vector Search y Unity Catalog/` — `00_setup` → `05_reto`, `solutions/`, `data/`
  - `Modulo 1 - .../06_demo_indexacion_rapida` — demo **autocontenida** (compartible sola): por qué tarda la indexación (endpoint frío vs caliente), actualización incremental con `MERGE` + `chunk_id` determinístico, y Direct Access. Probada en clásico DBR 17.3 (2026-10-14).
- `build/lafise-agentes-alertas-aml.dbc` — archivo DBC para importar todo el árbol en un clic.
- `es/` — `invitacion.md`, `guia-rapida.md` (facilitador) y `lafise-agentes-s1-intro.html` (deck de los primeros 40 min).
- `RUNBOOK.md` — guía de despliegue: requisitos de entorno, importación, permisos por asistente, smoke test y día de la sesión.

## Cómo cargar el contenido al workspace

**Opción A (un clic, UI):** Workspace → carpeta compartida → Import → File → sube
`build/lafise-agentes-alertas-aml.dbc`.

**Opción B (CLI):**
```
databricks workspace import-dir \
  "lafise-agentes-alertas-aml" \
  "/Workspace/Shared/LAFISE-Agentes-S1" \
  --overwrite --profile <perfil-lafise>
```

Ver `RUNBOOK.md` para el detalle (cómputo clásico recomendado DBR 17.3+, dependencias de
`requirements.txt`, modelo de aislamiento por asistente, y limpieza al cierre).
