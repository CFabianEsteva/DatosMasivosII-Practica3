# wiki_full: enwiki completa (117 GB)

Estos tres scripts repiten el procesamiento de la muestra de Wikipedia sobre todo el dump. Se corren
desde VS Code, en orden, con el entorno conda ya creado (`pip install -r ../requirements.txt`):

| Orden | Archivo | Qué hace | Salida |
|---|---|---|---|
| 1 | `01_ingest.py` | lee `enwiki.xml` con Spark y guarda una fila por artículo/redirect | `data/processed/wiki_full/pages/` |
| 2 | `02_graph.py` | resuelve redirects, poda por grado de entrada, ids densos y semillas por tópico | `data/processed/wiki_full/{edges,nodes,seeds}.parquet` |
| 3 | `03_algos.py` | PageRank, HITS y PageRank sensible a tópico | `results/wiki_full/` |

Antes de lanzarlos, desarrolla y valida todo con la muestra (`src/ingest/parse_wiki.py` y
`build_wiki_graph.py` con `OUT_NAME = "wiki_sample"`) y mide cuánto tarda.

- El paso 1 es reanudable: escribe un parquet por bloque de 64 MB; si se interrumpe, se vuelve a correr y
  omite los bloques terminados. Tarda ~30 min (~64 MB/s con 8 núcleos).
- Mientras corre, deja la computadora despierta (sin suspender) y el script abierto.
- Hay ideas para aligerar el paso 2 en el encabezado de `02_graph.py`.
