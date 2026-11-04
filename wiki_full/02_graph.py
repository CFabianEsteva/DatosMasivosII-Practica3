"""Wikipedia COMPLETA, paso 2: pages/ -> grafo podado (edges, nodes, seeds) en data/processed/wiki_full/.

Igual que la muestra (src/ingest/build_wiki_graph.py) pero con umbrales para el dump completo.
Es el paso más pesado en memoria: ajusta MIN_INDEGREE / TOP_N si se queda sin RAM o disco.
Ideas para aligerar (en orden):
  1. Subir MIN_INDEGREE (p. ej. 10 -> 20): cada vez quedan menos nodos y aristas.
  2. TOP_N = 1_500_000: se queda con los artículos más enlazados.
  3. BROADCAST_REDIRECTS = False si el broadcast de redirects da error de memoria.
Al terminar se puede borrar data/processed/wiki_full/graph_tmp/ (CLEAN_TMP = True).
"""
import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import processed
from src.common.spark_session import get_spark
from src.ingest.build_wiki_graph import run_build

# %% parámetros
OUT_NAME = "wiki_full"
MIN_INDEGREE = 10
TOP_N = None
BROADCAST_REDIRECTS = True
CLEAN_TMP = False

# %% Spark (más particiones de shuffle para el grafo completo)
spark = get_spark("wiki-full-graph", shuffle_partitions=200, driver_memory="10g")

# %% grafo
run_build(spark, out_dir=processed(OUT_NAME), min_indegree=MIN_INDEGREE, top_n=TOP_N,
          broadcast_redirects=BROADCAST_REDIRECTS, clean_tmp=CLEAN_TMP)
