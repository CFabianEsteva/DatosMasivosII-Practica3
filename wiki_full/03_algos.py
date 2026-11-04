"""Wikipedia COMPLETA, paso 3: PageRank, HITS y PageRank sensible a tópico sobre wiki_full.

Salida: results/wiki_full/ranks.parquet y run_info.json.
Si la memoria no alcanza, baja PARTITIONS o sube MIN_INDEGREE en 02_graph.py.
"""
import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.algos.run_all import run_dataset
from src.common.spark_session import get_spark

# %% parámetros
NAME = "wiki_full"
PARTITIONS = 32   # pocas particiones: cada tarea devuelve un vector de N sumas, más particiones = más tráfico
BETA = 0.85

# %% Spark
spark = get_spark("wiki-full-algos", shuffle_partitions=PARTITIONS, driver_memory="10g")

# %% algoritmos
run_dataset(spark, NAME, beta=BETA, partitions=PARTITIONS)
