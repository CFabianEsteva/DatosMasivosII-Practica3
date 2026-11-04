"""Wikipedia COMPLETA, paso 1: enwiki.xml (117 GB) -> data/processed/wiki_full/pages/.

- Una tarea de Spark por bloque de 64 MB (1 756 bloques); cada una escribe su propio
  data/processed/wiki_full/pages/blk_NNNNN.parquet. Si se interrumpe, vuelve a correr este archivo:
  los bloques que ya tienen su parquet se omiten (RESUME = True).
"""
import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import processed
from src.common.spark_session import get_spark
from src.ingest.parse_wiki import run_ingest

# %% parámetros
OUT_NAME = "wiki_full"
MAX_BYTES = None      # None = todo el archivo
RESUME = True

# %% Spark
spark = get_spark("wiki-full-ingest", shuffle_partitions=64)

# %% ingesta (larga)
run_ingest(spark, out_dir=processed(OUT_NAME), max_bytes=MAX_BYTES, resume=RESUME)
