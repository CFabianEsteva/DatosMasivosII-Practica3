"""Sesión de Spark en modo local, con los ajustes pensados para una sola máquina."""
import os
import sys
from pathlib import Path

from src.common.paths import ROOT


def get_spark(app_name="practica3", cores=8, driver_memory="8g", shuffle_partitions=64, tmp_dir=None):
    """Crea (o reutiliza) la SparkSession.

    `driver_memory` solo surte efecto si la JVM aún no arrancó: si ya hay una sesión viva en el
    kernel del notebook, hay que reiniciar el kernel para cambiarla. En modo local los "executors"
    viven dentro del driver, así que esa memoria es la que se reparte entre las tareas.
    `spark.local.dir` va a data/spark_tmp (disco) porque ahí caen los derrames del shuffle.
    """
    # Que los workers de Python usen el mismo intérprete (el del entorno conda) que el driver
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Los workers importan `src.*` (las funciones de los mappers viven ahí): que vean la raíz del proyecto
    old = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = str(ROOT) + (os.pathsep + old if old else "")

    from pyspark.sql import SparkSession

    tmp = Path(tmp_dir) if tmp_dir else ROOT / "data" / "spark_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.broadcastTimeout", "1800")   # los broadcast de Wikipedia completa tardan
        .config("spark.local.dir", str(tmp))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.python.worker.reuse", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # Checkpoints periódicos de los RDD iterativos (cortan el linaje de PageRank/HITS)
    spark.sparkContext.setCheckpointDir(str(tmp / "checkpoints"))
    return spark
