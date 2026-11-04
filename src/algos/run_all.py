"""Corre PageRank, HITS y PageRank Sensible a Tópico sobre un dataset ya procesado.

Entrada : data/processed/<name>/{edges,nodes,seeds}.parquet   (ids de nodo densos 0..N-1)
Salida  : results/<name>/ranks.parquet  una fila por nodo: id, pagerank, authority, hub, tspr_<topico>...
          results/<name>/run_info.json  parámetros, tamaños, iteraciones, tiempos y curvas de convergencia
Los títulos/nombres de los nodos se unen después (en el notebook) con nodes.parquet por `id`.
"""
import json
import re
import sys
import time
from pathlib import Path

try:  
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

import numpy as np
import pandas as pd

from src.algos.hits import hits
from src.algos.pagerank import pagerank
from src.algos.topic_pagerank import build_teleport, topic_pagerank
from src.common.graph_io import Graph
from src.common.paths import processed, results

#  parámetros
BETA = 0.85          # factor de amortiguamiento 
TOL = 1e-8
MAX_ITER = 100
RUN = ["hepph", "spotify"]   # "wiki_sample" y "wiki_full" se agregan cuando estén listos
PARTITIONS = {"hepph": 8, "spotify": 8, "wiki_sample": 32, "wiki_full": 32}
TOPIC_BATCH = {"wiki_full": 3}   # tópicos por grupo cuando N*T no cabe en memoria (None = todos juntos)


def run_dataset(spark, name, beta=BETA, tol=TOL, max_iter=MAX_ITER, partitions=None, topic_batch=None):
    src_dir, out_dir = processed(name), results(name)
    out_dir.mkdir(parents=True, exist_ok=True)
    P = partitions or PARTITIONS.get(name, 32)
    t0 = time.time()
    info = {"dataset": name, "beta": beta, "tol": tol, "max_iter": max_iter, "particiones": P}

    n_nodes = len(pd.read_parquet(src_dir / "nodes.parquet", columns=["id"]))
    graph = Graph(spark.read.parquet(str(src_dir / "edges.parquet")), n_nodes, P)
    info.update(nodos=graph.N, aristas=graph.num_edges(), nodos_sin_salida=graph.num_dangling())
    print(f"[{name}] nodos={info['nodos']:,} aristas={info['aristas']:,} sin_salida={info['nodos_sin_salida']:,}",
          flush=True)

    # PageRank
    t = time.time()
    pr, pr_hist = pagerank(graph, beta=beta, tol=tol, max_iter=max_iter)
    info["pagerank"] = {"iteraciones": len(pr_hist), "segundos": round(time.time() - t, 1), "historia": pr_hist}

    # HITS
    t = time.time()
    auth, hub, hits_hist = hits(graph, tol=tol, max_iter=max_iter)
    info["hits"] = {"iteraciones": len(hits_hist), "segundos": round(time.time() - t, 1), "historia": hits_hist}

    # PageRank sensible a tópico
    seeds = pd.read_parquet(src_dir / "seeds.parquet")
    tele, topics_used = build_teleport(graph.N, seeds)
    t = time.time()
    ts, ts_hist = topic_pagerank(graph, tele, beta=beta, tol=tol, max_iter=max_iter,
                                 batch=topic_batch or TOPIC_BATCH.get(name))
    info["tspr"] = {"topicos": topics_used, "iteraciones": len(ts_hist),
                    "segundos": round(time.time() - t, 1), "historia": ts_hist}

    # Un solo parquet con todos los vectores
    df = pd.DataFrame({"id": np.arange(graph.N), "pagerank": pr, "authority": auth, "hub": hub})
    for j, tp in enumerate(topics_used):
        df["tspr_" + re.sub(r"\W", "_", tp)] = ts[:, j]
    df.to_parquet(out_dir / "ranks.parquet", index=False)

    info["segundos_total"] = round(time.time() - t0, 1)
    (out_dir / "run_info.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(f"[{name}] listo en {info['segundos_total']}s -> {out_dir}", flush=True)
    graph.unpersist()
    return info


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark

    spark = get_spark("practica3-algos", shuffle_partitions=32)
    # %%
    for dataset in RUN:
        run_dataset(spark, dataset)
