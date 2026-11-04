"""Etapa 2 de Wikipedia: parquet de páginas -> grafo podado con ids densos (edges, nodes, seeds).

Entrada : <data/processed/NAME>/pages/blk_*.parquet   (salida de parse_wiki.py)
Salidas : <data/processed/NAME>/edges.parquet   (src, dst) ids enteros densos 0..N-1
          <data/processed/NAME>/nodes.parquet   (id, h, page_id, title, cats)
          <data/processed/NAME>/seeds.parquet   (node, topic)  semillas del PageRank sensible a tópico
          <data/processed/NAME>/graph_stats.json  conteos antes/después de cada paso
          <data/processed/NAME>/graph_tmp/      intermedios (se pueden borrar al terminar)

Pasos (cada uno escribe a disco para poder reanudar y para no arrastrar planes largos):
  1. Enlaces de artículos -> resolver redirects (2 saltos) -> quedarse solo con destinos que son
     artículos -> quitar auto-lazos y repetidos.          (graph_tmp/edges_resolved)
  2. Poda de relevancia (tipo k-core por grado de entrada): se quitan nodos con menos de
     MIN_INDEGREE enlaces entrantes, y se repite hasta que no cambie. (graph_tmp/prune_i)
  3. Ids densos con zipWithIndex, tabla de nodos y semillas por tópico.

Aligerar: solo se usan columnas de hashes (8 bytes), los joins pequeños van por broadcast y se
excluyen listas ("List of ...") y páginas de desambiguación, que son hubs sin contenido propio.
"""
import json
import shutil
import sys
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import processed
from src.common.topics import WIKI_TOPICS, compile_topics, match_topics
from src.ingest.parse_wiki import block_files

# ----------------------------------------------------------------------------- parámetros
OUT_NAME = "wiki_sample"
MIN_INDEGREE = 3        # umbral único de poda por grado de entrada
MAX_PRUNE_ITER = 8
TOP_N = None            # si se da (p. ej. 1_500_000), se queda con los N artículos más enlazados
DROP_LISTS = True       # excluir "List of ..." / "Lists of ..."
DROP_DISAMBIG = True    # excluir páginas de desambiguación
BROADCAST_REDIRECTS = True  # los redirects (~10 M filas en el dump completo) caben en broadcast


def _seed_rows(rows, compiled):
    for r in rows:
        for t in match_topics(r["cats"], compiled):
            yield (r["id"], t)


def run_build(spark, out_dir=None, min_indegree=MIN_INDEGREE, max_prune_iter=MAX_PRUNE_ITER,
              top_n=TOP_N, drop_lists=DROP_LISTS, drop_disambig=DROP_DISAMBIG,
              broadcast_redirects=BROADCAST_REDIRECTS, clean_tmp=False):
    from pyspark.sql import functions as F

    out = Path(out_dir) if out_dir else processed(OUT_NAME)
    tmp = out / "graph_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    parts = block_files(out / "pages")
    if not parts:
        raise FileNotFoundError(f"No hay bloques terminados en {out / 'pages'}: corre primero parse_wiki.py")
    stats = {"parametros": dict(min_indegree=min_indegree, top_n=top_n, drop_lists=drop_lists,
                                drop_disambig=drop_disambig), "bloques_leidos": len(parts), "poda": []}

    pages = spark.read.parquet(*parts)
    arts = pages.filter(F.col("redirect_h").isNull())
    if drop_lists:
        arts = arts.filter(~F.col("title").rlike(r"^Lists? of "))
    if drop_disambig:
        arts = arts.filter(~F.exists("cats", lambda c: c.rlike("(?i)disambiguation")))
    redirects = pages.filter(F.col("redirect_h").isNotNull()).select(
        F.col("h").alias("rh"), F.col("redirect_h").alias("rt"))
    stats["articulos"] = arts.count()
    stats["redirects"] = redirects.count()
    print(f"[grafo] articulos={stats['articulos']:,} redirects={stats['redirects']:,}")

    # ------------------------------------------------------------------ 1. resolver redirects
    resolved = tmp / "edges_resolved"
    if not (resolved / "_SUCCESS").exists():
        a, b = redirects.alias("a"), redirects.alias("b")
        r2 = a.join(b, F.col("a.rt") == F.col("b.rh"), "left").select(
            F.col("a.rh").alias("rh"), F.coalesce(F.col("b.rt"), F.col("a.rt")).alias("rt"))
        rd = F.broadcast(r2) if broadcast_redirects else r2
        e = arts.select(F.col("h").alias("src"), F.explode("links").alias("dst0"))
        e = e.join(rd, e["dst0"] == rd["rh"], "left").select(
            "src", F.coalesce(F.col("rt"), F.col("dst0")).alias("dst"))
        ah = F.broadcast(arts.select(F.col("h").alias("ah")))
        e = e.join(ah, e["dst"] == ah["ah"], "left_semi")
        e = e.filter(F.col("src") != F.col("dst")).dropDuplicates(["src", "dst"])
        e.write.mode("overwrite").parquet(str(resolved))
    n_prev = spark.read.parquet(str(resolved)).count()
    stats["aristas_resueltas"] = n_prev
    print(f"[grafo] aristas tras resolver redirects y filtrar destinos: {n_prev:,}")

    # ------------------------------------------------------------------ 2. poda por grado de entrada
    cur = resolved
    for it in range(1, max_prune_iter + 1):
        e = spark.read.parquet(str(cur))
        keep = e.groupBy("dst").count().filter(F.col("count") >= min_indegree).select(F.col("dst").alias("k"))
        k1, k2 = F.broadcast(keep), F.broadcast(keep)
        e2 = e.join(k1, e["src"] == k1["k"], "left_semi")
        e2 = e2.join(k2, e2["dst"] == k2["k"], "left_semi")
        target = tmp / f"prune_{it}"
        e2.write.mode("overwrite").parquet(str(target))
        n_edges = spark.read.parquet(str(target)).count()
        stats["poda"].append({"iter": it, "aristas": n_edges})
        print(f"[grafo] poda {it}: aristas={n_edges:,}")
        cur = target
        if n_edges == n_prev:
            break
        n_prev = n_edges

    if top_n:
        e = spark.read.parquet(str(cur))
        top = (e.groupBy("dst").count().orderBy(F.desc("count")).limit(top_n)
               .select(F.col("dst").alias("k")))
        k1, k2 = F.broadcast(top), F.broadcast(top)
        e2 = e.join(k1, e["src"] == k1["k"], "left_semi")
        e2 = e2.join(k2, e2["dst"] == k2["k"], "left_semi")
        target = tmp / "prune_top"
        e2.write.mode("overwrite").parquet(str(target))
        cur = target
        stats["poda"].append({"iter": "top_n", "aristas": spark.read.parquet(str(cur)).count()})

    # ------------------------------------------------------------------ 3. ids densos, nodos y semillas
    e = spark.read.parquet(str(cur))
    node_h = e.select(F.col("src").alias("h")).union(e.select(F.col("dst").alias("h"))).distinct()
    meta = arts.select("h", "page_id", "title", "cats").join(node_h, "h", "inner").select(
        "h", "page_id", "title", "cats")
    nodes_rdd = meta.rdd.zipWithIndex().map(lambda x: (x[1], x[0][0], x[0][1], x[0][2], x[0][3]))
    nodes = spark.createDataFrame(
        nodes_rdd, "id long, h long, page_id long, title string, cats array<string>")
    nodes.write.mode("overwrite").parquet(str(out / "nodes.parquet"))
    nodes = spark.read.parquet(str(out / "nodes.parquet"))

    ids = nodes.select("id", "h")
    s = F.broadcast(ids.select(F.col("h").alias("sh"), F.col("id").alias("s")))
    d = F.broadcast(ids.select(F.col("h").alias("dh"), F.col("id").alias("d")))
    dense = (e.join(s, e["src"] == s["sh"]).join(d, F.col("dst") == d["dh"])
             .select(F.col("s").alias("src"), F.col("d").alias("dst")))
    dense.write.mode("overwrite").parquet(str(out / "edges.parquet"))

    compiled = compile_topics(WIKI_TOPICS, word_start=True)
    seeds_rdd = nodes.select("id", "cats").rdd.mapPartitions(lambda it: _seed_rows(it, compiled))
    seeds = spark.createDataFrame(seeds_rdd, "node long, topic string")
    seeds.write.mode("overwrite").parquet(str(out / "seeds.parquet"))

    stats["nodos_finales"] = nodes.count()
    stats["aristas_finales"] = spark.read.parquet(str(out / "edges.parquet")).count()
    stats["semillas_por_topico"] = {
        r["topic"]: r["count"] for r in spark.read.parquet(str(out / "seeds.parquet")).groupBy("topic").count().collect()
    }
    (out / "graph_stats.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False))
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    if clean_tmp:
        shutil.rmtree(tmp, ignore_errors=True)
    return out


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark
    from src.ingest.build_wiki_graph import run_build as run_build_module  # importar el módulo para los workers

    spark = get_spark("wiki-graph", shuffle_partitions=64)
    # %%
    run_build_module(spark)
