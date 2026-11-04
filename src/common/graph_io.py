"""Grafo particionado por hash para los algoritmos MapReduce (PageRank, HITS, PageRank sensible a tópico).

Esquema (Leskovec et al., cap. 5.2, "el vector de rangos cabe en memoria principal"):
  - Las aristas se reparten en `P` particiones por HASH del nodo origen (`repartition(P, "src")`), y cada
    partición se guarda una sola vez como dos arreglos numpy (origen, destino) cacheados. Así cada tarea
    "map" tiene completas las listas de salida de sus nodos.
  - El vector de rangos (N valores) vive en el driver y se difunde (`broadcast`) a las tareas en cada ronda.
  - MAP    : cada tarea emite las contribuciones de sus aristas y las consolida localmente (combiner,
             `np.bincount`), devolviendo un vector parcial de N sumas.
  - REDUCE : los vectores parciales se suman. Para no mover P vectores de N valores, las P particiones
             se agrupan sin shuffle (`coalesce`) en tantas tareas como núcleos: cada tarea suma ya sus
             bloques (otro nivel de combiner) y el driver suma solo esos pocos vectores.
Una ronda de PageRank/HITS es entonces un solo trabajo de Spark, sin join ni shuffle por arista.
Los ids se guardan como int32 cuando caben (N < 2^31): la mitad de bytes que leer en cada ronda.
Requiere ids de nodo densos 0..N-1 (los datasets procesados ya los traen).
"""
import os
from array import array

import numpy as np
from pyspark import StorageLevel

LEVEL = StorageLevel.MEMORY_AND_DISK


def _rows_to_block(rows):
    """Filas (src, dst) de una partición -> un bloque (src, dst) de arreglos int64 sin aristas repetidas."""
    # array('q') guarda enteros de 8 bytes sin crear un objeto por arista (en Wikipedia completa
    # hay millones de aristas por partición; una lista de tuplas ocuparía ~10 veces más memoria)
    s, d = array("q"), array("q")
    for r in rows:
        s.append(r[0])
        d.append(r[1])
    arr = np.stack([np.frombuffer(s, dtype=np.int64), np.frombuffer(d, dtype=np.int64)], axis=1)
    del s, d
    if len(arr):
        arr = np.unique(arr, axis=0)
    dt = np.int32 if (not len(arr) or arr.max() < 2**31 - 1) else np.int64
    yield (arr[:, 0].astype(dt), arr[:, 1].astype(dt))


def _scatter(src, dst, x, n):
    """Suma por destino: y[d] = sum_{s->d} x[s]   (x puede ser (N,) o (N, T))."""
    if x.ndim == 1:
        return np.bincount(dst, weights=x[src], minlength=n)
    out = np.empty((n, x.shape[1]))
    for t in range(x.shape[1]):
        out[:, t] = np.bincount(dst, weights=x[src, t], minlength=n)
    return out


def _gather(src, dst, x, n):
    """Suma por origen: z[s] = sum_{s->d} x[d]   (x puede ser (N,) o (N, T))."""
    if x.ndim == 1:
        return np.bincount(src, weights=x[dst], minlength=n)
    out = np.empty((n, x.shape[1]))
    for t in range(x.shape[1]):
        out[:, t] = np.bincount(src, weights=x[dst, t], minlength=n)
    return out


class Graph:
    """Grafo dirigido con N nodos (ids 0..N-1) listo para los algoritmos.

    edges : DataFrame de Spark con columnas (src, dst), o RDD de pares (src, dst)
    N     : número de nodos (incluye nodos aislados)
    P     : número de particiones por hash del origen
    """

    def __init__(self, edges, N, num_partitions=16):
        self.N, self.P = int(N), int(num_partitions)
        self.tasks = max(1, min(self.P, os.cpu_count() or 8))   # tareas por ronda (una por núcleo)
        if hasattr(edges, "repartition"):  # DataFrame
            self.sc = edges.sparkSession.sparkContext
            rdd = edges.select("src", "dst").repartition(self.P, "src").rdd
        else:  # RDD de pares
            self.sc = edges.context
            rdd = edges.partitionBy(self.P)
        self.blocks = rdd.mapPartitions(_rows_to_block).persist(LEVEL)
        n = self.N
        self.out_deg = self.blocks.map(lambda b: np.bincount(b[0], minlength=n)).treeReduce(np.add)
        self.in_deg = self.blocks.map(lambda b: np.bincount(b[1], minlength=n)).treeReduce(np.add)
        self.dangling = self.out_deg == 0
        self.inv_out = np.where(self.out_deg > 0, 1.0 / np.maximum(self.out_deg, 1), 0.0)
        if self.in_deg.shape[0] != n:
            raise ValueError(f"Hay ids de nodo fuera de 0..{n - 1}")

    @classmethod
    def from_pairs(cls, spark, pairs, N, num_partitions=4):
        df = spark.createDataFrame(list(pairs), "src long, dst long")
        return cls(df, N, num_partitions)

    def num_edges(self):
        return int(self.out_deg.sum())

    def num_dangling(self):
        return int(self.dangling.sum())

    def propagate(self, x, forward=True):
        """Una ronda MapReduce sobre las aristas.

        forward=True : y[v] = sum_{u->v} x[u]   (contribuciones que llegan a v: PageRank, autoridades)
        forward=False: z[u] = sum_{u->v} x[v]   (suma de lo que reciben los destinos: hubs)
        """
        n = self.N
        bc = self.sc.broadcast(np.ascontiguousarray(x, dtype=np.float64))
        op = _scatter if forward else _gather

        def partial_sums(blocks):
            acc = None
            for src, dst in blocks:
                y = op(src, dst, bc.value, n)
                if acc is None:
                    acc = y
                else:
                    acc += y
            if acc is not None:
                yield acc

        try:
            return self.blocks.coalesce(self.tasks).mapPartitions(partial_sums).reduce(np.add)
        finally:
            bc.unpersist()

    def unpersist(self):
        self.blocks.unpersist()
