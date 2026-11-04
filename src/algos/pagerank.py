"""PageRank en MapReduce sobre PySpark.

Una iteración (r = vector de rangos, difundido a las tareas; aristas particionadas por hash del origen):
  Map    : cada tarea recorre sus aristas u -> v y emite r_u / deg(u) hacia v, consolidando localmente
           (combiner) en un vector parcial de N sumas
  Reduce : los vectores parciales se suman en árbol -> s_v = suma de contribuciones que llegan a v
  Update : r_v = beta * s_v + (1 - beta)/N + beta * D/N        (D = masa de los nodos sin salida)
El teletransporte (1-beta) cubre las trampas de araña, y la masa D de los nodos colgantes se
redistribuye de forma uniforme, de modo que el vector siempre suma 1.
"""
import time

import numpy as np


def pagerank(graph, beta=0.85, tol=1e-8, max_iter=100, verbose=True):
    """Devuelve (ranks, history).

    ranks   : ndarray de N rangos (suma 1), indexado por id de nodo
    history : distancia L1 entre iteraciones consecutivas (curva de convergencia)
    """
    N = graph.N
    r = np.full(N, 1.0 / N)
    history = []
    t0 = time.time()
    for it in range(1, max_iter + 1):
        s = graph.propagate(r * graph.inv_out)
        dangling = r[graph.dangling].sum()
        new = beta * s + (1.0 - beta) / N + beta * dangling / N
        delta = float(np.abs(new - r).sum())
        r = new
        history.append(delta)
        if verbose:
            print(f"[PageRank] iter {it:3d}  L1={delta:.3e}  ({time.time() - t0:.0f}s)", flush=True)
        if delta < tol:
            break
    return r, history
