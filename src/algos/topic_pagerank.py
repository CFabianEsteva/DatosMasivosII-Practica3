"""PageRank Sensible a Tópico en MapReduce.

Para cada tópico t con conjunto semilla S_t, el teletransporte salta solo a S_t:
    r_t = beta * M r_t + (1 - beta) * e_{S_t} / |S_t|
Los T vectores se calculan a la vez: el rango es una matriz (N, T) y cada ronda MapReduce
(`graph.propagate`) suma las contribuciones de todas las columnas en un solo paso. La masa de los
nodos colgantes de cada tópico también vuelve a teletransportarse a S_t, de modo que cada r_t suma 1.
Si N*T es muy grande, `batch` calcula los tópicos por grupos de columnas.
En la consulta se combinan con pesos: r = sum_t w_t * r_t (ver `combine_topics`).
"""
import time

import numpy as np


def build_teleport(N, seeds, topics=None):
    """Matriz de teletransporte (N, T) a partir de un DataFrame de pandas con columnas (node, topic).

    Devuelve (tele, topics_used). Los tópicos sin semillas dentro del grafo se omiten.
    """
    topics = sorted(seeds.topic.unique()) if topics is None else list(topics)
    cols, used = [], []
    for t in topics:
        nodes = seeds.loc[seeds.topic == t, "node"].to_numpy()
        nodes = np.unique(nodes[(nodes >= 0) & (nodes < N)])
        if len(nodes) == 0:
            print(f"[TSPR] aviso: el tópico '{t}' no tiene semillas en el grafo, se omite")
            continue
        v = np.zeros(N)
        v[nodes] = 1.0 / len(nodes)
        cols.append(v)
        used.append(t)
    if not cols:
        raise ValueError("Ningún tópico tiene semillas dentro del grafo")
    return np.stack(cols, axis=1), used


def _run_batch(graph, tele, beta, tol, max_iter, verbose, label):
    r = tele.copy()  # cada tópico parte de su distribución de teletransporte
    history = []
    t0 = time.time()
    for it in range(1, max_iter + 1):
        s = graph.propagate(r * graph.inv_out[:, None])
        dangling = r[graph.dangling].sum(axis=0)           # masa colgante por tópico, shape (T,)
        new = beta * s + ((1.0 - beta) + beta * dangling)[None, :] * tele
        delta = float(np.abs(new - r).sum(axis=0).max())   # peor tópico
        r = new
        history.append(delta)
        if verbose:
            print(f"[TSPR{label}] iter {it:3d}  max L1={delta:.3e}  ({time.time() - t0:.0f}s)", flush=True)
        if delta < tol:
            break
    return r, history


def topic_pagerank(graph, tele, beta=0.85, tol=1e-8, max_iter=100, batch=None, verbose=True):
    """Devuelve (ranks, history): ranks es (N, T); history es la curva del peor tópico por iteración."""
    T = tele.shape[1]
    batch = batch or T
    out, hist = [], []
    for a in range(0, T, batch):
        r, h = _run_batch(graph, tele[:, a:a + batch], beta, tol, max_iter, verbose,
                          f" {a}-{min(a + batch, T) - 1}" if batch < T else "")
        out.append(r)
        hist = h if len(h) > len(hist) else hist
    return np.concatenate(out, axis=1), hist


def combine_topics(ranks, topics_used, weights):
    """Combinación en la consulta: r = sum_t w_t * r_t.

    weights: dict {tópico: peso}. Se normalizan para que sumen 1 (los tópicos no incluidos valen 0).
    """
    w = np.array([weights.get(t, 0.0) for t in topics_used], dtype=float)
    if w.sum() <= 0:
        raise ValueError("Los pesos deben sumar algo positivo")
    return ranks @ (w / w.sum())
