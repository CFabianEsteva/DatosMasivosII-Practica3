"""HITS (hubs y autoridades) en MapReduce sobre PySpark.

Cada ronda son dos pasos MapReduce sobre las aristas particionadas por hash del origen:
  autoridad a_v = suma de h_u sobre los u con u -> v (propagate hacia adelante)
  hub       h_u = suma de a_v sobre los v con u -> v (propagate hacia atrás)
Después de cada paso se normaliza en L2 (reducción global sobre el vector).
"""
import time

import numpy as np


def _l2(x):
    norm = float(np.sqrt((x * x).sum()))
    return x / norm if norm > 0 else x


def hits(graph, tol=1e-8, max_iter=100, verbose=True):
    """Devuelve (authority, hub, history). Ambos son ndarray de N valores con norma L2 = 1.

    history: L1(a_t - a_{t-1}) + L1(h_t - h_{t-1}) por iteración.
    """
    N = graph.N
    hub = np.full(N, 1.0 / np.sqrt(N))
    auth = np.zeros(N)
    history = []
    t0 = time.time()
    for it in range(1, max_iter + 1):
        new_auth = _l2(graph.propagate(hub, forward=True))
        new_hub = _l2(graph.propagate(new_auth, forward=False))
        delta = float(np.abs(new_auth - auth).sum() + np.abs(new_hub - hub).sum())
        auth, hub = new_auth, new_hub
        history.append(delta)
        if verbose:
            print(f"[HITS] iter {it:3d}  L1={delta:.3e}  ({time.time() - t0:.0f}s)", flush=True)
        if it > 1 and delta < tol:
            break
    return auth, hub, history
