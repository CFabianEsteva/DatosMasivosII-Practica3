"""Métricas pequeñas para comparar vectores de rango."""
import numpy as np


def top_k(rdd, k=20):
    """Los k pares (nodo, valor escalar) con mayor valor de un RDD."""
    return rdd.takeOrdered(k, key=lambda kv: -kv[1])


def l1_distance(a, b):
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    return float(np.abs(a - b).sum())


def max_abs_diff(a, b):
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    return float(np.abs(a - b).max())


def overlap_at_k(ids_a, ids_b, k=20):
    """Fracción de elementos compartidos entre los k primeros de dos rankings (listas ordenadas de ids)."""
    return len(set(list(ids_a)[:k]) & set(list(ids_b)[:k])) / k


def spearman(x, y):
    """Correlación de Spearman entre dos vectores (rangos promedio en empates)."""
    import pandas as pd

    return float(pd.Series(x).rank().corr(pd.Series(y).rank()))
