"""Títulos de los papers de cit-HepPh para poder leer los rankings (el dataset de SNAP solo trae ids).

Solo se piden a la API pública de arXiv los papers que aparecen en los primeros TOP lugares de algún
ranking (PageRank, autoridades, hubs y cada tópico): unos miles de ids, no los 34 mil.
Salida: data/processed/hepph/titles.parquet  (paper, arxiv_id, title)
Requiere haber corrido run_all.py para hepph (lee results/hepph/ranks.parquet).
"""
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

import pandas as pd

from src.common.paths import processed, results

TOP = 1000   # cubre también la tabla de "lift" por tópico del notebook (top-1000 de cada tópico)
BATCH = 100
API = "http://export.arxiv.org/api/query"
ATOM = "{http://www.w3.org/2005/Atom}"


def arxiv_id(paper):
    """Id numérico de SNAP (sin ceros a la izquierda) -> 'hep-ph/YYMMNNN'."""
    return f"hep-ph/{int(paper):07d}"


def fetch_titles(ids):
    out = {}
    for a in range(0, len(ids), BATCH):
        chunk = ids[a:a + BATCH]
        url = API + "?" + urllib.parse.urlencode({"id_list": ",".join(chunk), "max_results": len(chunk)})
        req = urllib.request.Request(url, headers={"User-Agent": "practica3-datos-masivos"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            root = ET.fromstring(resp.read())
        for entry in root.findall(ATOM + "entry"):
            eid = entry.findtext(ATOM + "id", "")
            title = " ".join(entry.findtext(ATOM + "title", "").split())
            key = eid.split("/abs/")[-1].rsplit("v", 1)[0]
            if title and key in chunk:
                out[key] = title
        print(f"[titles] {min(a + BATCH, len(ids))}/{len(ids)}", flush=True)
        time.sleep(3)  # la API de arXiv pide 3 s entre consultas
    return out


def run(top=TOP):
    ranks = pd.read_parquet(results("hepph") / "ranks.parquet")
    nodes = pd.read_parquet(processed("hepph") / "nodes.parquet", columns=["id", "paper"])
    cols = [c for c in ranks.columns if c != "id"]
    ids = sorted(set().union(*(ranks.nlargest(top, c).id for c in cols)))
    papers = nodes.set_index("id").loc[ids, "paper"]
    keys = [arxiv_id(p) for p in papers]
    titles = fetch_titles(keys)
    df = pd.DataFrame({"paper": papers.values, "arxiv_id": keys, "title": [titles.get(k) for k in keys]})
    df.to_parquet(processed("hepph") / "titles.parquet", index=False)
    print(f"[titles] {df.title.notna().sum()}/{len(df)} títulos -> {processed('hepph') / 'titles.parquet'}")
    return df


if __name__ == "__main__":
    run()
