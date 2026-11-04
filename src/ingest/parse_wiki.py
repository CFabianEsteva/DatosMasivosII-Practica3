"""Etapa 1 de Wikipedia: enwiki.xml (117 GB) -> parquet con una fila por artículo/redirect.

Cómo se reparte el trabajo (MapReduce sin cargar el XML en memoria):
  - El archivo se parte en bloques de `BLOCK_MB` por rango de bytes, igual que Hadoop parte un
    archivo en splits: cada página pertenece al bloque donde empieza su etiqueta `<page>`.
  - Cada bloque es una tarea map de Spark que lee su rango directamente del disco (`iter_pages`),
    aplica `parse_record` (regex sobre el texto, sin parser XML) y escribe su propio parquet.
    Antes se usaba `newAPIHadoopFile` con delimitador `</page>`, pero el texto viajaba de la JVM a
    Python y cada escritura por grupos volvía a recorrer todo el archivo: ~7.6 MB/s. Leyendo los
    rangos desde Python se aprovechan los 8 núcleos (~30 MB/s por núcleo).

Cómo se aligera desde la ingesta (ver PLAN_DE_TRABAJO.md, "aligerar todo"):
  - Solo se guardan páginas del espacio de nombres 0 (artículos y redirects). Nada de texto.
  - Cada título se identifica con un hash de 64 bits (blake2b) y las aristas guardan hashes
    en vez de strings. Así no hace falta un join título->id sobre cientos de millones de enlaces.
  - Los enlaces de cada página se guardan como arreglo de hashes ya deduplicado, sin auto-lazos
    y sin enlaces a otros espacios de nombres (File:, Category:, Template:, ...).
  - No se cuentan los enlaces dentro de referencias ni de plantillas de cita (`strip_refs`).

Salida (un parquet por bloque, para poder reanudar si se interrumpe): <out_dir>/pages/blk_NNNNN.parquet
  h BIGINT, page_id BIGINT, title STRING, redirect_h BIGINT (nulo si no es redirect),
  links ARRAY<BIGINT>, cats ARRAY<STRING>

Para la muestra se usa MAX_BYTES (prefijo del dump, ordenado por id de página); para el dump
completo, MAX_BYTES = None. Es el mismo código.
"""
import hashlib
import html
import math
import re
import sys
import time
from functools import lru_cache
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import ENWIKI_XML, processed

# ----------------------------------------------------------------------------- parámetros
MAX_BYTES = 6 * 1024**3   # muestra: ~6 GB del inicio del dump. None = todo el archivo
OUT_NAME = "wiki_sample"  # data/processed/<OUT_NAME>/pages/
BLOCK_MB = 64             # tamaño de bloque de lectura: una tarea de Spark (y un parquet) por bloque

# ----------------------------------------------------------------------------- parseo de una página
RE_TITLE = re.compile(r"<title>(.*?)</title>", re.S)
RE_NS = re.compile(r"<ns>(\d+)</ns>")
RE_ID = re.compile(r"<id>(\d+)</id>")
RE_REDIRECT = re.compile(r'<redirect title="([^"]*)"')
RE_TEXT = re.compile(r"<text[^>]*>(.*?)</text>", re.S)
RE_LINK = re.compile(r"\[\[([^\[\]|\n#]+)")
RE_CAT = re.compile(r"\[\[\s*category\s*:\s*([^\]|\n]+)", re.I)
# Referencias y plantillas de cita: sus enlaces apuntan a la fuente (periódico, editorial), no al tema.
# Sin quitarlas, The New York Times u Oxford University Press quedan arriba de todos los rankings.
# En el XML el wikitext viene escapado: <ref> aparece como &lt;ref&gt;.
STRIP_REFS = True
RE_REF_SELF = re.compile(r"&lt;ref\b(?:(?!&gt;).)*?/&gt;", re.S | re.I)              # <ref name=x />
RE_REF = re.compile(r"&lt;ref\b(?:(?!&gt;).)*?&gt;.*?&lt;/ref\s*&gt;", re.S | re.I)  # <ref ...>...</ref>
RE_CITE = re.compile(r"\{\{\s*(?:cite|citation)\b(?:[^{}]|\{\{[^{}]*\}\})*\}\}", re.S | re.I)  # {{cite ...}}

# Prefijos de otros espacios de nombres / proyectos: esos enlaces no son artículos
NS_PREFIXES = {
    "file", "image", "media", "category", "template", "talk", "user", "user talk", "wikipedia", "wp",
    "project", "help", "portal", "draft", "module", "mediawiki", "special", "wiktionary", "wikt",
    "commons", "c", "w", "wikipedia talk", "file talk", "template talk", "category talk", "help talk",
    "portal talk", "draft talk", "module talk", "mediawiki talk", "book", "topic", "timedtext",
}


def norm_title(t):
    """Normaliza como MediaWiki (case=first-letter): '_'->' ', espacios colapsados, 1ª letra mayúscula."""
    t = t.replace("_", " ").strip()
    if "  " in t:
        t = " ".join(t.split())
    if t[:1].islower():
        t = t[0].upper() + t[1:]
    return t


def h64(s):
    """Hash de 64 bits con signo (cabe en BIGINT) de un título ya normalizado."""
    return int.from_bytes(hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest(), "big", signed=True)


@lru_cache(maxsize=500_000)
def target_hash(raw):
    """Texto de un enlace [[raw|...]] -> hash del título destino, o None si no es un artículo posible."""
    if "&" in raw:
        raw = html.unescape(raw)
    t = raw.strip()
    if not t or t[0] == ":":
        return None
    colon = t.find(":")
    if colon > 0 and t[:colon].strip().lower().replace("_", " ") in NS_PREFIXES:
        return None
    return h64(norm_title(t))


def strip_refs(text):
    """Quita referencias (<ref>...</ref>, <ref/>) y plantillas {{cite ...}} del wikitext (~4 % de los enlaces)."""
    if "&lt;ref" in text:
        text = RE_REF.sub(" ", RE_REF_SELF.sub(" ", text))
    if "{{" in text:
        text = RE_CITE.sub(" ", text)
    return text


def parse_record(rec):
    """Registro (texto hasta `</page>`) -> tupla (h, page_id, title, redirect_h, links, cats) o None."""
    i = rec.find("<page>")
    if i < 0:  # cabecera/siteinfo suelta o cierre </mediawiki>
        return None
    rec = rec[i:]
    m = RE_NS.search(rec)
    if m is None or m.group(1) != "0":
        return None
    title = html.unescape(RE_TITLE.search(rec).group(1))
    page_id = int(RE_ID.search(rec).group(1))
    h = h64(norm_title(title))

    r = RE_REDIRECT.search(rec)
    if r is not None:
        rh = target_hash(r.group(1).split("#")[0])
        if rh is None or rh == h:
            return None  # redirect a otro espacio de nombres o a sí mismo
        return (h, page_id, title, rh, [], [])

    m = RE_TEXT.search(rec)
    text = m.group(1) if m else ""
    if STRIP_REFS:
        text = strip_refs(text)
    links = set()
    for raw in RE_LINK.findall(text):
        th = target_hash(raw)
        if th is not None and th != h:
            links.add(th)
    cats = {norm_title(html.unescape(c)) for c in RE_CAT.findall(text)}
    return (h, page_id, title, None, sorted(links), sorted(cats))


# ----------------------------------------------------------------------------- lectura por rangos de bytes
READ_CHUNK = 32 * 1024**2


def iter_pages(path, start, end, chunk=READ_CHUNK):
    """Registros `<page>...</page>` cuyo `<page>` empieza en el rango de bytes [start, end).

    Es la misma regla que usa Hadoop para partir un archivo de texto en splits: cada registro
    pertenece al bloque donde empieza, y el último se termina de leer aunque cruce el límite.
    Dentro del XML los `<` del texto van escapados (`&lt;`), así que `<page>` solo marca páginas.
    """
    with open(path, "rb") as f:
        f.seek(start)
        base, buf, pos = start, f.read(chunk), 0   # base = posición en el archivo de buf[0]
        eof = len(buf) < chunk
        while True:
            i = buf.find(b"<page>", pos)
            if i < 0:
                if eof or base + len(buf) >= end + 6:
                    return
                keep = max(pos, len(buf) - 6)       # por si `<page>` quedó partido entre lecturas
                base, buf, pos = base + keep, buf[keep:], 0
                more = f.read(chunk)
                eof = len(more) < chunk
                buf += more
                continue
            if base + i >= end:
                return
            j = buf.find(b"</page>", i)
            if j < 0:
                if eof:
                    return
                base, buf, pos = base + i, buf[i:], 0
                more = f.read(chunk)
                eof = len(more) < chunk
                buf += more
                continue
            yield buf[i:j].decode("utf-8", errors="replace")
            pos = j + 7


def _arrow_schema():
    import pyarrow as pa
    return pa.schema([
        pa.field("h", pa.int64(), False),
        pa.field("page_id", pa.int64(), False),
        pa.field("title", pa.string(), False),
        pa.field("redirect_h", pa.int64(), True),
        pa.field("links", pa.list_(pa.int64()), False),
        pa.field("cats", pa.list_(pa.string()), False),
    ])


def process_block(task):
    """Tarea map: (índice, inicio, fin, xml, carpeta) -> escribe pages/blk_NNNNN.parquet y devuelve conteos.

    El archivo se escribe con nombre temporal y se renombra al final (operación atómica): si la
    corrida se interrumpe, solo quedan bloques completos y al reanudar se omiten.
    """
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq

    idx, start, end, xml, pages_dir = task
    t0 = time.time()
    cols = {k: [] for k in ("h", "page_id", "title", "redirect_h", "links", "cats")}
    n_pages = n_links = 0
    for rec in iter_pages(xml, start, end):
        n_pages += 1
        row = parse_record(rec)
        if row is None:
            continue
        for k, v in zip(cols, row):
            cols[k].append(v)
        n_links += len(row[4])
    table = pa.table(cols, schema=_arrow_schema())
    final = os.path.join(pages_dir, f"blk_{idx:05d}.parquet")
    tmp = os.path.join(pages_dir, f".blk_{idx:05d}.parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, final)
    return {"bloque": idx, "paginas": n_pages, "filas": table.num_rows,
            "redirects": int(table.num_rows - table.column("redirect_h").null_count),
            "enlaces": n_links,
            "segundos": round(time.time() - t0, 1)}


def block_files(pages_dir):
    """Bloques terminados de una carpeta pages/: formato nuevo (blk_*.parquet) y viejo (part_*/ con _SUCCESS)."""
    pages_dir = Path(pages_dir)
    files = sorted(str(p) for p in pages_dir.glob("blk_*.parquet"))
    files += sorted(str(p) for p in pages_dir.glob("part_*") if (p / "_SUCCESS").exists())
    return files


# ----------------------------------------------------------------------------- Spark
def run_ingest(spark, xml_path=ENWIKI_XML, out_dir=None, max_bytes=MAX_BYTES, block_mb=BLOCK_MB, resume=True):
    """Map sobre rangos de bytes del XML: una tarea de Spark por bloque de `block_mb`.

    Cada tarea abre el archivo, salta a su rango y parsea sus páginas en Python puro (sin pasar el
    texto por la JVM), y escribe su propio parquet. Es un solo trabajo de Spark que lee el archivo
    una vez; la reanudación es por bloque (los que ya tienen su parquet se omiten).
    """
    import json

    out = Path(out_dir) if out_dir else processed(OUT_NAME)
    pages_dir = out / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    xml = str(Path(xml_path).resolve())
    size = Path(xml).stat().st_size
    limit = size if max_bytes is None else min(size, max_bytes)
    block = block_mb * 1024**2
    n_blocks = math.ceil(limit / block)
    tasks = [(i, i * block, min((i + 1) * block, limit), xml, str(pages_dir)) for i in range(n_blocks)]
    todo = [t for t in tasks if not (resume and (pages_dir / f"blk_{t[0]:05d}.parquet").exists())]
    print(f"[ingest] {limit / 1024**3:.1f} GB en {n_blocks} bloques de {block_mb} MB; "
          f"pendientes {len(todo)} (ya hechos {n_blocks - len(todo)})", flush=True)
    if not todo:
        return out
    t0 = time.time()
    stats = spark.sparkContext.parallelize(todo, len(todo)).map(process_block).collect()
    secs = time.time() - t0
    gb = sum(t[2] - t[1] for t in todo) / 1024**3
    print(f"[ingest] {len(todo)} bloques ({gb:.1f} GB) en {secs / 60:.1f} min "
          f"({gb * 1024 / secs:.0f} MB/s)", flush=True)
    log = out / "ingest_log.jsonl"
    with open(log, "a") as fh:
        for st in stats:
            fh.write(json.dumps(st) + "\n")
    return out


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark
    # Se importa el módulo (en vez de usar las funciones de __main__) para que los workers de Spark
    # puedan deserializar `process_block`, `parse_record` y `target_hash` por referencia.
    from src.ingest.parse_wiki import run_ingest as run_ingest_module

    spark = get_spark("wiki-ingest", shuffle_partitions=64)
    # %%
    run_ingest_module(spark)
