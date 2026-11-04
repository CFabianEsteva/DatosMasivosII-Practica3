"""Definición de tópicos (conjuntos semilla) para el PageRank Sensible a Tópico.

Cada dataset tiene sus propios tópicos:
  - Wikipedia: a partir de las categorías de cada artículo (coincidencia por palabra clave).
  - Spotify:   a partir de los géneros de cada artista (coincidencia por palabra clave).
  - cit-HepPh: no hay etiquetas temáticas; el tópico es el periodo de publicación (por el id de arXiv).
Un nodo puede pertenecer a varios tópicos. Los nombres son ASCII porque se usan como nombres de columna.
"""
import re

# ----------------------------------------------------------------------------- Wikipedia
# Palabras clave (en minúsculas) buscadas como subcadena en los nombres de categoría.
WIKI_TOPICS = {
    "ciencia": ["physic", "chemi", "biolog", "mathemat", "astronom", "scientist", "geolog", "genetic",
                "zoolog", "botan", "neuroscien", "ecolog", "species", "taxa", "astrophys"],
    "deportes": ["sport", "football", "basketball", "baseball", "olympic", "tennis", "cricket", "rugby",
                 "hockey", "athlet", "golf", "swimmers", "cyclists", "boxing", "wrestl", "racing",
                 "nba", "nfl", "fifa", "formula one"],
    "arte_cultura": ["films", "novels", "albums", "songs", "paintings", "television", "actors",
                     "actresses", "musicians", "singers", "writers", "poets", "artists", "theatre",
                     "theater", "literature", "comics", "anime", "bands", "operas"],
    "historia": ["history", "wars", "battles", "ancient", "medieval", "dynast", "empire", "kingdom",
                 "revolution", "treaties", "conflicts", "massacres", "military"],
    "geografia": ["cities", "towns", "villages", "rivers", "mountains", "counties", "districts",
                  "municipalities", "populated places", "provinces", "islands", "communes", "lakes",
                  "geography", "unincorporated", "census-designated", "regions of"],
    "politica": ["politician", "political", "election", "government", "parliament", "senators",
                 "presidents", "ministers", "mayors", "legislat", "diplomat", "supreme court",
                 "constitution"],
    "tecnologia": ["software", "computer", "internet", "engineering", "technology", "programming",
                   "electronics", "aircraft", "telecommunication", "video games", "websites",
                   "operating systems"],
    "salud": ["disease", "medicine", "medical", "health", "hospital", "surgery", "pharma", "drugs",
              "syndrome", "cancer", "psycholog"],
    "religion": ["christian", "church", "islam", "religio", "buddhis", "hindu", "bishops", "catholic",
                 "theolog", "mythology", "saints", "judaism"],
    "negocios": ["companies", "business", "economics", "banks", "corporation", "industr", "brands",
                 "finance", "manufacturers"],
}

# ----------------------------------------------------------------------------- Spotify
SPOTIFY_TOPICS = {
    "hip_hop": ["hip hop", "rap", "trap", "drill", "grime"],
    "pop": ["pop"],
    "rock": ["rock", "punk", "grunge", "indie", "alternative", "shoegaze", "emo"],
    "metal": ["metal", "deathcore", "hardcore"],
    "electronica": ["edm", "house", "techno", "trance", "electro", "dubstep", "drum and bass", "bass",
                    "downtempo", "ambient", "idm", "synth"],
    "latino": ["latin", "reggaeton", "salsa", "bachata", "cumbia", "mariachi", "corrido", "banda",
               "sertanejo", "urbano", "tango", "flamenco", "mpb", "samba", "bossa", "ranchera"],
    "jazz_blues_soul": ["jazz", "blues", "soul", "r&b", "funk", "gospel", "motown"],
    "country_folk": ["country", "folk", "bluegrass", "americana", "singer-songwriter"],
    "clasica": ["classical", "orchestra", "opera", "baroque", "soundtrack", "score", "choral"],
}

# ----------------------------------------------------------------------------- cit-HepPh
# Periodos de publicación (años de arXiv hep-ph: 1992-2003)
HEPPH_PERIODS = [(1992, 1994), (1995, 1997), (1998, 2000), (2001, 2003)]
HEPPH_TOPICS = [f"{a}_{b}" for a, b in HEPPH_PERIODS]


def hepph_topic(year):
    """Año -> nombre del periodo, o None si queda fuera."""
    for (a, b), name in zip(HEPPH_PERIODS, HEPPH_TOPICS):
        if a <= year <= b:
            return name
    return None


def compile_topics(topics, word_start=False):
    """{topic: [kw, ...]} -> {topic: regex}, para evaluar muchos textos rápido.

    word_start=True exige que la palabra clave empiece una palabra (`\\b`): en las categorías de
    Wikipedia evita falsos positivos como "sport" dentro de "transport" o "nfl" dentro de "conflicts".
    En los géneros de Spotify se deja la subcadena ("synthpop" debe contar como pop).
    """
    pre = r"\b" if word_start else ""
    return {t: re.compile(pre + "(?:" + "|".join(re.escape(k) for k in kws) + ")") for t, kws in topics.items()}


def match_topics(texts, compiled):
    """Tópicos cuyo regex aparece en alguno de los textos (minúsculas)."""
    low = [t.lower() for t in texts]
    return [t for t, rx in compiled.items() if any(rx.search(x) for x in low)]
