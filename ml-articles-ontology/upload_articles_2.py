import pandas as pd
import re
import requests
import ast

GRAPHDB_UPDATE = "http://127.0.0.1:7200/repositories/ML-Ontology/statements"

BASE = "http://example.com/ml-articles/"
ML = "http://h-da.de/ml-ontology/"

AREA_MAP = {
    "supervised": f"<{ML}supervised_learning>",
    "unsupervised": f"<{ML}unsupervised_learning>",
    "reinforcement": f"<{ML}reinforcement_learning>",
}

def sanitize(text: str) -> str:
    text = str(text).replace("/", "_")
    text = re.sub(r"[^A-Za-z0-9_]", "_", text)
    return text

def escape_literal(text: str) -> str:
    return str(text).replace("\\", "\\\\").replace('"', '\\"')

def parse_methods(cell):
    """Safely parse a list-like string such as "['SVM','RF']"."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    try:
        v = ast.literal_eval(cell)
        return v if isinstance(v, list) else []
    except Exception:
        return []

df = pd.read_csv("ml_articles_dataset.csv")

triples = []

for _, row in df.iterrows():
    doi = str(row.get("doi", "")).strip()
    title = str(row.get("title", "")).strip()
    paradigm = str(row.get("ml_category", "")).strip().lower()  # supervised|unsupervised|reinforcement
    methods = parse_methods(row.get("ml_methods", ""))

    if not doi:
        continue

    article_uri = BASE + "doi_" + sanitize(doi)

    title_escaped = escape_literal(title)
    doi_escaped = escape_literal(doi)

    ml_area = AREA_MAP.get(paradigm)  # already wrapped in <...>

    # Article triples (NO phase/cluster/paradigm resources)
    block = [
        f"<{article_uri}>",
        f"  a <{BASE}Article> ;",
        f'  <https://schema.org/doi> "{doi_escaped}" ;',
        f'  <http://purl.org/dc/terms/title> "{title_escaped}" ;',
    ]

    # Direct link to ML Ontology area if recognized
    if ml_area:
        block.append(f"  <{BASE}ml_area> {ml_area} ;")

    # End the subject with "."
    block[-1] = block[-1].rstrip(" ;") + " ."
    triples.append("\n".join(block))

    # Methods
    for m in methods:
        if m is None:
            continue
        m = str(m).strip()
        if not m:
            continue

        method_uri = BASE + "Method_" + sanitize(m)
        triples.append(f"<{article_uri}> <{BASE}mentionsMethod> <{method_uri}> .")
        triples.append(
            f"<{method_uri}> a <{BASE}Method> ; "
            f'<http://www.w3.org/2000/01/rdf-schema#label> "{escape_literal(m)}" .'
        )

ttl = (
    f"@prefix mla: <{BASE}> .\n"
    f"@prefix ml: <{ML}> .\n\n"
    + "\n\n".join(triples)
    + "\n"
)

with open("out.ttl", "w", encoding="utf-8") as f:
    f.write(ttl)
print("Wrote out.ttl")

r = requests.post(
    GRAPHDB_UPDATE,
    data=ttl.encode("utf-8"),
    headers={"Content-Type": "text/turtle"},
)

print("Upload status:", r.status_code)
print(r.text)