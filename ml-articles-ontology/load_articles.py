import pandas as pd
import re
import requests

GRAPHDB_UPDATE = "http://127.0.0.1:7200/repositories/ML-Ontology/statements"
BASE = "http://example.com/ml-articles/"

def sanitize(text):
    text = text.replace("/", "_")
    text = re.sub(r"[^A-Za-z0-9_]", "_", text)
    return text

df = pd.read_csv("ml_articles_dataset.csv")

triples = []

for _, row in df.iterrows():
    doi = row["doi"]
    title = row["title"]
    phase = int(row["phase"])
    paradigm = row["ml_category"].strip().lower()
    cluster = int(row["prod_category"])
    methods = eval(row["ml_methods"]) if isinstance(row["ml_methods"], str) else []

    article_uri = BASE + "doi_" + sanitize(doi)

    title_escaped = title.replace('"', '\\"')

    triples.append(f"""
    <{article_uri}>
    a <{BASE}Article> ;
    <https://schema.org/doi> "{doi}" ;
    <http://purl.org/dc/terms/title> "{title_escaped}" ;
    <{BASE}hasPhase> <{BASE}Phase{phase}_{"Planning" if phase==1 else "Development_Production" if phase==2 else "Optimization" if phase==3 else "Use_Reuse"}> ;
    <{BASE}hasParadigm> <{BASE}{paradigm.capitalize()}> ;
    <{BASE}hasCluster> <{BASE}Cluster{cluster}> .
    """)


    for m in methods:
        m_clean = sanitize(m)
        method_uri = BASE + "Method_" + m_clean
        triples.append(f"<{article_uri}> <{BASE}mentionsMethod> <{method_uri}> .")
        triples.append(f"<{method_uri}> a <{BASE}Method> ; <http://www.w3.org/2000/01/rdf-schema#label> \"{m}\" .")

    # triples.append(".")

ttl = "@prefix mla: <http://example.com/ml-articles/> .\n" + "\n".join(triples)

with open("out.ttl", "w", encoding="utf-8") as f:
    f.write(ttl)
print("Wrote out.ttl")

r = requests.post(
    GRAPHDB_UPDATE,
    data=ttl.encode("utf-8"),
    headers={"Content-Type": "text/turtle"}
)

print("Upload status:", r.status_code, r.text)
