import pandas as pd
import re

# This script maps ML methods between two datasets and generates SKOS exactMatch triples.
# The datasets are ML methods from the articles and from the external ML ontology.

# Normalize string: lowercase, trim whitespace, collapse multiple spaces
def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

# Load method datasets from CSV files
left = pd.read_csv("data/article_methods.csv")   # ml_method,label  (mla-uri)
right = pd.read_csv("data/ml_methods.csv")       # ml_method,label  (ml-uri)

# Create normalized keys for matching
left["k"] = left["label"].map(norm)
right["k"] = right["label"].map(norm)

# Merge datasets on normalized keys
merged = left.merge(right, on="k", how="left", suffixes=("_mla", "_ml"))

# Initialize TTL output with SKOS namespace prefix
ttl_lines = [
    "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .",
    "",
]

# Filter for successfully matched methods
mapped = merged.dropna(subset=["ml_method_ml"])

# Generate SKOS exactMatch triples for mapped methods
for _, r in mapped.iterrows():
    ttl_lines.append(f"<{r['ml_method_mla']}> skos:exactMatch <{r['ml_method_ml']}> .")

# Write mapping to TTL file
with open("ttl_files/method_mapping.ttl", "w", encoding="utf-8") as f:
    f.write("\n".join(ttl_lines) + "\n")

# Export unmapped methods to CSV for manual review
unmapped = merged[merged["ml_method_ml"].isna()][["ml_method_mla", "label_mla"]].drop_duplicates()
unmapped.to_csv("data/unmapped_methods.csv", index=False)

# Print summary statistics
print(f"Mapped: {len(mapped)}")
print(f"Unmapped: {len(unmapped)}")
print("Wrote: method_mapping.ttl, unmapped_methods.csv")
