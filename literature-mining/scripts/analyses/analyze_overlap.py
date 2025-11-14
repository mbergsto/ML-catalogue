import os
import sys
import json
from pathlib import Path
from collections import defaultdict
import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
OUT_DIR = Path(__file__).resolve().parents[2] / "reports" / "tables"

def read_jsonl(p: Path):
    # Read JSONL
    out = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

def read_json(p: Path):
    # Read JSON
    with p.open("r", encoding="utf-8") as f:
        return [json.load(f)]

def norm_doi(x: str | None):
    # Normalize DOI
    if not x:
        return None
    x = x.strip().lower()
    x = x.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return x

def extract_id(rec: dict):
    # Choose stable id
    eid = rec.get("eid")
    doi = norm_doi(rec.get("doi") or rec.get("dc:identifier") or rec.get("prism:doi"))
    return eid or doi

def infer_query_id(file_path: Path, rec: dict) -> str:
    # Prefer field, fallback to filename stem
    return rec.get("query_id") or file_path.stem.replace(".jsonl", "")

def load_all_records(raw_dir: Path):
    # Load all raw files
    files = sorted([*raw_dir.glob("*.jsonl"), *raw_dir.glob("*.json")])
    if not files:
        print(f"No raw files in {raw_dir}", file=sys.stderr)
        sys.exit(1)

    q2ids = defaultdict(set)
    for fp in files:
        if fp.suffix == ".jsonl":
            records = read_jsonl(fp)
        else:
            records = read_json(fp)

        for rec in records:
            rid = extract_id(rec)
            if not rid:
                continue
            qid = infer_query_id(fp, rec)
            q2ids[qid].add(rid)

    return q2ids

def build_pair_table(q2ids: dict[str, set]):
    # Pairwise intersections and Jaccard
    rows = []
    qids = sorted(q2ids.keys())
    for i, a in enumerate(qids):
        for b in qids[i:]:
            set_a = q2ids[a]
            set_b = q2ids[b]
            inter = len(set_a & set_b)
            union = len(set_a | set_b)
            jacc = inter / union if union else 0.0
            rows.append({
                "query_a": a,
                "size_a": len(set_a),
                "query_b": b,
                "size_b": len(set_b),
                "overlap": inter,
                "union": union,
                "jaccard": round(jacc, 6),
                "overlap_pct_of_a": round(inter / len(set_a), 6) if set_a else 0.0,
                "overlap_pct_of_b": round(inter / len(set_b), 6) if set_b else 0.0,
            })
    return pd.DataFrame(rows)

def build_overlap_matrix(q2ids: dict[str, set]):
    # Symmetric overlap matrix
    qids = sorted(q2ids.keys())
    data = []
    for a in qids:
        row = []
        for b in qids:
            row.append(len(q2ids[a] & q2ids[b]))
        data.append(row)
    return pd.DataFrame(data, index=qids, columns=qids)

def list_top_overlaps(pairs_path: Path, n: int = 10):
    # List top n most similar query pairs
    df = pd.read_csv(pairs_path)
    df = df[df["query_a"] != df["query_b"]]
    df_sorted = df.sort_values("jaccard", ascending=False).head(n)
    print("\nTop overlapping query pairs:")
    print(df_sorted[["query_a", "query_b", "overlap", "jaccard"]].to_string(index=False))
    return df_sorted

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    q2ids = load_all_records(RAW_DIR)

    pair_df = build_pair_table(q2ids)
    matrix_df = build_overlap_matrix(q2ids)

    pair_out = OUT_DIR / "overlap_pairs.csv"
    matrix_out = OUT_DIR / "overlap_matrix.csv"
    sizes_out = OUT_DIR / "query_sizes.csv"

    pair_df.to_csv(pair_out, index=False)
    matrix_df.to_csv(matrix_out)
    pd.DataFrame(
        [{"query_id": q, "n_docs": len(s)} for q, s in sorted(q2ids.items())]
    ).to_csv(sizes_out, index=False)

    print(f"Wrote: {pair_out}")
    print(f"Wrote: {matrix_out}")
    print(f"Wrote: {sizes_out}")

    list_top_overlaps(pair_out)

if __name__ == "__main__":
    main()
