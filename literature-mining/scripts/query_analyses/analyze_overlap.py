import os
import sys
import json
from pathlib import Path
from collections import defaultdict
import pandas as pd

# Input/output directories
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "short-raw-refs-abs"
OUT_DIR = Path(__file__).resolve().parents[2] / "reports" / "tables" / "overlap_analysis"


# Read a JSONL file -> list of dicts
def read_jsonl(p: Path):
    out = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


# Read a JSON file -> list with one dict
def read_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return [json.load(f)]


# Normalize DOI to lowercase without prefix
def norm_doi(x: str | None):
    if not x:
        return None
    x = x.strip().lower()
    x = x.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return x


# Extract unique record id: prefer DOI
def extract_id(rec: dict):
    return norm_doi(
        rec.get("doi") or rec.get("dc:identifier") or rec.get("prism:doi")
    )


# Infer query_id: prefer field inside record; fallback to parent directory name
def infer_query_id(file_path: Path, rec: dict) -> str:
    return rec.get("query_id") or file_path.parent.name


# Load all records recursively under RAW_DIR
def load_all_records(raw_dir: Path):
    files = sorted(
        [*raw_dir.rglob("*.jsonl"), *raw_dir.rglob("*.json")]
    )

    if not files:
        print(f"No raw files in {raw_dir}", file=sys.stderr)
        sys.exit(1)

    q2ids: dict[str, set] = defaultdict(set)

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


# Build table with pairwise overlap and Jaccard similarity
def build_pair_table(q2ids: dict[str, set]):
    rows = []
    qids = sorted(q2ids.keys())
    for i, a in enumerate(qids):
        for b in qids[i:]:
            set_a = q2ids[a]
            set_b = q2ids[b]
            inter = len(set_a & set_b)
            union = len(set_a | set_b)
            jacc = inter / union if union else 0.0
            rows.append(
                {
                    "query_a": a,
                    "size_a": len(set_a),
                    "query_b": b,
                    "size_b": len(set_b),
                    "overlap": inter,
                    "union": union,
                    "jaccard": round(jacc, 6),
                    "overlap_pct_of_a": round(inter / len(set_a), 6)
                    if set_a
                    else 0.0,
                    "overlap_pct_of_b": round(inter / len(set_b), 6)
                    if set_b
                    else 0.0,
                }
            )
    return pd.DataFrame(rows)


# Create a symmetric overlap matrix
def build_overlap_matrix(q2ids: dict[str, set]):
    qids = sorted(q2ids.keys())
    data = []
    for a in qids:
        row = []
        for b in qids:
            row.append(len(q2ids[a] & q2ids[b]))
        data.append(row)
    return pd.DataFrame(data, index=qids, columns=qids)


# Print the top-N most overlapping query pairs
def list_top_overlaps(pairs_path: Path, n: int = 20):
    df = pd.read_csv(pairs_path)
    df = df[df["query_a"] != df["query_b"]]  # remove identical pairs
    df_sorted = df.sort_values("jaccard", ascending=False).head(n)
    print("\nTop overlapping query pairs:")
    print(df_sorted[["query_a", "query_b", "overlap", "overlap_pct_of_b", "jaccard"]].to_string(index=False))
    return df_sorted

# Deduplicate across queries: smallest query keeps the record id
def dedup_keep_smallest(q2ids: dict[str, set]):
    # rid -> queries that contain it
    rid2qs: dict[str, list[str]] = defaultdict(list)
    for qid, ids in q2ids.items():
        for rid in ids:
            rid2qs[rid].append(qid)

    out = {q: set(ids) for q, ids in q2ids.items()}

    removed = 0
    for rid, qs in rid2qs.items():
        if len(qs) <= 1:
            continue

        # Keep in smallest group (stable tie-break on name)
        keep = min(qs, key=lambda q: (len(out[q]), q))

        for q in qs:
            if q != keep and rid in out[q]:
                out[q].remove(rid)
                removed += 1

    return out, removed

# Write query sizes before/after dedup, with totals
def write_query_sizes_dedup(
    q2ids_raw: dict[str, set],
    q2ids_dedup: dict[str, set],
    out_path: Path,
):
    rows = []
    for q in sorted(q2ids_raw.keys()):
        rows.append(
            {
                "query_id": q,
                "n_docs_before": len(q2ids_raw.get(q, set())),
                "n_docs_after": len(q2ids_dedup.get(q, set())),
            }
        )

    # Add total row
    rows.append(
        {
            "query_id": "__TOTAL__",
            "n_docs_before": sum(len(s) for s in q2ids_raw.values()),
            "n_docs_after": sum(len(s) for s in q2ids_dedup.values()),
        }
    )

    pd.DataFrame(rows).to_csv(out_path, index=False)


# Main entry point: load, compute tables, write results
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
    
    q2ids_dedup, removed = dedup_keep_smallest(q2ids)

    sizes_dedup_out = OUT_DIR / "query_sizes_dedup.csv"
    write_query_sizes_dedup(q2ids, q2ids_dedup, sizes_dedup_out)

    print(f"Wrote: {sizes_dedup_out}")
    print(f"Dedup removals: {removed}")

    list_top_overlaps(pair_out)


if __name__ == "__main__":
    main()
