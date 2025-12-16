import sys
from pathlib import Path
import pandas as pd
from collections import defaultdict

# Input/output paths
ABS_PATH = Path(__file__).resolve().parents[2] / "data" / "processed" / "abstracts" / "abstracts.csv"
OUT_DIR = Path(__file__).resolve().parents[2] / "reports" / "tables" / "dedup_analyses"


# Load all records from abstracts.csv -> query_id -> set(doi)
def load_query_sets_from_abstracts(csv_path: Path):
    if not csv_path.exists():
        print(f"Missing file: {csv_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(csv_path)

    need = {"query_id", "doi"}
    missing = need - set(df.columns)
    if missing:
        print(f"Missing columns in {csv_path}: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)

    q2ids: dict[str, set] = defaultdict(set)

    for _, row in df.iterrows():
        qid = row["query_id"]
        doi = row["doi"]
        if not qid or not doi:
            continue
        q2ids[str(qid)].add(str(doi))

    return q2ids


# Deduplicate across queries: smallest query keeps the DOI
def dedup_keep_smallest(q2ids: dict[str, set]):
    # doi -> queries that contain it
    doi2qs: dict[str, list[str]] = defaultdict(list)
    for qid, dois in q2ids.items():
        for doi in dois:
            doi2qs[doi].append(qid)

    out = {q: set(dois) for q, dois in q2ids.items()}

    removed = 0
    for doi, qs in doi2qs.items():
        if len(qs) <= 1:
            continue

        # Keep in smallest group (stable tie-break on name)
        keep = min(qs, key=lambda q: (len(out[q]), q))

        for q in qs:
            if q != keep and doi in out[q]:
                out[q].remove(doi)
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
                "n_docs_before": len(q2ids_raw[q]),
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


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    q2ids = load_query_sets_from_abstracts(ABS_PATH)
    q2ids_dedup, removed = dedup_keep_smallest(q2ids)

    out_path = OUT_DIR / "abstract_query_sizes_dedup.csv"
    write_query_sizes_dedup(q2ids, q2ids_dedup, out_path)

    print(f"Wrote: {out_path}")
    print(f"Dedup removals: {removed}")


if __name__ == "__main__":
    main()
