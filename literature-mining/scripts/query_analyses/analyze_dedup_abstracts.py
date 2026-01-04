import sys
from pathlib import Path
import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt

# Input/output paths
ABS_PATH = Path(__file__).resolve().parents[2] / "data" / "processed" / "abstracts" / "abstracts.csv"
TABLE_DIR = Path(__file__).resolve().parents[2] / "reports" / "tables" / "dedup_analyses"
FIGURE_DIR = Path(__file__).resolve().parents[2] / "reports" / "figures" / "dedup_analyses"

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


# Build query sizes df before/after dedup, with totals
def build_query_sizes_dedup_df(q2ids_raw: dict[str, set], q2ids_dedup: dict[str, set]):
    rows = []
    for q in sorted(q2ids_raw.keys()):
        rows.append(
            {
                "query_id": q,
                "n_docs_before": len(q2ids_raw[q]),
                "n_docs_after": len(q2ids_dedup.get(q, set())),
            }
        )

    rows.append(
        {
            "query_id": "__TOTAL__",
            "n_docs_before": sum(len(s) for s in q2ids_raw.values()),
            "n_docs_after": sum(len(s) for s in q2ids_dedup.values()),
        }
    )

    return pd.DataFrame(rows)


# Save query sizes table to CSV
def write_query_sizes_dedup(df_sizes: pd.DataFrame, out_path: Path):
    df_sizes.to_csv(out_path, index=False)


# Plot bars: before vs after per query
def plot_query_sizes_pre_post(df_sizes: pd.DataFrame, out_path: Path):
    dfp = df_sizes[df_sizes["query_id"] != "__TOTAL__"].copy()
    dfp = dfp.sort_values("n_docs_before", ascending=False)

    x = range(len(dfp))
    w = 0.45

    fig_w = max(10, 0.35 * len(dfp))
    fig, ax = plt.subplots(figsize=(fig_w, 6))

    ax.bar([i - w / 2 for i in x], dfp["n_docs_before"], width=w, label="before")
    ax.bar([i + w / 2 for i in x], dfp["n_docs_after"], width=w, label="after")

    ax.set_title("Query sizes: before vs after deduplication")
    ax.set_xlabel("Query ID")
    ax.set_ylabel("Number of articles")
    ax.set_xticks(list(x))
    ax.set_xticklabels(dfp["query_id"], rotation=45, ha="right")
    ax.legend()

    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def main():
    q2ids = load_query_sets_from_abstracts(ABS_PATH)
    q2ids_dedup, removed = dedup_keep_smallest(q2ids)

    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    df_sizes = build_query_sizes_dedup_df(q2ids, q2ids_dedup)

    csv_out = TABLE_DIR / "abstract_query_sizes_dedup.csv"
    plot_out = FIGURE_DIR / "abstract_query_sizes_pre_post.png"

    write_query_sizes_dedup(df_sizes, csv_out)
    plot_query_sizes_pre_post(df_sizes, plot_out)

    print(f"Wrote: {csv_out}")
    print(f"Wrote: {plot_out}")
    print(f"Dedup removals: {removed}")


if __name__ == "__main__":
    main()
