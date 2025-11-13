"""
Analyze JSONL files in each subfolder of a given root directory.

For each subfolder:
- Counts records, empty ref_docs, and empty abstracts
- Summarizes reference frequencies
- Writes _analysis_summary.txt and _reference_counts.csv

Also creates an overall summary CSV at the root.
"""

from collections import Counter
from pathlib import Path
import pandas as pd
import json

# SETTINGS
ROOT_PATH = Path(__file__).resolve().parents[2] / "data" / "short-raw-refs-abs"  # set main path here
TOP_N = 10  # how many top references to list per folder

# Basic normalization helpers
def norm_doi(value):
    if not value:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    value = str(value).strip().lower()
    value = value.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return value

def norm_title(value):
    if not value:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()

def ref_key(ref):
    doi = norm_doi(ref.get("doi"))
    title = norm_title(ref.get("title") or ref.get("sourcetitle"))
    refid = str(ref.get("id") or "").strip()
    return (doi, title, refid)

# Abstract helper 
def extract_abstract(rec) -> str:
    val = rec.get("abstract")
    if val is None:
        return ""
    if isinstance(val, (list, tuple)):
        # pick first non-empty stringy value
        for v in val:
            s = "" if v is None else str(v).strip()
            if s:
                return s
        return ""
    if isinstance(val, dict):
        # common patterns if nested
        for k in ("text", "value", "#text"):
            if k in val and str(val[k]).strip():
                return str(val[k]).strip()
        return ""
    return str(val).strip()

# Folder processing
def analyze_folder(folder: Path, top_n: int = 10):
    jsonl_files = sorted(folder.glob("*.jsonl"))
    if not jsonl_files:
        jsonl_files = sorted(folder.rglob("*.jsonl"))
    if not jsonl_files:
        return None

    n_records = 0
    n_empty_refs = 0
    n_empty_abs = 0
    total_refs = 0
    ref_counts = Counter()

    for file in jsonl_files:
        with file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                n_records += 1

                # Abstract:
                abs_txt = extract_abstract(rec)
                if not abs_txt:
                    n_empty_abs += 1

                # References
                refs = rec.get("ref_docs")
                if not refs:
                    n_empty_refs += 1
                    continue

                total_refs += len(refs)
                for r in refs:
                    if isinstance(r, dict):
                        ref_counts[ref_key(r)] += 1

    # Summary
    summary_lines = [
        f"Folder: {folder.name}",
        f"JSONL files: {len(jsonl_files)}",
        f"Records: {n_records}",
        f"Empty ref_docs: {n_empty_refs} ({n_empty_refs / n_records:.2%})",
        f"Empty abstracts: {n_empty_abs} ({n_empty_abs / n_records:.2%})",
        f"Total references: {total_refs}",
        f"Unique references: {len(ref_counts)}",
        "",
        "Top references (count, DOI, title):",
    ]

    top_refs = [{"doi": k[0], "title": k[1], "refid": k[2], "count": c}
                for k, c in ref_counts.most_common(top_n)]

    for r in top_refs:
        short_title = (r["title"][:100] + "…") if len(r["title"]) > 100 else r["title"]
        summary_lines.append(f"{r['count']}\t{r['doi']}\t{short_title}")

    (folder / "_analysis_summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    # Save all reference counts (sorted by count descending)
    counts_df = pd.DataFrame(
        [{"doi": k[0], "title": k[1], "refid": k[2], "count": c} for k, c in ref_counts.items()]
    )
    counts_df = counts_df.sort_values("count", ascending=False)
    counts_df.to_csv(folder / "_reference_counts.csv", index=False)

    print(f"\nAnalyzed: {folder.name}")
    print(f"  Records: {n_records}")
    print(f"  Empty ref_docs: {n_empty_refs} ({n_empty_refs / n_records:.2%})")
    print(f"  Empty abstracts: {n_empty_abs} ({n_empty_abs / n_records:.2%})")
    print(f"  Unique refs: {len(ref_counts)}")

    return {
        "folder": folder.name,
        "jsonl_files": len(jsonl_files),
        "records": n_records,
        "empty_ref_docs": n_empty_refs,
        "empty_ref_docs_share": round(n_empty_refs / n_records, 3) if n_records else 0,
        "empty_abstracts": n_empty_abs,
        "empty_abstracts_share": round(n_empty_abs / n_records, 3) if n_records else 0,
        "total_references": total_refs,
        "unique_references": len(ref_counts),
    }

# Main
def main():
    if not ROOT_PATH.exists() or not ROOT_PATH.is_dir():
        raise FileNotFoundError(f"Root folder not found: {ROOT_PATH}")

    subfolders = [d for d in sorted(ROOT_PATH.iterdir()) if d.is_dir()]
    if not subfolders:
        print("No subfolders found.")
        return

    results = []
    for sub in subfolders:
        res = analyze_folder(sub, top_n=TOP_N)
        if res:
            results.append(res)

    if results:
        df = pd.DataFrame(results)
        out_path = ROOT_PATH / "_overall_folder_summary.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved overall summary → {out_path}")

if __name__ == "__main__":
    main()
