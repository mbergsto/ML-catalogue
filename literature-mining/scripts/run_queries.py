import os
import sys
import json
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import yaml
from tqdm import tqdm
from pybliometrics.scopus import ScopusSearch


def load_queries(yaml_path: Path):
    # Load YAML queries
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("queries", [])


def ensure_api_key():
    # Load API key from .env and export for pybliometrics
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Missing API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    os.environ.setdefault("PYBLIOMETRICS_API_KEY", api_key)


def init_pybliometrics():
    # Init pybliometrics
    import pybliometrics
    pybliometrics.scopus.init()


def write_jsonl(records, out_path: Path):
    # Append-safe JSONL writer
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    # Resolve paths
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    yaml_path = project_root / "queries" / "queries.yaml"
    out_dir = project_root / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Setup
    ensure_api_key()
    init_pybliometrics()
    queries = load_queries(yaml_path)
    if not queries:
        print("No queries found", file=sys.stderr)
        sys.exit(1)

    # Run queries
    run_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    summary = []

    for q in queries:
        qid = q["id"]
        qstr = q["query"]

        out_file = out_dir / f"{qid}.jsonl"
        meta_file = out_dir / f"{qid}_meta.json"

        try:
            s = ScopusSearch(qstr, view="STANDARD")
            results = s.results or []
            records = []
            for doc in results:
                d = doc._asdict()
                d["query_id"] = qid
                d["retrieved_at"] = run_ts
                records.append(d)

            write_jsonl(records, out_file)

            meta = {
                "query_id": qid,
                "query": qstr,
                "n_returned": len(records),
                "retrieved_at": run_ts,
            }
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"{qid}: {len(records)}")

            summary.append(meta)

        except Exception as e:
            print(f"{qid}: error: {e}", file=sys.stderr)

    # Write run summary
    (out_dir / "_run_summary.json").write_text(
        json.dumps({"run_at": run_ts, "queries": summary}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
