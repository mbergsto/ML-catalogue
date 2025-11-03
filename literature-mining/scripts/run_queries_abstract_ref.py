# Run queries from queries.yaml 
# Include references and abstracts

import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import yaml
from tqdm import tqdm
from pybliometrics.scopus import ScopusSearch, AbstractRetrieval


def load_queries(yaml_path: Path):
    # Load YAML
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("queries", [])


def ensure_api_key():
    # Load API key
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Missing API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    os.environ.setdefault("PYBLIOMETRICS_API_KEY", api_key)


def init_pybliometrics():
    # Init client
    import pybliometrics
    pybliometrics.scopus.init()


def write_jsonl(records, out_path: Path):
    # Write JSONL
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def fetch_refs_for_eid(eid: str, retries: int):
    # Get references
    for _ in range(retries):
        try:
            doc = AbstractRetrieval(eid, id_type="eid", view="REF")
            refs = doc.references or []
            return [
                {
                    "doi": getattr(r, "doi", None),
                    "title": getattr(r, "title", None),
                    "id": getattr(r, "id", None),
                    "sourcetitle": getattr(r, "sourcetitle", None),
                }
                for r in refs
            ]
        except Exception:
            continue
    return []


def fetch_abstract_for_eid(eid: str, retries: int):
    # Get abstract
    for _ in range(retries):
        try:
            doc = AbstractRetrieval(eid, id_type="eid", view="FULL")
            return getattr(doc, "abstract", None) or getattr(doc, "description", None)
        except Exception:
            continue
    return None


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    yaml_path = project_root / "queries" / "queries.yaml"
    base_out_dir = project_root / "data" / "raw-refs"
    base_out_dir.mkdir(parents=True, exist_ok=True)

    ensure_api_key()
    init_pybliometrics()
    queries = load_queries(yaml_path)
    if not queries:
        print("No queries found", file=sys.stderr)
        sys.exit(1)

    retries = int(os.getenv("REF_RETRIES", "3"))
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    summary = []

    # Use all queries; narrow with slicing if testing
    for q in queries[1]:
        qid = q["id"]
        qstr = q["query"]

        q_out_dir = base_out_dir / qid
        q_out_dir.mkdir(parents=True, exist_ok=True)

        out_file = q_out_dir / f"{qid}.jsonl"
        meta_file = q_out_dir / f"{qid}_meta.json"

        try:
            s = ScopusSearch(qstr, view="STANDARD")
            results = s.results or []
            records = []

            for doc in tqdm(results, desc=f"{qid}"):
                d = doc._asdict()
                d["query_id"] = qid
                d["retrieved_at"] = run_ts

                eid = d.get("eid")
                if eid:
                    d["ref_docs"] = fetch_refs_for_eid(eid, retries=retries)
                    d["abstract"] = fetch_abstract_for_eid(eid, retries=retries)
                else:
                    d["ref_docs"] = []
                    d["abstract"] = None

                records.append(d)

            write_jsonl(records, out_file)

            meta = {
                "query_id": qid,
                "query": qstr,
                "n_returned": len(records),
                "refs_attempted": len(records),
                "abstracts_attempted": len(records),
                "retrieved_at": run_ts,
                "fetch_refs": True,
                "fetch_abstracts": True,
            }
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"{qid}: {len(records)} results, refs+abstracts attempted for all")

            summary.append(meta)

        except Exception as e:
            print(f"{qid}: error: {e}", file=sys.stderr)

    (base_out_dir / "_run_summary.json").write_text(
        json.dumps({"run_at": run_ts, "queries": summary}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
