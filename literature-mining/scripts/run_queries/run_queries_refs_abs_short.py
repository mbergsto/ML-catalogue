# Run queries from queries.yaml 
# Include references and abstracts
# Less metadata for efficiency

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
    # Load YAML queries
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
    # os.environ.setdefault("PYBLIOMETRICS_API_KEY", api_key)   # Changed to be able to extract new key
    os.environ["PYBLIOMETRICS_API_KEY"] = api_key


def init_pybliometrics():
    # Init client
    import pybliometrics
    api_key = os.environ["PYBLIOMETRICS_API_KEY"]
    pybliometrics.scopus.init(keys=[api_key])


def write_jsonl(records, out_path: Path):
    # Write JSONL
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def fetch_full_for_eid(eid: str, retries: int):
    # Get references and abstract in one call
    for _ in range(retries):
        try:
            return AbstractRetrieval(eid, id_type="eid", view="FULL")
        except Exception:
            continue
    return None


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parents[1]
    yaml_path = project_root / "queries" / "queries.yaml"
    base_out_dir = project_root / "data" / "short-raw-refs-abs"
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

    # Use all queries, narrow with slicing if testing
    for q in queries:
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

            for i, doc in enumerate(tqdm(results, desc=f"{qid}")):
                # Collect minimal metadata
                eid = getattr(doc, "eid", None)
                d = {
                    "eid": eid,
                    "doi": getattr(doc, "doi", None),
                    "title": getattr(doc, "title", None),
                    "query_id": qid,
                    "retrieved_at": run_ts,
                }

                if eid:
                    # Get both abstract and references in one retrieval call
                    full_doc = fetch_full_for_eid(eid, retries=retries)
                    if full_doc:
                        d["abstract"] = getattr(full_doc, "abstract", None) or getattr(full_doc, "description", None)
                        refs = full_doc.references or []
                        d["ref_docs"] = [
                            {
                                "doi": getattr(r, "doi", None),
                                "title": getattr(r, "title", None),
                            }
                            for r in refs
                        ]
                    else:
                        d["ref_docs"] = []
                        d["abstract"] = None
                else:
                    d["ref_docs"] = []
                    d["abstract"] = None
                    
                if i % 500 == 0 and full_doc:
                    print(f"Quota check ({i} docs): {full_doc.get_key_remaining_quota()} retrievals left")
                    print(f"[{qid}] SEARCH quota resets at:", s.get_key_reset_time())

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
