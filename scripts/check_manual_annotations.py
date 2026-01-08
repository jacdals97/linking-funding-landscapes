"""
Evaluate manual annotations by hitting the OpenAlex funder API.

Modes:
- id: look up each manual ID directly (https://api.openalex.org/funders/{id})
- search: search by the manual key (grant agency name)

Outputs a CSV with the original key, manual_id, status, matched_id, display_name, relevance_score (if search).
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Dict, Optional

import requests


def clean_id_val(v: str) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    if not v:
        return None
    v = v.replace('"', "")
    v = v.split("/")[-1]
    v = v.lstrip("Ff")
    return v or None


def load_manual(manual_path: Path) -> Dict[str, str]:
    data = json.loads(manual_path.read_text())
    return {k: clean_id_val(v) for k, v in data.items() if clean_id_val(v)}


def fetch_by_id(fid: str, mailto: str, sleep: float) -> dict:
    url = f"https://api.openalex.org/funders/F{fid}"
    params = {"mailto": mailto} if mailto else {}
    r = requests.get(url, params=params)
    time.sleep(sleep)
    if r.status_code != 200:
        return {"status": f"http_{r.status_code}"}
    data = r.json()
    return {
        "status": "ok",
        "matched_id": clean_id_val(data.get("id")),
        "display_name": data.get("display_name"),
        "relevance_score": None,
    }


def fetch_by_search(term: str, mailto: str, sleep: float) -> dict:
    url = "https://api.openalex.org/funders"
    params = {"search": term, "mailto": mailto} if mailto else {"search": term}
    r = requests.get(url, params=params)
    time.sleep(sleep)
    if r.status_code != 200:
        return {"status": f"http_{r.status_code}"}
    data = r.json()
    if not data.get("results"):
        return {"status": "no_results"}
    top = data["results"][0]
    return {
        "status": "ok",
        "matched_id": clean_id_val(top.get("id")),
        "display_name": top.get("display_name"),
        "relevance_score": top.get("relevance_score"),
    }


def main():
    parser = argparse.ArgumentParser(description="Check manual annotations against OpenAlex API.")
    parser.add_argument("--manual-annotations", default=Path("manual_annotations.json"), type=Path)
    parser.add_argument("--mode", choices=["id", "search"], default="id")
    parser.add_argument("--mailto", default=None, help="Optional mailto for OpenAlex")
    parser.add_argument("--out", default=Path("manual_annotations_checked.csv"), type=Path)
    parser.add_argument("--max", type=int, default=None, help="Limit number of records to check")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between requests (seconds)")
    args = parser.parse_args()

    manual = load_manual(args.manual_annotations)
    items = list(manual.items())
    if args.max:
        items = items[: args.max]

    rows = []
    for key, fid in items:
        if args.mode == "id":
            res = fetch_by_id(fid, mailto=args.mailto, sleep=args.sleep)
        else:
            res = fetch_by_search(key, mailto=args.mailto, sleep=args.sleep)
        rows.append(
            {
                "grant_agency": key,
                "manual_id": fid,
                "status": res.get("status"),
                "matched_id": res.get("matched_id"),
                "display_name": res.get("display_name"),
                "relevance_score": res.get("relevance_score"),
            }
        )

    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["grant_agency", "manual_id", "status", "matched_id", "display_name", "relevance_score"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} checks to {args.out}")


if __name__ == "__main__":
    main()
