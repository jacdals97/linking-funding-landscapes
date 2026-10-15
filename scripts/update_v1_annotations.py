#!/usr/bin/env python3
"""Refresh the v1 manual-review tables from the current pipeline outputs.

Rows are joined to current data by normalized DOI.  Pipeline-controlled fields
are replaced, prior values are retained for audit, and manual-review fields are
cleared whenever the current paper/funder data no longer agrees with v1.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ANNOTATIONS = REPO_ROOT / "data" / "v1_annotations"
DEFAULT_OUTPUT = REPO_ROOT / "outputs" / "v1_annotations_updated"
DEFAULT_SOURCE_FUNDERS = REPO_ROOT / "Results" / "SourcePaperFunders"
DEFAULT_WOS_MAIN = REPO_ROOT / "data" / "WoS_2025_main.parquet"
DEFAULT_WOS_MATCHES = REPO_ROOT / "Results" / "grant_institutions_2025_affiliation_match_20251023.parquet"
DEFAULT_PATHS_FILES = [REPO_ROOT / "PATHS.md", REPO_ROOT / "scisci_skills" / "PATHS.md"]

SUMMARY_COLUMNS = ["Avg. Recall", "Avg error rate", "Hit rate", "Avg. Precision"]
REVIEW_COLUMNS = [
    "Number of Funders Indicated by Paper",
    "Number of Correct Matches",
    "Number of Wrong Matches",
    "Coder",
    "Paper level recall",
    "Paper level error rate",
    "Paper level hit rate",
    "Paper level precision",
    "Paper is complete hit",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--annotations-dir", type=Path, default=DEFAULT_ANNOTATIONS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-funders-dir", type=Path, default=DEFAULT_SOURCE_FUNDERS)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--wos-matches", type=Path, default=DEFAULT_WOS_MATCHES)
    parser.add_argument(
        "--dimensions-organizations",
        type=Path,
        default=None,
        help="Dimensions organizations parquet file or directory (used for current Wikipedia URLs).",
    )
    return parser.parse_args()


def registered_path(key: str) -> Path:
    for path in DEFAULT_PATHS_FILES:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            found_key, value = line.split("=", 1)
            if found_key.strip() == key:
                return Path(value.strip()).expanduser()
    raise KeyError(f"Could not resolve {key!r} from: {', '.join(map(str, DEFAULT_PATHS_FILES))}")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_doi(value: Any) -> str:
    text = clean(value).lower()
    text = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", text)
    return text.rstrip("/")


def doi_url(value: Any) -> str:
    doi = normalize_doi(value)
    return f"https://doi.org/{doi}" if doi else ""


def normalized_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", clean(value)).casefold()
    return " ".join(text.split())


def normalized_url(value: Any) -> str:
    normalized = clean(value).lower().replace("http://", "https://").rstrip("/")
    if "openalex.org/" in normalized:
        entity_id = normalized.rsplit("/", 1)[-1]
        if re.fullmatch(r"[afiscwpt]\d+", entity_id):
            # Treat legacy /funders/f123 and canonical /F123 URLs as the same entity.
            return f"openalex:{entity_id.upper()}"
    return normalized


def unique_in_order(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean(value)
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def split_groups(value: Any, separator: str = "||") -> list[str]:
    return [part.strip() for part in clean(value).split(separator) if part.strip()]


def primary_name_signature(value: Any, separator: str = "||") -> Counter[str]:
    groups = split_groups(value, separator)
    return Counter(normalized_text(group.split(";", 1)[0]) for group in groups if group)


def url_signature(value: Any, separator: str = "||") -> Counter[str]:
    return Counter(normalized_url(group) for group in split_groups(value, separator) if group)


def read_annotations(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [name for name in (reader.fieldnames or []) if name and not name.startswith("_duplicated_")]
        rows = []
        for raw in reader:
            row = {name: clean(raw.get(name)) for name in fieldnames}
            # Blank spreadsheet/formula rows are not annotation records.
            if normalize_doi(row.get("DOI")):
                rows.append(row)
    if not rows:
        raise ValueError(f"No DOI-bearing annotation rows found in {path}")
    return rows, fieldnames


def doi_filter_expression(column: str, targets: set[str]) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.to_lowercase()
        .str.replace(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "")
        .str.strip_suffix("/")
        .is_in(sorted(targets))
    )


def collect_source_rows(path: Path, targets: set[str], label: str) -> pl.DataFrame:
    print(f"[{label}] scanning {path}", flush=True)
    started = time.perf_counter()
    lazy = (
        pl.scan_parquet(path)
        .select(["paper_id", "title", "doi", "funder_name", "funder_id", "has_funding_info"])
        .filter(doi_filter_expression("doi", targets))
    )
    try:
        frame = lazy.collect(engine="streaming")
    except TypeError:  # Polars < 1.25
        frame = lazy.collect(streaming=True)
    print(
        f"[{label}] selected {frame.height:,} rows / {frame['paper_id'].n_unique():,} paper records "
        f"in {time.perf_counter() - started:.1f}s",
        flush=True,
    )
    return frame


def aggregate_source(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frame.iter_rows(named=True):
        grouped[normalize_doi(row["doi"])].append(row)

    result: dict[str, dict[str, Any]] = {}
    for doi, rows in grouped.items():
        record_ids = unique_in_order(row["paper_id"] for row in rows)
        titles = unique_in_order(row["title"] for row in rows)
        names = unique_in_order(row["funder_name"] for row in rows)
        ids = unique_in_order(row["funder_id"] for row in rows)
        funders: list[dict[str, str]] = []
        seen_funders: set[tuple[str, str]] = set()
        for row in rows:
            pair = (clean(row["funder_name"]), clean(row["funder_id"]))
            if pair == ("", "") or pair in seen_funders:
                continue
            seen_funders.add(pair)
            funders.append({"name": pair[0], "id": pair[1]})
        signatures: set[tuple[str, ...]] = set()
        for record_id in record_ids:
            record_names = sorted(
                normalized_text(row["funder_name"])
                for row in rows
                if clean(row["paper_id"]) == record_id and clean(row["funder_name"])
            )
            signatures.add(tuple(record_names))
        result[doi] = {
            "record_ids": record_ids,
            "title": titles[0] if titles else "",
            "funder_names": names,
            "funder_ids": ids,
            "funders": funders,
            "ambiguous": len(signatures) > 1,
        }
    return result


def load_dimensions_orgs(path: Path, target_ids: set[str]) -> dict[str, dict[str, str]]:
    glob = str(path / "*.parquet") if path.is_dir() else str(path)
    if not target_ids:
        return {}
    frame = (
        pl.scan_parquet(glob)
        .select(["id", "name", "wikipedia_url"])
        .filter(pl.col("id").is_in(sorted(target_ids)))
        .collect()
    )
    return {
        clean(row["id"]): {
            "name": clean(row["name"]),
            "wikipedia_url": clean(row["wikipedia_url"]),
        }
        for row in frame.iter_rows(named=True)
    }


def openalex_id(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    return "https://openalex.org/" + text.rsplit("/", 1)[-1].upper()


def wos_openalex_url(gid: Any, source: Any) -> str:
    gid_text = clean(gid)
    source_text = clean(source).lower()
    if not gid_text or not source_text.endswith("s") or len(gid_text) < 4:
        return ""
    return f"https://openalex.org/{source_text[0].upper()}{gid_text[3:]}"


def collect_wos(rows: list[dict[str, str]], main_path: Path, matches_path: Path) -> dict[str, dict[str, Any]]:
    targets = {normalize_doi(row["DOI"]) for row in rows}
    print(f"[web_of_science] scanning {main_path}", flush=True)
    main = (
        pl.scan_parquet(main_path)
        .select(["UT", "TI", "DI"])
        .filter(doi_filter_expression("DI", targets))
        .collect(engine="streaming")
    )
    uts = set(main["UT"].drop_nulls().to_list())
    print(f"[web_of_science] matched {main.height:,} WOS records; scanning current matches", flush=True)
    matches = (
        pl.scan_parquet(matches_path)
        .select(["UT", "grant_agency", "gid", "ids:ror", "source", "display_name"])
        .filter(pl.col("UT").is_in(sorted(uts)))
        .collect(engine="streaming")
    )

    match_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in matches.iter_rows(named=True):
        match_rows[clean(row["UT"])].append(row)

    by_doi: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in main.iter_rows(named=True):
        ut = clean(row["UT"])
        current = match_rows.get(ut, [])
        display_names = unique_in_order(clean(item["display_name"]) or "NOT MATCHED" for item in current)
        urls = unique_in_order(wos_openalex_url(item["gid"], item["source"]) for item in current)
        funders: list[dict[str, str]] = []
        seen_funders: set[tuple[str, str, str, str]] = set()
        for item in current:
            funder = {
                "name": clean(item["display_name"]) or clean(item["grant_agency"]) or "NOT MATCHED",
                "source_name": clean(item["grant_agency"]),
                "openalex_url": wos_openalex_url(item["gid"], item["source"]),
                "ror_url": clean(item["ids:ror"]),
            }
            signature = tuple(funder.values())
            if signature not in seen_funders:
                seen_funders.add(signature)
                funders.append(funder)
        by_doi[normalize_doi(row["DI"])].append(
            {
                "record_id": ut,
                "title": clean(row["TI"]),
                "display_names": display_names,
                "urls": urls,
                "funders": funders,
            }
        )

    result: dict[str, dict[str, Any]] = {}
    for doi, records in by_doi.items():
        signatures = {
            (tuple(sorted(map(normalized_text, record["display_names"]))), tuple(sorted(map(normalized_url, record["urls"]))))
            for record in records
        }
        consolidated_funders: list[dict[str, str]] = []
        seen_funders: set[tuple[str, str, str, str]] = set()
        for record in records:
            for funder in record["funders"]:
                signature = tuple(funder.values())
                if signature not in seen_funders:
                    seen_funders.add(signature)
                    consolidated_funders.append(funder)
        result[doi] = {
            "record_ids": unique_in_order(record["record_id"] for record in records),
            "title": records[0]["title"],
            "display_names": unique_in_order(name for record in records for name in record["display_names"]),
            "urls": unique_in_order(url for record in records for url in record["urls"]),
            "funders": consolidated_funders,
            "ambiguous": len(signatures) > 1,
        }
    return result


def apply_review_status(
    row: dict[str, str],
    *,
    changed_fields: list[str],
    missing: bool,
    ambiguous: bool,
) -> None:
    reasons = []
    if missing:
        reasons.append("missing_from_current_pipeline")
    if ambiguous:
        reasons.append("ambiguous_current_records")
    if changed_fields:
        reasons.append("pipeline_data_changed")
    needs_revision = bool(reasons)
    row["needs_revision"] = "true" if needs_revision else "false"
    row["revision_reason"] = " | ".join(reasons)
    row["changed_fields"] = " | ".join(changed_fields)
    row["revision_status"] = "needs_revision" if needs_revision else "up_to_date"
    for column in SUMMARY_COLUMNS:
        if column in row:
            row[column] = ""
    if needs_revision:
        for column in REVIEW_COLUMNS:
            if column in row:
                row[column] = ""


def reconcile_source(
    source: str,
    rows: list[dict[str, str]],
    current: dict[str, dict[str, Any]],
    orgs: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    output = []
    name_column = "funder_display_name" if source == "openalex" else "display_names"
    url_column = "WIKI_data_url" if source == "dimensions" else None
    old_separator = ";" if source == "openalex" else "||"
    new_separator = ";" if source == "openalex" else " || "

    for original in rows:
        row = dict(original)
        doi = normalize_doi(row["DOI"])
        found = current.get(doi)
        row["previous_title"] = row.get("title", "")
        row[f"previous_{name_column}"] = row.get(name_column, "")
        if url_column:
            row[f"previous_{url_column}"] = row.get(url_column, "")
        row["DOI"] = doi_url(doi)

        changed_fields: list[str] = []
        if found:
            current_names = found["funder_names"]
            row["title"] = found["title"]
            row[name_column] = new_separator.join(current_names)
            row["current_record_ids"] = " || ".join(found["record_ids"])
            row["current_funder_ids"] = " || ".join(openalex_id(v) if source == "openalex" else v for v in found["funder_ids"])
            if normalized_text(row["previous_title"]) != normalized_text(row["title"]):
                changed_fields.append("title")
            if primary_name_signature(row[f"previous_{name_column}"], old_separator) != primary_name_signature(
                row[name_column], new_separator
            ):
                changed_fields.append(name_column)
            if url_column:
                organizations = orgs or {}
                urls = [organizations.get(funder_id, {}).get("wikipedia_url", "") for funder_id in found["funder_ids"]]
                row[url_column] = " || ".join(urls)
                if url_signature(row[f"previous_{url_column}"]) != url_signature(row[url_column]):
                    changed_fields.append(url_column)
        else:
            row["current_record_ids"] = ""
            row["current_funder_ids"] = ""

        apply_review_status(
            row,
            changed_fields=changed_fields,
            missing=found is None,
            ambiguous=bool(found and found["ambiguous"]),
        )
        output.append(row)
    return output


def reconcile_wos(rows: list[dict[str, str]], current: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    output = []
    for original in rows:
        row = dict(original)
        doi = normalize_doi(row["DOI"])
        found = current.get(doi)
        row["previous_title"] = row.get("title", "")
        row["previous_display_names"] = row.get("display_names", "")
        row["previous_OA_URLs"] = row.get("OA_URLs", "")
        row["DOI"] = doi_url(doi)
        changed_fields: list[str] = []
        if found:
            row["title"] = found["title"]
            row["display_names"] = " || ".join(found["display_names"])
            row["OA_URLs"] = " || ".join(found["urls"])
            row["current_record_ids"] = " || ".join(found["record_ids"])
            row["current_funder_ids"] = row["OA_URLs"]
            if normalized_text(row["previous_title"]) != normalized_text(row["title"]):
                changed_fields.append("title")
            if primary_name_signature(row["previous_display_names"]) != primary_name_signature(row["display_names"]):
                changed_fields.append("display_names")
            if url_signature(row["previous_OA_URLs"]) != url_signature(row["OA_URLs"]):
                changed_fields.append("OA_URLs")
        else:
            row["current_record_ids"] = ""
            row["current_funder_ids"] = ""
        apply_review_status(
            row,
            changed_fields=changed_fields,
            missing=found is None,
            ambiguous=bool(found and found["ambiguous"]),
        )
        output.append(row)
    return output


def write_csv(path: Path, rows: list[dict[str, str]], original_fields: list[str]) -> None:
    audit_fields = [
        "current_record_ids",
        "current_funder_ids",
        "needs_revision",
        "revision_status",
        "revision_reason",
        "changed_fields",
    ]
    previous_fields = unique_in_order(key for row in rows for key in row if key.startswith("previous_"))
    fieldnames = unique_in_order(original_fields + audit_fields + previous_fields)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    if args.dimensions_organizations is None:
        args.dimensions_organizations = registered_path("dimensions_raw_parquet_root") / "organizations"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    loaded: dict[str, tuple[list[dict[str, str]], list[str]]] = {}
    for source in ["dimensions", "openalex", "web_of_science"]:
        loaded[source] = read_annotations(args.annotations_dir / f"{source}.csv")

    outputs: dict[str, list[dict[str, str]]] = {}
    for source in ["dimensions", "openalex"]:
        rows, _ = loaded[source]
        targets = {normalize_doi(row["DOI"]) for row in rows}
        frame = collect_source_rows(
            args.source_funders_dir / f"{source}_paper_funders.parquet", targets, source
        )
        current = aggregate_source(frame)
        orgs = None
        if source == "dimensions":
            ids = set(frame["funder_id"].drop_nulls().to_list())
            orgs = load_dimensions_orgs(args.dimensions_organizations, ids)
        outputs[source] = reconcile_source(source, rows, current, orgs)

    wos_rows, _ = loaded["web_of_science"]
    outputs["web_of_science"] = reconcile_wos(
        wos_rows, collect_wos(wos_rows, args.wos_main, args.wos_matches)
    )

    summary = []
    for source, rows in outputs.items():
        write_csv(args.output_dir / f"{source}.csv", rows, loaded[source][1])
        revision_rows = [row for row in rows if row["needs_revision"] == "true"]
        missing_rows = [row for row in rows if "missing_from_current_pipeline" in row["revision_reason"]]
        ambiguous_rows = [row for row in rows if "ambiguous_current_records" in row["revision_reason"]]
        counts = Counter(field for row in revision_rows for field in row["changed_fields"].split(" | ") if field)
        summary.append(
            {
                "source": source,
                "rows": len(rows),
                "up_to_date": len(rows) - len(revision_rows),
                "needs_revision": len(revision_rows),
                "missing": len(missing_rows),
                "ambiguous": len(ambiguous_rows),
                "changed_field_counts": json.dumps(dict(sorted(counts.items())), sort_keys=True),
            }
        )

    summary_fields = list(summary[0])
    with (args.output_dir / "revision_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary)
    (args.output_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "annotations_dir": str(args.annotations_dir.resolve()),
                "source_funders_dir": str(args.source_funders_dir.resolve()),
                "wos_main": str(args.wos_main.resolve()),
                "wos_matches": str(args.wos_matches.resolve()),
                "comparison": "normalized DOI join; canonical primary funder names plus available entity URLs",
                "summary": summary,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"Wrote updated annotations and audit summary to {args.output_dir}")


if __name__ == "__main__":
    main()
