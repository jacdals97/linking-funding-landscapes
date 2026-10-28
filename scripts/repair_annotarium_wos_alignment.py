#!/usr/bin/env python3
"""Repair shifted WOS funder metadata without changing rows, labels, or annotations."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from build_annotarium_funder_evaluation import (
    DEFAULT_NORMALIZED_INSTITUTIONS,
    DEFAULT_ROR_REGISTRY,
    DEFAULT_WOS_MATCHES,
    build_enrichment_index,
    enriched_funder_item,
)
from build_annotarium_wos_expansion import aligned_wos_funders, load_wos_matches_by_label
from update_v1_annotations import normalized_text


WOS_COLUMN = "wos_detected_funders"
HUMAN_ANNOTATION_SUFFIXES = (
    "_declared_funder_count",
    "_correct_match_count",
    "_wrong_match_count",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records_csv", type=Path)
    parser.add_argument("--output", type=Path, default=None, help="Defaults to replacing records_csv atomically.")
    parser.add_argument("--backup", type=Path, default=None, help="Optional byte-for-byte backup of the input CSV.")
    parser.add_argument("--wos-matches", type=Path, default=DEFAULT_WOS_MATCHES)
    parser.add_argument("--normalized-institutions", type=Path, default=DEFAULT_NORMALIZED_INSTITUTIONS)
    parser.add_argument("--ror-registry", type=Path, default=DEFAULT_ROR_REGISTRY)
    parser.add_argument("--report", type=Path, default=None)
    return parser.parse_args()


def read_records(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    if "record_id" not in fieldnames or WOS_COLUMN not in fieldnames:
        raise ValueError(f"{path} must contain record_id and {WOS_COLUMN}")
    return fieldnames, rows


def is_not_matched(item: dict[str, Any]) -> bool:
    return normalized_text(item.get("label")) == "not matched"


def has_entity_metadata(item: dict[str, Any]) -> bool:
    return bool(
        item.get("ror_url")
        or item.get("openalex_url")
        or item.get("dimensions_id")
        or item.get("url")
        or item.get("alternative_names")
        or item.get("parents")
    )


def repair_rows(rows: list[dict[str, str]], matches_path: Path, enrichment: dict[str, Any]) -> dict[str, Any]:
    record_ids = [row["record_id"] for row in rows]
    if len(record_ids) != len(set(record_ids)):
        raise ValueError("record_id values must be unique")
    matches_by_label = load_wos_matches_by_label(record_ids, matches_path)
    before_annotations = [
        tuple((key, row[key]) for key in row if key.endswith(HUMAN_ANNOTATION_SUFFIXES))
        for row in rows
    ]
    changed_record_ids: list[str] = []
    total_not_matched = 0
    for row in rows:
        try:
            old_items = json.loads(row[WOS_COLUMN])
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid WOS JSON for {row['record_id']}: {error}") from error
        if not isinstance(old_items, list) or any(not isinstance(item, dict) for item in old_items):
            raise ValueError(f"WOS JSON must be an array of objects for {row['record_id']}")
        labels = [str(item.get("label", "")).strip() for item in old_items]
        if any(not label for label in labels):
            raise ValueError(f"Blank WOS funder label for {row['record_id']}")
        raw_funders = aligned_wos_funders(row["record_id"], labels, matches_by_label)
        new_items = [enriched_funder_item("wos", funder, enrichment) for funder in raw_funders]
        if [item["label"] for item in new_items] != labels:
            raise AssertionError(f"WOS label order changed for {row['record_id']}")
        total_not_matched += sum(is_not_matched(item) for item in new_items)
        new_json = json.dumps(new_items, ensure_ascii=False, separators=(",", ":"))
        if json.loads(row[WOS_COLUMN]) != new_items:
            changed_record_ids.append(row["record_id"])
            row[WOS_COLUMN] = new_json

    after_annotations = [
        tuple((key, row[key]) for key in row if key.endswith(HUMAN_ANNOTATION_SUFFIXES))
        for row in rows
    ]
    if after_annotations != before_annotations:
        raise AssertionError("Human annotation values changed during WOS repair")

    repaired_items = [json.loads(row[WOS_COLUMN]) for row in rows]
    bad_not_matched = [
        row["record_id"]
        for row, items in zip(rows, repaired_items)
        if any(is_not_matched(item) and has_entity_metadata(item) for item in items)
    ]
    named_without_metadata = [
        (row["record_id"], item["label"])
        for row, items in zip(rows, repaired_items)
        for item in items
        if not is_not_matched(item) and not (item.get("ror_url") or item.get("openalex_url") or item.get("url"))
    ]
    if bad_not_matched:
        raise AssertionError(f"NOT MATCHED objects still have metadata: {bad_not_matched[:5]}")
    if named_without_metadata:
        raise AssertionError(f"Matched labels lack entity metadata: {named_without_metadata[:5]}")
    return {
        "records": len(rows),
        "repaired_records": len(changed_record_ids),
        "repaired_record_ids": changed_record_ids,
        "not_matched_objects": total_not_matched,
        "not_matched_with_metadata_after": len(bad_not_matched),
        "matched_labels_without_metadata_after": len(named_without_metadata),
        "row_order_preserved": True,
        "label_order_and_entries_preserved": True,
        "human_annotation_values_preserved": True,
    }


def write_records_atomic(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporary_name, path)
    except BaseException:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
        raise


def main() -> None:
    args = parse_args()
    output = args.output or args.records_csv
    fieldnames, rows = read_records(args.records_csv)
    if args.backup:
        args.backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.records_csv, args.backup)
    enrichment = build_enrichment_index(
        args.ror_registry,
        args.normalized_institutions,
        Path("."),
        set(),
    )
    report = repair_rows(rows, args.wos_matches, enrichment)
    write_records_atomic(output, fieldnames, rows)
    report.update({"input": str(args.records_csv.resolve()), "output": str(output.resolve())})
    if args.backup:
        report["backup"] = str(args.backup.resolve())
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
