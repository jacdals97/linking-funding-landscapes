#!/usr/bin/env python3
"""Apply a precomputed WOS JSON patch while preserving all annotation cells."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import tempfile
from pathlib import Path


WOS_COLUMN = "wos_detected_funders"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records_csv", type=Path)
    parser.add_argument("patch_csv", type=Path)
    parser.add_argument("--output", type=Path, default=None, help="Defaults to replacing records_csv atomically.")
    parser.add_argument("--backup", type=Path, default=None)
    return parser.parse_args()


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames or [], [dict(row) for row in reader]


def write_atomic(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
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
    fieldnames, rows = read_csv(args.records_csv)
    patch_fields, patch_rows = read_csv(args.patch_csv)
    required = {"record_id", WOS_COLUMN}
    if not required.issubset(fieldnames) or not required.issubset(patch_fields):
        raise ValueError(f"Both CSV files must contain: {sorted(required)}")
    patches = {row["record_id"]: row[WOS_COLUMN] for row in patch_rows}
    if len(patches) != len(patch_rows):
        raise ValueError("Patch record_id values must be unique")

    seen: set[str] = set()
    before_non_wos = [tuple((key, row[key]) for key in fieldnames if key != WOS_COLUMN) for row in rows]
    for row in rows:
        replacement = patches.get(row["record_id"])
        if replacement is None:
            continue
        old_labels = [item.get("label") for item in json.loads(row[WOS_COLUMN])]
        new_items = json.loads(replacement)
        new_labels = [item.get("label") for item in new_items]
        if old_labels != new_labels:
            raise ValueError(f"Patch would change WOS labels or their order for {row['record_id']}")
        row[WOS_COLUMN] = json.dumps(new_items, ensure_ascii=False, separators=(",", ":"))
        seen.add(row["record_id"])
    missing = sorted(set(patches) - seen)
    if missing:
        raise ValueError(f"Patch IDs missing from records CSV: {missing[:5]}")
    after_non_wos = [tuple((key, row[key]) for key in fieldnames if key != WOS_COLUMN) for row in rows]
    if before_non_wos != after_non_wos:
        raise AssertionError("A non-WOS field changed")

    if args.backup:
        args.backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.records_csv, args.backup)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(output, fieldnames, rows)
    print(
        json.dumps(
            {
                "records": len(rows),
                "patched_records": len(seen),
                "row_and_label_order_preserved": True,
                "all_non_wos_fields_preserved": True,
                "output": str(output.resolve()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
