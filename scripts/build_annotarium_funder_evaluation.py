#!/usr/bin/env python3
"""Build a consolidated Annotarium project for paper-level funder evaluation."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from update_v1_annotations import (
    DEFAULT_PATHS_FILES,
    aggregate_source,
    collect_source_rows,
    collect_wos,
    normalize_doi,
    registered_path,
    unique_in_order,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UPDATED_ANNOTATIONS = REPO_ROOT / "outputs" / "v1_annotations_updated"
DEFAULT_SOURCE_FUNDERS = REPO_ROOT / "Results" / "SourcePaperFunders"
DEFAULT_WOS_MAIN = REPO_ROOT / "data" / "WoS_2025_main.parquet"
DEFAULT_WOS_MATCHES = REPO_ROOT / "Results" / "grant_institutions_2025_affiliation_match_20251023.parquet"
DEFAULT_NORMALIZED_INSTITUTIONS = REPO_ROOT / "artifacts" / "institutions_normalized.parquet"
DEFAULT_ROR_REGISTRY = REPO_ROOT / "data" / "v1.72-2025-10-06-ror-data_schema_v2.csv"
DEFAULT_OUTPUT = REPO_ROOT / "outputs" / "annotarium_funder_evaluation"

SOURCE_SPECS = {
    "wos": {
        "label": "Web of Science",
        "annotation_file": "web_of_science.csv",
        "detected_column": "display_names",
        "previous_column": "previous_display_names",
    },
    "openalex": {
        "label": "OpenAlex",
        "annotation_file": "openalex.csv",
        "detected_column": "funder_display_name",
        "previous_column": "previous_funder_display_name",
    },
    "dimensions": {
        "label": "Dimensions",
        "annotation_file": "dimensions.csv",
        "detected_column": "display_names",
        "previous_column": "previous_display_names",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--updated-annotations-dir", type=Path, default=DEFAULT_UPDATED_ANNOTATIONS)
    parser.add_argument("--source-funders-dir", type=Path, default=DEFAULT_SOURCE_FUNDERS)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--wos-matches", type=Path, default=DEFAULT_WOS_MATCHES)
    parser.add_argument("--normalized-institutions", type=Path, default=DEFAULT_NORMALIZED_INSTITUTIONS)
    parser.add_argument("--ror-registry", type=Path, default=DEFAULT_ROR_REGISTRY)
    parser.add_argument("--dimensions-organizations", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--project-title", default="Consolidated Funding Detection Evaluation")
    parser.add_argument(
        "--include-new-source-entries",
        action="store_true",
        help="Require annotation for current source-paper pairs that had no previous annotation.",
    )
    return parser.parse_args()


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [
            {key: clean(value) for key, value in row.items() if key}
            for row in csv.DictReader(handle)
            if normalize_doi(row.get("DOI"))
        ]


def annotation_maps(annotation_dir: Path) -> tuple[dict[str, dict[str, dict[str, str]]], list[str]]:
    maps: dict[str, dict[str, dict[str, str]]] = {}
    ordered_dois: list[str] = []
    seen: set[str] = set()

    # Preserve the familiar WOS order, then append OpenAlex- and Dimensions-only papers.
    for source in ["wos", "openalex", "dimensions"]:
        spec = SOURCE_SPECS[source]
        rows = read_csv_rows(annotation_dir / spec["annotation_file"])
        maps[source] = {normalize_doi(row["DOI"]): row for row in rows}
        for row in rows:
            doi = normalize_doi(row["DOI"])
            if doi not in seen:
                seen.add(doi)
                ordered_dois.append(doi)
    return maps, ordered_dois


def openalex_url(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    return "https://openalex.org/" + text.rsplit("/", 1)[-1].upper()


def normalize_ror_url(value: Any) -> str:
    text = clean(value).lower().rstrip("/")
    if not text:
        return ""
    identifier = text.rsplit("/", 1)[-1]
    return f"https://ror.org/{identifier}" if re.fullmatch(r"[0-9a-z]{9}", identifier) else ""


def parse_json_or_delimited_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return unique_in_order(value)
    text = clean(value)
    if not text:
        return []
    if text.startswith("["):
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                return unique_in_order(decoded)
        except json.JSONDecodeError:
            pass
    separator = " || " if " || " in text else ";" if ";" in text else None
    return unique_in_order(text.split(separator) if separator else [text])


def strip_ror_language(value: Any) -> str:
    return re.sub(r"^(?:[a-z]{2,3}|no_lang_code):\s*", "", clean(value), flags=re.IGNORECASE)


def ror_names(value: Any) -> list[str]:
    return unique_in_order(strip_ror_language(part) for part in clean(value).split(";") if clean(part))


def relationship_urls(value: Any, relationship_type: str) -> list[str]:
    urls: list[str] = []
    for section in clean(value).split(";"):
        if ":" not in section:
            continue
        kind, entries = section.split(":", 1)
        if kind.strip().lower() != relationship_type.lower():
            continue
        urls.extend(re.findall(r"https?://ror\.org/[0-9a-z]{9}", entries, flags=re.IGNORECASE))
    return unique_in_order(normalize_ror_url(url) for url in urls)


def load_ror_registry(path: Path) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            ror_url = normalize_ror_url(row.get("id"))
            if not ror_url:
                continue
            display_names = ror_names(row.get("names.types.ror_display"))
            labels = ror_names(row.get("names.types.label"))
            label = display_names[0] if display_names else labels[0] if labels else ror_url
            alternative_names = unique_in_order(
                ror_names(row.get("names.types.alias"))
                + ror_names(row.get("names.types.acronym"))
                + labels
            )
            registry[ror_url] = {
                "label": label,
                "alternative_names": [name for name in alternative_names if name.casefold() != label.casefold()],
                "parent_urls": relationship_urls(row.get("relationships"), "parent"),
            }
    return registry


def load_openalex_references(
    path: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    frame = pl.read_parquet(
        path,
        columns=["id", "display_name", "ids:ror", "other_ids:ror", "alternate_titles", "all_names", "source"],
    )
    references: dict[str, dict[str, Any]] = {}
    ror_to_openalex: dict[str, list[str]] = {}
    for row in frame.iter_rows(named=True):
        prefix = "F" if clean(row["source"]).lower() == "funders" else "I"
        entity_id = clean(row["id"]).removesuffix(".0")
        url = f"https://openalex.org/{prefix}{entity_id}"
        ror_urls = unique_in_order(
            normalize_ror_url(value)
            for value in [row["ids:ror"]] + parse_json_or_delimited_list(row["other_ids:ror"])
        )
        display_name = clean(row["display_name"])
        alternative_names = unique_in_order(
            parse_json_or_delimited_list(row["all_names"])
            + parse_json_or_delimited_list(row["alternate_titles"])
        )
        references[url] = {
            "label": display_name,
            "alternative_names": [
                name for name in alternative_names if not display_name or name.casefold() != display_name.casefold()
            ],
            "ror_urls": ror_urls,
        }
        for ror_url in ror_urls:
            ror_to_openalex.setdefault(ror_url, [])
            if url not in ror_to_openalex[ror_url]:
                ror_to_openalex[ror_url].append(url)
    for urls in ror_to_openalex.values():
        urls.sort(key=lambda url: ("/F" not in url, url))
    return references, ror_to_openalex


def load_dimensions_references(path: Path, grid_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not grid_ids:
        return {}
    org_glob = str(path / "*.parquet") if path.is_dir() else str(path)
    frame = (
        pl.scan_parquet(org_glob)
        .select(
            "id",
            "name",
            "aliases",
            "acronyms",
            "labels",
            pl.col("external_ids").struct.field("ror").struct.field("preferred").alias("ror"),
        )
        .filter(pl.col("id").is_in(sorted(grid_ids)))
        .collect()
    )
    references: dict[str, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        label = clean(row["name"])
        translated_labels = [clean(item.get("label")) for item in (row["labels"] or [])]
        alternative_names = unique_in_order(
            parse_json_or_delimited_list(row["aliases"])
            + parse_json_or_delimited_list(row["acronyms"])
            + translated_labels
        )
        references[clean(row["id"])] = {
            "label": label,
            "alternative_names": [name for name in alternative_names if not label or name.casefold() != label.casefold()],
            "ror_url": normalize_ror_url(row["ror"]),
        }
    return references


def build_enrichment_index(
    ror_registry_path: Path,
    normalized_institutions: Path,
    dimensions_organizations: Path,
    grid_ids: set[str],
) -> dict[str, Any]:
    openalex, ror_to_openalex = load_openalex_references(normalized_institutions)
    return {
        "ror": load_ror_registry(ror_registry_path),
        "openalex": openalex,
        "ror_to_openalex": ror_to_openalex,
        "dimensions": load_dimensions_references(dimensions_organizations, grid_ids),
    }


def source_funder_pairs(source: str, current: dict[str, Any]) -> list[dict[str, str]]:
    if current.get("funders") is not None:
        return current["funders"]
    names_key = "display_names" if source == "wos" else "funder_names"
    ids_key = "urls" if source == "wos" else "funder_ids"
    return [
        {"name": clean(name), "id": clean(identifier)}
        for name, identifier in zip_longest(current.get(names_key, []), current.get(ids_key, []), fillvalue="")
        if clean(name) or clean(identifier)
    ]


def enriched_funder_item(
    source: str,
    funder: dict[str, str],
    enrichment: dict[str, Any],
) -> dict[str, Any]:
    source_id = clean(funder.get("id"))
    reported_name = clean(funder.get("source_name"))
    label = clean(funder.get("name"))
    openalex_url_value = clean(funder.get("openalex_url"))
    ror_url = normalize_ror_url(funder.get("ror_url"))
    dimensions_reference: dict[str, Any] = {}

    if source == "openalex":
        openalex_url_value = openalex_url(source_id)
    elif source == "dimensions":
        dimensions_reference = enrichment["dimensions"].get(source_id, {})
        ror_url = dimensions_reference.get("ror_url", "")
    elif source == "wos" and not openalex_url_value:
        openalex_url_value = openalex_url(source_id)

    openalex_reference = enrichment["openalex"].get(openalex_url_value, {})
    if not ror_url:
        ror_urls = openalex_reference.get("ror_urls", [])
        ror_url = ror_urls[0] if ror_urls else ""
    if not openalex_url_value and ror_url:
        candidates = enrichment["ror_to_openalex"].get(ror_url, [])
        openalex_url_value = candidates[0] if candidates else ""

    ror_reference = enrichment["ror"].get(ror_url, {})
    label = label or dimensions_reference.get("label", "") or openalex_reference.get("label", "") or ror_reference.get("label", "")
    alternative_names = unique_in_order(
        ror_reference.get("alternative_names", [])
        + openalex_reference.get("alternative_names", [])
        + dimensions_reference.get("alternative_names", [])
    )
    alternative_names = [name for name in alternative_names if not label or name.casefold() != label.casefold()]

    parents = []
    for parent_url in ror_reference.get("parent_urls", []):
        parent_reference = enrichment["ror"].get(parent_url, {})
        parents.append(
            {
                "label": parent_reference.get("label", parent_url),
                "ror_url": parent_url,
                "url": parent_url,
            }
        )

    item: dict[str, Any] = {
        "label": label or source_id or "Unknown funder",
        "alternative_names": alternative_names,
        "parents": parents,
    }
    if reported_name and reported_name.casefold() != item["label"].casefold():
        item["reported_name"] = reported_name
    if ror_url:
        item["ror_url"] = ror_url
    if openalex_url_value:
        item["openalex_url"] = openalex_url_value
    if source == "dimensions" and source_id:
        item["dimensions_id"] = source_id
    link = ror_url or openalex_url_value
    if link:
        item["url"] = link
    return item


def funder_list_json(source: str, current: dict[str, Any] | None, enrichment: dict[str, Any]) -> str:
    if current is None:
        return "[]"
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for funder in source_funder_pairs(source, current):
        item = enriched_funder_item(source, funder, enrichment)
        signature = (
            item["label"].casefold(),
            clean(item.get("ror_url")),
            clean(item.get("openalex_url")) or clean(item.get("dimensions_id")),
        )
        if signature not in seen:
            seen.add(signature)
            items.append(item)
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"))


def status_and_context(
    current: dict[str, Any] | None,
    previous: dict[str, str] | None,
    *,
    include_new_source_entries: bool,
) -> tuple[str, str, bool]:
    if current is None:
        return "not_in_sample", "No current record was found for this source.", False
    multiple_note = " Multiple source paper records for this DOI were consolidated."
    if len(current.get("record_ids", [])) <= 1:
        multiple_note = ""
    if previous is None and not include_new_source_entries:
        return (
            "out_of_scope",
            "Current detection shown for context; this source-paper pair was not previously annotated." + multiple_note,
            False,
        )
    if current.get("ambiguous"):
        return (
            "",
            "REVIEW REQUIRED — multiple current source paper records have different detected funder sets." + multiple_note,
            True,
        )
    if previous is None:
        return "", "NEW SOURCE ENTRY — no previous annotation exists for this source." + multiple_note, True
    if previous.get("needs_revision", "").lower() == "true":
        changed = previous.get("changed_fields") or "current pipeline values"
        reason = previous.get("revision_reason") or "pipeline data changed"
        return "", f"REANNOTATION REQUIRED — {reason}; changed: {changed}." + multiple_note, True
    return (
        "carried_forward",
        "Existing annotation carried forward; current source data remains equivalent." + multiple_note,
        False,
    )


def annotation_values(source: str, previous: dict[str, str] | None, pending: bool) -> dict[str, str]:
    prefix = source
    if previous is None or pending:
        declared = correct = wrong = ""
    else:
        declared = previous.get("Number of Funders Indicated by Paper", "")
        correct = previous.get("Number of Correct Matches", "")
        wrong = previous.get("Number of Wrong Matches", "")
    return {
        f"{prefix}_declared_funder_count": declared,
        f"{prefix}_correct_match_count": correct,
        f"{prefix}_wrong_match_count": wrong,
    }


def source_fields(
    source: str,
    current: dict[str, Any] | None,
    previous: dict[str, str] | None,
    enrichment: dict[str, Any],
    *,
    include_new_source_entries: bool,
) -> tuple[dict[str, str], bool]:
    _status, _context, pending = status_and_context(
        current,
        previous,
        include_new_source_entries=include_new_source_entries,
    )
    row = {f"{source}_detected_funders": funder_list_json(source, current, enrichment)}
    row.update(annotation_values(source, previous, pending))
    return row, pending


def yaml_field_lines(source: str) -> list[str]:
    label = SOURCE_SPECS[source]["label"]
    lines = [
        f"  - name: {source}_detected_funders",
        f"    label: {label} Detected Funders",
        "    type: readonly_list",
        "    list:",
        "      format: json",
        "      display: list",
        "      preview: true",
        "      external_links: true",
        "      label_attribute: label",
        "      url_attribute: url",
    ]
    lines.extend(
        [
            f"  - group: {source}_evaluation",
            f"    label: {label} Evaluation",
            "    fields:",
            f"      - name: {source}_declared_funder_count",
            "        label: Declared",
            "        type: integer",
            "        validation:",
            "          minimum: 0",
            f"      - name: {source}_correct_match_count",
            "        label: Correct",
            "        type: integer",
            "        validation:",
            "          minimum: 0",
            f"      - name: {source}_wrong_match_count",
            "        label: Wrong",
            "        type: integer",
            "        validation:",
            "          minimum: 0",
        ]
    )
    return lines


def config_text(title: str = "Consolidated Funding Detection Evaluation") -> str:
    lines = [
        "schema_version: 1",
        f"title: {title}",
        "",
        "data:",
        "  csv: records.csv",
        "  id_column: record_id",
        "  encoding: utf-8",
        "",
        "record:",
        '  display_name: "{title}"',
        '  url_template: "https://doi.org/{doi}"',
        "",
        "fields:",
    ]
    for source in ["wos", "openalex", "dimensions"]:
        lines.extend(yaml_field_lines(source))
    lines.extend(
        [
            "  - name: has_programs",
            "    label: Has Programs",
            "    type: checkbox",
            "  - name: notes",
            "    label: Notes",
            "    type: multiline_text",
            "",
            "completion:",
            "  strategy: required_fields",
            "  allow_invalid_navigation: true",
            "",
            "browser:",
            "  search_enabled: true",
            "  open_external_links: false",
            "  persist_cookies: true",
            "",
            "autosave:",
            "  enabled: true",
            "  debounce_milliseconds: 500",
            "  backup_interval_minutes: 15",
            "  maximum_backups: 25",
        ]
    )
    return "\n".join(lines) + "\n"


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = unique_in_order(key for row in rows for key in row)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    if args.dimensions_organizations is None:
        args.dimensions_organizations = registered_path("dimensions_raw_parquet_root") / "organizations"

    annotations, ordered_dois = annotation_maps(args.updated_annotations_dir)
    targets = set(ordered_dois)
    target_rows = [{"DOI": doi} for doi in ordered_dois]

    current = {
        "wos": collect_wos(target_rows, args.wos_main, args.wos_matches),
        "openalex": aggregate_source(
            collect_source_rows(args.source_funders_dir / "openalex_paper_funders.parquet", targets, "openalex")
        ),
        "dimensions": aggregate_source(
            collect_source_rows(args.source_funders_dir / "dimensions_paper_funders.parquet", targets, "dimensions")
        ),
    }

    grid_ids = {
        grid_id
        for record in current["dimensions"].values()
        for grid_id in record["funder_ids"]
    }
    enrichment = build_enrichment_index(
        args.ror_registry,
        args.normalized_institutions,
        args.dimensions_organizations,
        grid_ids,
    )

    output_rows: list[dict[str, str]] = []
    pending_counts: Counter[str] = Counter()
    coverage_counts: Counter[str] = Counter()
    for order, doi in enumerate(ordered_dois, start=1):
        present_sources = [source for source in SOURCE_SPECS if doi in current[source]]
        for source in present_sources:
            coverage_counts[source] += 1
        titles = unique_in_order(
            [
                current[source].get(doi, {}).get("title", "")
                for source in ["wos", "openalex", "dimensions"]
            ]
            + [annotations[source].get(doi, {}).get("title", "") for source in SOURCE_SPECS]
        )
        row = {
            "record_id": doi,
            "sample_order": str(order),
            "title": titles[0] if titles else doi,
            "doi": doi,
            "source_coverage": " | ".join(SOURCE_SPECS[source]["label"] for source in present_sources),
            "pending_sources": "",
        }
        pending_sources = []
        for source in ["wos", "openalex", "dimensions"]:
            fields, pending = source_fields(
                source,
                current[source].get(doi),
                annotations[source].get(doi),
                enrichment,
                include_new_source_entries=args.include_new_source_entries,
            )
            row.update(fields)
            if pending:
                pending_sources.append(SOURCE_SPECS[source]["label"])
                pending_counts[source] += 1
        row["has_programs"] = ""
        row["notes"] = ""
        row["pending_sources"] = " | ".join(pending_sources)
        output_rows.append(row)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    records_rows = [
        {key: value for key, value in row.items() if key not in {"source_coverage", "pending_sources"}}
        for row in output_rows
    ]
    write_csv(args.output_dir / "records.csv", records_rows)
    (args.output_dir / "config.yml").write_text(config_text(args.project_title), encoding="utf-8")

    queue_rows = [row for row in output_rows if row["pending_sources"]]
    queue_fields = ["record_id", "sample_order", "title", "doi"]
    with (args.output_dir / "review_queue.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=queue_fields)
        writer.writeheader()
        writer.writerows({key: row[key] for key in queue_fields} for row in queue_rows)

    summary = {
        "records": len(output_rows),
        "records_needing_annotation": len(queue_rows),
        "source_coverage": {SOURCE_SPECS[key]["label"]: coverage_counts[key] for key in SOURCE_SPECS},
        "pending_source_annotations": {SOURCE_SPECS[key]["label"]: pending_counts[key] for key in SOURCE_SPECS},
        "multiple_current_record_dois": {
            SOURCE_SPECS[key]["label"]: sum(len(record["record_ids"]) > 1 for record in current[key].values())
            for key in SOURCE_SPECS
        },
        "ambiguous_current_record_dois": {
            SOURCE_SPECS[key]["label"]: sum(bool(record.get("ambiguous")) for record in current[key].values())
            for key in SOURCE_SPECS
        },
        "dimensions_grid_ids": len(grid_ids),
        "dimensions_grid_ids_with_ror": sum(
            bool(enrichment["dimensions"].get(grid_id, {}).get("ror_url")) for grid_id in grid_ids
        ),
        "dimensions_grid_ids_with_openalex_crosswalk": sum(
            bool(
                enrichment["ror_to_openalex"].get(
                    enrichment["dimensions"].get(grid_id, {}).get("ror_url", ""),
                    [],
                )
            )
            for grid_id in grid_ids
        ),
        "ror_registry": str(args.ror_registry.resolve()),
        "input_updated_annotations": str(args.updated_annotations_dir.resolve()),
        "include_new_source_entries": args.include_new_source_entries,
        "dimensions_path_registry": [str(path) for path in DEFAULT_PATHS_FILES],
    }
    (args.output_dir / "build_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (args.output_dir / "README.md").write_text(
        "# Consolidated Funding Detection Evaluation\n\n"
        "Open this folder in Annotarium. `records.csv` is authoritative, and `review_queue.csv` identifies papers "
        "whose source data is new or changed. Each detected-funder field is a read-only JSON list enriched with "
        "alternative names, ROR/OpenAlex links, and ROR parents when available.\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    print(f"Wrote Annotarium project to {args.output_dir}")


if __name__ == "__main__":
    main()
