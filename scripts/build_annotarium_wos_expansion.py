#!/usr/bin/env python3
"""Build a deterministic WOS-paper expansion sample with cross-source detections."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from build_annotarium_funder_evaluation import (
    DEFAULT_NORMALIZED_INSTITUTIONS,
    DEFAULT_ROR_REGISTRY,
    DEFAULT_SOURCE_FUNDERS,
    DEFAULT_UPDATED_ANNOTATIONS,
    DEFAULT_WOS_MATCHES,
    REPO_ROOT,
    SOURCE_SPECS,
    build_enrichment_index,
    config_text,
    source_fields,
    write_csv,
)
from update_v1_annotations import (
    aggregate_source,
    collect_source_rows,
    normalize_doi,
    normalized_text,
    registered_path,
    unique_in_order,
    wos_openalex_url,
)


DEFAULT_WOS_SAMPLE = (
    REPO_ROOT
    / "Results"
    / "Samples"
    / "sample_by_paper_validation_dataset_2025_affiliation_match_20251023.csv"
)
DEFAULT_OUTPUT = REPO_ROOT / "outputs" / "funder_annotation_projects" / "02_wos_expansion_500"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wos-sample", type=Path, default=DEFAULT_WOS_SAMPLE)
    parser.add_argument("--wos-matches", type=Path, default=DEFAULT_WOS_MATCHES)
    parser.add_argument("--exclude-annotations-dir", type=Path, default=DEFAULT_UPDATED_ANNOTATIONS)
    parser.add_argument("--source-funders-dir", type=Path, default=DEFAULT_SOURCE_FUNDERS)
    parser.add_argument("--normalized-institutions", type=Path, default=DEFAULT_NORMALIZED_INSTITUTIONS)
    parser.add_argument("--ror-registry", type=Path, default=DEFAULT_ROR_REGISTRY)
    parser.add_argument("--dimensions-organizations", type=Path, default=None)
    parser.add_argument("--sample-size", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260806)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--project-title", default="WOS Expansion Sample — 500 Papers")
    return parser.parse_args()


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def split_groups(value: Any) -> list[str]:
    return [part.strip() for part in clean(value).split("||") if part.strip()]


def excluded_dois(annotation_dir: Path) -> set[str]:
    result: set[str] = set()
    for filename in ["web_of_science.csv", "openalex.csv", "dimensions.csv"]:
        path = annotation_dir / filename
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                doi = normalize_doi(row.get("DOI"))
                if doi:
                    result.add(doi)
    return result


def select_wos_sample(path: Path, *, sample_size: int, seed: int, exclusions: set[str]) -> pl.DataFrame:
    frame = (
        pl.read_csv(path, infer_schema_length=10_000)
        .with_columns(
            pl.col("DOI")
            .cast(pl.String)
            .str.strip_chars()
            .str.to_lowercase()
            .str.replace(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "")
            .str.strip_suffix("/")
            .alias("doi")
        )
        .filter(pl.col("doi").is_not_null() & (pl.col("doi") != ""))
        .filter(~pl.col("doi").is_in(sorted(exclusions)))
        .sort("UT")
        .unique(subset=["doi"], keep="first", maintain_order=True)
    )
    if frame.height < sample_size:
        raise ValueError(f"Only {frame.height} eligible unique-DOI WOS papers remain; requested {sample_size}")
    return frame.sample(n=sample_size, seed=seed, shuffle=True)


def load_wos_matches_by_label(
    selected_uts: list[str], matches_path: Path
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    matches = (
        pl.scan_parquet(matches_path)
        .select(["UT", "grant_agency", "gid", "ids:ror", "source", "display_name"])
        .filter(pl.col("UT").is_in(selected_uts))
        .collect()
    )
    matches_by_label: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for match in matches.iter_rows(named=True):
        label_key = normalized_text(match["display_name"])
        if label_key:
            matches_by_label[clean(match["UT"])][label_key].append(match)
    return matches_by_label


def aligned_wos_funders(
    ut: str,
    labels: list[str],
    matches_by_label: dict[str, dict[str, list[dict[str, Any]]]],
) -> list[dict[str, str]]:
    funders: list[dict[str, str]] = []
    for label in labels:
        if normalized_text(label) == "not matched":
            # Unmatched labels have no entity record and must not consume matched metadata.
            funders.append({"name": label})
            continue
        candidates = matches_by_label.get(ut, {}).get(normalized_text(label), [])
        if not candidates:
            raise ValueError(f"No matched WOS metadata found for {ut}: {label!r}")
        signatures = {
            (clean(item["gid"]), clean(item["ids:ror"]), clean(item["source"]))
            for item in candidates
        }
        if len(signatures) != 1:
            raise ValueError(f"Ambiguous matched WOS metadata for {ut}: {label!r} -> {sorted(signatures)}")
        match = candidates[0]
        funders.append(
            {
                "name": label,
                "openalex_url": wos_openalex_url(match["gid"], match["source"]),
                "ror_url": clean(match["ids:ror"]),
            }
        )
    return funders


def current_wos(selected: pl.DataFrame, matches_path: Path) -> dict[str, dict[str, Any]]:
    selected_uts = selected["UT"].cast(pl.String).to_list()
    matches_by_label = load_wos_matches_by_label(selected_uts, matches_path)

    result = {}
    for row in selected.iter_rows(named=True):
        doi = clean(row["doi"])
        ut = clean(row["UT"])
        names = split_groups(row["display_names"])
        urls = split_groups(row["OA_URLs"])
        funders = aligned_wos_funders(ut, names, matches_by_label)
        result[doi] = {
            "record_ids": [ut],
            "title": clean(row["title"]),
            "display_names": names,
            "urls": urls,
            "funders": funders,
            "ambiguous": False,
        }
    return result


def main() -> None:
    args = parse_args()
    if args.dimensions_organizations is None:
        args.dimensions_organizations = registered_path("dimensions_raw_parquet_root") / "organizations"

    exclusions = excluded_dois(args.exclude_annotations_dir)
    input_dois = {
        normalize_doi(value)
        for value in pl.read_csv(args.wos_sample, columns=["DOI"])["DOI"].to_list()
        if normalize_doi(value)
    }
    excluded_candidate_dois = input_dois & exclusions
    selected = select_wos_sample(
        args.wos_sample,
        sample_size=args.sample_size,
        seed=args.seed,
        exclusions=exclusions,
    )
    ordered_dois = selected["doi"].to_list()
    targets = set(ordered_dois)
    current = {
        "wos": current_wos(selected, args.wos_matches),
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

    selected_by_doi = {clean(row["doi"]): row for row in selected.iter_rows(named=True)}
    output_rows: list[dict[str, str]] = []
    coverage_counts: Counter[str] = Counter()
    pending_counts: Counter[str] = Counter()
    for order, doi in enumerate(ordered_dois, start=1):
        input_row = selected_by_doi[doi]
        present_sources = [source for source in SOURCE_SPECS if doi in current[source]]
        for source in present_sources:
            coverage_counts[source] += 1
        row = {
            "record_id": clean(input_row["UT"]),
            "sample_order": str(order),
            "title": clean(input_row["title"]),
            "doi": doi,
            "source_coverage": " | ".join(SOURCE_SPECS[source]["label"] for source in present_sources),
            "pending_sources": "",
        }
        pending_sources = []
        for source in ["wos", "openalex", "dimensions"]:
            fields, pending = source_fields(
                source,
                current[source].get(doi),
                None,
                enrichment,
                include_new_source_entries=True,
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

    queue_fields = ["record_id", "sample_order", "title", "doi"]
    with (args.output_dir / "review_queue.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=queue_fields)
        writer.writeheader()
        writer.writerows({key: row[key] for key in queue_fields} for row in output_rows)

    summary = {
        "records": len(output_rows),
        "selection_source": str(args.wos_sample.resolve()),
        "selection_unit": "unique nonempty DOI, one WOS UT per DOI",
        "sample_seed": args.seed,
        "input_unique_nonempty_dois": len(input_dois),
        "previously_annotated_exclusion_list_size": len(exclusions),
        "excluded_candidate_dois": len(excluded_candidate_dois),
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
    }
    (args.output_dir / "build_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (args.output_dir / "README.md").write_text(
        "# WOS Expansion Sample — 500 Papers\n\n"
        "This project is a deterministic 500-paper subset of the current sample-by-paper WOS validation data. "
        "All papers have a unique nonempty DOI and exclude the previously annotated DOI union. Current OpenAlex "
        "and Dimensions detections are included where available. Each detected-funder field is a read-only JSON "
        "list enriched with alternative names, ROR/OpenAlex links, and ROR parents when available.\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    print(f"Wrote expansion Annotarium project to {args.output_dir}")


if __name__ == "__main__":
    main()
