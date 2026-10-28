from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pyarrow.parquet as pq
from tqdm.auto import tqdm

from source_paper_funder_utils import (
    COUNTS_SCHEMA,
    DETAIL_SCHEMA,
    coerce_sequence,
    countries_from_dimensions_author,
    country_priority_from_authors,
    counts_dataframe,
    normalize_text,
    normalize_year,
    path_from_registry,
    repo_root_from_file,
    resolve_paths_file,
    rows_dataframe,
    unique_in_order,
)


PUBLICATION_COLUMNS = [
    "id",
    "doi",
    "year",
    "title",
    "authors",
    "funder_orgs",
    "funding_details",
    "supporting_grant_ids",
    "funder_countries",
]

FUNDER_NAME_BY_ID: dict[str, str] = {}


def parse_args() -> argparse.Namespace:
    repo_root = repo_root_from_file(__file__)
    parser = argparse.ArgumentParser(
        description="Build a paper-funder parquet extract from raw Dimensions publications."
    )
    parser.add_argument("--paths-file", type=Path, default=None)
    parser.add_argument("--dimensions-root", type=Path, default=None)
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_root / "Results" / "SourcePaperFunders" / "dimensions_paper_funders.parquet",
    )
    parser.add_argument(
        "--counts-output",
        type=Path,
        default=repo_root / "Results" / "SourcePaperFunders" / "dimensions_paper_counts_by_year.parquet",
    )
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--max-piece-size", type=int, default=500_000)
    parser.add_argument(
        "--file-batch-size",
        type=int,
        default=100,
        help="Number of publication parquet files to hand to each PieceReader.",
    )
    parser.add_argument("--max-files", type=int, default=None, help="Optional file cap for smoke tests.")
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args()


def clamp_workers(workers: int) -> int:
    return min(20, max(1, int(workers)))


def iter_batches(values: list[Path], batch_size: int) -> list[list[Path]]:
    size = max(1, int(batch_size))
    return [values[start : start + size] for start in range(0, len(values), size)]


def load_funder_names(dimensions_root: Path) -> dict[str, str]:
    org_dir = dimensions_root / "organizations"
    org_files = sorted(org_dir.glob("*.parquet"))
    if not org_files:
        return {}
    table = pq.read_table(org_files, columns=["id", "name"])
    df = table.to_pandas()
    return {
        str(row.id): str(row.name)
        for row in df.itertuples(index=False)
        if normalize_text(row.id) and normalize_text(row.name)
    }


def dimensions_funder_ids(row: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    ids.extend(normalize_text(value) for value in coerce_sequence(row.get("funder_orgs")))

    for detail in coerce_sequence(row.get("funding_details")):
        if not isinstance(detail, Mapping):
            continue
        ids.append(normalize_text(detail.get("grid_id")))

    return unique_in_order(value for value in ids if value)


def dimensions_has_funding(row: Mapping[str, Any], funder_ids: list[str]) -> bool:
    if funder_ids:
        return True
    for field in ["funding_details", "supporting_grant_ids", "funder_countries"]:
        if coerce_sequence(row.get(field)):
            return True
    return False


def detail_rows_for_publication(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    funder_ids = dimensions_funder_ids(row)
    has_funding = dimensions_has_funding(row, funder_ids)
    if not funder_ids:
        funder_ids = [None]

    year = normalize_year(row.get("year"))
    country_codes = country_priority_from_authors(
        row.get("authors"),
        corresponding_key="corresponding",
        country_extractor=countries_from_dimensions_author,
    )

    rows = []
    for funder_id in funder_ids:
        rows.append(
            {
                "paper_id": normalize_text(row.get("id")),
                "title": normalize_text(
                    (row.get("title") or {}).get("preferred")
                    if isinstance(row.get("title"), Mapping)
                    else row.get("title")
                ),
                "publication_year": year,
                "country_codes_priority": country_codes,
                "doi": normalize_text(row.get("doi")),
                "funder_name": FUNDER_NAME_BY_ID.get(funder_id) if funder_id else None,
                "funder_id": funder_id,
                "has_funding_info": has_funding,
            }
        )
    return rows


def initialize_funder_names(names: dict[str, str]) -> None:
    global FUNDER_NAME_BY_ID
    FUNDER_NAME_BY_ID = names


def process_publications_chunk(df: pd.DataFrame, *args: Any) -> tuple[pd.DataFrame, dict[int, tuple[int, int]]]:
    rows: list[dict[str, Any]] = []
    counts: defaultdict[int, list[int]] = defaultdict(lambda: [0, 0])

    for record in df.to_dict("records"):
        detail_rows = detail_rows_for_publication(record)
        year = detail_rows[0]["publication_year"]
        has_funding = detail_rows[0]["has_funding_info"]
        if year is not None:
            counts[year][0] += 1
            counts[year][1] += int(has_funding)
        rows.extend(detail_rows)

    return rows_dataframe(rows), {year: tuple(values) for year, values in counts.items()}


def merge_counts(target: defaultdict[int, list[int]], incoming: Mapping[int, tuple[int, int]]) -> None:
    for year, values in incoming.items():
        target[year][0] += int(values[0])
        target[year][1] += int(values[1])


def main() -> None:
    args = parse_args()
    repo_root = repo_root_from_file(__file__)

    from parcake import PieceReader, PieceSaver

    dimensions_root = args.dimensions_root or path_from_registry("dimensions_raw_parquet_root", resolve_paths_file(repo_root, args.paths_file))
    publication_files = sorted((dimensions_root / "publications").glob("*.parquet"))
    if args.max_files is not None:
        publication_files = publication_files[: args.max_files]
    if not publication_files:
        raise FileNotFoundError(f"No publication parquet files found under {dimensions_root / 'publications'}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.counts_output.parent.mkdir(parents=True, exist_ok=True)

    global FUNDER_NAME_BY_ID
    FUNDER_NAME_BY_ID = load_funder_names(dimensions_root)
    print(f"[dimensions] loaded {len(FUNDER_NAME_BY_ID):,} organization names")

    workers = clamp_workers(args.workers)
    file_batches = iter_batches(publication_files, args.file_batch_size)
    counts: defaultdict[int, list[int]] = defaultdict(lambda: [0, 0])

    with ProcessPoolExecutor(max_workers=workers, initializer=initialize_funder_names,
                             initargs=(FUNDER_NAME_BY_ID,)) as executor, \
            PieceSaver(DETAIL_SCHEMA, args.output, max_piece_size=args.max_piece_size) as detail_saver:
        for batch_idx, batch_files in enumerate(file_batches, start=1):
            print(
                f"[dimensions] preparing batch {batch_idx}/{len(file_batches)} "
                f"({len(batch_files)} files)",
                flush=True,
            )
            reader = PieceReader(batch_files, columns=PUBLICATION_COLUMNS)
            total_tasks = reader.task_count()
            for detail_df, chunk_counts in tqdm(
                reader.process(process_publications_chunk, executor=executor, keep_order=False),
                total=total_tasks,
                desc=f"Dimensions batch {batch_idx}/{len(file_batches)} ({workers} workers)",
                unit="task",
                disable=args.no_progress,
                file=sys.stdout,
            ):
                if detail_df is not None and not detail_df.empty:
                    detail_saver.write_dataframe(detail_df)
                merge_counts(counts, chunk_counts)

    with PieceSaver(COUNTS_SCHEMA, args.counts_output, max_piece_size=max(len(counts), 1)) as counts_saver:
        counts_saver.write_dataframe(counts_dataframe({year: tuple(values) for year, values in counts.items()}))

    print(f"[dimensions] detail parquet: {args.output}")
    print(f"[dimensions] year counts parquet: {args.counts_output}")


if __name__ == "__main__":
    main()
