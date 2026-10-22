from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from tqdm.auto import tqdm

from source_paper_funder_utils import (
    COUNTS_SCHEMA,
    DETAIL_SCHEMA,
    countries_from_openalex_authorship,
    country_priority_from_authors,
    counts_dataframe,
    ensure_local_library,
    normalize_text,
    normalize_year,
    path_from_registry,
    repo_root_from_file,
    resolve_paths_file,
    rows_dataframe,
    unique_in_order,
)


def parse_args() -> argparse.Namespace:
    repo_root = repo_root_from_file(__file__)
    parser = argparse.ArgumentParser(
        description="Build a paper-funder parquet extract from the local OpenAlex works snapshot."
    )
    parser.add_argument("--paths-file", type=Path, default=None)
    parser.add_argument("--snapshot-root", type=Path, default=None)
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_root / "Results" / "SourcePaperFunders" / "openalex_paper_funders.parquet",
    )
    parser.add_argument(
        "--counts-output",
        type=Path,
        default=repo_root / "Results" / "SourcePaperFunders" / "openalex_paper_counts_by_year.parquet",
    )
    parser.add_argument("--max-piece-size", type=int, default=500_000)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional record limit for smoke tests.",
    )
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args()


def openalex_funders(work: Mapping[str, Any]) -> list[tuple[str | None, str | None]]:
    pairs: list[tuple[str | None, str | None]] = []

    for grant in work.get("grants") or []:
        if not isinstance(grant, Mapping):
            continue
        pairs.append(
            (
                normalize_text(grant.get("funder")),
                normalize_text(grant.get("funder_display_name")),
            )
        )

    for funder in work.get("funders") or []:
        if not isinstance(funder, Mapping):
            continue
        pairs.append(
            (
                normalize_text(funder.get("id")) or normalize_text((funder.get("ids") or {}).get("openalex"))
                if isinstance(funder.get("ids"), Mapping)
                else normalize_text(funder.get("id")),
                normalize_text(funder.get("display_name")),
            )
        )

    deduped: list[tuple[str | None, str | None]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for funder_id, funder_name in pairs:
        key = (funder_id, funder_name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def detail_rows_for_work(work: Mapping[str, Any]) -> list[dict[str, Any]]:
    funders = openalex_funders(work)
    has_funding = bool(funders)
    if not funders:
        funders = [(None, None)]

    year = normalize_year(work.get("publication_year"))
    country_codes = country_priority_from_authors(
        work.get("authorships"),
        corresponding_key="is_corresponding",
        country_extractor=countries_from_openalex_authorship,
    )

    rows = []
    for funder_id, funder_name in funders:
        rows.append(
            {
                "paper_id": normalize_text(work.get("id")),
                "title": normalize_text(work.get("display_name")),
                "publication_year": year,
                "country_codes_priority": country_codes,
                "doi": normalize_text(work.get("doi")),
                "funder_name": funder_name,
                "funder_id": funder_id,
                "has_funding_info": has_funding,
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    repo_root = repo_root_from_file(__file__)
    ensure_local_library(repo_root, "preprocessing/libraries/openalex-raw")

    from parcake import PieceSaver
    import openalexraw as oaraw

    snapshot_root = args.snapshot_root or path_from_registry("openalex_snapshot_root", resolve_paths_file(repo_root, args.paths_file))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.counts_output.parent.mkdir(parents=True, exist_ok=True)

    oa = oaraw.OpenAlex(openAlexPath=snapshot_root)
    total = oa.getRawEntityCount("works")
    if args.limit is not None:
        total = min(total, args.limit)

    counts: defaultdict[int, list[int]] = defaultdict(lambda: [0, 0])
    pending_rows: list[dict[str, Any]] = []

    iterator = oa.rawEntities("works")
    with PieceSaver(DETAIL_SCHEMA, args.output, max_piece_size=args.max_piece_size) as detail_saver:
        for idx, work in enumerate(
            tqdm(
                iterator,
                total=total,
                desc="OpenAlex works",
                unit="work",
                disable=args.no_progress,
                file=sys.stdout,
            )
        ):
            if args.limit is not None and idx >= args.limit:
                break

            rows = detail_rows_for_work(work)
            year = rows[0]["publication_year"]
            has_funding = rows[0]["has_funding_info"]
            if year is not None:
                counts[year][0] += 1
                counts[year][1] += int(has_funding)

            pending_rows.extend(rows)
            if len(pending_rows) >= args.max_piece_size:
                detail_saver.write_dataframe(rows_dataframe(pending_rows))
                pending_rows.clear()

        if pending_rows:
            detail_saver.write_dataframe(rows_dataframe(pending_rows))

    with PieceSaver(COUNTS_SCHEMA, args.counts_output, max_piece_size=max(len(counts), 1)) as counts_saver:
        counts_saver.write_dataframe(counts_dataframe({year: tuple(values) for year, values in counts.items()}))

    print(f"[openalex] detail parquet: {args.output}")
    print(f"[openalex] year counts parquet: {args.counts_output}")


if __name__ == "__main__":
    main()
