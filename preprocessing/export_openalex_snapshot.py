from __future__ import annotations

import argparse
from pathlib import Path

import openalexraw as oaraw


INSTITUTIONS_SELECTION = [
    "core",
    "basic",
    "alternate_names",
    "ids",
    "geo",
    "urls",
    "roles",
    "metrics",
]

FUNDERS_SELECTION = [
    "core",
    "basic",
    "ids",
    "extra",
    "counts",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export OpenAlex institutions and funders tables used by this repository."
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Path to the local OpenAlex snapshot directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/openalex_institutions"),
        type=Path,
        help="Directory where exported files will be written.",
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=["institutions", "funders"],
        default=["institutions", "funders"],
        help="Which OpenAlex entities to export.",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["tsv", "parquet"],
        default=["tsv"],
        help="Output formats to generate. TSV also writes the matching schema JSON file.",
    )
    parser.add_argument(
        "--ncpu",
        type=int,
        default=8,
        help="Number of worker processes for parallel OpenAlex reads.",
    )
    parser.add_argument(
        "--max-piece-size",
        type=int,
        default=100000,
        help="Piece size for parquet output.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars.",
    )
    return parser.parse_args()


def export_entity(
    oa: oaraw.OpenAlex,
    entity_type: str,
    selection: list[str],
    output_dir: Path,
    formats: list[str],
    ncpu: int,
    max_piece_size: int,
    show_progress: bool,
) -> None:
    if "tsv" in formats:
        tsv_path = oaraw.archive.createTSV(
            oa,
            entity_type,
            output_dir,
            selection=selection,
            parallel=True,
            ncpu=ncpu,
        )
        print(f"[openalex] TSV export complete: {tsv_path}")

    if "parquet" in formats:
        parquet_path = oaraw.archive.createParquet(
            oa,
            entity_type,
            output_dir,
            selection=selection,
            parallel=True,
            ncpu=ncpu,
            maxPieceSize=max_piece_size,
            showProgressbar=show_progress,
        )
        print(f"[openalex] Parquet export complete: {parquet_path}")


def main() -> None:
    args = parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    oa = oaraw.OpenAlex(openAlexPath=args.snapshot_path)

    show_progress = not args.no_progress

    if "institutions" in args.entities:
        export_entity(
            oa=oa,
            entity_type="institutions",
            selection=INSTITUTIONS_SELECTION,
            output_dir=args.output_dir,
            formats=args.formats,
            ncpu=args.ncpu,
            max_piece_size=args.max_piece_size,
            show_progress=show_progress,
        )

    if "funders" in args.entities:
        export_entity(
            oa=oa,
            entity_type="funders",
            selection=FUNDERS_SELECTION,
            output_dir=args.output_dir,
            formats=args.formats,
            ncpu=args.ncpu,
            max_piece_size=args.max_piece_size,
            show_progress=show_progress,
        )


if __name__ == "__main__":
    main()