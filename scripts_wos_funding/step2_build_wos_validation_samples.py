from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Iterable, List, Optional

import numpy as np
import pandas as pd

try:
    from parcake_utils import PieceReader, default_worker_count
except ModuleNotFoundError:
    from scripts_wos_funding.parcake_utils import PieceReader, default_worker_count

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_MATCHES = Path("Results/grant_institutions_2025_affiliation_match_20251023.parquet")
DEFAULT_WOS_MAIN = Path("data/WoS_2025_main.parquet")
DEFAULT_OUTPUT_DIR = Path("Results/Samples")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build validation samples from Step2 outputs and WOS processed parquet inputs."
    )
    parser.add_argument("--matches", type=Path, default=DEFAULT_MATCHES)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--output-basename",
        default=None,
        help="Filename stem after complete_/sample_by_*_; defaults to the matches stem with grant_institutions replaced by validation_dataset.",
    )
    parser.add_argument("--workers", type=int, default=-1)
    parser.add_argument("--sample-size", type=int, default=1000)
    return parser.parse_args()


def iter_progress(iterable: Iterable[Any], *, total: Optional[int] = None, desc: str = "", unit: str = "it") -> Iterable[Any]:
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc, unit=unit)


def build_openalex_url(gid: Any, source: Any) -> str:
    if pd.isna(gid) or not gid:
        return ""
    source_text = str(source or "").strip().lower()
    if source_text.endswith("s"):
        return f"https://openalex.org/{source_text[0].upper()}{str(gid)[3:]}"
    return ""


def extract_wos_rows(frame: pd.DataFrame, _row_group: Any = None, *, target_uts: set[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UT", "title", "DOI"])
    filtered = frame[frame["UT"].isin(target_uts)]
    if filtered.empty:
        return pd.DataFrame(columns=["UT", "title", "DOI"])
    rows = filtered[["UT", "TI", "DI"]].copy()
    rows.rename(columns={"TI": "title"}, inplace=True)
    rows["DOI"] = rows["DI"].fillna("").map(lambda doi: f"https://doi.org/{doi}" if doi else "")
    return rows[["UT", "title", "DOI"]]


TARGET_UTS: set[str] = set()


def initialize_target_uts(target_uts: set[str]) -> None:
    global TARGET_UTS
    TARGET_UTS = target_uts


def extract_wos_rows_from_global(frame: pd.DataFrame, *_piece_args: Any) -> pd.DataFrame:
    return extract_wos_rows(frame, *_piece_args, target_uts=TARGET_UTS)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    matches = pd.read_parquet(args.matches).copy()
    matches["display_names"] = matches["display_name"].fillna("NOT MATCHED")
    matches["OA_URLs"] = matches.apply(lambda row: build_openalex_url(row.get("gid"), row.get("source")), axis=1)

    display_name_lookup = matches.groupby("UT", sort=False)["display_names"].agg(
        lambda values: " || ".join(dict.fromkeys(value for value in values if value))
    )
    url_lookup = matches.groupby("UT", sort=False)["OA_URLs"].agg(
        lambda values: " || ".join(url for url in dict.fromkeys(values) if url)
    )

    target_uts = set(matches["UT"].dropna().astype(str).unique())
    rows_by_ut: dict[str, tuple[str, str]] = {}
    with ProcessPoolExecutor(max_workers=default_worker_count(args.workers),
                             initializer=initialize_target_uts, initargs=(target_uts,)) as executor:
        reader = PieceReader(args.wos_main, columns=["UT", "TI", "DI"])
        iterator = reader.process(extract_wos_rows_from_global, executor=executor, keep_order=False)
        for frame in iter_progress(
            iterator,
            total=reader.task_count(),
            desc="Reading WOS titles and DOIs",
            unit="task",
        ):
            if frame.empty:
                continue
            for ut, title, doi in frame[["UT", "title", "DOI"]].itertuples(index=False):
                rows_by_ut.setdefault(str(ut), (title, doi))

    if not rows_by_ut:
        raise RuntimeError("No matching WOS records were found in the provided main parquet file.")

    # Sampling uses row positions, so fixed seeds also require fixed row order.
    results = pd.DataFrame(
        [{"UT": ut, "title": title, "DOI": doi} for ut, (title, doi) in sorted(rows_by_ut.items())]
    )
    results["display_names"] = results["UT"].map(display_name_lookup).fillna("NOT MATCHED")
    results["OA_URLs"] = results["UT"].map(url_lookup).fillna("")

    output_base_name = args.output_basename or args.matches.stem.replace("grant_institutions", "validation_dataset")
    sample_by_paper_csv = args.output_dir / f"sample_by_paper_{output_base_name}.csv"
    sample_by_agency_csv = args.output_dir / f"sample_by_agency_{output_base_name}.csv"
    complete_csv = args.output_dir / f"complete_{output_base_name}.csv"
    complete_feather = args.output_dir / f"complete_{output_base_name}.feather"

    results.to_feather(complete_feather)
    results.to_csv(complete_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

    sample_size = min(args.sample_size, len(results))
    if sample_size:
        paper_sample = results.sample(n=sample_size, random_state=42)
        paper_sample.to_csv(sample_by_paper_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

    unique_agencies = matches["grant_agency"].dropna().unique()
    if len(unique_agencies):
        agency_sample_size = min(args.sample_size, len(unique_agencies))
        sampled_agencies = set(np.random.default_rng(42).choice(unique_agencies, size=agency_sample_size, replace=False))
        sample_by_agency = matches[matches["grant_agency"].isin(sampled_agencies)]
        sample_by_agency = sample_by_agency.groupby("grant_agency", sort=False).first().reset_index()
        sample_by_agency_results = results[results["UT"].isin(sample_by_agency["UT"])]
        sample_by_agency_results.to_csv(sample_by_agency_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

    print(complete_feather)


if __name__ == "__main__":
    main()
