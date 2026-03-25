from __future__ import annotations

import argparse
import csv
from functools import partial
from pathlib import Path
from typing import Any, Iterable, List, Optional

import numpy as np
import pandas as pd

try:
    from parcake_utils import iter_piece_results
except ModuleNotFoundError:
    from scripts_wos_funding.parcake_utils import iter_piece_results

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_MATCHES = Path("Results/grant_institutions_2023_affiliation_match_20251023.parquet")
DEFAULT_WOS_MAIN = Path("data/WoS_2023_main.parquet")
DEFAULT_OUTPUT_DIR = Path("Results/Samples")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build validation samples from Step2 outputs and WOS processed parquet inputs."
    )
    parser.add_argument("--matches", type=Path, default=DEFAULT_MATCHES)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=-1)
    parser.add_argument("--sample-size", type=int, default=1000)
    return parser.parse_args()


def iter_progress(iterable: Iterable[Any], *, total: Optional[int] = None, desc: str = "") -> Iterable[Any]:
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


def build_openalex_url(gid: Any, source: Any) -> str:
    if pd.isna(gid) or not gid:
        return ""
    source_text = str(source or "").strip().lower()
    if source_text.endswith("s"):
        return f"https://openalex.org/{source_text[0].upper()}{str(gid)[3:]}"
    return ""


def extract_wos_rows(frame: pd.DataFrame, target_uts: set[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UT", "title", "DOI"])
    filtered = frame[frame["UT"].isin(target_uts)]
    if filtered.empty:
        return pd.DataFrame(columns=["UT", "title", "DOI"])
    rows = filtered[["UT", "TI", "DI"]].copy()
    rows.rename(columns={"TI": "title"}, inplace=True)
    rows["DOI"] = rows["DI"].fillna("").map(lambda doi: f"https://doi.org/{doi}" if doi else "")
    return rows[["UT", "title", "DOI"]]


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
    frames: List[pd.DataFrame] = []
    iterator = iter_piece_results(
        args.wos_main,
        columns=["UT", "TI", "DI"],
        processor=partial(extract_wos_rows, target_uts=target_uts),
        workers=args.workers,
        keep_order=True,
    )
    for frame in iter_progress(iterator, desc="Reading WOS titles and DOIs"):
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError("No matching WOS records were found in the provided main parquet file.")

    results = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["UT"])
    results["display_names"] = results["UT"].map(display_name_lookup).fillna("NOT MATCHED")
    results["OA_URLs"] = results["UT"].map(url_lookup).fillna("")

    output_base_name = args.matches.stem.replace("grant_institutions", "validation_dataset")
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