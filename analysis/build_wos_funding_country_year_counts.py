from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

try:
    from parcake import PieceReader, PieceSaver
except ModuleNotFoundError:
    repo_root = Path(__file__).resolve().parents[1]
    vendored_src = repo_root / "preprocessing" / "libraries" / "parcake" / "src"
    if vendored_src.exists() and str(vendored_src) not in sys.path:
        sys.path.insert(0, str(vendored_src))
    from parcake import PieceReader, PieceSaver

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_COUNTS = Path(
    "Results/FundingYearCounts/wos_funding_paper_counts_by_year_1950_2025_with_disambiguated_papers.csv"
)
DEFAULT_MATCHES = Path("Results/grant_institutions_2025_affiliation_match_20251023.parquet")
DEFAULT_WOS_MAIN = Path("data/WoS_2025_main.parquet")
DEFAULT_OUTPUT_DIR = Path("Results/FundingYearCounts")
MAX_WORKERS = 23

EUROPE_COUNTRY_CODES = {
    "AD",
    "AL",
    "AM",
    "AT",
    "AX",
    "AZ",
    "BA",
    "BE",
    "BG",
    "BY",
    "CH",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FO",
    "FR",
    "GB",
    "GE",
    "GG",
    "GI",
    "GR",
    "HR",
    "HU",
    "IE",
    "IM",
    "IS",
    "IT",
    "JE",
    "LI",
    "LT",
    "LU",
    "LV",
    "MC",
    "MD",
    "ME",
    "MK",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "RS",
    "RU",
    "SE",
    "SI",
    "SJ",
    "SK",
    "SM",
    "TR",
    "UA",
    "VA",
    "XK",
}

COUNTRY_COLUMNS = {
    "US": "funded_papers_disambiguated_us",
    "Europe": "funded_papers_disambiguated_europe",
    "Brazil": "funded_papers_disambiguated_brazil",
    "Japan": "funded_papers_disambiguated_japan",
    "China": "funded_papers_disambiguated_china",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Add preferred-paper-country curves for funded papers with at least one "
            "disambiguated funding entity."
        )
    )
    parser.add_argument("--counts", type=Path, default=DEFAULT_COUNTS)
    parser.add_argument("--matches", type=Path, default=DEFAULT_MATCHES)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument(
        "--candidate-parquet",
        type=Path,
        default=DEFAULT_OUTPUT_DIR / "disambiguated_funded_paper_country_candidates.parquet",
    )
    return parser.parse_args()


def worker_count(requested: int) -> int:
    return min(MAX_WORKERS, max(1, int(requested)))


def first_country(group: str) -> str:
    for part in group.split(";"):
        code = part.strip().upper()
        if code:
            return code
    return ""


def preferred_author_country(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text in {"[]", "{}", "nan", "NaN", "None", "null"}:
        return ""
    groups = [group.strip() for group in text.split("||")]
    if not groups:
        return ""
    first = first_country(groups[0])
    if first:
        return first
    return first_country(groups[-1])


def country_group(code: str) -> str:
    if code == "US":
        return "US"
    if code in EUROPE_COUNTRY_CODES:
        return "Europe"
    if code == "BR":
        return "Brazil"
    if code == "JP":
        return "Japan"
    if code == "CN":
        return "China"
    return ""


def extract_country_candidates(frame: pd.DataFrame, *_piece_args: Any) -> pd.DataFrame:
    columns = ["UT", "country_group"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    valid = frame["UT"].notna() & frame["id"].notna()
    if not valid.any():
        return pd.DataFrame(columns=columns)

    if "preferred_paper_country_code" in frame.columns:
        data = frame.loc[valid, ["UT", "preferred_paper_country_code"]].copy()
        data["country_group"] = data["preferred_paper_country_code"].map(
            lambda value: country_group(str(value or "").strip().upper())
        )
    else:
        data = frame.loc[valid, ["UT", "author_country_codes_ordered"]].copy()
        data["country_group"] = data["author_country_codes_ordered"].map(
            lambda value: country_group(preferred_author_country(value))
        )
    data = data.loc[data["country_group"] != "", ["UT", "country_group"]]
    if data.empty:
        return pd.DataFrame(columns=columns)
    return data.drop_duplicates(["UT", "country_group"], keep="first")


def write_country_candidates(matches: Path, output_path: Path, workers: int) -> None:
    if output_path.exists():
        output_path.unlink()

    try:
        import pyarrow.parquet as pq

        available_columns = set(pq.ParquetFile(matches).schema_arrow.names)
    except Exception:
        available_columns = set()
    country_column = (
        "preferred_paper_country_code"
        if "preferred_paper_country_code" in available_columns
        else "author_country_codes_ordered"
    )
    reader = PieceReader(matches, columns=["UT", "id", country_column])
    iterator = reader.process(
        extract_country_candidates,
        ncpu=worker_count(workers),
        keep_order=False,
    )
    if tqdm is not None:
        iterator = tqdm(
            iterator,
            total=reader.task_count(),
            desc="Extract funded paper country groups",
            unit="task",
            file=sys.stdout,
        )

    schema = {"UT": "str", "country_group": "str"}
    with PieceSaver(schema, output_path, max_piece_size=1_000_000) as saver:
        for chunk in iterator:
            if chunk is None or chunk.empty:
                continue
            saver.write_dataframe(chunk)


def collect_streaming(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def build_country_counts(candidate_parquet: Path, wos_main: Path, start_year: int, end_year: int) -> pd.DataFrame:
    candidates = (
        pl.scan_parquet(candidate_parquet)
        .group_by("UT")
        .agg(pl.col("country_group").first())
    )
    years = (
        pl.scan_parquet(wos_main)
        .select(
            pl.col("UT"),
            pl.col("PY").cast(pl.Int64, strict=False).alias("year"),
        )
        .filter(pl.col("year").is_between(start_year, end_year))
    )
    counts = (
        candidates.join(years, on="UT", how="inner")
        .group_by(["year", "country_group"])
        .agg(pl.col("UT").n_unique().alias("funded_papers"))
        .sort(["year", "country_group"])
    )
    return collect_streaming(counts).to_pandas()


def merge_counts(base_counts: Path, country_counts: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    base = pd.read_csv(base_counts)
    base = base[(base["year"] >= start_year) & (base["year"] <= end_year)].copy()
    for column in COUNTRY_COLUMNS.values():
        base[column] = 0

    if not country_counts.empty:
        wide = country_counts.pivot_table(
            index="year",
            columns="country_group",
            values="funded_papers",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()
        wide.columns.name = None
        rename = {label: column for label, column in COUNTRY_COLUMNS.items() if label in wide.columns}
        wide = wide.rename(columns=rename)
        base = base.drop(columns=list(rename.values()), errors="ignore").merge(wide, on="year", how="left")

    for column in COUNTRY_COLUMNS.values():
        if column not in base.columns:
            base[column] = 0
        base[column] = base[column].fillna(0).astype("int64")
    return base


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = args.candidate_parquet
    if not candidate_path.is_absolute():
        candidate_path = args.output_dir / candidate_path.name

    write_country_candidates(args.matches, candidate_path, args.workers)
    country_counts = build_country_counts(candidate_path, args.wos_main, args.start_year, args.end_year)
    merged = merge_counts(args.counts, country_counts, args.start_year, args.end_year)

    csv_path = args.output_dir / (
        f"wos_funding_paper_counts_by_year_{args.start_year}_{args.end_year}"
        "_with_disambiguated_country_curves.csv"
    )
    merged.to_csv(csv_path, index=False)

    print(candidate_path)
    print(csv_path)


if __name__ == "__main__":
    main()
