from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


DEFAULT_MATCHES = Path("Results/grant_institutions_2025_affiliation_match_20251023.parquet")
DEFAULT_OUTPUT_DIR = Path("Results/Diagnostics")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize how affiliation/country priority information resolved ambiguous funder matches."
    )
    parser.add_argument("--matches", type=Path, default=DEFAULT_MATCHES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--basename", default=None)
    return parser.parse_args()


def collect_streaming(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def with_percentages(
    frame: pl.DataFrame,
    denominators: dict[str, int],
    *,
    include_multi_candidate_pct: bool = False,
    include_resolved_pct: bool = False,
) -> pl.DataFrame:
    result = frame
    total_rows = denominators.get("total_rows", 0)
    multi_rows = denominators.get("multi_candidate_rows", 0)
    resolved_rows = denominators.get("resolved_by_country_info_rows", 0)
    if "rows" in result.columns:
        if total_rows:
            result = result.with_columns((pl.col("rows") / total_rows * 100).alias("pct_total_rows"))
        if include_multi_candidate_pct and multi_rows:
            result = result.with_columns((pl.col("rows") / multi_rows * 100).alias("pct_multi_candidate_rows"))
        if include_resolved_pct and resolved_rows:
            result = result.with_columns((pl.col("rows") / resolved_rows * 100).alias("pct_resolved_by_country_info_rows"))
    return result


def write_group_summary(
    matches: pl.LazyFrame,
    group_columns: list[str],
    output_path: Path,
    denominators: dict[str, int],
    filter_expr: pl.Expr | None = None,
    include_multi_candidate_pct: bool = False,
    include_resolved_pct: bool = False,
) -> None:
    frame = matches
    if filter_expr is not None:
        frame = frame.filter(filter_expr)
    summary = (
        frame.group_by(group_columns)
        .agg(
            pl.len().alias("rows"),
            pl.col("UT").n_unique().alias("papers"),
        )
        .sort("rows", descending=True)
    )
    result = with_percentages(
        collect_streaming(summary),
        denominators,
        include_multi_candidate_pct=include_multi_candidate_pct,
        include_resolved_pct=include_resolved_pct,
    )
    result.write_csv(output_path)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    basename = args.basename or args.matches.with_suffix("").name

    matches = pl.scan_parquet(args.matches)
    total = collect_streaming(
        matches.select(
            pl.len().alias("total_rows"),
            pl.col("UT").n_unique().alias("total_papers"),
            pl.col("id").is_not_null().sum().alias("matched_rows"),
            (pl.col("candidate_count") > 1).sum().alias("multi_candidate_rows"),
            (pl.col("candidate_selection_reason").str.starts_with("multi_candidate_") & (pl.col("candidate_match_type") != "")).sum().alias(
                "resolved_by_country_info_rows"
            ),
            (pl.col("candidate_selection_reason") == "multi_candidate_default").sum().alias("multi_candidate_default_rows"),
            (pl.col("candidate_count") == 1).sum().alias("single_candidate_rows"),
            (pl.col("candidate_count") == 0).sum().alias("no_candidate_rows"),
        )
    )
    denominators = {name: int(total[name][0]) for name in total.columns}

    overall = total.with_columns(
        (pl.col("matched_rows") / pl.col("total_rows") * 100).alias("pct_rows_matched"),
        (pl.col("multi_candidate_rows") / pl.col("total_rows") * 100).alias("pct_rows_multi_candidate"),
        (pl.col("resolved_by_country_info_rows") / pl.col("total_rows") * 100).alias(
            "pct_rows_resolved_by_country_info"
        ),
        (pl.col("resolved_by_country_info_rows") / pl.col("multi_candidate_rows") * 100).alias(
            "pct_multi_candidate_resolved_by_country_info"
        ),
        (pl.col("multi_candidate_default_rows") / pl.col("multi_candidate_rows") * 100).alias(
            "pct_multi_candidate_default"
        ),
    )
    overall.write_csv(args.output_dir / f"matching_resolution_overall_{basename}.csv")

    write_group_summary(
        matches,
        ["candidate_selection_reason"],
        args.output_dir / f"matching_resolution_by_reason_{basename}.csv",
        denominators,
    )
    write_group_summary(
        matches,
        ["candidate_match_type"],
        args.output_dir / f"matching_resolution_by_match_type_{basename}.csv",
        denominators,
    )
    write_group_summary(
        matches,
        ["tie_break_country_source"],
        args.output_dir / f"matching_resolution_by_country_source_{basename}.csv",
        denominators,
        filter_expr=pl.col("tie_break_country_source") != "",
        include_resolved_pct=True,
    )
    write_group_summary(
        matches,
        ["funder_scope_type", "funder_scope_label"],
        args.output_dir / f"matching_resolution_by_funder_scope_{basename}.csv",
        denominators,
        filter_expr=pl.col("funder_scope_type") != "",
    )
    write_group_summary(
        matches,
        ["funder_scope_provenance"],
        args.output_dir / f"matching_resolution_by_funder_scope_provenance_{basename}.csv",
        denominators,
        filter_expr=pl.col("funder_scope_provenance") != "",
    )

    print(args.output_dir / f"matching_resolution_overall_{basename}.csv")
    print(args.output_dir / f"matching_resolution_by_reason_{basename}.csv")
    print(args.output_dir / f"matching_resolution_by_match_type_{basename}.csv")
    print(args.output_dir / f"matching_resolution_by_country_source_{basename}.csv")
    print(args.output_dir / f"matching_resolution_by_funder_scope_{basename}.csv")
    print(args.output_dir / f"matching_resolution_by_funder_scope_provenance_{basename}.csv")


if __name__ == "__main__":
    main()
