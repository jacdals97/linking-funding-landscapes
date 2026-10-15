from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from parcake_utils import PieceReader, default_worker_count
except ModuleNotFoundError:
    from scripts_wos_funding.parcake_utils import PieceReader, default_worker_count

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_WOS_MAIN = Path("data/WoS_2025_main.parquet")
DEFAULT_OUTPUT_DIR = Path("Results/FundingYearCounts")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count WoS papers and papers with any funding information by publication year."
    )
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--workers", type=int, default=23)
    return parser.parse_args()


def has_value(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    text = str(value).strip()
    return text not in {"", "[]", "{}", "nan", "NaN", "None", "null"}


def count_year_chunk(frame: pd.DataFrame, *_piece_args: Any) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["year", "total_papers", "funded_papers"])

    years = pd.to_numeric(frame["PY"], errors="coerce")
    valid = years.notna()
    if not valid.any():
        return pd.DataFrame(columns=["year", "total_papers", "funded_papers"])

    data = pd.DataFrame({"year": years[valid].astype("int64")})
    data["has_funding"] = (
        frame.loc[valid, "FP"].map(has_value)
        | frame.loc[valid, "FU"].map(has_value)
        | frame.loc[valid, "FX"].map(has_value)
    )
    grouped = data.groupby("year", sort=False)["has_funding"].agg(total_papers="size", funded_papers="sum")
    return grouped.reset_index()


def build_counts(wos_main: Path, start_year: int, workers: int) -> pd.DataFrame:
    reader = PieceReader(wos_main, columns=["PY", "FP", "FU", "FX"])
    iterator = reader.process(count_year_chunk, ncpu=default_worker_count(workers), keep_order=False)
    if tqdm is not None:
        iterator = tqdm(
            iterator,
            total=reader.task_count(),
            desc="Count WoS funding by year",
            unit="task",
            file=sys.stdout,
        )

    yearly: dict[int, list[int]] = {}
    for chunk in iterator:
        if chunk.empty:
            continue
        for year, total_papers, funded_papers in chunk[["year", "total_papers", "funded_papers"]].itertuples(index=False):
            if int(year) < start_year:
                continue
            totals = yearly.setdefault(int(year), [0, 0])
            totals[0] += int(total_papers)
            totals[1] += int(funded_papers)

    if not yearly:
        return pd.DataFrame(columns=["year", "funded_papers", "total_papers"])

    years = range(start_year, max(yearly) + 1)
    rows = [
        {
            "year": year,
            "funded_papers": yearly.get(year, [0, 0])[1],
            "total_papers": yearly.get(year, [0, 0])[0],
        }
        for year in years
    ]
    return pd.DataFrame(rows)


def write_plot(counts: pd.DataFrame, output_path: Path) -> None:
    width, height = 1100, 680
    left, right, top, bottom = 90, 35, 55, 75
    plot_width = width - left - right
    plot_height = height - top - bottom

    positive_values = pd.concat(
        [
            counts.loc[counts["total_papers"] > 0, "total_papers"],
            counts.loc[counts["funded_papers"] > 0, "funded_papers"],
        ],
        ignore_index=True,
    )
    if positive_values.empty:
        raise RuntimeError("No positive counts available for log-scale plotting.")

    min_year = int(counts["year"].min())
    max_year = int(counts["year"].max())
    min_exp = math.floor(math.log10(float(positive_values.min())))
    max_exp = math.ceil(math.log10(float(positive_values.max())))
    min_log = float(min_exp)
    max_log = float(max_exp)

    def x_pos(year: int) -> float:
        if max_year == min_year:
            return left + plot_width / 2
        return left + (year - min_year) / (max_year - min_year) * plot_width

    def y_pos(value: int) -> float:
        log_value = math.log10(float(value))
        return top + (max_log - log_value) / (max_log - min_log) * plot_height

    def polyline(column: str) -> str:
        points = [
            f"{x_pos(int(row.year)):.1f},{y_pos(int(getattr(row, column))):.1f}"
            for row in counts.itertuples(index=False)
            if int(getattr(row, column)) > 0
        ]
        return " ".join(points)

    y_ticks = [10**exp for exp in range(min_exp, max_exp + 1)]
    x_step = 10
    first_tick = ((min_year + x_step - 1) // x_step) * x_step
    x_ticks = list(range(first_tick, max_year + 1, x_step))
    if min_year not in x_ticks:
        x_ticks.insert(0, min_year)
    if max_year not in x_ticks:
        x_ticks.append(max_year)

    y_grid = "\n".join(
        f'<line x1="{left}" y1="{y_pos(tick):.1f}" x2="{width - right}" y2="{y_pos(tick):.1f}" stroke="#d7d7d7" stroke-width="1"/>'
        f'<text x="{left - 10}" y="{y_pos(tick) + 4:.1f}" text-anchor="end" font-size="12">{tick:g}</text>'
        for tick in y_ticks
    )
    x_labels = "\n".join(
        f'<line x1="{x_pos(tick):.1f}" y1="{height - bottom}" x2="{x_pos(tick):.1f}" y2="{height - bottom + 5}" stroke="#333"/>'
        f'<text x="{x_pos(tick):.1f}" y="{height - bottom + 24}" text-anchor="middle" font-size="12">{tick}</text>'
        for tick in x_ticks
    )

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="{width / 2}" y="28" text-anchor="middle" font-size="20" font-family="Arial, sans-serif">Web of Science papers and funding information over time</text>
<text x="{width / 2}" y="{height - 25}" text-anchor="middle" font-size="15" font-family="Arial, sans-serif">Publication year</text>
<text x="22" y="{height / 2}" text-anchor="middle" font-size="15" font-family="Arial, sans-serif" transform="rotate(-90 22 {height / 2})">Number of papers, log scale</text>
<g font-family="Arial, sans-serif" fill="#333">
{y_grid}
{x_labels}
<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#333" stroke-width="1.5"/>
<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#333" stroke-width="1.5"/>
<polyline points="{polyline("total_papers")}" fill="none" stroke="#1f77b4" stroke-width="3"/>
<polyline points="{polyline("funded_papers")}" fill="none" stroke="#d62728" stroke-width="3"/>
<line x1="{width - 310}" y1="58" x2="{width - 270}" y2="58" stroke="#1f77b4" stroke-width="3"/>
<text x="{width - 260}" y="63" font-size="14">Total papers</text>
<line x1="{width - 310}" y1="82" x2="{width - 270}" y2="82" stroke="#d62728" stroke-width="3"/>
<text x="{width - 260}" y="87" font-size="14">Papers with funding info</text>
</g>
</svg>
'''
    output_path.write_text(svg)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    counts = build_counts(args.wos_main, args.start_year, args.workers)
    csv_path = args.output_dir / f"wos_funding_paper_counts_by_year_from_{args.start_year}.csv"
    plot_path = args.output_dir / f"wos_funding_paper_counts_by_year_from_{args.start_year}_log.svg"
    counts.to_csv(csv_path, index=False)
    write_plot(counts, plot_path)

    print(csv_path)
    print(plot_path)


if __name__ == "__main__":
    main()
