from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import LogLocator, NullFormatter


DEFAULT_INPUT = Path("Results/FundingYearCounts/wos_funding_paper_counts_by_year_1950_2025_with_disambiguated_papers.csv")
DEFAULT_OUTPUT_DIR = Path("Results/FundingYearCounts")
COUNTRY_SERIES = [
    ("funded_papers_disambiguated_us", "US", "#5b6f95", "--"),
    ("funded_papers_disambiguated_europe", "Europe", "#8a6f2a", "--"),
    ("funded_papers_disambiguated_brazil", "Brazil", "#2a8c7a", "--"),
    ("funded_papers_disambiguated_japan", "Japan", "#7b5aa6", "--"),
    ("funded_papers_disambiguated_china", "China", "#c06f2c", "--"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot WoS funding paper counts by year.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=2025)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    counts = pd.read_csv(args.input)
    counts = counts[(counts["year"] >= args.start_year) & (counts["year"] <= args.end_year)].copy()
    if counts.empty:
        raise RuntimeError("No rows remain after applying the requested year window.")

    disambiguated_column = None
    disambiguated_label = ""
    suffix = ""
    if "papers_with_disambiguated_funding_entity" in counts.columns:
        disambiguated_column = "papers_with_disambiguated_funding_entity"
        disambiguated_label = "Papers with disambiguated funding entity"
        suffix = "_with_disambiguated_papers"
    elif "disambiguated_entries" in counts.columns:
        disambiguated_column = "disambiguated_entries"
        disambiguated_label = "Disambiguated funding entries"
        suffix = "_with_disambiguated_entries"
    country_series = [series for series in COUNTRY_SERIES if series[0] in counts.columns]
    if country_series:
        suffix = "_with_disambiguated_country_curves"
    csv_path = args.output_dir / (
        f"wos_funding_paper_counts_by_year_{args.start_year}_{args.end_year}{suffix}.csv"
    )
    png_path = args.output_dir / (
        f"wos_funding_paper_counts_by_year_{args.start_year}_{args.end_year}{suffix}_log.png"
    )
    pdf_path = png_path.with_suffix(".pdf")
    counts.to_csv(csv_path, index=False)

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.labelsize": 11,
            "axes.titlesize": 12,
            "legend.fontsize": 8,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.dpi": 150,
        }
    )

    fig, ax = plt.subplots(figsize=(8.4, 5.1))
    ax.plot(
        counts["year"],
        counts["total_papers"],
        color="#1f4e79",
        linewidth=2.0,
        label="Total papers",
    )
    ax.plot(
        counts["year"],
        counts["funded_papers"],
        color="#b23a33",
        linewidth=2.0,
        label="Papers with funding information",
    )
    if disambiguated_column is not None:
        ax.plot(
            counts["year"],
            counts[disambiguated_column],
            color="#4f7d3a",
            linewidth=2.0,
            label=disambiguated_label,
        )
    for column, label, color, linestyle in country_series:
        positive = counts[column].where(counts[column] > 0)
        ax.plot(
            counts["year"],
            positive,
            color=color,
            linestyle=linestyle,
            linewidth=1.6,
            label=label,
        )

    ax.set_yscale("log")
    ax.set_xlim(args.start_year, args.end_year)
    ax.set_xlabel("Publication year")
    ax.set_ylabel("Number of papers, log scale")
    ax.set_title(f"Web of Science funding information, {args.start_year}-{args.end_year}")
    ax.yaxis.set_major_locator(LogLocator(base=10))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=range(2, 10)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.grid(True, axis="y", which="major", color="#d0d0d0", linewidth=0.8)
    ax.grid(True, axis="y", which="minor", color="#eeeeee", linewidth=0.4)
    ax.grid(True, axis="x", which="major", color="#eeeeee", linewidth=0.5)
    legend = ax.legend(
        frameon=bool(country_series),
        loc="lower right",
        ncol=2 if country_series else 1,
        handlelength=2.6,
        columnspacing=1.0,
        borderpad=0.6,
        labelspacing=0.45,
    )
    if country_series:
        legend.get_frame().set_facecolor("white")
        legend.get_frame().set_edgecolor("#d8d8d8")
        legend.get_frame().set_alpha(0.92)
    fig.tight_layout()
    fig.savefig(png_path, dpi=300)
    fig.savefig(pdf_path)
    plt.close(fig)

    print(csv_path)
    print(png_path)
    print(pdf_path)


if __name__ == "__main__":
    main()
