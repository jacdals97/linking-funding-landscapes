from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.colors import LogNorm, Normalize
from matplotlib.patches import Polygon
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PLOTTING_DIR = REPO_ROOT / "plotting_data"
OUTPUT_DIR = REPO_ROOT / "paper_figures_output"
FIGURE_DATA_DIR = OUTPUT_DIR / "data"
RESULTS_DIR = REPO_ROOT / "Results"
SOURCE_RESULTS_DIR = RESULTS_DIR / "SourcePaperFunders"
WOS_YEAR_COUNTS = RESULTS_DIR / "FundingYearCounts/wos_funding_paper_counts_by_year_from_1950.csv"
SCOPE_MAP = REPO_ROOT / "scripts_wos_funding/funder_multicountry_scope_map_top5000_reviewed.csv"
DIMENSIONS_ROOT = None
COUNTRY_CACHE_DIR = FIGURE_DATA_DIR
WORLD_GEOJSON = PLOTTING_DIR / "ne_110m_admin_0_countries.geojson"
WORLD_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_110m_admin_0_countries.geojson"
)


DATASET_STYLES = {
    "Web of Science": {"color": "#1f77b4", "marker": "o"},
    "Dimensions": {"color": "#ff7f0e", "marker": "o"},
    "OpenAlex": {"color": "#2ca02c", "marker": "o"},
}


FUNDER_STYLES = {
    "All publications": {"sector": None, "color": "#7f7f7f"},
    "Government": {"sector": "government", "color": "#1f77b4"},
    "Philanthropy": {"sector": "nonprofit", "color": "#ff7f0e"},
    "Business": {"sector": "company", "color": "#2ca02c"},
}


GROUP_LABELS = {
    "africa": "African",
    "americas": "Americas",
    "asean": "ASEAN",
    "asia_pacific": "Asia-Pacific",
    "commonwealth": "Commonwealth",
    "europe_africa": "Europe-Africa",
    "europe_research_area": "European",
    "global": "Global",
    "isdb_members": "IsDB members",
    "nato_members": "NATO",
    "visegrad": "Visegrad",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regenerate paper figures from plotting summaries.")
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--source-results-dir", type=Path, default=None)
    parser.add_argument("--plotting-dir", type=Path, default=PLOTTING_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--wos-year-counts", type=Path, default=None)
    parser.add_argument("--scope-map", type=Path, default=SCOPE_MAP)
    parser.add_argument("--dimensions-root", type=Path, default=None)
    parser.add_argument("--country-cache-dir", type=Path, default=None)
    parser.add_argument("--groups", nargs="+", choices=["coverage", "rank-frequency", "country-maps", "scope"],
                        default=["coverage", "rank-frequency", "country-maps", "scope"])
    parser.add_argument("--start-year", type=int, default=1980)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--formats", nargs="+", default=["pdf", "png"], choices=["pdf", "png", "svg"])
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading Natural Earth GeoJSON if it is not already cached.",
    )
    parser.add_argument(
        "--rebuild-dimensions-country",
        action="store_true",
        help="Rebuild the cached Dimensions funder-country summary from the processed parquet.",
    )
    parser.add_argument(
        "--rebuild-dimensions-author-country",
        action="store_true",
        help="Rebuild the cached Dimensions author-country coverage summary from the processed parquet.",
    )
    return parser.parse_args()


def configure_paths(*, results_dir=RESULTS_DIR, source_results_dir=None,
                    plotting_dir=PLOTTING_DIR, output_dir=OUTPUT_DIR,
                    wos_year_counts=None, scope_map=SCOPE_MAP,
                    dimensions_root=None, country_cache_dir=None):
    global RESULTS_DIR, SOURCE_RESULTS_DIR, PLOTTING_DIR, OUTPUT_DIR, FIGURE_DATA_DIR
    global WOS_YEAR_COUNTS, SCOPE_MAP, DIMENSIONS_ROOT, WORLD_GEOJSON, COUNTRY_CACHE_DIR
    RESULTS_DIR = Path(results_dir)
    SOURCE_RESULTS_DIR = Path(source_results_dir) if source_results_dir else RESULTS_DIR / "SourcePaperFunders"
    PLOTTING_DIR, OUTPUT_DIR = Path(plotting_dir), Path(output_dir)
    FIGURE_DATA_DIR = OUTPUT_DIR / "data"
    WOS_YEAR_COUNTS = Path(wos_year_counts) if wos_year_counts else RESULTS_DIR / "FundingYearCounts/wos_funding_paper_counts_by_year_from_1950.csv"
    SCOPE_MAP = Path(scope_map)
    DIMENSIONS_ROOT = Path(dimensions_root) if dimensions_root else None
    WORLD_GEOJSON = PLOTTING_DIR / "ne_110m_admin_0_countries.geojson"
    COUNTRY_CACHE_DIR = Path(country_cache_dir) if country_cache_dir else FIGURE_DATA_DIR


def set_pub_defaults() -> None:
    plt.style.use("default")
    mpl.rcParams.update(
        {
            "font.size": 8,
            "axes.labelsize": 8,
            "axes.titlesize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "lines.linewidth": 1.8,
            "axes.linewidth": 0.8,
            "figure.dpi": 140,
            "savefig.dpi": 300,
        }
    )


def format_large_tick(value: float, _pos=None) -> str:
    if value == 0 or not np.isfinite(value):
        return "0"
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000:
        return f"{sign}{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{sign}{value / 1_000:.0f}K"
    return f"{sign}{value:.0f}"


def save_figure(fig: plt.Figure, stem: str, formats: list[str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(OUTPUT_DIR / f"{stem}.{fmt}", bbox_inches="tight")


def save_figure_aliases(fig: plt.Figure, stems: list[str], formats: list[str]) -> None:
    for stem in stems:
        save_figure(fig, stem, formats)


def lighten_color(hex_color: str, amount: float = 0.35) -> tuple[float, float, float]:
    rgb = np.array(mpl.colors.to_rgb(hex_color))
    return tuple(rgb + (1.0 - rgb) * amount)


def load_coverage_data(start_year: int, end_year: int) -> pd.DataFrame:
    wos = pd.read_csv(WOS_YEAR_COUNTS)
    wos = wos.rename(columns={"year": "publication_year", "funded_papers": "papers_with_funding_info"})
    wos["dataset"] = "Web of Science"

    oa = pd.read_parquet(SOURCE_RESULTS_DIR / "openalex_paper_counts_by_year.parquet")
    oa["dataset"] = "OpenAlex"

    dimensions = pd.read_parquet(SOURCE_RESULTS_DIR / "dimensions_paper_counts_by_year.parquet")
    dimensions["dataset"] = "Dimensions"

    columns = ["dataset", "publication_year", "total_papers", "papers_with_funding_info"]
    combined = pd.concat([wos[columns], oa[columns], dimensions[columns]], ignore_index=True)
    combined["publication_year"] = pd.to_numeric(combined["publication_year"], errors="coerce")
    combined = combined.dropna(subset=["publication_year"])
    combined["publication_year"] = combined["publication_year"].astype(int)
    combined = combined.loc[
        (combined["publication_year"] >= start_year) & (combined["publication_year"] <= end_year)
    ].copy()
    combined["share_with_funding_info"] = (
        combined["papers_with_funding_info"] / combined["total_papers"].replace(0, np.nan)
    )
    combined = combined.sort_values(["dataset", "publication_year"])

    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(FIGURE_DATA_DIR / f"coverage_over_time_{start_year}_{end_year}.csv", index=False)
    return combined


def plot_coverage_over_time(data: pd.DataFrame, start_year: int, end_year: int, formats: list[str]) -> None:
    datasets = list(DATASET_STYLES)
    bar_alpha = 0.24
    bar_width = 0.8

    variants = ["solid", "dashed_counts"]

    for variant_name in variants:
      for log_y in [False, True]:
        fig, ax = plt.subplots(figsize=(5.2, 3.8))
        ax2 = ax.twinx()

        if variant_name == "solid":
            for dataset in datasets:
                style = DATASET_STYLES[dataset]
                sub = data.loc[data["dataset"] == dataset]
                if sub.empty:
                    continue
                ax2.bar(
                    sub["publication_year"],
                    sub["papers_with_funding_info"],
                    width=bar_width,
                    color=style["color"],
                    alpha=bar_alpha,
                    linewidth=0,
                    zorder=0,
                )
        else:
            for dataset in datasets:
                style = DATASET_STYLES[dataset]
                sub = data.loc[data["dataset"] == dataset]
                if sub.empty:
                    continue
                ax2.plot(
                    sub["publication_year"],
                    sub["papers_with_funding_info"],
                    color=lighten_color(style["color"], 0.38),
                    linestyle="--",
                    marker=style["marker"],
                    markersize=3.0,
                    linewidth=1.4,
                    alpha=0.88,
                    zorder=1,
                )

        for dataset in datasets:
            style = DATASET_STYLES[dataset]
            sub = data.loc[data["dataset"] == dataset]
            if sub.empty:
                continue
            ax.plot(
                sub["publication_year"],
                sub["share_with_funding_info"],
                label=dataset,
                color=style["color"],
                marker=style["marker"],
                linestyle="-",
                markersize=4.0,
                zorder=3,
            )

        ax.set_xlim(start_year - 0.7, end_year + 0.7)
        ax.set_xlabel("Publication year")
        ax.set_ylabel("% Publications with grants (lines)")
        ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax=1.0))
        ax.grid(True, linestyle="--", color="0.75", alpha=0.8)
        ax.tick_params(axis="x", rotation=45)

        count_kind = "bars" if variant_name == "solid" else "dashed lines"
        ax2.set_ylabel(f"Publications with grants ({count_kind})")
        ax2.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(format_large_tick))

        if log_y:
            positive_share = data.loc[data["share_with_funding_info"] > 0, "share_with_funding_info"]
            positive_counts = data.loc[data["papers_with_funding_info"] > 0, "papers_with_funding_info"]
            ax.set_yscale("log")
            ax2.set_yscale("log")
            ax.set_ylim(positive_share.min() * 0.75, min(1.0, positive_share.max() * 1.25))
            ax2.set_ylim(positive_counts.min() * 0.75, positive_counts.max() * 1.8)
        else:
            ax.set_ylim(0, min(1.0, data["share_with_funding_info"].max() * 1.12))
            ax2.set_ylim(0, data["papers_with_funding_info"].max() * 1.16)

        ax2.set_zorder(0)
        ax.set_zorder(1)
        ax.patch.set_alpha(0)
        ax.legend(loc="upper left", frameon=True, framealpha=1)
        ax.spines[["top"]].set_visible(False)
        ax2.spines[["top"]].set_visible(False)

        suffix = "log_y" if log_y else "linear_y"
        legacy_suffix = "log_counts" if log_y else "linear"
        variant_part = "" if variant_name == "solid" else f"_{variant_name}"
        stems = [
            f"coverage_counts_rate_bars_curves{variant_part}_{start_year}_{end_year}_{suffix}",
        ]
        if variant_name == "solid":
            stems.append(f"coverage_counts_ratio_{start_year}_{end_year}_{legacy_suffix}")
        fig.tight_layout()
        save_figure_aliases(fig, stems, formats)
        plt.close(fig)


def read_tsv(name: str) -> pd.DataFrame:
    return pd.read_csv(
        PLOTTING_DIR / name,
        sep="\t",
        na_values=["", "NaN", "None", "null"],
        keep_default_na=True,
    )


def build_rank_frequency_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    funders = read_tsv("wos_funder_types.tsv")
    funders["n_pubs"] = pd.to_numeric(funders["n_pubs"], errors="coerce")
    funders = funders.loc[funders["n_pubs"] > 0].copy()

    rank_frames: list[pd.DataFrame] = []
    ccdf_frames: list[pd.DataFrame] = []
    for label, style in FUNDER_STYLES.items():
        sector = style["sector"]
        subset = funders if sector is None else funders.loc[funders["oa_ancestortypes"] == sector]
        counts = subset["n_pubs"].sort_values(ascending=False).reset_index(drop=True)
        if counts.empty:
            continue

        rank_frames.append(
            pd.DataFrame(
                {
                    "series": label,
                    "rank_index": np.arange(len(counts)),
                    "funder_rank": np.arange(1, len(counts) + 1),
                    "share_of_funders": np.arange(1, len(counts) + 1) / len(counts),
                    "complementary_cumulative_share_of_publications": 1.0
                    - counts.cumsum() / counts.sum(),
                    "publications": counts,
                }
            )
        )

        frequencies = counts.value_counts().sort_index()
        survival = frequencies.iloc[::-1].cumsum().iloc[::-1] / frequencies.sum()
        ccdf_frames.append(
            pd.DataFrame(
                {
                    "series": label,
                    "publications": survival.index.astype(float),
                    "share_of_funders_at_or_above": survival.to_numpy(),
                }
            )
        )

    rank_data = pd.concat(rank_frames, ignore_index=True)
    ccdf_data = pd.concat(ccdf_frames, ignore_index=True)
    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    rank_data.to_csv(FIGURE_DATA_DIR / "wos_funder_rank_frequency.csv", index=False)
    ccdf_data.to_csv(FIGURE_DATA_DIR / "wos_funder_publication_ccdf.csv", index=False)
    return rank_data, ccdf_data


def plot_wos_funder_rank_frequency(formats: list[str]) -> None:
    rank_data, ccdf_data = build_rank_frequency_data()
    fig, ax = plt.subplots(figsize=(5.6, 5.3))

    for label, style in FUNDER_STYLES.items():
        curve = rank_data.loc[rank_data["series"] == label]
        if curve.empty:
            continue
        ax.plot(
            curve["share_of_funders"],
            curve["complementary_cumulative_share_of_publications"],
            color=style["color"],
            label=label,
            linewidth=2.0,
        )

    all_curve = rank_data.loc[rank_data["series"] == "All publications"].reset_index(drop=True)
    eighty = all_curve.loc[
        all_curve["complementary_cumulative_share_of_publications"] <= 0.20
    ].iloc[0]
    ninety_seven = all_curve.loc[
        all_curve["complementary_cumulative_share_of_publications"] <= 0.025
    ].iloc[0]

    guide_color = "#7f7f7f"
    ax.hlines(0.20, -0.035, eighty["share_of_funders"], color=guide_color, linestyle=":", linewidth=1.5)
    ax.vlines(eighty["share_of_funders"], -0.035, 0.20, color=guide_color, linestyle=":", linewidth=1.5)
    ax.hlines(0.025, -0.035, ninety_seven["share_of_funders"], color=guide_color, linestyle=":", linewidth=1.5)
    ax.vlines(ninety_seven["share_of_funders"], -0.035, 0.025, color=guide_color, linestyle=":", linewidth=1.5)

    ax.annotate(
        f"{int(eighty['funder_rank']):,} unique funders",
        xy=(eighty["share_of_funders"], 0.20),
        xytext=(0.065, 0.25),
        arrowprops={"arrowstyle": "->", "color": "black", "linewidth": 1.0},
        fontsize=8,
    )
    ax.annotate(
        "97.5% of publications",
        xy=(ninety_seven["share_of_funders"], 0.025),
        xytext=(0.25, 0.075),
        arrowprops={"arrowstyle": "->", "color": "black", "linewidth": 1.0},
        fontsize=8,
    )

    inset = ax.inset_axes([0.52, 0.53, 0.45, 0.44])
    for label, style in FUNDER_STYLES.items():
        curve = ccdf_data.loc[ccdf_data["series"] == label]
        if curve.empty:
            continue
        inset.step(
            curve["publications"],
            curve["share_of_funders_at_or_above"],
            where="post",
            color=style["color"],
            linewidth=1.8,
        )
    inset.set_xscale("log")
    inset.set_yscale("log")
    inset.set_xlabel("Publications", labelpad=2)
    inset.set_ylabel("P(N ≥ n)", labelpad=2)
    inset.set_ylim(6e-6, 1.8)
    inset.grid(True, color="0.82", alpha=0.45, linewidth=0.7)
    inset.spines[["top", "right"]].set_visible(False)

    ax.set_xlim(-0.05, 1.0)
    ax.set_ylim(-0.05, 1.0)
    ax.set_xticks(np.arange(0, 1.0, 0.2))
    ax.set_xlabel("Share of funders by size (descending)")
    ax.set_ylabel("Complementary Cumulative share of publications")
    ax.legend(loc="lower right", bbox_to_anchor=(1.0, 0.08), frameon=True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save_figure_aliases(
        fig,
        ["wos_funder_rank_frequency", "figure_5_wos_funder_rank_frequency"],
        formats,
    )
    plt.close(fig)


def country_only(df: pd.DataFrame, country_col: str) -> pd.DataFrame:
    out = df.copy()
    out[country_col] = out[country_col].astype("string").str.upper()
    return out.loc[out[country_col].str.fullmatch(r"[A-Z]{2}", na=False)].copy()


def path_from_registry(key: str) -> Path:
    for path in [REPO_ROOT / "PATHS.md", REPO_ROOT / "scisci_skills" / "PATHS.md"]:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            clean = line.strip()
            if not clean or clean.startswith("#") or "=" not in clean:
                continue
            found_key, value = clean.split("=", 1)
            if found_key.strip() == key:
                return Path(value.strip())
    raise KeyError(f"Could not resolve {key} from PATHS.md")


def collect_streaming(lazy_frame):
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def dimensions_funder_country_summary(rebuild: bool = False) -> pd.DataFrame:
    output_path = FIGURE_DATA_DIR / "dimensions_funder_country_country_only.csv"
    cache_path = COUNTRY_CACHE_DIR / output_path.name
    if cache_path.exists() and not rebuild:
        return pd.read_csv(cache_path)

    import polars as pl

    dimensions_root = DIMENSIONS_ROOT or path_from_registry("dimensions_raw_parquet_root")
    organizations = (
        pl.scan_parquet(str(dimensions_root / "organizations" / "*.parquet"))
        .select(
            pl.col("id").alias("funder_id"),
            pl.coalesce(
                [
                    pl.col("legal_country_code"),
                    pl.col("address").struct.field("country_code"),
                ]
            ).alias("funder_countries"),
        )
        .with_columns(pl.col("funder_countries").str.to_uppercase())
        .filter(pl.col("funder_countries").str.contains(r"^[A-Z]{2}$"))
        .unique("funder_id")
    )

    details = (
        pl.scan_parquet(str(SOURCE_RESULTS_DIR / "dimensions_paper_funders.parquet"))
        .filter(pl.col("has_funding_info") & pl.col("funder_id").is_not_null())
        .select("paper_id", "funder_id")
        .unique()
        .join(organizations, on="funder_id", how="left")
        .filter(pl.col("funder_countries").is_not_null())
        .select("paper_id", "funder_countries")
        .unique()
        .with_columns(pl.len().over("paper_id").alias("n_funder_countries"))
        .with_columns((1.0 / pl.col("n_funder_countries")).alias("funder_fraction"))
        .group_by("funder_countries")
        .agg(pl.col("funder_fraction").sum().alias("fractional_funded_paper_count"))
        .with_columns(
            (
                pl.col("fractional_funded_paper_count")
                / pl.col("fractional_funded_paper_count").sum()
            ).alias("global_share")
        )
        .sort("fractional_funded_paper_count", descending=True)
    )

    df = collect_streaming(details).to_pandas()
    df = country_only(df, "funder_countries")
    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


def dimensions_author_country_summary(rebuild: bool = False) -> pd.DataFrame:
    output_path = FIGURE_DATA_DIR / "dimensions_author_country_country_only.csv"
    cache_path = COUNTRY_CACHE_DIR / output_path.name
    if cache_path.exists() and not rebuild:
        return pd.read_csv(cache_path)

    import polars as pl

    details = (
        pl.scan_parquet(str(SOURCE_RESULTS_DIR / "dimensions_paper_funders.parquet"))
        .select("paper_id", "country_codes_priority", "has_funding_info")
        .unique()
        .filter(pl.col("country_codes_priority").is_not_null())
        .with_columns(pl.col("country_codes_priority").str.split(",").alias("auth_countries"))
        .explode("auth_countries")
        .with_columns(pl.col("auth_countries").str.strip_chars().str.to_uppercase())
        .filter(pl.col("auth_countries").str.contains(r"^[A-Z]{2}$"))
        .unique(["paper_id", "auth_countries"])
        .with_columns(pl.len().over("paper_id").alias("n_auth_countries"))
        .with_columns(
            (
                pl.col("has_funding_info").cast(pl.Float64) / pl.col("n_auth_countries")
            ).alias("funded_fraction")
        )
        .group_by("auth_countries")
        .agg(
            pl.col("has_funding_info").mean().alias("share_has_grant"),
            pl.col("funded_fraction").sum().alias("fractional_funded_paper_count"),
            pl.len().alias("paper_country_rows"),
        )
        .sort("fractional_funded_paper_count", descending=True)
    )

    df = collect_streaming(details).to_pandas()
    df = country_only(df, "auth_countries")
    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


def build_scope_group_summary() -> pd.DataFrame:
    top = pd.read_csv(RESULTS_DIR / "TopFunders/top5000_disambiguated_funders_scope_priority.csv")
    scoped = top.loc[top["funder_scope_label"].notna()].copy()
    scoped["group"] = scoped["funder_scope_label"].map(lambda x: GROUP_LABELS.get(str(x), str(x).replace("_", " ").title()))
    paper_grouped = (
        scoped.groupby("group", as_index=False)
        .agg(papers=("papers", "sum"), funders=("id", "nunique"))
        .sort_values("papers", ascending=False)
    )

    reviewed_path = SCOPE_MAP
    reviewed = pd.read_csv(reviewed_path)
    reviewed["group"] = reviewed["scope_label"].map(lambda x: GROUP_LABELS.get(str(x), str(x).replace("_", " ").title()))
    reviewed_grouped = (
        reviewed.groupby("group", as_index=False)
        .agg(reviewed_scope_funders=("openalex_id", "nunique"))
        .sort_values("reviewed_scope_funders", ascending=False)
    )

    grouped = reviewed_grouped.merge(paper_grouped, on="group", how="outer").fillna(0)
    grouped["papers"] = grouped["papers"].astype(int)
    grouped["funders"] = grouped["funders"].astype(int)
    grouped["reviewed_scope_funders"] = grouped["reviewed_scope_funders"].astype(int)
    grouped = grouped.sort_values(["papers", "reviewed_scope_funders"], ascending=False)
    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(FIGURE_DATA_DIR / "regional_global_funder_groups_top5000.csv", index=False)
    return grouped


def plot_scope_group_summary(formats: list[str]) -> None:
    grouped = build_scope_group_summary()
    if grouped.empty:
        return

    paper_groups = grouped.loc[grouped["papers"] > 0].sort_values("papers", ascending=True)
    reviewed_groups = grouped.sort_values("reviewed_scope_funders", ascending=True).tail(12)

    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.1))

    axes[0].barh(paper_groups["group"], paper_groups["papers"], color="#6cc5a8")
    axes[0].set_xlabel("Funded papers")
    axes[0].set_xscale("log")
    axes[0].xaxis.set_major_formatter(mpl.ticker.FuncFormatter(format_large_tick))
    axes[0].set_title("Observed in top-5000 matches")

    axes[1].barh(reviewed_groups["group"], reviewed_groups["reviewed_scope_funders"], color="#8da0cb")
    axes[1].set_xlabel("Reviewed scoped funders")
    axes[1].set_title("Scope inventory")

    for ax in axes:
        ax.grid(axis="x", linestyle=":", alpha=0.35)
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save_figure(fig, "regional_global_funder_groups_top5000", formats)
    plt.close(fig)


def ensure_world_geojson(skip_download: bool) -> dict | None:
    if not WORLD_GEOJSON.exists() and not skip_download:
        PLOTTING_DIR.mkdir(parents=True, exist_ok=True)
        req = Request(WORLD_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=60) as response, WORLD_GEOJSON.open("wb") as fh:
            fh.write(response.read())
    if not WORLD_GEOJSON.exists():
        print(f"Skipping maps: {WORLD_GEOJSON} does not exist.")
        return None
    return json.loads(WORLD_GEOJSON.read_text())


def feature_iso_a2(properties: dict) -> str | None:
    for key in ["ISO_A2_EH", "ISO_A2", "iso_a2"]:
        value = properties.get(key)
        if isinstance(value, str) and len(value) == 2 and value != "-99":
            return value.upper()
    return None


def geometry_polygons(geometry: dict) -> list[np.ndarray]:
    polygons: list[np.ndarray] = []
    if not geometry:
        return polygons
    if geometry["type"] == "Polygon":
        rings = geometry["coordinates"]
        if rings:
            polygons.append(np.asarray(rings[0], dtype=float))
    elif geometry["type"] == "MultiPolygon":
        for poly in geometry["coordinates"]:
            if poly:
                polygons.append(np.asarray(poly[0], dtype=float))
    return polygons


def map_data_to_patches(world: dict, values: dict[str, float]) -> tuple[list[Polygon], list[float]]:
    patches: list[Polygon] = []
    patch_values: list[float] = []
    for feature in world["features"]:
        iso = feature_iso_a2(feature.get("properties", {}))
        if not iso or iso == "AQ":
            continue
        value = values.get(iso)
        if value is None or not np.isfinite(value) or value <= 0:
            value = np.nan
        for coords in geometry_polygons(feature.get("geometry", {})):
            patches.append(Polygon(coords, closed=True))
            patch_values.append(value)
    return patches, patch_values


def country_values(df: pd.DataFrame, country_col: str, value_col: str) -> dict[str, float]:
    clean = country_only(df, country_col)
    values = pd.to_numeric(clean[value_col], errors="coerce")
    return dict(zip(clean[country_col], values))


def draw_country_map_panel(
    ax: plt.Axes,
    world: dict,
    values: dict[str, float],
    *,
    cmap,
    norm: Normalize,
) -> mpl.cm.ScalarMappable:
    patches, patch_values = map_data_to_patches(world, values)
    arr = np.asarray(patch_values, dtype=float)
    facecolors = [
        cmap(norm(value)) if np.isfinite(value) else (0.84, 0.84, 0.84, 1.0)
        for value in arr
    ]
    collection = PatchCollection(patches, facecolor=facecolors, edgecolor="white", linewidth=0.18)
    ax.add_collection(collection)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-58, 85)
    ax.set_aspect("equal", adjustable="box")
    ax.axis("off")
    sm = mpl.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    return sm


def make_country_norm(
    values: list[float],
    *,
    log_scale: bool,
    vmin: float | None,
    vmax: float | None,
) -> Normalize:
    arr = np.asarray(values, dtype=float)
    finite = arr[np.isfinite(arr)]
    if log_scale:
        finite = finite[finite > 0]
    if finite.size == 0:
        raise ValueError("No finite positive map values available")
    low = float(np.nanmin(finite)) if vmin is None else float(vmin)
    high = float(np.nanmax(finite)) if vmax is None else float(vmax)
    if log_scale:
        if low <= 0:
            low = float(np.nanmin(finite))
        return LogNorm(vmin=low, vmax=high)
    return Normalize(vmin=low, vmax=high)


def plot_country_map_stack(
    world: dict | None,
    datasets: list[tuple[str, pd.DataFrame, str]],
    *,
    value_col: str,
    legend_label: str,
    out_stem: str,
    formats: list[str],
    log_scale: bool = False,
    vmin: float | None = None,
    vmax: float | None = None,
    aliases: list[str] | None = None,
) -> None:
    if world is None:
        return

    all_values: list[float] = []
    prepared: list[tuple[str, dict[str, float]]] = []
    for label, df, country_col in datasets:
        values = country_values(df, country_col, value_col)
        prepared.append((label, values))
        for value in values.values():
            if value is None or not np.isfinite(value) or value <= 0:
                continue
            all_values.append(float(value))
    if not all_values:
        return

    cmap = plt.cm.viridis
    norm = make_country_norm(all_values, log_scale=log_scale, vmin=vmin, vmax=vmax)

    fig, axes = plt.subplots(len(prepared), 1, figsize=(6.1, 8.8))
    if len(prepared) == 1:
        axes = [axes]
    panel_labels = ["(a)", "(b)", "(c)", "(d)"]

    for idx, (ax, (label, values)) in enumerate(zip(axes, prepared)):
        sm = draw_country_map_panel(ax, world, values, cmap=cmap, norm=norm)
        cbar = fig.colorbar(sm, ax=ax, fraction=0.024, pad=0.012)
        cbar.set_label(legend_label)
        ax.text(
            0.5,
            -0.08,
            f"{panel_labels[idx]} {label}",
            transform=ax.transAxes,
            ha="center",
            va="top",
            fontsize=10,
        )

    fig.tight_layout(h_pad=1.6)
    save_figure_aliases(fig, [out_stem, *(aliases or [])], formats)
    plt.close(fig)


def plot_country_map(
    world: dict | None,
    df: pd.DataFrame,
    country_col: str,
    value_col: str,
    legend_label: str,
    out_stem: str,
    formats: list[str],
    *,
    log_scale: bool = False,
    vmin: float | None = None,
    vmax: float | None = None,
) -> None:
    if world is None:
        return
    clean = country_only(df, country_col)
    values = clean.set_index(country_col)[value_col].to_dict()
    patches, patch_values = map_data_to_patches(world, values)
    if not patches:
        return

    arr = np.asarray(patch_values, dtype=float)
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return
    norm = make_country_norm(finite.tolist(), log_scale=log_scale, vmin=vmin, vmax=vmax)
    cmap = plt.cm.viridis
    facecolors = [cmap(norm(v)) if np.isfinite(v) else (0.86, 0.86, 0.86, 1.0) for v in arr]

    fig, ax = plt.subplots(figsize=(5.4, 2.8))
    collection = PatchCollection(patches, facecolor=facecolors, edgecolor="white", linewidth=0.25)
    ax.add_collection(collection)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-58, 85)
    ax.set_aspect("equal", adjustable="box")
    ax.axis("off")

    sm = mpl.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.026, pad=0.01)
    cbar.set_label(legend_label)
    fig.tight_layout()
    save_figure(fig, out_stem, formats)
    plt.close(fig)


def plot_country_maps(
    formats: list[str],
    skip_download: bool,
    rebuild_dimensions_country: bool,
    rebuild_dimensions_author_country: bool,
) -> None:
    world = ensure_world_geojson(skip_download)
    wos_author = country_only(read_tsv("wos_author_country.tsv"), "auth_countries")
    openalex_author = country_only(read_tsv("openalex_author_country.tsv"), "auth_countries")
    dimensions_author = dimensions_author_country_summary(rebuild_dimensions_author_country)

    author_datasets = [
        ("Dimensions", dimensions_author, "auth_countries"),
        ("Web of Science", wos_author, "auth_countries"),
        ("OpenAlex", openalex_author, "auth_countries"),
    ]
    for stem, df in [
        ("wos_author_country_country_only", wos_author),
        ("openalex_author_country_country_only", openalex_author),
        ("dimensions_author_country_country_only", dimensions_author),
    ]:
        df.to_csv(FIGURE_DATA_DIR / f"{stem}.csv", index=False)

    author_map_specs = [
        ("dimensions", dimensions_author, "auth_countries"),
        ("wos", wos_author, "auth_countries"),
        ("openalex", openalex_author, "auth_countries"),
    ]
    for stem, df, country_col in author_map_specs:
        plot_country_map(
            world,
            df,
            country_col,
            "share_has_grant",
            "share of publications with funding",
            f"{stem}_author_affiliation_coverage_share_country_only",
            formats,
            log_scale=False,
            vmin=0,
            vmax=0.9,
        )
        plot_country_map(
            world,
            df,
            country_col,
            "fractional_funded_paper_count",
            "Funded publications by author country",
            f"{stem}_author_affiliation_funded_count_log_scale_country_only",
            formats,
            log_scale=True,
            vmin=1,
            vmax=10**7.5,
        )

    plot_country_map_stack(
        world,
        author_datasets,
        value_col="share_has_grant",
        legend_label="Share of publications\nwith funding",
        out_stem="author_affiliation_coverage_share_three_databases",
        formats=formats,
        log_scale=False,
        vmin=0,
        vmax=0.9,
        aliases=["figure_3_country_level_coverage_by_author_affiliation"],
    )
    plot_country_map_stack(
        world,
        author_datasets,
        value_col="fractional_funded_paper_count",
        legend_label="Funded publications\nby author country",
        out_stem="author_affiliation_funded_count_log_scale_three_databases",
        formats=formats,
        log_scale=True,
        vmin=1,
        vmax=10**7.5,
    )

    dimensions_funder = dimensions_funder_country_summary(rebuild_dimensions_country)
    wos_funder = country_only(read_tsv("wos_funder_country.tsv"), "funder_countries")
    openalex_funder = country_only(read_tsv("openalex_funder_country.tsv"), "funder_countries")
    funder_datasets = [
        ("Dimensions", dimensions_funder, "funder_countries"),
        ("Web of Science", wos_funder, "funder_countries"),
        ("OpenAlex", openalex_funder, "funder_countries"),
    ]

    plot_country_map_stack(
        world,
        funder_datasets,
        value_col="fractional_funded_paper_count",
        legend_label="Publications\nby funder country",
        out_stem="funder_country_count_log_scale_three_databases",
        formats=formats,
        log_scale=True,
        vmin=1,
        vmax=10**7.5,
    )
    plot_country_map_stack(
        world,
        funder_datasets,
        value_col="global_share",
        legend_label="Share of publications\nby funder country",
        out_stem="funder_country_share_log_scale_three_databases",
        formats=formats,
        log_scale=True,
        vmin=1e-8,
        vmax=1,
    )

    map_specs = [
        (lambda: country_only(read_tsv("wos_funder_country.tsv"), "funder_countries"), "funder_countries", "wos"),
        (
            lambda: country_only(read_tsv("openalex_funder_country.tsv"), "funder_countries"),
            "funder_countries",
            "openalex",
        ),
        (
            lambda: dimensions_funder,
            "funder_countries",
            "dimensions",
        ),
    ]
    for loader, country_col, stem in map_specs:
        df = country_only(loader(), country_col)
        df.to_csv(FIGURE_DATA_DIR / f"{stem}_funder_country_country_only.csv", index=False)
        plot_country_map(
            world,
            df,
            country_col,
            "fractional_funded_paper_count",
            "Funded papers",
            f"{stem}_funder_country_count_country_only",
            formats,
            log_scale=True,
            vmin=1,
            vmax=10**7.5,
        )
        plot_country_map(
            world,
            df,
            country_col,
            "fractional_funded_paper_count",
            "Funded papers",
            f"{stem}_funder_country_count_log_scale_country_only",
            formats,
            log_scale=True,
            vmin=1,
            vmax=10**7.5,
        )
        plot_country_map(
            world,
            df,
            country_col,
            "global_share",
            "share of funded papers",
            f"{stem}_funder_country_share_country_only",
            formats,
            log_scale=False,
            vmin=0,
            vmax=0.35,
        )
        plot_country_map(
            world,
            df,
            country_col,
            "global_share",
            "Share of funded papers",
            f"{stem}_funder_country_share_log_scale_country_only",
            formats,
            log_scale=True,
            vmin=1e-8,
            vmax=1,
        )


def main() -> None:
    args = parse_args()
    configure_paths(**{key: getattr(args, key) for key in [
        "results_dir", "source_results_dir", "plotting_dir", "output_dir", "wos_year_counts",
        "scope_map", "dimensions_root", "country_cache_dir"]})
    set_pub_defaults()
    FIGURE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if "coverage" in args.groups:
        coverage = load_coverage_data(args.start_year, args.end_year)
        plot_coverage_over_time(coverage, args.start_year, args.end_year, args.formats)
    if "rank-frequency" in args.groups:
        plot_wos_funder_rank_frequency(args.formats)
    if "country-maps" in args.groups:
        plot_country_maps(args.formats, args.skip_download,
                          args.rebuild_dimensions_country, args.rebuild_dimensions_author_country)
    if "scope" in args.groups:
        plot_scope_group_summary(args.formats)

    print(f"Wrote figures to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
