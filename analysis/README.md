# Analysis and figures

This directory contains the maintained analysis and plotting scripts. Matching
still lives in `scripts/`; attachment and validation remain in
`scripts_wos_funding/`. Run commands from the repository root, or use the runner
by absolute path. No scheduler or machine-specific paths are required.

## WoS annual analysis

```bash
python run_pipeline.py --workflow analysis --set workers=4
```

This runs:

1. `summarize_matching_resolution.py`: resolution diagnostics from attached matches.
2. `build_wos_funding_year_counts.py`: counts all WoS source rows by year using
   nonempty FP, FU or FX as the funding-information criterion; saves CSV and SVG.
3. `plot_wos_funding_year_counts.py`: publication-quality PNG/PDF annual plots.

Counts in this all-edition helper count source rows; the SCIE/SSCI analysis below
explicitly deduplicates retained paper IDs. Do not interpret these two populations
as identical. A funding acknowledgement does not necessarily imply a resolved funder.

`plot_wos_funding_year_counts.py` also accepts count tables with disambiguated
entry/paper columns and preferred-country curves. `build_wos_funding_country_year_counts.py`
adds country curves when given an annual table with disambiguated-paper counts:

```bash
python analysis/build_wos_funding_country_year_counts.py --counts /path/to/annual_counts.csv --matches /path/to/matches.parquet --wos-main /path/to/main.parquet --output-dir /path/to/country_counts --workers 4
python analysis/plot_wos_funding_year_counts.py --input /path/to/country_counts/table.csv --output-dir /path/to/plots
```

Use the actual generated CSV filename printed by the first command. The
SCIE/SSCI `tables` phase generates the disambiguated entry/paper and country
count variants directly from retained matches.

## Publication figures

```bash
python run_pipeline.py --workflow figures
python analysis/generate_paper_figures.py --groups rank-frequency --formats pdf png
```

`--groups` can select one or several of `coverage`, `rank-frequency`,
`country-maps`, and `scope`. Defaults select all groups. Output defaults to
`paper_figures_output/`, with plotted summaries under its `data/` subdirectory.

| Figure group | Required inputs |
| --- | --- |
| `coverage` | WoS annual count CSV and OpenAlex/Dimensions `*_paper_counts_by_year.parquet` |
| `rank-frequency` | Versioned `plotting_data/wos_funder_types.tsv` |
| `country-maps` | Versioned WoS/OpenAlex author/funder country TSVs, two Dimensions country-summary CSVs, Natural Earth country geometry |
| `scope` | `Results/TopFunders/top5000_disambiguated_funders_scope_priority.csv` and the reviewed scope-map CSV |

**A fresh clone can render the rank-frequency figures from the versioned table.**
The complete current figure set also needs generated/local inputs. In particular,
the all-edition top-5000 summary is an existing analysis input; the matching
workflow does not generate it. The SCIE/SSCI table phase does generate the
corresponding subset ranking. Changing `--matches` for matching does not
regenerate the versioned historical WoS plotting summaries.

For comparison-database annual counts and detail extracts:

```bash
python run_pipeline.py --workflow sources --set openalex_snapshot=/path/to/openalex --set dimensions_root=/path/to/dimensions
```

The Dimensions country caches are named
`dimensions_funder_country_country_only.csv` and
`dimensions_author_country_country_only.csv`. If absent, country-map generation
builds them from `Results/SourcePaperFunders/dimensions_paper_funders.parquet`
and the Dimensions `organizations/` tables. Pass `--dimensions-root` explicitly;
legacy `PATHS.md` discovery is only a fallback for direct script use. Use
`--country-cache-dir` to reuse existing summaries with a different figure output
directory. Use `--rebuild-dimensions-country` or
`--rebuild-dimensions-author-country` when inputs have changed.

The other figure CLI path options are `--results-dir`, `--source-results-dir`,
`--plotting-dir`, `--output-dir`, `--wos-year-counts`, and `--scope-map`.
`--skip-download` skips maps if geometry is unavailable; otherwise Natural Earth
GeoJSON is downloaded on first use. `--start-year`/`--end-year` restrict the
comparative coverage curves, not all ranking/map populations.

The notebook in `notebooks/` calls this script and displays the results.

## SCIE/SSCI analysis from existing matches

```bash
python run_pipeline.py --workflow scie-ssci --dry-run
python run_pipeline.py --workflow scie-ssci --set matches=/path/to/final_matches.parquet --set subset_run=outputs/my_subset --set subset_bundle=bundles/my_subset
```

`recalculate_scie_ssci_results.py` exposes six phases, in this order:

1. `main`: retain records whose `WE` contains `WOS.SCI` or `WOS.SSCI`; verify and deduplicate repeated UTs.
2. `matches`: select existing record-level matches by retained UT.
3. `tables`: aggregate annual coverage, disambiguation counts, rankings, country curves and funder summaries.
4. `authors`: aggregate existing author-country metadata for retained papers.
5. `plots`: render the subset and comparative figures.
6. `bundle`: package readable tables, plots and provenance with checksums.

The runner passes paths to every phase. Direct invocation also accepts
`--run-dir`, `--bundle-dir`, `--wos-main`, `--wos-authors`, `--matches`,
`--plotting-dir`, `--source-results-dir`, `--figure-cache-dir`, and `--scope-map`.
Standalone defaults retain the historical release paths; runner defaults use
`outputs/wos_scie_ssci` and `bundles/wos_scie_ssci`.

The plots phase requires the existing Dimensions country caches and geometry;
it does not rebuild external databases. Prepare these with the paper-figure
script first. The reduced WoS tables are used for WoS panels, while the OpenAlex
and Dimensions comparison data remain their original populations. Sector labels
are inherited from `plotting_data/wos_funder_types.tsv`. Concentration curves
count paper–funder links, which can count a paper more than once across funders.

Restart with `--from subset-tables`, for example. Use a new `subset_run` directory
when changing input data: intermediate record projections are cached. Bundle
creation refuses to overwrite an existing directory or ZIP.

## Historical release rendering utilities

`restyle_scie_ssci_results.py` and `update_funder_distribution_labels.py` preserve
the code used for the October 2026 release corrections. They accept explicit
source/output paths (`--help`), compare rendered outputs and table values, and
require the corresponding prior bundles. They are optional release-maintenance
steps, not prerequisites for matching or producing new figures. PDF comparisons
require PyMuPDF; image checks require Pillow (both in the analysis dependencies).
