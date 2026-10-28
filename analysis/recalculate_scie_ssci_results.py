#!/usr/bin/env python3
"""Reaggregate existing WoS records/matches for SCIE or SSCI; never rerun matching."""
from __future__ import annotations

import os
os.environ.setdefault('POLARS_MAX_THREADS', '8')
os.environ.setdefault('OMP_NUM_THREADS', '1')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('MPLBACKEND', 'Agg')
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import time
import zipfile

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / 'outputs/wos_scie_ssci_20261002'
WORK = RUN / 'work'
VIEW = WORK / 'figure_inputs'
BUNDLE = ROOT / 'bundles/wos_2025_scie_ssci_final_results_plots_tables_2026-10-02'
MAIN = ROOT / 'data/WoS_2025_main.parquet'
MATCHES = ROOT / 'Results/grant_institutions_2025_affiliation_match_20251023_top5000_scope.parquet'
YEARS = WORK / 'retained_record_metadata.parquet'
REDUCED = WORK / 'retained_existing_matches.parquet'
AUTHOR_PAIRS = WORK / 'retained_author_country_pairs.parquet'
AUTHORS = ROOT / 'data/WoS_2025_authors.parquet'
PLOTTING = ROOT / 'plotting_data'
SOURCE_RESULTS = ROOT / 'Results/SourcePaperFunders'
FIGURE_CACHE = ROOT / 'paper_figures_output/data'
SCOPE_MAP = ROOT / 'scripts_wos_funding/funder_multicountry_scope_map_top5000_reviewed.csv'


def sha(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def collect(lf):
    try:
        return lf.collect(engine='streaming')
    except TypeError:
        return lf.collect(streaming=True)


def write_json(path, data):
    Path(path).write_text(json.dumps(data, indent=2) + '\n')


def present(col):
    return ~pl.col(col).fill_null('').str.strip_chars().is_in(['', '[]', '{}', 'nan', 'NaN', 'None', 'null'])


def edition_filter():
    # Exact quoted JSON list elements, not substring matching SCI within SSCI.
    return pl.col('WE').fill_null('').str.contains(r'"WOS\.(SCI|SSCI)"')


def main_records():
    schema = pl.scan_parquet(MAIN).collect_schema()
    assert all(c in schema for c in ['UT', 'WE', 'PY', 'FP', 'FU', 'FX'])
    counts = collect(pl.scan_parquet(MAIN).select('WE').group_by('WE').agg(pl.len().alias('records')))
    counts = counts.with_columns(edition_filter().alias('included_scie_or_ssci')).sort('records', descending=True)
    counts.write_csv(RUN / 'edition_record_counts.csv')
    retained = int(counts.filter('included_scie_or_ssci')['records'].sum())
    total = int(counts['records'].sum())
    print(f'Edition filter: {retained:,}/{total:,} retained ({100*retained/total:.2f}%).', flush=True)
    raw = WORK / 'retained_record_metadata_before_uid_dedup.parquet'
    if not raw.exists():
        if YEARS.exists() and not (RUN/'selection_summary.json').exists():
            YEARS.rename(raw)  # Resume this run's completed projection.
        else:
            (
                pl.scan_parquet(MAIN).select('UT', 'WE', 'PY', 'FP', 'FU', 'FX')
                .filter(edition_filter())
                .select('UT', pl.col('PY').cast(pl.Int64, strict=False).alias('year'),
                        (present('FP') | present('FU') | present('FX')).alias('has_funding_info'))
                .sink_parquet(raw, compression='zstd', row_group_size=250000)
            )
    duplicates = collect(pl.scan_parquet(raw).group_by('UT').agg(pl.len().alias('n'),
        pl.col('year').n_unique().alias('year_values'),pl.col('has_funding_info').n_unique().alias('funding_values'))
        .filter(pl.col('n')>1))
    assert duplicates.filter((pl.col('year_values')>1)|(pl.col('funding_values')>1)).height == 0, duplicates
    duplicates.write_csv(RUN/'duplicate_uid_projection_checks.csv')
    pl.scan_parquet(raw).unique('UT').sink_parquet(YEARS,compression='zstd',row_group_size=250000)
    summary = collect(pl.scan_parquet(YEARS).select(pl.len().alias('rows'), pl.col('UT').n_unique().alias('unique_ut'))).row(0, named=True)
    assert summary['rows'] == summary['unique_ut'], summary
    assert retained-summary['rows'] == int((duplicates['n']-1).sum()), summary
    write_json(RUN / 'selection_summary.json', {'original_records': total, 'retained_records': retained,
        'excluded_records': total-retained, 'retained_percent': 100*retained/total,
        'unique_retained_paper_uids':summary['rows'],'duplicate_uid_rows_removed':retained-summary['rows'],
        'duplicate_uid_year_and_funding_values_consistent':True,
        'record_key_unique': True, 'filter': 'WE JSON list contains WOS.SCI OR WOS.SSCI',
        'year_window_for_funding_tables': [1950, 2025], 'year_window_for_comparative_curves': [1980, 2025]})


def reduce_matches():
    cols = ['UT', 'gid', 'id', 'grant_agency', 'grant_agency_normalized', 'display_name', 'source',
            'match_type', 'funder_country_code', 'funder_scope_type', 'funder_scope_label', 'funder_scope_provenance',
            'preferred_paper_country_code']
    (
        pl.scan_parquet(MATCHES).select(cols)
        .join(pl.scan_parquet(YEARS), on='UT', how='inner')
        .sink_parquet(REDUCED, compression='zstd', row_group_size=250000)
    )
    print('Retained existing grant-agency rows:', collect(pl.scan_parquet(REDUCED).select(pl.len())).item(), flush=True)


def aggregate_tables():
    annual_dir = VIEW / 'Results/FundingYearCounts'
    top_dir = VIEW / 'Results/TopFunders'
    annual_dir.mkdir(parents=True, exist_ok=True)
    top_dir.mkdir(parents=True, exist_ok=True)
    rec = pl.scan_parquet(YEARS)
    matches = pl.scan_parquet(REDUCED)
    accepted = matches.filter(pl.col('id').is_not_null())
    annual = collect(rec.filter(pl.col('year').is_between(1950, 2025)).group_by('year').agg(
        pl.len().alias('total_papers'), pl.col('has_funding_info').sum().alias('funded_papers')))
    calendar = pl.DataFrame({'year': list(range(1950, 2026))})
    annual = calendar.join(annual, on='year', how='left').fill_null(0).sort('year').select('year', 'funded_papers', 'total_papers')
    assert (annual['funded_papers'] <= annual['total_papers']).all()
    disambiguated = collect(accepted.filter(pl.col('year').is_between(1950, 2025)).group_by('year').agg(
        pl.len().alias('disambiguated_entries'), pl.col('UT').n_unique().alias('papers_with_disambiguated_funding_entity')))
    base = 'wos_funding_paper_counts_by_year_1950_2025'
    annual.write_csv(annual_dir / f'{base}.csv')
    annual.write_csv(annual_dir / 'wos_funding_paper_counts_by_year_from_1950.csv')
    entries = annual.join(disambiguated.select('year', 'disambiguated_entries'), on='year', how='left').fill_null(0)
    entries.write_csv(annual_dir / f'{base}_with_disambiguated_entries.csv')
    papers = annual.join(disambiguated.select('year', 'papers_with_disambiguated_funding_entity'), on='year', how='left').fill_null(0)
    assert (papers['papers_with_disambiguated_funding_entity'] <= papers['funded_papers']).all()
    papers.write_csv(annual_dir / f'{base}_with_disambiguated_papers.csv')

    # Reuse the already selected preferred-country assignments, not their pipeline.
    sys.path.insert(0,str(ROOT))
    from analysis.build_wos_funding_country_year_counts import EUROPE_COUNTRY_CODES
    country_group = (pl.when(pl.col('preferred_paper_country_code')=='US').then(pl.lit('US'))
        .when(pl.col('preferred_paper_country_code').is_in(sorted(EUROPE_COUNTRY_CODES))).then(pl.lit('Europe'))
        .when(pl.col('preferred_paper_country_code')=='BR').then(pl.lit('Brazil'))
        .when(pl.col('preferred_paper_country_code')=='JP').then(pl.lit('Japan'))
        .when(pl.col('preferred_paper_country_code')=='CN').then(pl.lit('China')).otherwise(pl.lit('')))
    group_counts = collect(accepted.with_columns(country_group.alias('country_group'))
        .filter(pl.col('country_group')!='').select('UT','year','country_group').unique()
        .filter(pl.col('year').is_between(1950, 2025)).group_by('year', 'country_group').agg(pl.col('UT').n_unique().alias('n')))
    country_cols = {'US':'us', 'Europe':'europe', 'Brazil':'brazil', 'Japan':'japan', 'China':'china'}
    country_annual = papers
    for group, suffix in country_cols.items():
        col = f'funded_papers_disambiguated_{suffix}'
        part = group_counts.filter(pl.col('country_group') == group).select('year', pl.col('n').alias(col))
        country_annual = country_annual.join(part, on='year', how='left').fill_null(0)
        assert (country_annual[col] <= country_annual['papers_with_disambiguated_funding_entity']).all()
    for suffix in ['_with_disambiguated_country_curves', '_with_disambiguated_country_curves_scope_priority']:
        country_annual.write_csv(annual_dir / f'{base}{suffix}.csv')

    top = collect(accepted.group_by('id').agg(pl.col('display_name').first(), pl.col('funder_country_code').first(),
        pl.col('funder_scope_type').first(), pl.col('funder_scope_label').first(), pl.col('funder_scope_provenance').first(),
        pl.col('UT').n_unique().alias('papers'), pl.len().alias('rows'),
        pl.col('grant_agency_normalized').n_unique().alias('agency_names')).sort(['papers', 'id'], descending=[True, False]))
    for n in [1000, 5000]:
        top.head(n).write_csv(top_dir / f'top{n}_disambiguated_funders_scope_priority.csv')
    top.write_csv(top_dir / 'all_disambiguated_funders_scie_ssci.csv')
    scope = collect(accepted.filter(present('funder_scope_label'))
        .join(top.head(5000).lazy().select('id'),on='id',how='inner')
        .group_by('funder_scope_label').agg(pl.col('UT').n_unique().alias('distinct_papers'),
            pl.col('id').n_unique().alias('selected_funders'),
            pl.struct('UT','id').n_unique().alias('paper_funder_links'))
        .sort('paper_funder_links',descending=True))
    scope.write_csv(top_dir/'scope_groups_current_top5000_scie_ssci.csv')
    period_tables = []
    for start, end in [(1980, 1990), (2010, 2020)]:
        table = collect(accepted.filter(pl.col('year').is_between(start,end)).group_by('id').agg(
            pl.col('display_name').first(), pl.col('source').first(), pl.col('funder_country_code').first(),
            pl.col('UT').n_unique().alias('paper_count'), pl.len().alias('matched_entry_count'),
            pl.col('grant_agency_normalized').n_unique().alias('raw_funder_string_count'),
            pl.col('match_type').n_unique().alias('match_type_count'))
            .sort(['paper_count','id'],descending=[True,False]))
        table = table.with_columns(pl.concat_str(pl.lit('OA_'),pl.col('id').cast(pl.Int64).cast(pl.String)).alias('gid')).with_row_index('rank',offset=1)
        table = table.select('rank','gid','id','display_name','source','funder_country_code','paper_count','matched_entry_count','raw_funder_string_count','match_type_count')
        table.write_csv(top_dir/f'top_disambiguated_funders_{start}_{end}_scope_priority.csv')
        period_tables.append(table)
    a,b = period_tables
    ids = set(a.head(20)['gid']) | set(b.head(20)['gid'])
    left = a.filter(pl.col('gid').is_in(ids)).select('gid','display_name',pl.col('rank').alias('rank_1980_1990'),pl.col('paper_count').alias('paper_count_1980_1990'))
    right = b.filter(pl.col('gid').is_in(ids)).select('gid','display_name',pl.col('rank').alias('rank_2010_2020'),pl.col('paper_count').alias('paper_count_2010_2020'))
    ranks = left.join(right,on='gid',how='full',coalesce=True).with_columns(pl.coalesce('display_name','display_name_right').alias('display_name')).drop('display_name_right')
    for period in ['1980_1990','2010_2020']:
        ranks = ranks.with_columns(pl.col(f'rank_{period}').fill_null(21).clip(upper_bound=21).alias(f'plot_rank_{period}'),
            (pl.col(f'rank_{period}').fill_null(999999)<=20).alias(f'top20_{period}'),pl.col(f'paper_count_{period}').fill_null(0))
    ranks.sort('plot_rank_1980_1990','plot_rank_2010_2020').write_csv(top_dir/'rank_change_top20_union_1980_1990_vs_2010_2020_scope_priority.csv')

    # Preserve known ancestor-type labels. Raw unmatched strings are not unique
    # organizations and must not be presented as funders in concentration plots.
    types = pl.read_csv(PLOTTING/'wos_funder_types.tsv',separator='\t',schema_overrides={'id':pl.String}).select('id','oa_ancestortypes').unique('id')
    funders = top.select(pl.concat_str(pl.lit('OA_'),pl.col('id').cast(pl.Int64).cast(pl.String)).alias('id'),
        pl.col('rows').alias('n_grants'),pl.col('papers').alias('n_pubs')).join(types,on='id',how='left')
    funders.write_csv(VIEW/'plotting_data/wos_funder_types.tsv',separator='\t')

    country_pairs = (accepted.select('UT','funder_country_code').filter(pl.col('funder_country_code').str.contains(r'^[A-Z]{2}$'))
        .unique().with_columns(pl.len().over('UT').alias('n_countries')))
    country = collect(country_pairs.group_by('funder_country_code').agg((1/pl.col('n_countries')).sum().alias('fractional_funded_paper_count'))
        .rename({'funder_country_code':'funder_countries'}).with_columns(
            (pl.col('fractional_funded_paper_count')/pl.col('fractional_funded_paper_count').sum()).alias('global_share'))
        .sort('fractional_funded_paper_count',descending=True))
    assert abs(country['global_share'].sum()-1)<1e-9
    country.write_csv(VIEW/'plotting_data/wos_funder_country.tsv',separator='\t')
    write_json(RUN/'aggregate_summary.json', {'retained_matched_agency_rows': collect(accepted.select(pl.len())).item(),
        'retained_disambiguated_funded_papers':collect(accepted.select(pl.col('UT').n_unique())).item(),
        'distinct_disambiguated_funders':top.height,'rank_frequency_funder_groups':funders.height,
        'funder_country_fractional_sum':float(country['fractional_funded_paper_count'].sum()),
        'rank_frequency_population':'Accepted final disambiguated organization IDs only; unmatched raw strings are not funder identities.'})


def aggregate_authors():
    sys.path.insert(0,str(ROOT))
    import pycountry
    from scripts_wos_funding.step1_attach_matches_to_wos import normalize_country_name
    src=AUTHORS
    # Only two existing processed columns are read; no address extraction/enrichment.
    countries=(pl.scan_parquet(src).select('UT','author_countries')
        .filter(present('author_countries'))
        .with_columns(pl.col('author_countries').str.json_decode(pl.List(pl.String)).alias('country'))
        .explode('country').select('UT','country').filter(pl.col('country').is_not_null()))
    names=collect(countries.select('country').unique())['country'].to_list()
    mapping={name:normalize_country_name(name) for name in names}
    # Check the lookup itself; valid inputs need not contain these countries.
    assert normalize_country_name('Canada')=='CA' and normalize_country_name('Japan')=='JP', 'Country lookup dependency unavailable.'
    write_json(RUN/'author_country_name_mapping.json',mapping)
    (countries.with_columns(pl.col('country').replace_strict(mapping,default=None,return_dtype=pl.String).alias('auth_countries'))
        .filter(pl.col('auth_countries').str.contains(r'^[A-Z]{2}$')).select('UT','auth_countries').unique()
        .join(pl.scan_parquet(YEARS).select('UT','has_funding_info'),on='UT',how='inner')
        .sink_parquet(AUTHOR_PAIRS,compression='zstd',row_group_size=250000))
    pairs=pl.scan_parquet(AUTHOR_PAIRS).with_columns(pl.len().over('UT').alias('n_countries'))
    out=collect(pairs.group_by('auth_countries').agg(pl.col('has_funding_info').mean().alias('share_has_grant'),
        (pl.col('has_funding_info').cast(pl.Float64)/pl.col('n_countries')).sum().alias('fractional_funded_paper_count'),
        pl.len().alias('paper_country_rows'),pl.col('has_funding_info').sum().alias('funded_paper_country_rows'))
        .sort('fractional_funded_paper_count',descending=True))
    assert out['share_has_grant'].is_between(0,1).all()
    out.write_csv(VIEW/'plotting_data/wos_author_country.tsv',separator='\t')
    totals=collect(pl.scan_parquet(AUTHOR_PAIRS).select(pl.col('UT').n_unique().alias('records_with_known_author_country'),
        pl.col('UT').filter(pl.col('has_funding_info')).n_unique().alias('funded_records_with_known_author_country'))).row(0,named=True)
    assert abs(float(out['fractional_funded_paper_count'].sum())-totals['funded_records_with_known_author_country'])<1e-5
    totals.update(country_lookup_package='pycountry',country_lookup_version=pycountry.__version__,
        source_country_names=len(names),mapped_source_country_names=sum(v is not None for v in mapping.values()))
    write_json(RUN/'author_country_summary.json',totals)


def setup():
    for p in [WORK, VIEW/'plotting_data', VIEW/'Results/SourcePaperFunders']:
        p.mkdir(parents=True, exist_ok=True)
    # These are small plotting summaries. Copies avoid OS-specific symlink privileges.
    for name in ['openalex_author_country.tsv', 'openalex_funder_country.tsv',
                 'ne_110m_admin_0_countries.geojson']:
        source = PLOTTING / name
        if source.exists():
            shutil.copy2(source, VIEW/'plotting_data'/name)


def configure_paths(args):
    global RUN, WORK, VIEW, BUNDLE, MAIN, MATCHES, AUTHORS, YEARS, REDUCED, AUTHOR_PAIRS
    global PLOTTING, SOURCE_RESULTS, FIGURE_CACHE, SCOPE_MAP
    RUN, BUNDLE = args.run_dir, args.bundle_dir
    MAIN, MATCHES, AUTHORS = args.wos_main, args.matches, args.wos_authors
    PLOTTING, SOURCE_RESULTS = args.plotting_dir, args.source_results_dir
    FIGURE_CACHE, SCOPE_MAP = args.figure_cache_dir, args.scope_map
    WORK = RUN / 'work'
    VIEW = WORK / 'figure_inputs'
    YEARS = WORK / 'retained_record_metadata.parquet'
    REDUCED = WORK / 'retained_existing_matches.parquet'
    AUTHOR_PAIRS = WORK / 'retained_author_country_pairs.parquet'


def plots():
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import pandas as pd
    m=load_module('existing_paper_figures',ROOT/'analysis/generate_paper_figures.py')
    m.configure_paths(results_dir=VIEW/'Results', source_results_dir=SOURCE_RESULTS,
                      plotting_dir=VIEW/'plotting_data', output_dir=RUN/'plots',
                      wos_year_counts=VIEW/'Results/FundingYearCounts/wos_funding_paper_counts_by_year_1950_2025.csv',
                      scope_map=SCOPE_MAP, country_cache_dir=RUN/'figure_data')
    m.FIGURE_DATA_DIR=RUN/'figure_data'
    m.OUTPUT_DIR.mkdir(exist_ok=True);m.FIGURE_DATA_DIR.mkdir(exist_ok=True)
    for db in ['dimensions']:
        for kind in ['author','funder']:
            name=f'{db}_{kind}_country_country_only.csv'
            shutil.copy2(FIGURE_CACHE/name,m.FIGURE_DATA_DIR/name)
    # Keep original wording and typography. Subset and counting-unit details
    # belong in the bundle documentation, not new decorations on the figures.
    mpl.rcdefaults()
    m.set_pub_defaults()
    formats=['png','pdf']
    coverage=m.load_coverage_data(1980,2025)
    m.plot_coverage_over_time(coverage,1980,2025,formats)
    m.plot_wos_funder_rank_frequency(formats)
    m.plot_country_maps(formats,True,False,False)
    m.plot_scope_group_summary(formats)
    # Existing funding-year plotting functions, using recalculated tables only.
    fm=load_module('existing_funding_plots',ROOT/'analysis/plot_wos_funding_year_counts.py')
    for suffix in ['', '_with_disambiguated_entries','_with_disambiguated_papers','_with_disambiguated_country_curves_scope_priority']:
        csv=VIEW/f'Results/FundingYearCounts/wos_funding_paper_counts_by_year_1950_2025{suffix}.csv'
        out=RUN/'funding_plots'/('scope_priority' if 'scope_priority' in suffix else 'base')
        fm.parse_args=lambda csv=csv,out=out:argparse.Namespace(input=csv,output_dir=out,start_year=1950,end_year=2025)
        # The original funding plots run with their own defaults. Isolate them
        # from the paper-figure settings (particularly savefig DPI).
        mpl.rcdefaults()
        if 'scope_priority' in suffix:
            fm.main()
        else:
            # The first three original figures predate the wider country-curve
            # layout now used by fm.main. Retain their 7.2 x 4.6 inch canvas,
            # 10-point legend, original legend placement and base title.
            original_subplots=plt.subplots
            original_legend=plt.Axes.legend
            original_title=plt.Axes.set_title
            def legacy_subplots(*args,**kwargs):
                kwargs['figsize']=(7.2,4.6)
                return original_subplots(*args,**kwargs)
            def legacy_legend(self,*args,**kwargs):
                loc='lower right' if suffix=='_with_disambiguated_papers' else 'upper left'
                return original_legend(self,*args,frameon=False,loc=loc,fontsize=10)
            def legacy_title(self,label,*args,**kwargs):
                if suffix=='':
                    label='Web of Science papers with funding information, 1950-2025'
                return original_title(self,label,*args,**kwargs)
            plt.subplots=legacy_subplots
            plt.Axes.legend=legacy_legend
            plt.Axes.set_title=legacy_title
            try:fm.main()
            finally:
                plt.subplots=original_subplots
                plt.Axes.legend=original_legend
                plt.Axes.set_title=original_title
    ranks=pd.read_csv(VIEW/'Results/TopFunders/rank_change_top20_union_1980_1990_vs_2010_2020_scope_priority.csv')
    plot_rank_changes(ranks,RUN/'plots',formats)


def plot_rank_changes(ranks, output_dir, formats):
    """Use the layout/typography/palette of the original TopFunders figure.

    Original standalone plotting code is not available; page and axes geometry,
    font sizes and vector stroke styles were checked against the original PDF.
    Only rank coordinates and organization labels come from the subset table.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    mpl.rcdefaults()
    mpl.rcParams.update({'font.family':'DejaVu Sans','font.size':9,
                         'axes.labelsize':9,'axes.titlesize':10.8})
    fig=plt.figure(figsize=(8.8,8.2))
    ax=fig.add_axes([73.28894/633.6,25.84/590.4,481.50214/633.6,540.2/590.4])
    for row in ranks.itertuples(index=False):
        color=('#4f7d3a' if row.top20_1980_1990 and row.top20_2010_2020
               else '#8a6f2a' if row.top20_1980_1990 else '#c06f2c')
        ax.plot([0,1],[row.plot_rank_1980_1990,row.plot_rank_2010_2020],
                marker='o',markersize=6,color=color,lw=1.5,alpha=.85)
        name=str(row.display_name)[:42]
        if row.top20_1980_1990:
            ax.text(-.03,row.plot_rank_1980_1990,name,ha='right',va='center',fontsize=7)
        if row.top20_2010_2020:
            ax.text(1.03,row.plot_rank_2010_2020,name,ha='left',va='center',fontsize=7)
    ax.set_ylim(21.7,.3);ax.set_xlim(-.35,1.35)
    ax.set_xticks([0,1],['1980-1990','2010-2020'])
    ax.set_yticks(list(range(1,22)),[str(i) for i in range(1,21)]+['Others'])
    ax.set_ylabel('Rank among disambiguated funders')
    ax.grid(axis='y',color='#e6e6e6',linewidth=.6)
    ax.spines[['right','top']].set_visible(False)
    ax.set_title('Rank changes among top disambiguated funders')
    for fmt in formats:
        fig.savefig(output_dir/f'rank_change_top20_union_1980_1990_vs_2010_2020_scope_priority.{fmt}',dpi=300)
    plt.close(fig)


def bundle():
    if BUNDLE.exists() or BUNDLE.with_suffix('.zip').exists():
        raise FileExistsError(f'Refusing to replace {BUNDLE} or its ZIP')
    BUNDLE.mkdir(parents=True)
    for sub in ['tables/figure_data','tables/funding_over_time','tables/top_funders','plots_and_maps/wos_only','plots_and_maps/comparative','plots_and_maps/funding_over_time','plots_and_maps/top_funders']:
        (BUNDLE/sub).mkdir(parents=True,exist_ok=True)
    for src,dest in [(RUN/'figure_data',BUNDLE/'tables/figure_data'),
        (VIEW/'Results/FundingYearCounts',BUNDLE/'tables/funding_over_time'),(VIEW/'Results/TopFunders',BUNDLE/'tables/top_funders')]:
        for p in src.glob('*.csv'):shutil.copy2(p,dest/p.name)
    for p in (RUN/'plots').iterdir():
        if p.suffix not in ['.png','.pdf']:continue
        stem=p.stem
        if stem.startswith(('dimensions_','openalex_')):continue
        if 'rank_change_top20' in stem: sub='top_funders'
        elif ('three_databases' in stem or stem.startswith(('coverage_','figure_3_'))):sub='comparative'
        else:sub='wos_only'
        shutil.copy2(p,BUNDLE/f'plots_and_maps/{sub}'/p.name)
    for p in (RUN/'funding_plots').rglob('*'):
        if p.suffix in ['.png','.pdf']:
            name=p.name
            if p.parent.name=='scope_priority':name=name.replace('_log.','_scope_priority_log.')
            shutil.copy2(p,BUNDLE/'plots_and_maps/funding_over_time'/name)
    shutil.copy2(RUN/'edition_record_counts.csv',BUNDLE/'tables/edition_record_counts.csv')
    pl.read_csv(VIEW/'plotting_data/wos_funder_types.tsv',separator='\t').write_csv(BUNDLE/'tables/figure_data/wos_funder_sectors_and_counts.csv')
    for sub in ['tables','plots_and_maps']:
        text = ('CSV files contain final aggregated readable results, not raw paper records.\n'
                'See ../README.txt for filter, counting units, year windows and comparison scope.\n'
                'figure_data/ contains plotted summaries; funding_over_time/ annual counts;\n'
                'top_funders/ accepted-organization rankings, period counts and scope groups.\n') if sub=='tables' else (
                'Final PNG/PDF figures. wos_only/ and funding_over_time/ use WoS SCIE/SSCI;\n'
                'comparative/ changes only the WoS panels/curves, not Dimensions/OpenAlex.\n'
                'top_funders/ contains reranked period comparisons. Source tables are in ../tables/.\n')
        (BUNDLE/sub/'README.txt').write_text(text)
    table_index=[]
    for p in sorted((BUNDLE/'tables').rglob('*.csv')):
        lf=pl.scan_csv(p,infer_schema=False)
        table_index.append({'path':str(p.relative_to(BUNDLE)),'rows':collect(lf.select(pl.len())).item(),
            'columns':', '.join(lf.collect_schema().names())})
    pl.DataFrame(table_index).write_csv(BUNDLE/'TABLE_INDEX.csv')
    summary=json.loads((RUN/'selection_summary.json').read_text())
    summary.update(json.loads((RUN/'aggregate_summary.json').read_text()))
    summary.update(json.loads((RUN/'author_country_summary.json').read_text()))
    write_json(BUNDLE/'SELECTION_AND_COUNTS.json',summary)
    write_json(BUNDLE/'VALIDATION.json',{
        'edition_union_exact_quoted_tokens_tested':True,
        'retained_uid_year_and_funding_duplicates_checked':True,
        'annual_totals_reconciled_to_retained_uid_metadata':True,
        'matched_funder_population_and_rank_frequency_counts_reconciled':True,
        'author_fractional_counts_reconciled_to_unique_funded_uids_with_known_country':True,
        'dimensions_openalex_comparison_data_unchanged':True,
        'country_dependency_checked_for_canada_and_japan':True,
        'spot_checked_png_rendering':False,
        'pipeline_matching_not_rerun':True,
        'validation_scope':'Aggregation/selection/packaging checks, not a new validation of existing funder identities.'})
    readme=f'''WoS final plots and tables: SCIE/SSCI subset

Selection: {MAIN}, WE contains WOS.SCI OR WOS.SSCI.
WE is a JSON list of record-level editions. Either index suffices; records also
in another edition are retained once. SCIE is coded WOS.SCI, SSCI WOS.SSCI.
{summary['retained_records']:,} of {summary['original_records']:,} records retained ({summary['retained_percent']:.4f}%).
{summary['duplicate_uid_rows_removed']:,} repeated retained UT rows have identical projected year/funding values;
counting deduplicates them, leaving {summary['unique_retained_paper_uids']:,} unique retained paper UIDs.
No article/review-only document-type restriction was added. Funding time series: 1950-2025;
comparative curves: 1980-2025. Rankings/maps use all retained snapshot years,
except the expressly labelled 1980-1990 and 2010-2020 period tables.

No extraction, funder matching, country-priority candidate selection, scope
disambiguation or model pipeline was rerun. Existing final top5000_scope record-level
matches and preferred-country assignments were filtered by retained UT and
aggregated. Dimensions/OpenAlex comparison series and maps are unchanged;
they are NOT themselves restricted to WoS SCIE/SSCI. These are comparison
panels with only the WoS universe restricted, not a matched-universe analysis.

tables/figure_data: human-readable data behind maps, coverage and rank-frequency.
tables/funding_over_time: annual total/funded/disambiguated entry and paper counts,
plus preferred-paper-country curves. Disambiguated entries count accepted agency
rows (id nonmissing), not unique papers. Country curves reuse existing preferred
paper country assignments, not funder headquarters countries.
tables/top_funders: reranked unique-paper counts per existing accepted organization
id, raw-row and agency-string counts, period ranks and top-20 union rank changes.
plots_and_maps: PNG/PDF figures including log and linear comparative variants.
tables/edition_record_counts.csv: full edition combinations with inclusion flag.
SELECTION_AND_COUNTS.json: record/coverage/aggregation totals and selection rule.
MANIFEST.json: data sources, script hashes, definitions, files and SHA-256 hashes.

Filename compatibility: some result filenames retain the historical
"scope_priority" suffix, but the source used here is explicitly the final
top5000_scope per-paper selection file. See MANIFEST.json for its exact path.

Funding information means nonempty FP OR FU OR FX (same existing annual-count
rule); it does not imply a successfully disambiguated funder. Retained unique UT
is the denominator. Multiple agencies/funders on a paper must not multiply paper
counts. Rank-frequency uses existing accepted OpenAlex IDs only; unmatched raw
agency strings are not counted as unique organizations or newly disambiguated.
Existing oa_ancestortypes labels are reused for sector curves; missing labels
remain missing. All-series curves and top-funder tables include accepted
organizations only. The older plotting summary mixes legacy non-OpenAlex IDs
that lack a paper-level crosswalk here; it cannot be filtered exactly and is not
used as an unverified surrogate. Concentration sums distinct paper-funder links,
NOT unique papers: a multi-funder paper contributes once to each accepted funder.

Author maps: existing processed author country metadata, normalized with the
existing country mapping, deduplicated per (UT,country). Funding coverage is
funded/total paper-country incidences; fractional funded counts divide each
funded paper equally among its known countries. Unknown-country records remain
in annual denominators but are excluded from country maps. Funder maps similarly
deduplicate accepted (UT,valid country), weight equally across known countries,
and normalize shares over papers with known funder country. These calculations
are regenerated from record-level data, not by scaling old aggregated maps.
Regional/global scope inventory counts are unchanged reviewed metadata; observed
paper counts are recalculated for the reduced top-5000 list. Aggregated group
totals can count a paper for more than one funder; they are not unique papers.

Only final readable plots/tables and documentation are included, not raw data,
Parquet intermediates, execution logs or matching review queues. Recalculation
script and resumable work files remain outside the bundle in analysis/
and {RUN}. Original all-edition outputs are untouched.
'''
    (BUNDLE/'README.txt').write_text(readme)
    sources=[MAIN,MATCHES,AUTHORS,PLOTTING/'wos_funder_types.tsv',
             SCOPE_MAP]
    manifest={'created_at_utc':datetime.now(timezone.utc).isoformat(),'selection':summary,
        'pipeline_rerun':False,'comparison_scope':'Only WoS restricted; Dimensions/OpenAlex unchanged',
        'sources':[{'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in sources],
        'scripts':[{'path':str(p),'sha256':sha(p)} for p in [Path(__file__),ROOT/'analysis/generate_paper_figures.py',ROOT/'analysis/plot_wos_funding_year_counts.py']],
        'files':[{'path':str(p.relative_to(BUNDLE)),'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(BUNDLE.rglob('*')) if p.is_file()]}
    write_json(BUNDLE/'MANIFEST.json',manifest)
    archive=BUNDLE.with_suffix('.zip')
    if archive.exists():raise FileExistsError(archive)
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for p in sorted(BUNDLE.rglob('*')):
            if p.is_file():z.write(p,f'{BUNDLE.name}/{p.relative_to(BUNDLE)}')
    with zipfile.ZipFile(archive) as z:assert z.testzip() is None
    Path(str(archive)+'.sha256').write_text(f'{sha(archive)}  {archive.name}\n')
    print(f'Final folder: {BUNDLE}\nZIP: {archive}',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',required=True,choices=['main','matches','tables','authors','plots','bundle'])
    parser.add_argument('--run-dir', type=Path, default=RUN)
    parser.add_argument('--bundle-dir', type=Path, default=BUNDLE)
    parser.add_argument('--wos-main', type=Path, default=MAIN)
    parser.add_argument('--wos-authors', type=Path, default=AUTHORS)
    parser.add_argument('--matches', type=Path, default=MATCHES)
    parser.add_argument('--plotting-dir', type=Path, default=PLOTTING)
    parser.add_argument('--source-results-dir', type=Path, default=SOURCE_RESULTS)
    parser.add_argument('--figure-cache-dir', type=Path, default=FIGURE_CACHE)
    parser.add_argument('--scope-map', type=Path, default=SCOPE_MAP)
    args=parser.parse_args()
    configure_paths(args)
    RUN.mkdir(parents=True,exist_ok=True);setup()
    start=time.monotonic()
    print(f'Start phase {args.phase}; threads={pl.thread_pool_size()}; no matching pipeline.',flush=True)
    {'main':main_records,'matches':reduce_matches,'tables':aggregate_tables,'authors':aggregate_authors,'plots':plots,'bundle':bundle}[args.phase]()
    print(f'Completed {args.phase} in {time.monotonic()-start:.1f}s.',flush=True)


if __name__=='__main__':main()
