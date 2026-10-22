#!/usr/bin/env python3
"""Rename Corporate to Business and raise distribution legends, data unchanged."""
from __future__ import annotations

import os
os.environ.setdefault('POLARS_MAX_THREADS', '8')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('MPLBACKEND', 'Agg')
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import zipfile

import fitz
import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / 'outputs/funder_distribution_business_20261007'
FULL_SOURCE = ROOT / 'bundles/wos_2025_final_results_plots_tables_2026-09-18.zip'
SUBSET_SOURCE = ROOT / 'bundles/wos_2025_scie_ssci_final_results_plots_tables_2026-10-07'
TABLES = ['wos_funder_rank_frequency.csv', 'wos_funder_publication_ccdf.csv']
STEMS = ['wos_funder_rank_frequency', 'figure_5_wos_funder_rank_frequency']


def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def generate_bundle(kind, source, target):
    if target.exists() or target.with_suffix('.zip').exists():
        raise FileExistsError(target)
    if kind == 'full':
        target.mkdir()
        with zipfile.ZipFile(source) as z:
            for member in z.infolist():
                parts = Path(member.filename).parts
                assert parts[0] == 'wos_2025_final_results_plots_tables' and '..' not in parts
                dest = target.joinpath(*parts[1:])
                if member.is_dir():
                    dest.mkdir(parents=True, exist_ok=True)
                else:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with z.open(member) as f, dest.open('wb') as out:
                        shutil.copyfileobj(f, out)
    else:
        shutil.copytree(source, target)

    before = {str(p.relative_to(target)): sha(p) for p in target.rglob('*') if p.is_file()}
    spec = importlib.util.spec_from_file_location(f'distribution_{kind}', ROOT / 'analysis/generate_paper_figures.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.OUTPUT_DIR = RUN / kind
    m.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cached = []
    for name in TABLES:
        p = target / 'tables/figure_data' / name
        old = pl.read_csv(p)
        updated = old.with_columns(pl.col('series').replace({'Corporate': 'Business'}))
        assert updated.drop('series').equals(old.drop('series'))
        updated.write_csv(p)
        cached.append(updated.with_columns(pl.col('series').replace({'All matched funders': 'All publications'})).to_pandas())
    m.build_rank_frequency_data = lambda: tuple(cached)
    original_save = m.save_figure
    checks = []

    def validated_save(fig, stem, formats):
        ax = fig.axes[0]
        fig.canvas.draw()
        legend = ax.get_legend()
        box = legend.get_window_extent(fig.canvas.get_renderer()).transformed(ax.transData.inverted())
        curve_max = max(float(np.max(line.get_ydata()[(line.get_xdata() >= box.x0) & (line.get_xdata() <= box.x1)]))
                        for line in ax.lines)
        assert box.y0 > curve_max, (box.y0, curve_max)
        assert [t.get_text() for t in legend.get_texts()] == list(m.FUNDER_STYLES)
        for line in ax.lines:
            rows = cached[0].loc[cached[0]['series'] == line.get_label()]
            assert np.array_equal(line.get_xdata(), rows['share_of_funders'].to_numpy())
            assert np.array_equal(line.get_ydata(), rows['complementary_cumulative_share_of_publications'].to_numpy())
        checks.append({'stem': stem, 'legend_bottom_data_y': box.y0,
                       'maximum_curve_y_under_legend': curve_max,
                       'legend_clear_of_curves': True, 'curve_values_unchanged': True})
        original_save(fig, stem, formats)

    m.save_figure = validated_save
    m.set_pub_defaults()
    m.plot_wos_funder_rank_frequency(['png', 'pdf'])
    changed = {f'tables/figure_data/{name}' for name in TABLES}
    for stem in STEMS:
        old_pdf = target / 'plots_and_maps/wos_only' / f'{stem}.pdf'
        old_page = fitz.open(old_pdf)[0]
        new_page = fitz.open(m.OUTPUT_DIR / f'{stem}.pdf')[0]
        def fonts(page):
            return {(s['font'], round(s['size'], 3)) for b in page.get_text('dict')['blocks'] if 'lines' in b
                    for line in b['lines'] for s in line['spans']}
        assert fonts(old_page) == fonts(new_page)
        assert np.allclose(list(old_page.rect), list(new_page.rect), atol=.01)
        assert 'Business' in new_page.get_text() and 'Corporate' not in new_page.get_text()
        for fmt in ['png', 'pdf']:
            dest = target / 'plots_and_maps/wos_only' / f'{stem}.{fmt}'
            shutil.copy2(m.OUTPUT_DIR / dest.name, dest)
            changed.add(str(dest.relative_to(target)))

    for relative, digest in before.items():
        if relative not in changed:
            assert sha(target / relative) == digest, relative
    readme = target / ('README.md' if kind == 'full' else 'README.txt')
    with readme.open('a') as f:
        f.write('\nDistribution label/layout update (2026-10-07): Corporate renamed to Business;\n'
                'the legend moved slightly upward to clear the distribution curves.\n'
                'Only the two distribution aliases (PNG/PDF) and their CSV series labels\n'
                'changed. All numerical data and all other figures/tables are unchanged.\n')
    validation = {'date': '2026-10-07', 'cohort': kind, 'source': str(source),
                  'numerical_values_unchanged': True, 'other_figures_and_tables_unchanged': True,
                  'font_sizes_and_page_dimensions_unchanged': True,
                  'legend_checks': checks, 'changed_result_files': sorted(changed)}
    write_json(target / 'DISTRIBUTION_UPDATE_VALIDATION.json', validation)
    manifest_file = target / 'MANIFEST.json'
    manifest = json.loads(manifest_file.read_text()) if manifest_file.exists() else {'source_archive': str(source)}
    manifest['distribution_label_updated_on'] = '2026-10-07'
    manifest['distribution_label'] = 'Business'
    manifest['distribution_legend_anchor'] = [1.0, .08]
    manifest['rendering_scripts'] = [{'path': str(p), 'sha256': sha(p)}
                                     for p in [Path(__file__), ROOT / 'analysis/generate_paper_figures.py']]
    manifest['files'] = [{'path': str(p.relative_to(target)), 'bytes': p.stat().st_size, 'sha256': sha(p)}
                         for p in sorted(target.rglob('*')) if p.is_file() and p != manifest_file]
    write_json(manifest_file, manifest)
    archive = target.with_suffix('.zip')
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in sorted(target.rglob('*')):
            if p.is_file():
                z.write(p, f'{target.name}/{p.relative_to(target)}')
    with zipfile.ZipFile(archive) as z:
        assert z.testzip() is None
    Path(str(archive) + '.sha256').write_text(f'{sha(archive)}  {archive.name}\n')
    print(f'{kind}: legend clear; all curve values unchanged. {archive}', flush=True)

    # Keep canonical figure paths current, saving previous versions outside the
    # result bundles. Historical zipped bundles remain untouched.
    canonical = ROOT / ('paper_figures_output' if kind == 'full' else 'outputs/wos_scie_ssci_20261007_stylefix/plots')
    backup = RUN / 'previous' / kind
    backup.mkdir(parents=True, exist_ok=True)
    for stem in STEMS:
        for fmt in ['png', 'pdf']:
            path = canonical / f'{stem}.{fmt}'
            shutil.copy2(path, backup / path.name)
            shutil.copy2(m.OUTPUT_DIR / path.name, path)
    csv_canonical = canonical / 'data' if kind == 'full' else canonical.parent / 'figure_data'
    for name in TABLES:
        path = csv_canonical / name
        shutil.copy2(path, backup / name)
        old = pl.read_csv(path)
        old.with_columns(pl.col('series').replace({'Corporate': 'Business'})).write_csv(path)


def main():
    global RUN
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, default=RUN)
    parser.add_argument('--full-source', type=Path, default=FULL_SOURCE)
    parser.add_argument('--subset-source', type=Path, default=SUBSET_SOURCE)
    parser.add_argument('--full-target', type=Path, default=ROOT/'bundles/wos_2025_final_results_plots_tables_2026-10-07_business')
    parser.add_argument('--subset-target', type=Path, default=ROOT/'bundles/wos_2025_scie_ssci_final_results_plots_tables_2026-10-07_business')
    args = parser.parse_args()
    RUN = args.run_dir
    generate_bundle('full', args.full_source, args.full_target)
    generate_bundle('scie_ssci', args.subset_source, args.subset_target)


if __name__ == '__main__':
    main()
