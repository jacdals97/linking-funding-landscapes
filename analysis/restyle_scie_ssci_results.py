#!/usr/bin/env python3
"""Regenerate only SCIE/SSCI figure rendering; preserve all released tables."""
from __future__ import annotations

import importlib.util
import argparse
import json
import re
import shutil
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
OLD_RUN = ROOT / 'outputs/wos_scie_ssci_20261002'
RUN = ROOT / 'outputs/wos_scie_ssci_20261007_stylefix'
OLD_BUNDLE = ROOT / 'bundles/wos_2025_scie_ssci_final_results_plots_tables_2026-10-02'
BUNDLE = ROOT / 'bundles/wos_2025_scie_ssci_final_results_plots_tables_2026-10-07'
BASELINE = ROOT / 'bundles/wos_2025_final_results_plots_tables_2026-09-18.zip'


def load_module():
    spec = importlib.util.spec_from_file_location('scie_ssci_render', ROOT / 'analysis/recalculate_scie_ssci_results.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    global OLD_RUN, RUN, OLD_BUNDLE, BUNDLE, BASELINE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resume', action='store_true', help='Resume this script\'s unfinished dated bundle.')
    parser.add_argument('--source-run', type=Path, default=OLD_RUN)
    parser.add_argument('--run-dir', type=Path, default=RUN)
    parser.add_argument('--source-bundle', type=Path, default=OLD_BUNDLE)
    parser.add_argument('--bundle-dir', type=Path, default=BUNDLE)
    parser.add_argument('--baseline-zip', type=Path, default=BASELINE)
    parser.add_argument('--source-results-dir', type=Path, default=ROOT/'Results/SourcePaperFunders')
    parser.add_argument('--figure-cache-dir', type=Path, default=ROOT/'paper_figures_output/data')
    parser.add_argument('--scope-map', type=Path, default=ROOT/'scripts_wos_funding/funder_multicountry_scope_map_top5000_reviewed.csv')
    args = parser.parse_args()
    OLD_RUN, RUN, OLD_BUNDLE, BUNDLE, BASELINE = args.source_run, args.run_dir, args.source_bundle, args.bundle_dir, args.baseline_zip
    if BUNDLE.with_suffix('.zip').exists() or (BUNDLE.exists() and not args.resume):
        raise FileExistsError('Refusing to replace the dated corrected bundle.')
    if BUNDLE.exists():
        # A failed validation leaves the original manifest in our copied bundle.
        assert (BUNDLE / 'MANIFEST.json').read_bytes() == (OLD_BUNDLE / 'MANIFEST.json').read_bytes()
    module = load_module()
    import fitz
    import pandas as pd
    import numpy as np
    from PIL import Image

    # Keep source tables, caches and old deliverables untouched.
    before = {str(p.relative_to(OLD_BUNDLE)): module.sha(p)
              for p in OLD_BUNDLE.rglob('*') if p.is_file()}
    RUN.mkdir(parents=True, exist_ok=True)
    module.RUN = RUN
    module.VIEW = OLD_RUN/'work/figure_inputs'
    module.SOURCE_RESULTS = args.source_results_dir
    module.FIGURE_CACHE = args.figure_cache_dir
    module.SCOPE_MAP = args.scope_map
    # Deliberately reuse module.VIEW in the old work directory, read-only.
    print('Rendering original figure functions with the existing reduced tables.', flush=True)
    module.plots()

    # Check regenerated plotting summaries against the previous ones. Only the
    # restored legend's series label differs; no numerical values may change.
    summary_checks = []
    for old in sorted((OLD_RUN / 'figure_data').glob('*.csv')):
        new = RUN / 'figure_data' / old.name
        assert new.exists(), new
        left, right = pd.read_csv(old), pd.read_csv(new)
        for frame in [left, right]:
            if 'series' in frame:
                frame['series'] = frame['series'].replace({'All matched funders': 'All publications'})
        assert list(left.columns) == list(right.columns), old.name
        # Compare row sets without making aggregation order a requirement.
        keys = list(left.columns)
        left = left.sort_values(keys).reset_index(drop=True)
        right = right.sort_values(keys).reset_index(drop=True)
        pd.testing.assert_frame_equal(left, right, check_dtype=False, rtol=1e-12, atol=1e-12)
        summary_checks.append(old.name)

    if not BUNDLE.exists():
        shutil.copytree(OLD_BUNDLE, BUNDLE)
    plot_files = sorted((BUNDLE / 'plots_and_maps').rglob('*'))
    for dest in plot_files:
        if dest.suffix not in ['.png', '.pdf']:
            continue
        source = RUN / 'plots' / dest.name
        if 'funding_over_time' in dest.parts:
            scope = '_scope_priority_log' in dest.stem
            name = dest.name.replace('_scope_priority_log.', '_log.') if scope else dest.name
            source = RUN / 'funding_plots' / ('scope_priority' if scope else 'base') / name
        assert source.is_file(), source
        shutil.copy2(source, dest)

    checks = []
    with zipfile.ZipFile(BASELINE) as original:
        references = {Path(n).name: n for n in original.namelist() if n.endswith('.pdf')}
        for pdf in sorted((BUNDLE / 'plots_and_maps').rglob('*.pdf')):
            ref = references[pdf.name]
            with fitz.open(stream=original.read(ref), filetype='pdf') as baseline, fitz.open(pdf) as fixed:
                def spans(page):
                    return [s for b in page.get_text('dict')['blocks'] if 'lines' in b
                            for line in b['lines'] for s in line['spans']]
                old_spans, new_spans = spans(baseline[0]), spans(fixed[0])
                old_fonts = {(s['font'], round(s['size'], 3)) for s in old_spans}
                new_fonts = {(s['font'], round(s['size'], 3)) for s in new_spans}
                assert old_fonts == new_fonts, (pdf.name, old_fonts, new_fonts)
                text = fixed[0].get_text()
                assert 'SCIE/SSCI' not in text and 'other databases unchanged' not in text, pdf
                # Organization ranks and numbers can differ; fixed axis/legend
                # wording and font sizes must not.
                def static_text(items):
                    return {s['text'].strip() for s in items
                            if re.search(r'[A-Za-z]{3}', s['text'])
                            and not re.search(r'\d', s['text'])}
                if 'rank_change_top20' not in pdf.name:
                    old_text = static_text(old_spans)
                    if 'funder_rank_frequency' in pdf.name:
                        old_text = {'Business' if t == 'Corporate' else t for t in old_text}
                    assert old_text == static_text(new_spans), (
                        pdf.name, static_text(old_spans) ^ static_text(new_spans))
                # Tight crops may legitimately change slightly with tick widths.
                old_size, new_size = list(baseline[0].rect)[2:], list(fixed[0].rect)[2:]
                assert np.allclose(old_size, new_size, atol=2, rtol=0), (pdf.name, old_size, new_size)
                png = pdf.with_suffix('.png')
                with Image.open(png) as rendered:
                    assert rendered.width > 1000 and rendered.height > 500
                checks.append({'figure': str(pdf.relative_to(BUNDLE)),
                               'font_family_and_sizes_match_original': True,
                               'original_axis_and_legend_wording': True,
                               'no_added_subset_footer_or_title': True,
                               'original_page_size_points': old_size,
                               'corrected_page_size_points': new_size})

    for relative, digest in before.items():
        assert module.sha(OLD_BUNDLE / relative) == digest, relative
        if relative.startswith('tables/') or relative in ['TABLE_INDEX.csv', 'SELECTION_AND_COUNTS.json']:
            assert module.sha(BUNDLE / relative) == digest, relative

    readme = BUNDLE / 'README.txt'
    text = readme.read_text().replace('(2026-10-02)', '(style correction 2026-10-07)', 1)
    text += '''
Figure style correction (2026-10-07): all PNG/PDF figures regenerated using the
original axis/legend/title wording, typeface, font sizes, dimensions, colors,
and plotting layout. Added gray SCIE/SSCI footers and title suffixes removed.
The rank-change chart again uses the original grouped palette and rank axis.
All released CSV tables and selection/count totals are byte-for-byte unchanged.
Subset identification is kept here rather than added inside the figures.
Original publication-worded concentration labels are retained for consistency;
their counting unit is paper--funder links, as explained above, not unique papers.
The previous 2026-10-02 folder/ZIP and all-edition figures are preserved.
'''
    readme.write_text(text)
    validation = {'style_only': True, 'matching_pipeline_rerun': False,
                  'released_csv_tables_byte_identical': True,
                  'old_bundle_files_unchanged': True,
                  'recomputed_plot_summaries_numerically_unchanged': summary_checks,
                  'original_style_reference': str(BASELINE), 'pdf_checks': checks,
                  'rank_plot_style_reconstructed_from_original_vector_pdf': True}
    module.write_json(BUNDLE / 'STYLE_VALIDATION.json', validation)
    manifest = json.loads((BUNDLE / 'MANIFEST.json').read_text())
    manifest['style_corrected_on'] = '2026-10-07'
    manifest['previous_bundle_preserved'] = str(OLD_BUNDLE)
    manifest['style_only'] = True
    manifest['rendering_scripts'] = [{'path': str(p), 'sha256': module.sha(p)}
                                    for p in [Path(__file__), ROOT / 'analysis/recalculate_scie_ssci_results.py']]
    manifest['files'] = [{'path': str(p.relative_to(BUNDLE)), 'bytes': p.stat().st_size,
                          'sha256': module.sha(p)} for p in sorted(BUNDLE.rglob('*'))
                         if p.is_file() and p.name != 'MANIFEST.json']
    module.write_json(BUNDLE / 'MANIFEST.json', manifest)
    archive = BUNDLE.with_suffix('.zip')
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in sorted(BUNDLE.rglob('*')):
            if p.is_file():
                z.write(p, f'{BUNDLE.name}/{p.relative_to(BUNDLE)}')
    with zipfile.ZipFile(archive) as z:
        assert z.testzip() is None
    Path(str(archive) + '.sha256').write_text(f'{module.sha(archive)}  {archive.name}\n')
    print(f'Validated {len(checks)} PDF/PNG pairs; all tables unchanged.\n{archive}', flush=True)


if __name__ == '__main__':
    main()
