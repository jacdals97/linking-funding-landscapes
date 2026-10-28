# linking-funding-landscapes

Link funding-organization strings in Web of Science to OpenAlex funders and
institutions, attach matches to publications, and reproduce the associated
analyses and figures.

The project keeps the original script-based organization. `run_pipeline.py`
runs those scripts in order using the active Python interpreter. It runs in the
foreground on a workstation or server, without a scheduler, `screen`, or a
machine-specific environment activation command.

## Layout

| Location | Purpose |
| --- | --- |
| `run_pipeline.py`, `pipeline.json` | Ordered workflows, input/output paths, stage selection, logs |
| `preprocessing/` | WoS raw conversion, funding extract, OpenAlex and Dimensions source extracts |
| `scripts/` | Reference preparation, three matching stages, model export, annotation/evaluation utilities |
| `scripts_wos_funding/` | Attach matches to WoS records, country/scope resolution, validation samples |
| `analysis/` | Annual counts, matching diagnostics, plot generation, SCIE/SSCI reaggregation, release rendering |
| `notebooks/paper_figures.ipynb` | Interactive companion that calls the figure script |
| `plotting_data/` | Versioned plotting summaries; downloaded map geometry is cached here |
| `data/` | Versioned OpenAlex/ROR references and manual mappings; local licensed inputs |
| `evals/` | Independent annotator CSVs, and analysis notebook |
| `Results/`, `artifacts/`, `outputs/`, `paper_figures_output/`, `bundles/` | Generated products, excluded from Git |

The preprocessing libraries remain Git submodules. Local agent resources and
cluster configuration are not dependencies of the pipeline.

## Environment

Use Python 3.11 for the pinned matching environment. With Conda:

```bash
conda env update -f env.yml
conda activate linking_funding
conda env update -f env-paper.yml  # add analysis/figure dependencies
```

Alternatively, create a Python virtual environment, activate it using your
platform's usual command, then install:

```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-analysis.txt
```

Install the preprocessing libraries from the revisions recorded by this repository:

```bash
git submodule update --init --recursive
python -m pip install -e preprocessing/libraries/parcake
python -m pip install -e preprocessing/libraries/WOSRaw
python -m pip install -e preprocessing/libraries/openalex-raw
```

See [preprocessing/README.md](preprocessing/README.md) for raw-data preparation and
the `dbgz` compatibility requirement. No GPU is required by the runner; model
export and inference depend on the installed PyTorch/ONNX packages. The first
GLiNER export downloads model weights. Country maps download Natural Earth
geometry unless a cached copy is available or `--skip-download` is supplied.

## Run the pipeline

Commands below are shown from the repository root. The runner also works when
called by absolute path from another directory; relative configuration paths
always refer to the repository root.

```bash
# Inspect workflows and all configurable paths; these commands do not run jobs.
python run_pipeline.py --list
python run_pipeline.py --workflow matching --dry-run
python run_pipeline.py --workflow matching --check-inputs

# Start from already processed WoS main/extra/authors tables.
python run_pipeline.py --workflow matching --set workers=4

# Start from raw WoS deliveries, then match, validate, and make annual summaries.
python run_pipeline.py --workflow from-raw --set wos_raw_dir=/path/to/wos/raw --set workers=4

# Continue after a completed matching stage.
python run_pipeline.py --workflow matching --from match2

# Reuse an already exported model.
python run_pipeline.py --workflow matching --skip model

# Rebuild only the validation samples.
python run_pipeline.py --stage validation
```

`matching` runs this sequence:

1. **funding-extract**: `WoS_2025_main.parquet` → funding Feather (`UID`, `agency_name`, funding fields).
2. **reference**: OpenAlex/ROR reference files → normalized organization index.
3. **match1**: exact, manual, substring, acronym and clustering matches.
4. **model**: export the GLiNER ONNX model.
5. **match2**: NER-assisted recovery.
6. **match3**: final residual matching → `artifacts/wos_funders.parquet`.
7. **attach**: attach candidates to WoS papers and resolve country/scope information.
8. **validation**: build reproducible paper and agency samples.

`from-raw` prepends raw WoS conversion and appends diagnostics, annual counts,
and annual plots. Individual scripts remain runnable directly with `--help`.

| Workflow | Ordered purpose | Additional inputs |
| --- | --- | --- |
| `preprocessing` | Raw WoS conversion → funding Feather | Licensed raw deliveries |
| `matching` | Funding extract → reference → matching → attachment → validation | Processed WoS tables and versioned references |
| `from-raw` | Preprocessing → matching → annual analysis | Raw deliveries and references |
| `analysis` | Matching-resolution diagnostics → annual counts → annual plots | WoS main table and attached matches |
| `sources` | OpenAlex → Dimensions paper-funder extracts and annual counts | Local snapshots for both databases |
| `figures` | Publication figures from existing summaries | Plot tables, source counts and country summaries; see analysis README |
| `scie-ssci` | Record selection → match filtering → tables → author summaries → plots → bundle | Existing attached matches, WoS tables and comparison summaries |
| `evaluation` | Update manual annotations → create Annotarium evaluation files | Local annotations and source paper-funder extracts |

These workflows have different input requirements. `from-raw` produces the WoS
matching products and annual plots; cross-database figures also need the other
databases and plotting summaries. See [analysis/README.md](analysis/README.md)
for exactly which figure inputs are provided and which must be prepared locally.

## Configure inputs and outputs

Defaults preserve the original `data/`, `artifacts/`, and `Results/` layout.
Override a variable with repeated `--set KEY=value`, or copy `pipeline.json` to
`pipeline.local.json`, edit it, and pass `--config pipeline.local.json`.

```bash
python run_pipeline.py --workflow matching \
  --set data_dir=/path/to/processed-wos \
  --set wos_basename=WoS_2025 \
  --set artifacts_dir=/path/to/run/artifacts \
  --set results_dir=/path/to/run/results \
  --set model_dir=/path/to/models/gliner_news_small \
  --set workers=4
```

Paths derived from these variables propagate to later stages. `reference_dir`,
`ror_file`, and `manual_annotations` remain the versioned reference inputs unless
explicitly overridden. The default attachment uses the reviewed top-5000 scope
map; override `scope_map` to reproduce a different reviewed mapping. The
historical result basename is retained for filename compatibility, not as a run
date. Set `matches_basename` for a new run. To analyze an existing result file,
set `matches` directly; do not override `matches` during attachment, whose output
name is determined by `results_dir` and `matches_basename`.

The runner passes `validation_basename` explicitly to the validation script,
so its filenames and output checks agree even when `matches` points to another
file. It defaults to `validation_dataset_{matches_basename}`. Set
`--set validation_basename=validation_dataset_other` to keep a separate sample
set. Direct script calls without `--output-basename` derive filenames from the
selected matches file as before.

`--from` and `--to` select an inclusive range in a workflow. `--stage NAME` runs a
single stage (repeat it for an explicit sequence). Inputs are checked before
execution, including dependencies that earlier selected stages will create.
Each stage's output is checked before continuing. A failing stage stops the run.
Logs and a command/status manifest are written under `logs/pipeline/`.

Stages are **not automatically skipped** just because an output exists. Most
scripts overwrite their generated files when explicitly rerun. Use a new output
directory for an independent run, and use `--from`/`--skip` to reuse completed
work deliberately. SCIE/SSCI aggregation caches require a fresh `subset_run`
when changing source inputs. Bundle creation refuses to replace an existing bundle.

## External data and API access

Versioned references include OpenAlex funder/institution tables and schemas,
the ROR snapshot, manual string-to-ID annotations, and reviewed scope maps.
Licensed WoS and Dimensions data, raw snapshots, model weights, annotation
working files, and generated outputs are not distributed in Git. Their schemas
and provenance are documented in [data/README.md](data/README.md).
The three manual-evaluation rating CSVs in `evals/` are versioned separately
from those working files.

Optional stage-1 OpenAlex augmentation reads `OPENALEX_API_KEY` from the
environment. Without it, API augmentation is skipped. Set it outside the
configuration file; do not put credentials in versioned files. To disable API
augmentation even when a key exists, run stage 1 with `--skip-oa-api` or add that
flag to the stage command in your local configuration.

## Analysis and figures

All maintained plotting code lives in [analysis/](analysis/README.md):

```bash
python run_pipeline.py --workflow analysis
python run_pipeline.py --workflow figures --dry-run

# Reproduce the funder distribution using the versioned plotting table only.
python analysis/generate_paper_figures.py --groups rank-frequency

# Regenerate the SCIE/SSCI subset from existing record-level matches.
python run_pipeline.py --workflow scie-ssci --set matches=/path/to/final_matches.parquet
```

The notebook calls `analysis/generate_paper_figures.py` with its own Python
interpreter. It displays generated plots rather than containing a separate
implementation. Historical release-rendering corrections are available as
explicit utilities and are not part of a normal data-processing run.

## Manual funder evaluation

The self-contained [evaluation notebook](evals/manual_evaluation.ipynb) reads
the independent ratings from
[Jacob](evals/records_jacob_20261002.csv),
[Filipi](evals/records_filipi_20261002.csv), and
[Jin](evals/annotation_ja_20261002.csv). Open it from the repository root or
`evals/` and run the cells in order. The Conda environment above, including
`env-paper.yml`, supplies NumPy, pandas, Matplotlib, and Jupyter; a smaller
notebook-only environment needs those four packages.

The notebook joins ratings by `record_id` and reports evaluation metrics,
10,000 paper-level bootstrap resamples and paired database differences,
Krippendorff's interval alpha, and linearly weighted Gwet's AC2. It also shows
the Jacob–Filipi comparison of wrong-funder judgments and exact count agreement
with mean absolute differences across annotators. The 500-paper sample is
already present in the CSVs; the bootstrap does not redraw the original WoS
sample. The notebook reads the CSVs without changing them and needs no licensed
WoS source tables or generated files under `evals/results/`.

## Checks

```bash
python -m pip install pytest
python -m pytest -q
```

The tests exercise runner ordering/failure behavior and small synthetic data.
They do not rerun the complete licensed-data matching pipeline.

## Data release and citation

Pipeline output data are released on Zenodo:
[10.5281/zenodo.23200326](https://zenodo.org/records/23200326).

If you use the code or derived data, cite *Linking Global Science Funding to
Research Publications*, Jacob Aarup Dalsgaard, Filipi Nascimento Silva, and Jin Ai,
[arXiv:2603.24147](https://doi.org/10.48550/arXiv.2603.24147). Cite the upstream
OpenAlex, ROR, and bibliographic data resources as appropriate.
