# Preprocessing

This directory contains preprocessing helpers and vendored upstream libraries used to prepare Web of Science and OpenAlex inputs for the matching pipeline.

The external dependencies are vendored as git submodules under `preprocessing/libraries`:

- `openalex-raw`: https://github.com/filipinascimento/openalex-raw
- `WOSRaw`: https://github.com/filipinascimento/WOSRaw
- `parcake`: https://github.com/filipinascimento/parcake

## Install The Dependencies

### Option 1: Use The Vendored Submodules

If this repository was cloned without submodules, initialize them first:

```bash
git submodule update --init --recursive \
  preprocessing/libraries/openalex-raw \
  preprocessing/libraries/WOSRaw \
  preprocessing/libraries/parcake
```

Then install both packages into your active Python environment:

```bash
python -m pip install -e preprocessing/libraries/openalex-raw
python -m pip install -e preprocessing/libraries/WOSRaw
python -m pip install -e preprocessing/libraries/parcake
python -m pip install -U dbgz
```

### Option 2: Install Directly From GitHub

If you do not want to install from the vendored submodules, install directly from upstream:

```bash
python -m pip install git+https://github.com/filipinascimento/openalex-raw.git
python -m pip install git+https://github.com/filipinascimento/WOSRaw.git
python -m pip install git+https://github.com/filipinascimento/parcake.git
python -m pip install -U dbgz
```

## Web Of Science

The `WOSRaw` package exposes the `wosraw-pipeline` CLI. It creates a raw `.dbgz` archive and then exports processed tables.

### Recommended Command

```bash
wosraw-pipeline PATH/TO/YOUR/WOS/RAW/DATA \
  --output-dir PATH/TO/OUTPUT/DIRECTORY \
  --ncpu 16 \
  --max-piece-size 60000 \
  --xml-timeout-seconds 20000 \
  --heartbeat-seconds 60 \
  --worker-log-dir PATH/TO/WORKER/LOGS
```

This writes:

- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_All.dbgz`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_main.parquet`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_extra.parquet`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_abstract.parquet`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_references.parquet`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_authors.parquet`
- `PATH/TO/OUTPUT/DIRECTORY/WOS_TSV_FUNDING_2023.feather`
- `PATH/TO/OUTPUT/DIRECTORY/WoS_2023_errors.log`

Those processed parquet outputs are the useful starting point for downstream matching and validation work. The `WOS_TSV_FUNDING_2023.feather` export is derived from `WoS_2023_extra.parquet` for `scripts/match_stage1.py`. The downstream WOS helper scripts under `scripts_wos_funding` read the large WOS parquet tables with `parcake.PieceReader.process(...)` instead of direct `pyarrow` row-group reads.

The main downstream consumers are:

- `scripts/match_stage1.py`, which reads `WOS_TSV_FUNDING_2023.feather`;
- `scripts_wos_funding/step1_attach_matches_to_wos.py`, which reads `WoS_2023_extra.parquet` and `WoS_2023_authors.parquet`;
- `scripts_wos_funding/step2_build_wos_validation_samples.py`, which reads `WoS_2023_main.parquet`.

### Repo Wrapper Script

This repository also includes a thin wrapper with the same defaults:

```bash
bash preprocessing/run_wosraw_pipeline.sh PATH/TO/YOUR/WOS/RAW/DATA PATH/TO/OUTPUT/DIRECTORY
```

That wrapper also writes `PATH/TO/OUTPUT/DIRECTORY/WOS_TSV_FUNDING_2023.feather` after the main WOS export finishes.

You can pass extra `wosraw-pipeline` arguments at the end. For example, to also write a raw parquet archive:

```bash
bash preprocessing/run_wosraw_pipeline.sh PATH/TO/YOUR/WOS/RAW/DATA PATH/TO/OUTPUT/DIRECTORY \
  --raw-output-format both \
  --raw-parquet PATH/TO/OUTPUT/DIRECTORY/WoS_2023_All.parquet
```

## OpenAlex

The repository uses OpenAlex institutions and funders exports with the following exact file names:

- `institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv`
- `institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.json`
- `funders_core+basic+ids+extra+counts.tsv`
- `funders_core+basic+ids+extra+counts.json`

The schema sidecar `.json` files are written automatically when exporting TSV through `openalex-raw`.

### Repo Export Script

This repository includes a helper script that reproduces those exports from a local OpenAlex snapshot:

```bash
python preprocessing/export_openalex_snapshot.py \
  --snapshot-path PATH/TO/YOUR/OPENALEX/SNAPSHOT \
  --output-dir data/openalex_institutions \
  --entities institutions funders \
  --formats tsv \
  --ncpu 8
```

If you also want parquet copies of the same exports:

```bash
python preprocessing/export_openalex_snapshot.py \
  --snapshot-path PATH/TO/YOUR/OPENALEX/SNAPSHOT \
  --output-dir data/openalex_institutions \
  --entities institutions funders \
  --formats tsv parquet \
  --ncpu 8
```

This uses the upstream OpenAlex selection presets:

- institutions: `core + basic + alternate_names + ids + geo + urls + roles + metrics`
- funders: `core + basic + ids + extra + counts`

### Using The Exports In This Repository

The main pipeline expects these OpenAlex exports as inputs to `scripts/oa_prep.py`.

Example:

```bash
python scripts/oa_prep.py --input-dir data/openalex_institutions
```

That step reads:

- `data/openalex_institutions/funders_core+basic+ids+extra+counts.tsv`
- `data/openalex_institutions/funders_core+basic+ids+extra+counts.json`
- `data/openalex_institutions/institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv`
- `data/openalex_institutions/institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.json`

and writes the repo-local artifacts consumed by the matching stages.
