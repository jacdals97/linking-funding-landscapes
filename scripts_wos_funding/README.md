This directory contains the downstream WOS back-linking helpers built on top of the maintained matching pipeline in `scripts/`.

## Required Upstream Generation

Before running the WOS back-linking scripts, generate the upstream inputs in this order:

1. `python preprocessing/export_wos_funding_feather.py --wos-main data/WoS_2025_main.parquet --output data/WOS_TSV_FUNDING_2025.feather`
2. `python scripts/oa_prep.py`
3. `python scripts/match_stage1.py`
4. `python scripts/convert_gliner_news_to_onnx.py` (once per model directory)
5. `python scripts/match_stage2.py`
6. `python scripts/match_stage3.py`

Those upstream steps generate the repository-local inputs consumed here:

- `artifacts/institutions_normalized.parquet`
- `artifacts/wos_funders.parquet`

They also require the processed WOS parquet files produced by the preprocessing pipeline:

- `data/WoS_2025_main.parquet`
- `data/WoS_2025_extra.parquet` (fallback for country metadata)
- `data/WoS_2025_authors.parquet`

## WOS Back-Linking Workflow

### 1. Attach OpenAlex Matches Back To WOS Records

Run:

```bash
python scripts_wos_funding/step1_attach_matches_to_wos.py
```

Default inputs:

- `artifacts/institutions_normalized.parquet`
- `artifacts/wos_funders.parquet`
- `data/WoS_2025_main.parquet`
- `data/WoS_2025_authors.parquet`

Main generated outputs:

- `Results/grant_institutions_2025_affiliation_match_20251023.parquet`
- `Results/grant_institutions_2025_affiliation_match_20251023.feather` (only with `--write-feather`)
- `Results/grant_agency_matched_2025_affiliation_match_20251023.parquet`
- `Results/grant_agency_matched_2025_affiliation_match_20251023.feather`

Notes:

- This script reads the large WOS parquet tables with `parcake`.
- It preserves ordered author-country and author-affiliation information when attaching candidates back to papers.
- If your WOS processed files live outside `data/`, pass `--wos-main` and `--wos-authors` explicitly.

### 2. Build Validation Samples From The WOS-Linked Results

Run:

```bash
python scripts_wos_funding/step2_build_wos_validation_samples.py
```

Default inputs:

- `Results/grant_institutions_2025_affiliation_match_20251023.parquet`
- `data/WoS_2025_main.parquet`

Main generated outputs:

- `Results/Samples/complete_validation_dataset_2025_affiliation_match_20251023.feather`
- `Results/Samples/complete_validation_dataset_2025_affiliation_match_20251023.csv`
- `Results/Samples/sample_by_paper_validation_dataset_2025_affiliation_match_20251023.csv`
- `Results/Samples/sample_by_agency_validation_dataset_2025_affiliation_match_20251023.csv`

If you change the Step 1 basename, pass the matching `--matches` path here.


## Running through the pipeline

```bash
python run_pipeline.py --workflow matching --from attach --set workers=4
```

The runner consistently passes the main/authors/extra paths, reference index,
final map, result directory and basename to the scripts. Its default `scope_map`
is the reviewed top-5000 map; standalone Step 1 retains its older default map.
Set `scope_map` explicitly when reproducing a particular result. The large
paper-level Feather output is optional; Parquet is always written. Author and
validation workers are initialized explicitly so they also work with Python's
spawn multiprocessing method.

Annual summaries, matching diagnostics and plotting code now live in
[`analysis/`](../analysis/README.md). Run `python run_pipeline.py --workflow analysis`
after attachment, or call those scripts individually.
