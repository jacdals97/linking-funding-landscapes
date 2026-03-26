This directory contains the downstream WOS back-linking helpers built on top of the maintained matching pipeline in `scripts/`.

## Required Upstream Generation

Before running the WOS back-linking scripts, generate the upstream inputs in this order:

1. `python preprocessing/export_wos_funding_feather.py --wos-extra data/WoS_2023_extra.parquet --output data/WOS_TSV_FUNDING_2023.feather`
2. `python scripts/oa_prep.py`
3. `python scripts/match_stage1.py`
4. `python scripts/match_stage2.py`
5. `python scripts/match_stage3.py`

Those upstream steps generate the repository-local inputs consumed here:

- `artifacts/institutions_normalized.parquet`
- `artifacts/wos_funders.parquet`

They also require the processed WOS parquet files produced by the preprocessing pipeline:

- `data/WoS_2023_main.parquet`
- `data/WoS_2023_extra.parquet`
- `data/WoS_2023_authors.parquet`

## WOS Back-Linking Workflow

### 1. Attach OpenAlex Matches Back To WOS Records

Run:

```bash
python scripts_wos_funding/step1_attach_matches_to_wos.py
```

Default inputs:

- `artifacts/institutions_normalized.parquet`
- `artifacts/wos_funders.parquet`
- `data/WoS_2023_extra.parquet`
- `data/WoS_2023_authors.parquet`

Main generated outputs:

- `Results/grant_institutions_2023_affiliation_match_20251023.parquet`
- `Results/grant_institutions_2023_affiliation_match_20251023.feather`
- `Results/grant_agency_matched_2023_affiliation_match_20251023.parquet`
- `Results/grant_agency_matched_2023_affiliation_match_20251023.feather`

Notes:

- This script reads the large WOS parquet tables with `parcake`.
- It preserves ordered author-country and author-affiliation information when attaching candidates back to papers.
- If your WOS processed files live outside `data/`, pass `--wos-extra` and `--wos-authors` explicitly.

### 2. Build Validation Samples From The WOS-Linked Results

Run:

```bash
python scripts_wos_funding/step2_build_wos_validation_samples.py
```

Default inputs:

- `Results/grant_institutions_2023_affiliation_match_20251023.parquet`
- `data/WoS_2023_main.parquet`

Main generated outputs:

- `Results/Samples/complete_validation_dataset_2023_affiliation_match_20251023.feather`
- `Results/Samples/complete_validation_dataset_2023_affiliation_match_20251023.csv`
- `Results/Samples/sample_by_paper_validation_dataset_2023_affiliation_match_20251023.csv`
- `Results/Samples/sample_by_agency_validation_dataset_2023_affiliation_match_20251023.csv`

If you change the Step 1 basename, pass the matching `--matches` path here.
