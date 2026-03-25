This directory contains downstream WOS export and validation helpers built on top of the maintained matching pipeline in `scripts/`.

Expected upstream order:

1. `python preprocessing/export_wos_funding_feather.py --wos-extra data/WoS_2023_extra.parquet --output data/WOS_TSV_FUNDING_2023.feather`
2. `python scripts/oa_prep.py`
3. `python scripts/match_stage1.py`
4. `python scripts/match_stage2.py`
5. `python scripts/match_stage3.py`

Expected upstream artifacts:

- `artifacts/institutions_normalized.parquet`
- `artifacts/final_map.parquet`

Expected WOS processed parquet inputs:

- `WoS_2023_main.parquet`
- `WoS_2023_extra.parquet`
- `WoS_2023_authors.parquet`

Current scripts:

- `Step2_MapWOS2Funding.py`: reads WOS processed parquet with `parcake` and attaches OpenAlex matches from `/scripts` artifacts.
- `Step3_SampleForValidation.py`: reads WOS titles/DOIs from `WoS_2023_main.parquet` and builds validation samples from Step2 outputs.

The old Jin notebook, Jin-specific columns, and the former Step1 merge script are intentionally no longer part of this workflow.