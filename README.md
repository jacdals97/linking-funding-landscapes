# jacob 
# linking-funding-landscapes

Project pipeline for linking grant agency strings to OpenAlex funders/institutions.

## Script order

1) OpenAlex prep (required)
   - `python3 scripts/oa_prep.py`
   - Produces `artifacts/institutions_normalized.parquet` and other intermediate artifacts used downstream.

2) Stage 1 matching (required)
   - `python3 scripts/match_stage1.py`
   - Writes `artifacts/u_agencies_stage1.parquet` and related outputs.
   - Validate manual annotations with `python3 scripts/check_manual_annotations.py --mode id --mailto you@example.com` (optional).

3) NER model export (required for stage 2)
   - `python3 scripts/convert_gliner_news_to_onnx.py`
   - Writes `gliner_news_small/model_quantized.onnx` used by stage 2.

4) Stage 2 matching (mid-bucket NER)
   - `python3 scripts/match_stage2.py`
   - Writes `artifacts/u_agencies_stage2.parquet`.

5) Stage 3 final dedup + map outputs
   - `python3 scripts/match_stage3.py`
   - Writes `artifacts/final_map.parquet`, plus `institutions_processed_duplicates.feather` and `missing_agencies.feather`.

