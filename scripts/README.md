# Matching and evaluation scripts

The main pipeline uses these existing scripts in order:

1. `oa_prep.py`: normalize the versioned OpenAlex/ROR reference data.
2. `match_stage1.py`: initial matching, manual rules and optional API augmentation.
3. `convert_gliner_news_to_onnx.py`: export the local model used by stage 2.
4. `match_stage2.py`: NER-assisted matching.
5. `match_stage3.py`: final map export.

Run `python run_pipeline.py --workflow matching` to also prepare the funding
extract, attach matches and generate validation samples. Use `--from reference`
if the funding Feather already exists. All scripts expose their own `--help`.
`check_manual_annotations.py` is an optional OpenAlex API validation utility;
it reads `OPENALEX_API_KEY` from the environment.

## Annotation evaluation

After preparing source extracts (`--workflow sources`) and WoS attachment:

```bash
python run_pipeline.py --workflow evaluation --set annotations_dir=/path/to/annotations --set dimensions_root=/path/to/dimensions
```

This runs `update_v1_annotations.py` followed by
`build_annotarium_funder_evaluation.py`, writing a new local annotation/evaluation
project. The input annotations are local working data and are not committed.
The output includes `records.csv`, review queues, a configuration and a build
summary for Annotarium. Creating these files does not submit or publish them.

`build_annotarium_wos_expansion.py` can then create an additional sample from the
WoS validation dataset, excluding already annotated records. Its default sample
size is 500 and its random seed is fixed; use `--wos-sample`, `--wos-matches`,
`--exclude-annotations-dir`, `--source-funders-dir`, `--dimensions-organizations`
and `--output-dir` for other runs.

`repair_annotarium_wos_alignment.py` repairs funder-label/identifier alignment in
an existing records CSV. `apply_annotarium_wos_patch.py` applies an explicit patch
CSV. They are maintenance utilities, not automatic pipeline stages. Both default
to replacing the supplied records CSV; use `--output` for a separate result and
`--backup` when preserving a copy at a specified location.

Plotting and release-rendering scripts have moved to
[`analysis/`](../analysis/README.md).
