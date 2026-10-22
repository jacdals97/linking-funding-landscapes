# Web of Science processed data

This directory contains the completed local Web of Science (WoS) outputs used
by the `linking-funding-landscapes` and `sciencefunding` pipelines. The files
are derived from licensed Clarivate WoS raw XML zip deliveries. The local
snapshot documented here used the `WoS_20260112/Midyear` delivery: 82 CORE zip
files, 25 ESCI zip files and one `WOS_DAIS_2023_Q3.txt.gz` file. Configure your
own raw location with `wos_raw_dir`; processed files default to this `data/`
directory. Raw and processed licensed files are excluded from Git.

## Completed snapshot inventory

These sizes and counts were verified from filesystem and Parquet-footer
metadata on 2026-09-04. Reading the footers does not scan the data pages.

| File | Role | Rows | Top-level columns | Row groups | Size |
| --- | --- | ---: | ---: | ---: | ---: |
| `WoS_2025_All.dbgz` | Raw WOSRaw record archive derived from the XML deliveries | — | 2 logical fields (`UID`, `data`) | — | 375,797,239,638 bytes (349.99 GiB) |
| `WoS_2025_main.parquet` | Main record-level bibliographic table | 99,579,328 | 52 | 1,660 | 74,165,098,061 bytes (69.07 GiB) |
| `WoS_2025_extra.parquet` | Raw structured/JSON metadata fields | 99,579,328 | 14 | 1,660 | 488,433,130,387 bytes (454.89 GiB) |
| `WoS_2025_abstract.parquet` | Record-level abstracts where available | 58,919,737 | 2 | 982 | 42,248,003,250 bytes (39.35 GiB) |
| `WoS_2025_authors.parquet` | Paper-author relation | 385,299,776 | 14 | 6,422 | 53,954,897,019 bytes (50.25 GiB) |
| `WoS_2025_references.parquet` | Raw directed citing-cited relation | 1,903,457,668 | 2 | 31,725 | 23,046,449,155 bytes (21.46 GiB) |

All five Parquet tables are Snappy-compressed. The main and extra tables each
contain 99,578,246 unique canonical WoS IDs plus 1,082 exact repeated source
rows; the downstream `sciencefunding` pipeline deterministically collapses
those exact duplicates. The abstract table similarly contains 58,918,809
unique IDs plus 928 exact repeated rows. The citation relation is deduplicated
by normalized `(Citing, Cited)` pair downstream before derived measures are
computed.

The source data are licensed Web of Science data and must be obtained through
your institution's authorized access.

## Provenance

The historical extraction used a local checkout of `filipinascimento/WOSRaw`
and an upstream GitHub installation of `dbgz`, because the then-installed PyPI
package lacked the required piece reader/saver APIs. The inventory above records
that completed extraction; reorganizing the pipeline does not regenerate or
change those files. The exact historical external checkout revision was not
recorded here, so these inventory notes are not a pinned reproduction manifest.

For a new extraction, use the repository's WOSRaw submodule and portable runner:

```bash
python run_pipeline.py --workflow preprocessing --set wos_raw_dir=/path/to/WoS_20260112/Midyear --set wos_basename=WoS_2025 --set workers=4
```

This parses raw `.zip` deliveries containing `.xml.gz`, writes the DBGZ archive,
exports processed Parquet tables, and derives the funding Feather. The
`WoS_2025` basename is a snapshot label, independent of the delivery directory's
name. Worker count is configurable. The extraction implementations are
`preprocessing/libraries/WOSRaw/WOSRaw/processed.py` and `utilities.py`.
See [preprocessing instructions](../preprocessing/README.md) for installation.

## Encoding and data types

All processed parquet columns are written as strings by WOSRaw. Values that are
lists or dictionaries in the raw XML-derived Python record are serialized as
JSON strings with `ujson.dumps`. Empty or missing values are stored as empty
strings.

This means columns such as `AU`, `FU`, `FUR`, `author_addresses`, `PIR`, and
`IDR` should be parsed as JSON or semicolon-delimited strings depending on the
column. See the column notes below.

## Files

### `WoS_2025_All.dbgz`

Raw WOSRaw archive. Each record has:

| Column | Description |
| --- | --- |
| `UID` | Web of Science unique identifier, for example `WOS:...`. |
| `data` | Full XML-derived record as a Python dictionary stored inside DBGZ. |

This is the intermediate archive used by WOSRaw to produce the processed parquet
tables. Most downstream analysis should use the parquet files below instead of
reading this archive directly.

### `WoS_2025_main.parquet`

One row per WoS record. This table contains the main two-character WoS fields,
excluding abstracts. All columns are strings; list-valued fields are JSON
strings.

| Column | Description |
| --- | --- |
| `PT` | Publication type from publication metadata, e.g. journal, book, conference. |
| `AU` | Author display names, from WOSRaw `getAuthorNames(getAuthors(...))`; JSON string list. |
| `BE` | Book editor names; JSON string list. |
| `AF` | Author full names; JSON string list. |
| `TI` | Publication title. |
| `SO` | Source/publication name. |
| `SE` | Book series title. |
| `LA` | Primary language(s); JSON string list. |
| `DT` | Document type(s); JSON string list. |
| `CT` | Conference title(s); JSON string list. |
| `CY` | Conference date(s); JSON string list. |
| `CL` | Conference location(s); JSON string list. |
| `SP` | Conference sponsor(s); JSON string list. |
| `HO` | Conference host(s); JSON string list. |
| `DE` | Author keywords; JSON string list. |
| `ID` | Keywords Plus terms; JSON string list. |
| `C1` | Formatted author address strings; JSON string list. |
| `RP` | Formatted reprint address strings; JSON string list. |
| `EM` | Author email addresses; JSON string list. |
| `FU` | Formatted funding acknowledgements, usually `agency:grant_ids`; JSON string list. |
| `FP` | Funding agency names extracted from `FUR`; JSON string list. |
| `TS` | Citation topic subject labels from dynamic citation-related metadata; JSON string list. |
| `FX` | Funding acknowledgement text paragraphs; JSON string list. |
| `CI` | Referenced WoS UIDs extracted from cited references; JSON string list. |
| `PU` | Publisher name. |
| `PI` | Publisher city. |
| `PA` | Publisher address. |
| `SN` | ISSN. |
| `EI` | EISSN. |
| `BN` | ISBN. |
| `J9` | WoS 29-character source abbreviation. |
| `JI` | ISO source abbreviation. |
| `PD` | Publication month. |
| `PY` | Publication year. |
| `VL` | Volume. |
| `IS` | Issue. |
| `PN` | Part number. |
| `SU` | Supplement. |
| `SI` | Special issue marker/name. |
| `BP` | Beginning page. |
| `EP` | Ending page. |
| `AR` | Article number. |
| `DI` | DOI. |
| `EA` | Early access month. |
| `EB` | Early access year. |
| `PG` | Page count. |
| `WC` | WoS subject categories; JSON string list. |
| `WE` | WoS edition(s); JSON string list. |
| `SC` | Research areas; JSON string list. |
| `PM` | PubMed identifier. |
| `OA` | Open access labels from WoS metadata; JSON string list. |
| `UT` | Web of Science unique identifier. Primary join key for record-level tables. |

### `WoS_2025_extra.parquet`

One row per WoS record. This table contains richer three-character fields and
raw structured metadata. In this WOSRaw split, `FUR` lives here, while the
derived funding fields used by matching (`FP`, `FU`, and `FX`) live in
`WoS_2025_main.parquet`.

| Column | Description |
| --- | --- |
| `UT` | Web of Science unique identifier. Primary join key. |
| `PIR` | Raw publication info object from `static_data.summary.pub_info`; JSON string dictionary. |
| `ADR` | Raw author name records from `static_data.summary.names.name`; JSON string list. |
| `AUR` | Enriched author records with sequence number, role, addresses, countries, and affiliations; JSON string list. |
| `SOR` | Raw source metadata from summary source fields; JSON string dictionary/list depending on record. |
| `C1R` | Raw address and author-address mapping data; JSON string list/dictionary. |
| `RPR` | Raw reprint-address data; JSON string list/dictionary. |
| `FUR` | Raw funding records from WoS funding acknowledgement metadata; JSON string list. |
| `DYD` | Raw `dynamic_data` block from the XML-derived record; JSON string dictionary. |
| `CRD` | Citation-related dynamic metadata. In this WOSRaw checkout this is populated with the dynamic data block. |
| `SDG` | Sustainable Development Goal metadata from citation-related dynamic data; JSON string dictionary/list. |
| `CRR` | Raw cited reference records; JSON string list. |
| `IDR` | Identifier dictionary from WoS dynamic identifiers, e.g. DOI, ISSN, EISSN, PMID; JSON string dictionary. |
| `WCR` | Raw WoS category metadata; JSON string dictionary. |

### `WoS_2025_abstract.parquet`

Subset table with one row per WoS record that has an abstract.

| Column | Description |
| --- | --- |
| `UT` | Web of Science unique identifier. |
| `AB` | Abstract text. Stored as a string. If WoSRaw extracted multiple paragraphs, the value may be a JSON string list. |

### `WoS_2025_references.parquet`

Edge list of cited WoS references. There can be many rows per citing paper.

| Column | Description |
| --- | --- |
| `Citing` | Web of Science UID of the citing record. |
| `Cited` | Web of Science UID of a cited record, when the cited reference has a usable WoS UID. |

Only references whose UID did not contain a period are retained by WOSRaw's
`getReferencesUIDs` helper; other cited references are skipped when `Cited` is
empty.

### `WoS_2025_authors.parquet`

Author-level table. There can be many rows per WoS record, one per enriched
author record in `AUR`.

| Column | Description |
| --- | --- |
| `UT` | Web of Science unique identifier of the publication. |
| `author_seq_no` | Author sequence number from WoS metadata. |
| `author_role` | Author role, e.g. `author` or other contributor role when present. |
| `author_reprint` | Reprint-author marker from WoS metadata. |
| `author_display_name` | Display name for the author. |
| `author_full_name` | Full name for the author. |
| `author_wos_standard` | WoS-standardized author name. |
| `author_email` | Author email address if present. |
| `author_r_id` | ResearcherID if present. |
| `author_orcid_id` | ORCID identifier if present. |
| `author_addr_no` | Address number(s) linked to this author; JSON string list. |
| `author_countries` | Country names/codes extracted from linked author addresses; JSON string list. |
| `author_affiliations` | Full affiliation/address strings linked to this author; JSON string list. |
| `author_addresses` | Full structured address objects linked to this author; JSON string list. |

### `WOS_TSV_FUNDING_2025.feather`

Funding extract used by `scripts/match_stage1.py`. This file is produced after
`WoS_2025_main.parquet` finishes, by:

```bash
python preprocessing/export_wos_funding_feather.py \
  --wos-main data/WoS_2025_main.parquet \
  --output data/WOS_TSV_FUNDING_2025.feather
```

Expected columns:

| Column | Description |
| --- | --- |
| `UID` | Web of Science UID, copied from `WoS_2025_main.parquet` column `UT`. |
| `agency_name` | Semicolon-delimited funding agency names. Preferentially derived from `FP`; falls back to agency names parsed from `FU`. |
| `funding` | Semicolon-delimited formatted funding entries from `FU`. |
| `funding_text` | Funding acknowledgement text from `FX`. |
| `funding_data` | Reserved for raw funding records. The current export leaves this empty because `FUR` is in the separate `WoS_2025_extra.parquet` table and is not needed by `scripts/match_stage1.py`. |

Rows with empty `agency_name` are dropped.

### `WoS_2025_errors.log`

Extraction error log written by WOSRaw while producing the processed tables.
Each line, if present, contains a WoS UID and the exception encountered while
extracting that record. A zero-byte file means no per-record extraction errors
had been logged at the time checked.

### `worker_logs/`

Per-worker raw XML parsing logs from the WOSRaw raw archive stage. These are
useful for checking which zip/XML chunks were processed and whether any worker
hit a parsing issue.

### `openalex_institutions/`, `manual_annotations.json`, and ROR CSV

These are reference inputs distributed with or expected by the
`linking-funding-landscapes` workflow:

| Path | Description |
| --- | --- |
| `openalex_institutions/funders_core+basic+ids+extra+counts.tsv` | OpenAlex funder export used to build the normalized candidate index. |
| `openalex_institutions/funders_core+basic+ids+extra+counts.json` | Schema sidecar for the funder TSV. |
| `openalex_institutions/institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv` | OpenAlex institution export used to build the normalized candidate index. |
| `openalex_institutions/institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.json` | Schema sidecar for the institution TSV. |
| `v1.72-2025-10-06-ror-data_schema_v2.csv` | ROR snapshot used for identifier and country enrichment. |
| `manual_annotations.json` | Curated mapping from raw funding strings to OpenAlex funder IDs. |

The normalized OpenAlex/ROR outputs are written outside this directory under
`../artifacts/`.

## Downstream pipeline connections

The `linking-funding-landscapes` scripts use these files as follows:

| Consumer | Inputs from this directory |
| --- | --- |
| `preprocessing/export_wos_funding_feather.py` | `WoS_2025_main.parquet` |
| `scripts/match_stage1.py` | `WOS_TSV_FUNDING_2025.feather`, `manual_annotations.json` |
| `scripts_wos_funding/step1_attach_matches_to_wos.py` | `WoS_2025_main.parquet`, `WoS_2025_authors.parquet` |
| `scripts_wos_funding/step2_build_wos_validation_samples.py` | `WoS_2025_main.parquet` |

Generated matching outputs are expected in `../artifacts/` and `../Results/`,
not in this `data/` directory.

## Reading examples

Because the files are very large, use lazy scans and select/filter before
collecting:

```python
import polars as pl

main = (
    pl.scan_parquet("data/WoS_2025_main.parquet")
    .select("UT", "PY", "TI", "DI", "SO")
    .filter(pl.col("PY").cast(pl.Int16, strict=False) >= 2010)
)

# Keep `main` lazy; collect only a bounded or aggregated result.
sample = main.head(1000).collect()
```

List/dictionary fields are serialized as JSON strings. Project the needed
columns first and parse only the bounded result:

```python
import json
import polars as pl

extra = (
    pl.scan_parquet("data/WoS_2025_extra.parquet")
    .select("UT", "FUR")
    .filter(pl.col("FUR") != "")
    .head(1000)
    .collect()
)
funding_records = [json.loads(value) if value else [] for value in extra["FUR"]]
```

For row-group callbacks or Python-heavy transformations, use
`parcake.PieceReader`, following the downstream scripts in
`../scripts_wos_funding/`.
