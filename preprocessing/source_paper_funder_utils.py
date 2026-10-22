from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa


DETAIL_SCHEMA = {
    "paper_id": pa.string(),
    "title": pa.string(),
    "publication_year": pa.int32(),
    "country_codes_priority": pa.string(),
    "doi": pa.string(),
    "funder_name": pa.string(),
    "funder_id": pa.string(),
    "has_funding_info": pa.bool_(),
}

COUNTS_SCHEMA = {
    "publication_year": pa.int32(),
    "total_papers": pa.int64(),
    "papers_with_funding_info": pa.int64(),
    "papers_without_funding_info": pa.int64(),
}


def repo_root_from_file(file_path: str | Path) -> Path:
    return Path(file_path).resolve().parents[1]


def resolve_paths_file(repo_root: Path, explicit_path: Path | None = None) -> Path:
    candidates = []
    if explicit_path is not None:
        candidates.append(explicit_path)
    candidates.extend([repo_root / "PATHS.md", repo_root / "scisci_skills" / "PATHS.md"])

    for candidate in candidates:
        if candidate.exists():
            return candidate

    joined = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Could not find PATHS.md. Checked: {joined}")


def path_from_registry(key: str, paths_file: Path) -> Path:
    for line in paths_file.read_text().splitlines():
        clean = line.strip()
        if not clean or clean.startswith("#") or "=" not in clean:
            continue
        found_key, value = clean.split("=", 1)
        if found_key.strip() == key:
            return Path(value.strip()).expanduser()
    raise KeyError(f"Path key not found in {paths_file}: {key}")


def ensure_local_library(repo_root: Path, relative_path: str) -> None:
    library_path = repo_root / relative_path
    if library_path.exists() and str(library_path) not in sys.path:
        sys.path.insert(0, str(library_path))


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def normalize_year(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None


def coerce_sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, pd.Series):
        return value.tolist()
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def normalize_country_code(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    code = text.upper()
    if len(code) != 2 or not code.isalpha():
        return None
    return code


def unique_in_order(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = normalize_text(value)
        if text is None or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def priority_author_indices(authors: list[Any], corresponding_key: str) -> list[int]:
    if not authors:
        return []

    ordered: list[int] = []
    seen: set[int] = set()

    for idx, author in enumerate(authors):
        if isinstance(author, Mapping) and bool(author.get(corresponding_key)):
            ordered.append(idx)
            seen.add(idx)

    for idx in [0, len(authors) - 1]:
        if 0 <= idx < len(authors) and idx not in seen:
            ordered.append(idx)
            seen.add(idx)

    for idx in range(1, max(len(authors) - 1, 1)):
        if idx not in seen:
            ordered.append(idx)
            seen.add(idx)

    return ordered


def countries_from_openalex_authorship(author: Any) -> list[str]:
    if not isinstance(author, Mapping):
        return []

    countries: list[str] = []
    for country in coerce_sequence(author.get("countries")):
        code = normalize_country_code(country)
        if code:
            countries.append(code)

    for institution in coerce_sequence(author.get("institutions")):
        if not isinstance(institution, Mapping):
            continue
        code = normalize_country_code(institution.get("country_code"))
        if code:
            countries.append(code)

    return countries


def countries_from_dimensions_author(author: Any) -> list[str]:
    if not isinstance(author, Mapping):
        return []

    countries: list[str] = []
    for affiliation in coerce_sequence(author.get("affiliations_address")):
        if not isinstance(affiliation, Mapping):
            continue
        code = normalize_country_code(affiliation.get("country_code"))
        if code:
            countries.append(code)
    return countries


def country_priority_from_authors(
    authors: Any,
    *,
    corresponding_key: str,
    country_extractor,
) -> str | None:
    author_list = coerce_sequence(authors)
    country_codes: list[str] = []
    for idx in priority_author_indices(author_list, corresponding_key):
        country_codes.extend(country_extractor(author_list[idx]))
    ordered = unique_in_order(country_codes)
    return ",".join(ordered) if ordered else None


def title_from_dimensions(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return normalize_text(value.get("preferred")) or normalize_text(value.get("original"))
    return normalize_text(value)


def rows_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(rows, columns=list(DETAIL_SCHEMA))
    if df.empty:
        return df
    df["publication_year"] = pd.array(df["publication_year"], dtype="Int32")
    df["has_funding_info"] = pd.array(df["has_funding_info"], dtype="boolean")
    return df


def counts_dataframe(counts: Mapping[int, tuple[int, int]]) -> pd.DataFrame:
    rows = []
    for year in sorted(counts):
        total, with_funding = counts[year]
        rows.append(
            {
                "publication_year": int(year),
                "total_papers": int(total),
                "papers_with_funding_info": int(with_funding),
                "papers_without_funding_info": int(total - with_funding),
            }
        )
    df = pd.DataFrame.from_records(rows, columns=list(COUNTS_SCHEMA))
    if df.empty:
        return df
    df["publication_year"] = pd.array(df["publication_year"], dtype="Int32")
    df["total_papers"] = pd.array(df["total_papers"], dtype="Int64")
    df["papers_with_funding_info"] = pd.array(df["papers_with_funding_info"], dtype="Int64")
    df["papers_without_funding_info"] = pd.array(df["papers_without_funding_info"], dtype="Int64")
    return df
