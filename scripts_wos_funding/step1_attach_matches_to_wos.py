from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

try:
    import pycountry
except ModuleNotFoundError:  # pragma: no cover - optional dependency in existing envs
    pycountry = None  # type: ignore[assignment]

try:
    from parcake_utils import (
        PieceReader,
        PieceSaver,
        coerce_list,
        default_worker_count,
        normalize_agency_name,
    )
except ModuleNotFoundError:
    from scripts_wos_funding.parcake_utils import (
        PieceReader,
        PieceSaver,
        coerce_list,
        default_worker_count,
        normalize_agency_name,
    )

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_INSTITUTIONS_NORMALIZED = Path("artifacts/institutions_normalized.parquet")
DEFAULT_WOS_FUNDERS = Path("artifacts/wos_funders.parquet")
DEFAULT_WOS_MAIN = Path("data/WoS_2025_main.parquet")
DEFAULT_WOS_EXTRA = Path("data/WoS_2025_extra.parquet")
DEFAULT_WOS_AUTHORS = Path("data/WoS_2025_authors.parquet")
DEFAULT_RESULTS_DIR = Path("Results")
DEFAULT_MATCHES_BASENAME = "2025_affiliation_match_20251023"
DEFAULT_FUNDER_SCOPE_MAP = Path("scripts_wos_funding/funder_multicountry_scope_map.csv")
DEFAULT_ROR_CSV = Path("data/v1.72-2025-10-06-ror-data_schema_v2.csv")
AUTHOR_TARGET_UTS: set[str] = set()
EXTRA_TARGET_UTS: set[str] = set()


MANUAL_COUNTRY_MAPPING = {
    "Russia": "RU",
    "USA": "US",
    "U S A": "US",
    "United States": "US",
    "United States of America": "US",
    "United Kingdom": "GB",
    "Turkey": "TR",
    "Aland Islands": "AX",
    "Bonaire Sint Eustatius and Saba": "BQ",
    "Brunei": "BN",
    "Congo Republic": "CG",
    "Democratic Republic of the Congo": "CD",
    "DR Congo": "CD",
    "Curacao": "CW",
    "Falkland Islands": "FK",
    "Ivory Coast": "CI",
    "Kosovo": "XK",
    "Micronesia": "FM",
    "Palestine": "PS",
    "Palestinian Territory": "PS",
    "Reunion": "RE",
    "Sint Maarten": "SX",
    "St Kitts and Nevis": "KN",
    "St Vincent and Grenadines": "VC",
    "The Gambia": "GM",
    "The Netherlands": "NL",
    "US Virgin Islands": "VI",
    "Vatican": "VA",
    "Vatican City": "VA",
    "Bonaire": "BQ",
    "Bosnia & Herceg": "BA",
    "Cape Verde": "CV",
    "Cent Afr Republ": "CF",
    "Cote Ivoire": "CI",
    "DEM REP CONGO": "CD",
    "Dominican Rep": "DO",
    "England": "GB",
    "Equat Guinea": "GQ",
    "Falkland Island": "FK",
    "Guinea Bissau": "GW",
    "Macedonia": "MK",
    "Marshall Island": "MH",
    "Neth Antilles": "AN",
    "North Ireland": "GB",
    "Papua N Guinea": "PG",
    "China": "CN",
    "Peoples R China": "CN",
    "Rep Congo": "CG",
    "Sao Tome & Prin": "ST",
    "Scotland": "GB",
    "St Kitts & Nevi": "KN",
    "St Lucia": "LC",
    "St Vincent": "VC",
    "Swaziland": "SZ",
    "Trinidad Tobago": "TT",
    "U Arab Emirates": "AE",
    "Wales": "GB",
    "Yugoslavia": "YU",
    "*South Korea": "KR",
    "?England": "GB",
    "?India": "IN",
    "?Liechtenstein": "LI",
    "Africa": "AF",
    "Antigua & Barbu": "AG",
    "Ascension Isl": "AC",
    "Australia": "AU",
    "Bangladesh": "BD",
    "Belgium": "BE",
    "Brazil": "BR",
    "Brazil?": "BR",
    "Brit Ind Ocean": "IO",
    "British Virgin Isl": "VG",
    "Bulgaria": "BG",
    "Costa Rica": "CR",
    "Cote d'Ivoire": "CI",
    "Denmark": "DK",
    "France": "FR",
    "Germany": "DE",
    "Germany;": "DE",
    "Greece;": "GR",
    "Hungary;": "HU",
    "India;": "IN",
    "Indonesia;": "ID",
    "Iran,": "IR",
    "Iran, Islamic R": "IR",
    "Iran.": "IR",
    "Iran=": "IR",
    "Italy;": "IT",
    "Jan Mayen": "SJ",
    "Japan;": "JP",
    "Korea, Republic": "KR",
    "Lebanon;": "LB",
    "Malaysia;": "MY",
    "Moldova;": "MD",
    "Netherlands;": "NL",
    "Pakistan;": "PK",
    "Portugal*": "PT",
    "Portugal;": "PT",
    "Russia;": "RU",
    "Saudi Arabia.": "SA",
    "Scotland?": "GB",
    "Serbia Monteneg": "ME",
    "South Korea,": "KR",
    "South Korea.": "KR",
    "Spain;": "ES",
    "St Barthelemy": "BL",
    "St Helena": "SH",
    "St Martin": "MF",
    "Svalbard": "SJ",
    "Sweden;": "SE",
    "Switzerland;": "CH",
    "Tahiti": "PF",
    "Taiwan,": "TW",
    "Taiwan, Provinc": "TW",
    "Taiwan;": "TW",
    "Taiwan\\": "TW",
    "Tanzania,": "TZ",
    "Thailand;": "TH",
    "Tokelau Islands": "TK",
    "Tristan da Cunh": "TC",
    "Tukiye": "TR",
    "Tunisia;": "TN",
    "Turkey*": "TR",
    "Turkey;": "TR",
    "Turkiye": "TR",
    "Turks & Caicos": "TC",
    "Uganda;": "UG",
    "Ukraine)": "UA",
    "Vietnam;": "VN",
    "|Ira": "IR",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach /scripts OpenAlex matches to WOS funding agencies using WOS processed parquet inputs."
    )
    parser.add_argument("--institutions-normalized", type=Path, default=DEFAULT_INSTITUTIONS_NORMALIZED)
    parser.add_argument(
        "--wos-funders",
        type=Path,
        default=DEFAULT_WOS_FUNDERS,
        help="Stage-3 output parquet from scripts/match_stage3.py.",
    )
    parser.add_argument("--final-map", dest="wos_funders", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--wos-main", type=Path, default=DEFAULT_WOS_MAIN)
    parser.add_argument("--wos-extra", type=Path, default=DEFAULT_WOS_EXTRA)
    parser.add_argument("--wos-authors", type=Path, default=DEFAULT_WOS_AUTHORS)
    parser.add_argument("--funder-scope-map", type=Path, default=DEFAULT_FUNDER_SCOPE_MAP)
    parser.add_argument("--ror-csv", type=Path, default=DEFAULT_ROR_CSV)
    parser.add_argument("--scope-propagation-depth", type=int, default=2)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--basename", default=DEFAULT_MATCHES_BASENAME)
    parser.add_argument("--workers", type=int, default=-1)
    parser.add_argument(
        "--write-feather",
        action="store_true",
        help="Also write the large matched-paper Feather file. Disabled by default to avoid a high-memory final write.",
    )
    parser.add_argument(
        "--skip-author-country",
        action="store_true",
        help="Skip author-country enrichment and candidate disambiguation.",
    )
    return parser.parse_args()


def iter_progress(iterable: Iterable[Any], *, total: Optional[int] = None, desc: str = "", unit: str = "it") -> Iterable[Any]:
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc, unit=unit)


def normalize_country_name(country_name: Any) -> Optional[str]:
    if pd.isna(country_name):
        return None
    cleaned = re.sub(r"[^a-zA-Z0-9À-ž \-&]", "", str(country_name).strip())
    if not cleaned:
        return None
    if cleaned in MANUAL_COUNTRY_MAPPING:
        return MANUAL_COUNTRY_MAPPING[cleaned]
    if pycountry is None:
        return None
    try:
        return pycountry.countries.lookup(cleaned).alpha_2
    except LookupError:
        return None


def dedupe_preserve_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def append_priority_country(
    codes: List[str],
    sources: List[str],
    seen: set[str],
    country_code: str,
    source: str,
) -> None:
    if not country_code or country_code in seen:
        return
    seen.add(country_code)
    codes.append(country_code)
    sources.append(source)


def split_codes(value: str) -> List[str]:
    return [code for code in str(value or "").split(";") if code]


def parse_seq_no(value: Any) -> float:
    if pd.isna(value):
        return float("inf")
    try:
        return float(value)
    except (TypeError, ValueError):
        digits = re.findall(r"\d+", str(value))
        return float(digits[0]) if digits else float("inf")


def load_institutions_metadata(path: Path) -> pd.DataFrame:
    institutions = pd.read_parquet(path)
    metadata = institutions[["id", "country_code", "display_name", "ids:ror", "source"]].copy()
    metadata["gid"] = metadata["id"].map(lambda value: f"OA_{value}" if pd.notna(value) else None)
    metadata["country_code"] = metadata["country_code"].fillna("")
    return metadata


def parse_ror_relationships(value: Any) -> Dict[str, List[str]]:
    relationships: Dict[str, List[str]] = defaultdict(list)
    if value is None or pd.isna(value):
        return relationships
    text = str(value).strip()
    if not text:
        return relationships
    for group in text.split(";"):
        if ":" not in group:
            continue
        relationship_type, ids_text = group.split(":", 1)
        relationship_type = relationship_type.strip().lower()
        if relationship_type not in {"parent", "child"}:
            continue
        for ror_id in re.findall(r"https://ror\.org/[0-9a-z]+", ids_text):
            relationships[relationship_type].append(ror_id)
    return relationships


def load_ror_parent_child_network(path: Path) -> Dict[str, Dict[str, List[str]]]:
    if path is None or not path.exists():
        return {}
    ror = pd.read_csv(path, usecols=["id", "relationships"])
    network: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: {"parent": [], "child": []})
    for ror_id, relationships in ror[["id", "relationships"]].itertuples(index=False):
        source = str(ror_id)
        parsed = parse_ror_relationships(relationships)
        for relationship_type in ["parent", "child"]:
            for target in parsed.get(relationship_type, []):
                if target not in network[source][relationship_type]:
                    network[source][relationship_type].append(target)
                inverse = "child" if relationship_type == "parent" else "parent"
                if source not in network[target][inverse]:
                    network[target][inverse].append(source)
    return dict(network)


def related_ror_ids(
    root_ror_id: str,
    network: Dict[str, Dict[str, List[str]]],
    *,
    max_depth: int,
) -> List[tuple[str, str, int]]:
    if not root_ror_id or max_depth <= 0 or root_ror_id not in network:
        return []
    related: List[tuple[str, str, int]] = []
    seen = {root_ror_id}
    for direction in ["parent", "child"]:
        queue: List[tuple[str, int]] = [(root_ror_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for target in network.get(current, {}).get(direction, []):
                if target in seen:
                    continue
                seen.add(target)
                next_depth = depth + 1
                related.append((target, direction, next_depth))
                queue.append((target, next_depth))
    return related


def load_funder_scope_map(
    path: Path,
    institutions_metadata: pd.DataFrame,
    ror_csv: Path,
    propagation_depth: int,
) -> pd.DataFrame:
    columns = [
        "openalex_id",
        "scope_type",
        "scope_label",
        "funder_scope_country_codes",
        "funder_scope_provenance",
        "funder_scope_source_openalex_id",
    ]
    if path is None or not path.exists():
        return pd.DataFrame(columns=columns)

    scope_map = pd.read_csv(path, dtype={"country_codes": "string"}).copy()
    required = {"openalex_id", "scope_type", "scope_label", "country_codes"}
    missing = required - set(scope_map.columns)
    if missing:
        raise ValueError(f"Funder scope map is missing required columns: {sorted(missing)}")

    scope_map["openalex_id"] = pd.to_numeric(scope_map["openalex_id"], errors="coerce").astype("Int64")
    scope_map["scope_type"] = scope_map["scope_type"].fillna("").str.strip().str.lower()
    scope_map["scope_label"] = scope_map["scope_label"].fillna("").str.strip()
    scope_map["funder_scope_country_codes"] = (
        scope_map["country_codes"]
        .fillna("")
        .map(lambda value: ";".join(dedupe_preserve_order(code.strip().upper() for code in str(value).split(";") if code.strip())))
    )
    scope_map["funder_scope_provenance"] = "manual"
    scope_map["funder_scope_source_openalex_id"] = scope_map["openalex_id"].astype("string")
    invalid = scope_map.loc[~scope_map["scope_type"].isin(["global", "country_set"])]
    if not invalid.empty:
        raise ValueError(f"Unsupported scope_type values in {path}: {sorted(invalid['scope_type'].unique())}")
    scope_map = scope_map[columns].dropna(subset=["openalex_id"]).drop_duplicates("openalex_id", keep="first")

    if propagation_depth <= 0:
        return scope_map

    id_to_ror = (
        institutions_metadata[["id", "ids:ror"]]
        .dropna(subset=["id", "ids:ror"])
        .assign(id=lambda frame: pd.to_numeric(frame["id"], errors="coerce").astype("Int64"))
        .dropna(subset=["id"])
    )
    openalex_to_ror = dict(zip(id_to_ror["id"].astype(int), id_to_ror["ids:ror"].astype(str)))
    ror_to_openalex: Dict[str, List[int]] = defaultdict(list)
    for openalex_id, ror_id in openalex_to_ror.items():
        ror_to_openalex[ror_id].append(openalex_id)

    network = load_ror_parent_child_network(ror_csv)
    if not network:
        return scope_map

    propagated_rows: List[Dict[str, Any]] = []
    for row in scope_map.to_dict("records"):
        source_openalex_id = int(row["openalex_id"])
        root_ror_id = openalex_to_ror.get(source_openalex_id, "")
        for target_ror_id, direction, depth in related_ror_ids(root_ror_id, network, max_depth=propagation_depth):
            for target_openalex_id in ror_to_openalex.get(target_ror_id, []):
                propagated = dict(row)
                propagated["openalex_id"] = target_openalex_id
                propagated["funder_scope_provenance"] = f"ror_{direction}_depth_{depth}"
                propagated["funder_scope_source_openalex_id"] = str(source_openalex_id)
                propagated_rows.append(propagated)

    if propagated_rows:
        combined = pd.concat([scope_map, pd.DataFrame(propagated_rows)], ignore_index=True)
        provenance_rank = {
            "manual": 0,
            "ror_parent_depth_1": 1,
            "ror_child_depth_1": 1,
            "ror_parent_depth_2": 2,
            "ror_child_depth_2": 2,
        }
        combined["_rank"] = combined["funder_scope_provenance"].map(lambda value: provenance_rank.get(str(value), 99))
        combined = combined.sort_values(["openalex_id", "_rank"], kind="stable").drop_duplicates("openalex_id", keep="first")
        combined = combined.drop(columns=["_rank"])
        return combined[columns].reset_index(drop=True)

    return scope_map


def extract_funding_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UT", "grant_agency", "grant_agency_normalized"])

    rows: List[Dict[str, str]] = []
    for ut, agencies, formatted_funding in frame[["UT", "FP", "FU"]].itertuples(index=False):
        agency_names = [str(value).strip() for value in coerce_list(agencies) if str(value).strip()]
        if not agency_names:
            for funding_entry in coerce_list(formatted_funding):
                agency_name = str(funding_entry).split(":", 1)[0].strip()
                if agency_name:
                    agency_names.append(agency_name)

        seen_normalized: set[str] = set()
        for agency_name in agency_names:
            normalized_name = normalize_agency_name(agency_name)
            if not normalized_name or normalized_name in seen_normalized:
                continue
            seen_normalized.add(normalized_name)
            rows.append(
                {
                    "UT": str(ut),
                    "grant_agency": agency_name,
                    "grant_agency_normalized": normalized_name,
                }
            )

    return pd.DataFrame(rows)


def load_wos_grant_agencies(main_path: Path, workers: int) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    reader = PieceReader(
        main_path,
        columns=["UT", "FP", "FU"],
    )
    iterator = reader.process(
        extract_funding_rows,
        ncpu=default_worker_count(workers),
        keep_order=False,
    )
    for frame in iter_progress(
        iterator,
        total=reader.task_count(),
        desc="Reading WOS funding agencies",
        unit="task",
    ):
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["UT", "grant_agency", "grant_agency_normalized"])

    result = pd.concat(frames, ignore_index=True)
    return result.drop_duplicates(subset=["UT", "grant_agency_normalized"]).reset_index(drop=True)


def extract_author_rows(frame: pd.DataFrame, *_piece_args: Any, target_uts: set[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UT", "author_seq_no", "author_reprint", "country_codes", "affiliations"])

    filtered = frame[frame["UT"].isin(target_uts)]
    if filtered.empty:
        return pd.DataFrame(columns=["UT", "author_seq_no", "author_reprint", "country_codes", "affiliations"])

    rows: List[Dict[str, Any]] = []
    for ut, seq_no, author_reprint, author_countries, author_affiliations in filtered[
        ["UT", "author_seq_no", "author_reprint", "author_countries", "author_affiliations"]
    ].itertuples(index=False):
        country_codes = dedupe_preserve_order(
            code for code in (normalize_country_name(value) for value in coerce_list(author_countries)) if code
        )
        affiliations = dedupe_preserve_order(
            str(value).strip() for value in coerce_list(author_affiliations) if str(value).strip()
        )
        rows.append(
            {
                "UT": str(ut),
                "author_seq_no": parse_seq_no(seq_no),
                "author_reprint": str(author_reprint or "").strip().upper(),
                "country_codes": country_codes,
                "affiliations": affiliations,
            }
        )

    return pd.DataFrame(rows)


def initialize_target_uts(name: str, target_uts: set[str]) -> None:
    """Initialize workers explicitly, including on platforms that use spawn."""
    globals()[name] = target_uts


def extract_author_rows_from_global(frame: pd.DataFrame, *_piece_args: Any) -> pd.DataFrame:
    return extract_author_rows(frame, *_piece_args, target_uts=AUTHOR_TARGET_UTS)


def build_author_metadata(authors_path: Path, target_uts: set[str], workers: int) -> Dict[str, Dict[str, str]]:
    author_rows_by_ut: Dict[str, List[tuple[float, str, List[str], List[str]]]] = defaultdict(list)
    with ProcessPoolExecutor(max_workers=default_worker_count(workers),
                             initializer=initialize_target_uts,
                             initargs=("AUTHOR_TARGET_UTS", target_uts)) as executor:
        reader = PieceReader(
            authors_path,
            columns=["UT", "author_seq_no", "author_reprint", "author_countries", "author_affiliations"],
        )
        iterator = reader.process(
            extract_author_rows_from_global,
            executor=executor,
            keep_order=False,
        )
        for frame in iter_progress(
            iterator,
            total=reader.task_count(),
            desc="Reading WOS author countries",
            unit="task",
        ):
            if not frame.empty:
                for ut, seq_no, author_reprint, country_codes, affiliations in frame[
                    ["UT", "author_seq_no", "author_reprint", "country_codes", "affiliations"]
                ].itertuples(index=False):
                    author_rows_by_ut[str(ut)].append((float(seq_no), str(author_reprint), country_codes, affiliations))

    if not author_rows_by_ut:
        return {}

    metadata: Dict[str, Dict[str, str]] = {}
    for ut, author_rows in author_rows_by_ut.items():
        flat_country_codes: List[str] = []
        ordered_country_groups: List[str] = []
        ordered_affiliations: List[str] = []
        sorted_rows = sorted(author_rows, key=lambda row: row[0])
        for _, _, country_codes, affiliations in sorted_rows:
            if country_codes:
                flat_country_codes.extend(country_codes)
                ordered_country_groups.append(";".join(country_codes))
            if affiliations:
                ordered_affiliations.append("; ".join(affiliations))

        reprint_author_codes: List[str] = []
        reprint_author_sources: List[str] = []
        seen_reprint_author: set[str] = set()
        for _, author_reprint, country_codes, _ in sorted_rows:
            if author_reprint == "Y":
                for code in country_codes:
                    append_priority_country(
                        reprint_author_codes,
                        reprint_author_sources,
                        seen_reprint_author,
                        code,
                        "reprint_author",
                    )

        author_priority_indices: List[tuple[int, str]] = []
        if sorted_rows:
            author_priority_indices.append((0, "author_first"))
        if len(sorted_rows) > 1:
            author_priority_indices.append((len(sorted_rows) - 1, "author_last"))
        for index in range(1, max(1, len(sorted_rows) - 1)):
            author_priority_indices.append((index, f"author_{index + 1}"))

        priority_codes: List[str] = []
        priority_sources: List[str] = []
        seen_priority: set[str] = set()
        for index, source in author_priority_indices:
            _, _, country_codes, _ = sorted_rows[index]
            for code in country_codes:
                append_priority_country(priority_codes, priority_sources, seen_priority, code, source)

        metadata[str(ut)] = {
            "affiliation_country_codes": ";".join(flat_country_codes),
            "author_country_codes_ordered": " || ".join(ordered_country_groups),
            "author_affiliations_ordered": " || ".join(ordered_affiliations),
            "reprint_author_country_codes": ";".join(reprint_author_codes),
            "reprint_author_country_sources": ";".join(reprint_author_sources),
            "author_priority_country_codes": ";".join(priority_codes),
            "author_priority_country_sources": ";".join(priority_sources),
        }
    return metadata


def country_codes_from_address_json(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        data = value
    else:
        if pd.isna(value):
            return []
        text = str(value).strip()
        if not text or text in {"[]", "{}", "nan", "NaN", "None", "null"}:
            return []
        try:
            data = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []

    addresses = data.get("addresses", []) if isinstance(data, dict) else []
    codes: List[str] = []
    for address in addresses:
        if not isinstance(address, dict):
            continue
        country_code = normalize_country_name(address.get("country"))
        if country_code:
            codes.append(country_code)
    return dedupe_preserve_order(codes)


def extract_extra_rows(frame: pd.DataFrame, *_piece_args: Any, target_uts: set[str]) -> pd.DataFrame:
    columns = ["UT", "reprint_address_country_codes", "c1r_country_codes"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    filtered = frame[frame["UT"].isin(target_uts)]
    if filtered.empty:
        return pd.DataFrame(columns=columns)

    rows: List[Dict[str, str]] = []
    for ut, c1r, rpr in filtered[["UT", "C1R", "RPR"]].itertuples(index=False):
        reprint_codes = country_codes_from_address_json(rpr)
        c1r_codes = country_codes_from_address_json(c1r)
        if not reprint_codes and not c1r_codes:
            continue
        rows.append(
            {
                "UT": str(ut),
                "reprint_address_country_codes": ";".join(reprint_codes),
                "c1r_country_codes": ";".join(c1r_codes),
            }
        )
    return pd.DataFrame(rows)


def extract_extra_rows_from_global(frame: pd.DataFrame, *_piece_args: Any) -> pd.DataFrame:
    return extract_extra_rows(frame, *_piece_args, target_uts=EXTRA_TARGET_UTS)


def build_extra_metadata(extra_path: Path, target_uts: set[str], workers: int) -> Dict[str, Dict[str, str]]:
    metadata: Dict[str, Dict[str, str]] = {}
    with ProcessPoolExecutor(max_workers=default_worker_count(workers),
                             initializer=initialize_target_uts,
                             initargs=("EXTRA_TARGET_UTS", target_uts)) as executor:
        reader = PieceReader(extra_path, columns=["UT", "C1R", "RPR"])
        iterator = reader.process(
            extract_extra_rows_from_global,
            executor=executor,
            keep_order=False,
        )
        for frame in iter_progress(
            iterator,
            total=reader.task_count(),
            desc="Reading WOS reprint/C1R countries",
            unit="task",
        ):
            if frame.empty:
                continue
            for ut, reprint_codes, c1r_codes in frame[
                ["UT", "reprint_address_country_codes", "c1r_country_codes"]
            ].itertuples(index=False):
                metadata[str(ut)] = {
                    "reprint_address_country_codes": str(reprint_codes or ""),
                    "c1r_country_codes": str(c1r_codes or ""),
                }
    return metadata


def combine_country_metadata(
    author_metadata: Dict[str, Dict[str, str]],
    extra_metadata: Dict[str, Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    metadata: Dict[str, Dict[str, str]] = {}
    for ut in set(author_metadata) | set(extra_metadata):
        row: Dict[str, str] = {}
        row.update(author_metadata.get(ut, {}))
        row.update(extra_metadata.get(ut, {}))

        priority_codes: List[str] = []
        priority_sources: List[str] = []
        seen_priority: set[str] = set()
        for code, source in zip(
            split_codes(row.get("reprint_author_country_codes", "")),
            split_codes(row.get("reprint_author_country_sources", "")),
        ):
            append_priority_country(priority_codes, priority_sources, seen_priority, code, source)
        for code in split_codes(row.get("reprint_address_country_codes", "")):
            append_priority_country(priority_codes, priority_sources, seen_priority, code, "reprint_address")
        for code, source in zip(
            split_codes(row.get("author_priority_country_codes", "")),
            split_codes(row.get("author_priority_country_sources", "")),
        ):
            append_priority_country(priority_codes, priority_sources, seen_priority, code, source)
        for index, code in enumerate(split_codes(row.get("c1r_country_codes", "")), start=1):
            append_priority_country(priority_codes, priority_sources, seen_priority, code, f"c1r_address_{index}")

        row["country_priority_codes"] = ";".join(priority_codes)
        row["country_priority_sources"] = ";".join(priority_sources)
        row["preferred_paper_country_code"] = priority_codes[0] if priority_codes else ""
        row["preferred_paper_country_source"] = priority_sources[0] if priority_sources else ""
        metadata[ut] = row
    return metadata


def prepare_candidate_map(
    wos_funders_path: Path,
    institutions_metadata: pd.DataFrame,
    funder_scope_map: pd.DataFrame,
) -> pd.DataFrame:
    wos_funders = pd.read_parquet(wos_funders_path).copy()
    wos_funders["grant_agency_normalized"] = wos_funders["grant_agency"].map(normalize_agency_name)
    wos_funders["gid"] = wos_funders["id"].map(lambda value: f"OA_{value}" if pd.notna(value) else None)
    wos_funders["counts"] = wos_funders["counts"].fillna(0)

    id_total_counts = wos_funders.groupby("id", dropna=True)["counts"].sum().rename("matched_count_total")
    wos_funders = wos_funders.merge(id_total_counts, on="id", how="left")
    wos_funders = wos_funders.merge(
        institutions_metadata[["id", "country_code", "display_name", "ids:ror", "source"]],
        on="id",
        how="left",
        suffixes=("", "_inst"),
    )

    for column in ["display_name", "ids:ror", "source"]:
        inst_column = f"{column}_inst"
        if inst_column in wos_funders.columns:
            wos_funders[column] = wos_funders[column].fillna(wos_funders[inst_column])
            wos_funders.drop(columns=[inst_column], inplace=True)

    wos_funders.rename(columns={"country_code": "funder_country_code"}, inplace=True)
    if not funder_scope_map.empty:
        wos_funders = wos_funders.merge(
            funder_scope_map,
            left_on="id",
            right_on="openalex_id",
            how="left",
        )
        wos_funders.drop(columns=["openalex_id"], inplace=True)
    for column in [
        "scope_type",
        "scope_label",
        "funder_scope_country_codes",
        "funder_scope_provenance",
        "funder_scope_source_openalex_id",
    ]:
        if column not in wos_funders.columns:
            wos_funders[column] = ""
        wos_funders[column] = wos_funders[column].fillna("")

    wos_funders = wos_funders.sort_values(
        ["grant_agency_normalized", "matched_count_total", "counts"],
        ascending=[True, False, False],
        kind="stable",
    )
    return wos_funders.drop_duplicates(subset=["grant_agency_normalized", "gid"], keep="first").reset_index(drop=True)


def build_candidate_lookup(candidates: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    lookup: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in candidates.to_dict("records"):
        normalized_name = record.get("grant_agency_normalized")
        if normalized_name:
            record["_funder_scope_country_set"] = set(split_codes(record.get("funder_scope_country_codes", "")))
            lookup[str(normalized_name)].append(record)
    return lookup


def candidate_supports_country(candidate: Dict[str, Any], country_code: str) -> tuple[bool, str]:
    if not country_code:
        return False, ""
    if str(candidate.get("funder_country_code") or "") == country_code:
        return True, "exact_country"

    scope_type = str(candidate.get("scope_type") or "")
    if scope_type == "country_set" and country_code in candidate.get("_funder_scope_country_set", set()):
        return True, "scope_country_set"
    if scope_type == "global":
        return True, "scope_global"
    return False, ""


def choose_best_candidate(
    candidates: List[Dict[str, Any]],
    priority_country_codes: List[str],
    priority_country_sources: List[str],
) -> tuple[Optional[Dict[str, Any]], str, str, str, str]:
    if not candidates:
        return None, "", "", "", "no_candidate"
    if len(candidates) == 1:
        return candidates[0], "", "", "", "single_candidate"
    for index, country_code in enumerate(priority_country_codes):
        for match_type in ["exact_country", "scope_country_set", "scope_global"]:
            for candidate in candidates:
                supported, candidate_match_type = candidate_supports_country(candidate, country_code)
                if not supported or candidate_match_type != match_type:
                    continue
                source = priority_country_sources[index] if index < len(priority_country_sources) else ""
                return candidate, country_code, source, match_type, f"multi_candidate_{match_type}"
    return candidates[0], "", "", "", "multi_candidate_default"


def attach_candidate_columns(matches: pd.DataFrame, selected_candidates: List[Optional[Dict[str, Any]]]) -> pd.DataFrame:
    attached = matches.copy()
    field_mapping = {
        "gid": "gid",
        "id": "id",
        "ids:ror": "ids:ror",
        "source": "source",
        "match_type": "match_type",
        "display_name": "display_name",
        "counts": "matched_counts",
        "matched_count_total": "matched_count_total",
        "funder_country_code": "funder_country_code",
        "scope_type": "funder_scope_type",
        "scope_label": "funder_scope_label",
        "funder_scope_country_codes": "funder_scope_country_codes",
        "funder_scope_provenance": "funder_scope_provenance",
        "funder_scope_source_openalex_id": "funder_scope_source_openalex_id",
    }
    for source_field, target_field in field_mapping.items():
        attached[target_field] = [candidate.get(source_field) if candidate else None for candidate in selected_candidates]
    return attached


MATCHED_SCHEMA = {
    "UT": "str",
    "grant_agency": "str",
    "grant_agency_normalized": "str",
    "gid": "str",
    "id": "float64",
    "ids:ror": "str",
    "source": "str",
    "match_type": "str",
    "display_name": "str",
    "matched_counts": "float64",
    "matched_count_total": "float64",
    "funder_country_code": "str",
    "candidate_count": "int64",
    "candidate_selection_reason": "str",
    "candidate_match_type": "str",
    "funder_scope_type": "str",
    "funder_scope_label": "str",
    "funder_scope_country_codes": "str",
    "funder_scope_provenance": "str",
    "funder_scope_source_openalex_id": "str",
    "tie_break_country_code": "str",
    "tie_break_country_source": "str",
    "affiliation_country_codes": "str",
    "author_country_codes_ordered": "str",
    "author_affiliations_ordered": "str",
    "reprint_author_country_codes": "str",
    "reprint_address_country_codes": "str",
    "c1r_country_codes": "str",
    "country_priority_codes": "str",
    "country_priority_sources": "str",
    "preferred_paper_country_code": "str",
    "preferred_paper_country_source": "str",
}


def write_matched_outputs(
    grant_agencies: pd.DataFrame,
    candidate_lookup: Dict[str, List[Dict[str, Any]]],
    author_metadata: Dict[str, Dict[str, str]],
    parquet_path: Path,
    feather_path: Path,
    *,
    write_feather: bool,
    chunk_size: int = 1_000_000,
) -> None:
    if parquet_path.exists():
        parquet_path.unlink()
    if feather_path.exists() and not write_feather:
        feather_path.unlink()

    feather_chunks: List[pd.DataFrame] = []
    total = len(grant_agencies)
    iterator = range(0, total, chunk_size)
    with PieceSaver(MATCHED_SCHEMA, parquet_path, max_piece_size=chunk_size) as saver:
        for start in iter_progress(
            iterator,
            total=(total + chunk_size - 1) // chunk_size,
            desc="Writing matched rows",
            unit="chunk",
        ):
            stop = min(start + chunk_size, total)
            chunk = grant_agencies.iloc[start:stop].copy()
            selected_candidates: List[Optional[Dict[str, Any]]] = []
            candidate_counts: List[int] = []
            candidate_selection_reasons: List[str] = []
            candidate_match_types: List[str] = []
            tie_break_country_codes: List[str] = []
            tie_break_country_sources: List[str] = []
            for row in chunk.itertuples(index=False):
                metadata = author_metadata.get(str(row.UT), {})
                priority_country_codes = split_codes(metadata.get("country_priority_codes", ""))
                priority_country_sources = split_codes(metadata.get("country_priority_sources", ""))
                candidates = candidate_lookup.get(row.grant_agency_normalized, [])
                selected_candidate, tie_country_code, tie_country_source, match_type, selection_reason = choose_best_candidate(
                    candidates,
                    priority_country_codes,
                    priority_country_sources,
                )
                selected_candidates.append(selected_candidate)
                candidate_counts.append(len(candidates))
                candidate_selection_reasons.append(selection_reason)
                candidate_match_types.append(match_type)
                tie_break_country_codes.append(tie_country_code)
                tie_break_country_sources.append(tie_country_source)

            matched = attach_candidate_columns(chunk, selected_candidates)
            matched["candidate_count"] = candidate_counts
            matched["candidate_selection_reason"] = candidate_selection_reasons
            matched["candidate_match_type"] = candidate_match_types
            matched["tie_break_country_code"] = tie_break_country_codes
            matched["tie_break_country_source"] = tie_break_country_sources
            for column in [
                "affiliation_country_codes",
                "author_country_codes_ordered",
                "author_affiliations_ordered",
                "reprint_author_country_codes",
                "reprint_address_country_codes",
                "c1r_country_codes",
                "country_priority_codes",
                "country_priority_sources",
                "preferred_paper_country_code",
                "preferred_paper_country_source",
            ]:
                matched[column] = matched["UT"].map(lambda ut, col=column: author_metadata.get(str(ut), {}).get(col, ""))
            matched = matched[list(MATCHED_SCHEMA)]
            saver.write_dataframe(matched)
            if write_feather:
                feather_chunks.append(matched)

    if write_feather and feather_chunks:
        pd.concat(feather_chunks, ignore_index=True).to_feather(feather_path)


def main() -> None:
    args = parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    institutions_metadata = load_institutions_metadata(args.institutions_normalized)
    funder_scope_map = load_funder_scope_map(
        args.funder_scope_map,
        institutions_metadata,
        args.ror_csv,
        args.scope_propagation_depth,
    )
    candidates = prepare_candidate_map(args.wos_funders, institutions_metadata, funder_scope_map)
    candidate_lookup = build_candidate_lookup(candidates)

    grant_agencies = load_wos_grant_agencies(args.wos_main, args.workers)
    author_metadata: Dict[str, Dict[str, str]] = {}
    if not args.skip_author_country:
        target_uts = set(grant_agencies["UT"].unique())
        linked_author_metadata = build_author_metadata(args.wos_authors, target_uts, args.workers)
        extra_metadata = build_extra_metadata(args.wos_extra, target_uts, args.workers)
        author_metadata = combine_country_metadata(linked_author_metadata, extra_metadata)

    papers_path = args.results_dir / f"grant_institutions_{args.basename}.feather"
    agencies_path = args.results_dir / f"grant_agency_matched_{args.basename}.feather"
    write_matched_outputs(
        grant_agencies,
        candidate_lookup,
        author_metadata,
        papers_path.with_suffix(".parquet"),
        papers_path,
        write_feather=args.write_feather,
    )
    candidates.to_feather(agencies_path)
    candidates.to_parquet(agencies_path.with_suffix(".parquet"), index=False)

    print(papers_path.with_suffix(".parquet"))
    print(agencies_path.with_suffix(".parquet"))


if __name__ == "__main__":
    main()
