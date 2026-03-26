from __future__ import annotations

import argparse
import re
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

try:
    import pycountry
except ModuleNotFoundError:  # pragma: no cover - optional dependency in existing envs
    pycountry = None  # type: ignore[assignment]

try:
    from parcake_utils import coerce_list, iter_piece_results, normalize_agency_name
except ModuleNotFoundError:
    from scripts_wos_funding.parcake_utils import coerce_list, iter_piece_results, normalize_agency_name

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


DEFAULT_INSTITUTIONS_NORMALIZED = Path("artifacts/institutions_normalized.parquet")
DEFAULT_WOS_FUNDERS = Path("artifacts/wos_funders.parquet")
DEFAULT_WOS_EXTRA = Path("data/WoS_2023_extra.parquet")
DEFAULT_WOS_AUTHORS = Path("data/WoS_2023_authors.parquet")
DEFAULT_RESULTS_DIR = Path("Results")
DEFAULT_MATCHES_BASENAME = "2023_affiliation_match_20251023"


MANUAL_COUNTRY_MAPPING = {
    "Russia": "RU",
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
    parser.add_argument("--wos-extra", type=Path, default=DEFAULT_WOS_EXTRA)
    parser.add_argument("--wos-authors", type=Path, default=DEFAULT_WOS_AUTHORS)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--basename", default=DEFAULT_MATCHES_BASENAME)
    parser.add_argument("--workers", type=int, default=-1)
    parser.add_argument(
        "--skip-author-country",
        action="store_true",
        help="Skip author-country enrichment and candidate disambiguation.",
    )
    return parser.parse_args()


def iter_progress(iterable: Iterable[Any], *, total: Optional[int] = None, desc: str = "") -> Iterable[Any]:
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc)


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


def load_wos_grant_agencies(extra_path: Path, workers: int) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    iterator = iter_piece_results(
        extra_path,
        columns=["UT", "FP", "FU"],
        processor=extract_funding_rows,
        workers=workers,
        keep_order=True,
    )
    for frame in iter_progress(iterator, desc="Reading WOS funding agencies"):
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["UT", "grant_agency", "grant_agency_normalized"])

    result = pd.concat(frames, ignore_index=True)
    return result.drop_duplicates(subset=["UT", "grant_agency_normalized"]).reset_index(drop=True)


def extract_author_rows(frame: pd.DataFrame, target_uts: set[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UT", "author_seq_no", "country_codes", "affiliations"])

    filtered = frame[frame["UT"].isin(target_uts)]
    if filtered.empty:
        return pd.DataFrame(columns=["UT", "author_seq_no", "country_codes", "affiliations"])

    rows: List[Dict[str, Any]] = []
    for ut, seq_no, author_countries, author_affiliations in filtered[
        ["UT", "author_seq_no", "author_countries", "author_affiliations"]
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
                "country_codes": country_codes,
                "affiliations": affiliations,
            }
        )

    return pd.DataFrame(rows)


def build_author_metadata(authors_path: Path, target_uts: set[str], workers: int) -> Dict[str, Dict[str, str]]:
    frames: List[pd.DataFrame] = []
    iterator = iter_piece_results(
        authors_path,
        columns=["UT", "author_seq_no", "author_countries", "author_affiliations"],
        processor=partial(extract_author_rows, target_uts=target_uts),
        workers=workers,
        keep_order=True,
    )
    for frame in iter_progress(iterator, desc="Reading WOS author countries"):
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return {}

    authors = pd.concat(frames, ignore_index=True).sort_values(["UT", "author_seq_no"], kind="stable")
    metadata: Dict[str, Dict[str, str]] = {}
    for ut, group in authors.groupby("UT", sort=False):
        flat_country_codes: List[str] = []
        ordered_country_groups: List[str] = []
        ordered_affiliations: List[str] = []
        for country_codes, affiliations in zip(group["country_codes"], group["affiliations"]):
            if country_codes:
                flat_country_codes.extend(country_codes)
                ordered_country_groups.append(";".join(country_codes))
            if affiliations:
                ordered_affiliations.append("; ".join(affiliations))
        metadata[str(ut)] = {
            "affiliation_country_codes": ";".join(flat_country_codes),
            "author_country_codes_ordered": " || ".join(ordered_country_groups),
            "author_affiliations_ordered": " || ".join(ordered_affiliations),
        }
    return metadata


def prepare_candidate_map(wos_funders_path: Path, institutions_metadata: pd.DataFrame) -> pd.DataFrame:
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
            lookup[str(normalized_name)].append(record)
    return lookup


def choose_best_candidate(candidates: List[Dict[str, Any]], author_country_codes: List[str]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    for country_code in author_country_codes:
        for candidate in candidates:
            if candidate.get("funder_country_code") == country_code:
                return candidate
    return candidates[0]


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
    }
    for source_field, target_field in field_mapping.items():
        attached[target_field] = [candidate.get(source_field) if candidate else None for candidate in selected_candidates]
    return attached


def main() -> None:
    args = parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    institutions_metadata = load_institutions_metadata(args.institutions_normalized)
    candidates = prepare_candidate_map(args.wos_funders, institutions_metadata)
    candidate_lookup = build_candidate_lookup(candidates)

    grant_agencies = load_wos_grant_agencies(args.wos_extra, args.workers)
    author_metadata: Dict[str, Dict[str, str]] = {}
    if not args.skip_author_country:
        author_metadata = build_author_metadata(args.wos_authors, set(grant_agencies["UT"].unique()), args.workers)

    selected_candidates: List[Optional[Dict[str, Any]]] = []
    for row in iter_progress(
        grant_agencies.itertuples(index=False),
        total=len(grant_agencies),
        desc="Matching agencies to OpenAlex",
    ):
        metadata = author_metadata.get(str(row.UT), {})
        author_country_codes = [code for code in metadata.get("affiliation_country_codes", "").split(";") if code]
        selected_candidates.append(
            choose_best_candidate(candidate_lookup.get(row.grant_agency_normalized, []), author_country_codes)
        )

    matched = attach_candidate_columns(grant_agencies, selected_candidates)
    matched["affiliation_country_codes"] = matched["UT"].map(
        lambda ut: author_metadata.get(str(ut), {}).get("affiliation_country_codes", "")
    )
    matched["author_country_codes_ordered"] = matched["UT"].map(
        lambda ut: author_metadata.get(str(ut), {}).get("author_country_codes_ordered", "")
    )
    matched["author_affiliations_ordered"] = matched["UT"].map(
        lambda ut: author_metadata.get(str(ut), {}).get("author_affiliations_ordered", "")
    )

    papers_path = args.results_dir / f"grant_institutions_{args.basename}.feather"
    agencies_path = args.results_dir / f"grant_agency_matched_{args.basename}.feather"
    matched.to_feather(papers_path)
    matched.to_parquet(papers_path.with_suffix(".parquet"), index=False)
    candidates.to_feather(agencies_path)
    candidates.to_parquet(agencies_path.with_suffix(".parquet"), index=False)

    print(papers_path.with_suffix(".parquet"))
    print(agencies_path.with_suffix(".parquet"))


if __name__ == "__main__":
    main()