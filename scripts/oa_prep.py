"""
Reproducible OpenAlex prep script extracted from the legacy exploratory notebook.

Steps:
1) Load funder and institution TSVs using their JSON schemas.
2) Run first-pass linking (exact ID, homepage domain+country, simple name score).
3) Enrich funders with institution alternates/acronyms and append unmatched institutions.
4) Collapse near-duplicate records by (name, country, homepage domain) and normalize names.
5) Write intermediate artifacts for downstream matching.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple

import numpy as np
import pandas as pd
import tldextract


# ---------------------------- helpers ---------------------------- #


def norm_str(s: object) -> Optional[str]:
    if pd.isna(s) or s == "":
        return np.nan
    return str(s).strip()


def norm_id_url_to_id(s: object) -> Optional[str]:
    s = norm_str(s)
    if pd.isna(s):
        return s
    s = re.sub(r"https?://(www\\.)?", "", s.lower())
    s = s.replace("openalex.org/", "").replace("ror.org/", "").replace("www.wikidata.org/wiki/", "")
    return s


def reg_domain(url: object) -> Optional[str]:
    url = norm_str(url)
    if pd.isna(url):
        return np.nan
    ext = tldextract.extract(url)
    return ext.registered_domain.lower() if ext.registered_domain else np.nan


def to_set(x: object) -> Set[str]:
    if isinstance(x, list):
        return {str(s).strip() for s in x if str(s).strip()}
    if isinstance(x, str) and x.strip():
        try:
            arr = json.loads(x)
            if isinstance(arr, list):
                return {str(s).strip() for s in arr if str(s).strip()}
        except Exception:
            return {x.strip()}
    return set()


def merge_sets(*sets: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for s in sets:
        if isinstance(s, (set, list, tuple)):
            out.update(str(v).strip() for v in s if str(v).strip())
    return out


def as_json_or_empty(s: Iterable[str]) -> str:
    return "" if not s else json.dumps(sorted(set(s)))

def set_to_json(s):
    return "" if not s else json.dumps(sorted(s))

def normalize_acronyms(acr_set: Iterable[str], max_len: int = 15) -> Set[str]:
    out: Set[str] = set()
    for a in acr_set or []:
        a = re.sub(r"[^0-9A-Za-z&.-]", "", str(a)).upper()
        if a and len(a) <= max_len:
            out.add(a)
    return out


def safe_literal_eval(val: object):
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except Exception:
            return val
    return val


# ---------------------------- loading ---------------------------- #


def load_table(tsv_path: Path, schema_path: Path, list_cols: Tuple[str, ...]) -> pd.DataFrame:
    with schema_path.open() as f:
        schema = json.load(f)

    dtype_dict = {k: v["type"] for k, v in schema["schema"].items()}
    na_values = {k: v["NA value"] for k, v in schema["schema"].items()}

    df = pd.read_csv(
        tsv_path,
        sep="\t",
        encoding="utf-8",
        dtype=dtype_dict,
        na_values=na_values,
        doublequote=False,
        escapechar="\\",
        on_bad_lines="warn",
        low_memory=False,
        quoting=csv.QUOTE_MINIMAL,
    )

    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_literal_eval)

    return df


# ---------------------------- role parsing ---------------------------- #


def parse_roles(cell: object):
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            return json.loads(cell)
        except Exception:
            return []
    return []


def extract_role_flags_and_ids(roles: Iterable[dict]) -> pd.Series:
    flags = {"has_funder_role": False, "has_publisher_role": False, "has_institution_role": False}
    funder_ids, publisher_ids, institution_ids = set(), set(), set()
    for r in roles:
        role = r.get("role")
        rid = r.get("id", "")
        bare = norm_id_url_to_id(rid) if rid else None
        if role == "funder":
            flags["has_funder_role"] = True
            if bare and bare.startswith("f"):
                funder_ids.add(bare)
        elif role == "publisher":
            flags["has_publisher_role"] = True
            if bare and bare.startswith("p"):
                publisher_ids.add(bare)
        elif role == "institution":
            flags["has_institution_role"] = True
            if bare and bare.startswith("i"):
                institution_ids.add(bare)
    return pd.Series(
        {
            **flags,
            "openalex_funder_ids": list(funder_ids),
            "openalex_publisher_ids": list(publisher_ids),
            "openalex_institution_ids": list(institution_ids),
        }
    )


# ---------------------------- matching steps ---------------------------- #


def first_round_links(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return links (id_left -> id_right) plus normalized left/candidates DataFrames."""
    df1 = df1.copy()
    df2 = df2.copy()

    # Normalize key fields
    for col in ["ids:ror", "ids:wikidata", "homepage_url", "display_name", "country_code"]:
        if col in df1.columns:
            df1[col] = df1[col].map(norm_str)
        if col in df2.columns:
            df2[col] = df2[col].map(norm_str)

    if "homepage_url" in df1:
        df1["homepage_domain"] = df1["homepage_url"].map(reg_domain)
    if "homepage_url" in df2:
        df2["homepage_domain"] = df2["homepage_url"].map(reg_domain)

    for col in ["ids:ror", "ids:wikidata"]:
        if col in df1:
            df1[f"{col}_norm"] = df1[col].map(norm_id_url_to_id)
        if col in df2:
            df2[f"{col}_norm"] = df2[col].map(norm_id_url_to_id)

    df1["name_main"] = df1["display_name"].str.casefold()
    df2["name_main"] = df2["display_name"].str.casefold()

    df2["roles_list"] = df2["roles"].map(parse_roles)
    df2 = pd.concat([df2, df2["roles_list"].apply(extract_role_flags_and_ids)], axis=1)

    cand_cols = [
        "id",
        "ids:ror_norm",
        "ids:wikidata_norm",
        "homepage_domain",
        "country_code",
        "display_name",
        "name_main",
        "has_funder_role",
        "openalex_funder_ids",
    ]
    candidates = df2.loc[:, [c for c in cand_cols if c in df2.columns]].copy()
    candidates = candidates[candidates["has_funder_role"] == True]  # noqa: E712

    left_cols = ["id", "ids:ror_norm", "ids:wikidata_norm", "homepage_domain", "country_code", "display_name", "name_main"]
    left = df1.loc[:, [c for c in left_cols if c in df1.columns]].copy()

    # T1: exact ID map
    map_ror = dict(zip(candidates["ids:ror_norm"].dropna(), candidates["id"]))
    map_wd = dict(zip(candidates["ids:wikidata_norm"].dropna(), candidates["id"]))
    left["match_ror"] = left["ids:ror_norm"].map(map_ror)
    left["match_wd"] = left["ids:wikidata_norm"].map(map_wd)
    left["match_t1"] = left["match_ror"].fillna(left["match_wd"])
    t1_links = left.loc[left["match_t1"].notna(), ["id", "match_t1"]].rename(columns={"id": "id_left", "match_t1": "id_right"})
    resolved_ids = set(t1_links["id_left"])

    # T2: homepage domain + country
    rem = left[~left["id"].isin(resolved_ids)].copy()
    cand_dom = candidates.loc[:, ["id", "homepage_domain", "country_code"]].dropna(subset=["homepage_domain"]).copy()
    cand_dom = cand_dom.set_index(["homepage_domain", "country_code"])
    rem2 = rem.dropna(subset=["homepage_domain", "country_code"]).copy()
    rem2 = rem2.set_index(["homepage_domain", "country_code"])
    j = rem2.join(cand_dom[["id"]].rename(columns={"id": "id_right"}), how="left")
    t2 = j.reset_index()
    t2_links = t2.loc[t2["id_right"].notna(), ["id", "id_right"]].rename(columns={"id": "id_left"})
    resolved_ids |= set(t2_links["id_left"])

    # T3: coarse name score with optional domain bonus
    rem3 = left[~left["id"].isin(resolved_ids)][["id", "display_name", "name_main", "country_code", "homepage_domain"]].copy()
    cand_min = candidates[["id", "display_name", "name_main", "country_code", "homepage_domain"]].copy()
    cand_min["first_letter"] = cand_min["name_main"].str[:1]
    rem3["first_letter"] = rem3["name_main"].str[:1]
    right_groups = dict(tuple(cand_min.groupby(["country_code", "first_letter"], dropna=False)))

    def jaccard(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        ta, tb = set(re.findall(r"\\w+", str(a).lower())), set(re.findall(r"\\w+", str(b).lower()))
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)

    rows = []
    for _, L in rem3.iterrows():
        key = (L.get("country_code"), L.get("first_letter"))
        pool = right_groups.get(key, cand_min)
        best_id, best_s = None, -1
        for _, R in pool.iterrows():
            s= 0
            if pd.notna(L["name_main"]) and L["name_main"] == R["name_main"]:
                s = 100
            else:
                s = int(25 * jaccard(L["display_name"], R["display_name"]))
            if L.get("homepage_domain") and R.get("homepage_domain") and L["homepage_domain"] == R["homepage_domain"]:
                s += 10
            if s > best_s:
                best_s, best_id = s, R["id"]
        if best_s >= 100 and best_id is not None:
            rows.append((L["id"], best_id, best_s))

    t3_links = pd.DataFrame(rows, columns=["id_left", "id_right", "score"])

    links = (
        pd.concat(
            [
                t1_links.assign(method="id_exact"),
                t2_links.assign(method="domain_cc"),
                t3_links[["id_left", "id_right"]].assign(method="name_score"),
            ],
            ignore_index=True,
        )
        .drop_duplicates(subset=["id_left"])
    )
    return links, left, candidates


# ---------------------------- enrichment ---------------------------- #


@dataclass
class EnrichedFunders:
    funders: pd.DataFrame
    unmatched_institutions: pd.DataFrame
    links: pd.DataFrame


def enrich_funders_with_institutions(df1: pd.DataFrame, df2: pd.DataFrame, links: pd.DataFrame) -> EnrichedFunders:
    names_cols = [
        "id",
        "display_name",
        "display_name_alternatives",
        "display_name_acronyms",
        "country_code",
        "geo:country_code",
        "homepage_url",
        "ids:ror",
        "ids:wikidata",
    ]
    df2_names = df2.loc[:, [c for c in names_cols if c in df2.columns]].copy()
    alts2 = df2_names.get("display_name_alternatives", pd.Series([[]] * len(df2_names))).map(to_set)
    acrs2 = df2_names.get("display_name_acronyms", pd.Series([[]] * len(df2_names))).map(lambda s: normalize_acronyms(to_set(s)))
    df2_names["alts_union_set"] = [merge_sets({n}, a, ac) for n, a, ac in zip(df2_names["display_name"], alts2, acrs2)]
    df2_names["acronyms_set"] = acrs2

    funder_cols = [
        "id",
        "display_name",
        "alternate_titles",
        "country_code",
        "homepage_url",
        "ids:ror",
        "ids:wikidata",
        "ids:crossref",
        "ids:doi",
        "grants_count",
        "works_count",
        "cited_by_count",
        "roles",
        "description",
    ]
    funders_min = df1.loc[:, [c for c in funder_cols if c in df1.columns]].copy()
    funder_alts = funders_min.get("alternate_titles", pd.Series([[]] * len(funders_min))).map(to_set)

    enrich = (
        links[["id_left", "id_right"]]
        .merge(df2_names[["id", "alts_union_set", "acronyms_set"]], left_on="id_right", right_on="id", how="left", sort=False)
        .drop(columns=["id"])
    )
    agg = enrich.groupby("id_left", as_index=False).agg(
        {"alts_union_set": lambda lst: merge_sets(*lst), "acronyms_set": lambda lst: merge_sets(*lst)}
    )

    funders_min = funders_min.merge(funder_alts.rename("funder_alts_set"), left_index=True, right_index=True, how="left")
    funders_min = funders_min.merge(agg, left_on="id", right_on="id_left", how="left", sort=False).drop(columns=["id_left"])
    funders_min["alternate_titles"] = [
        as_json_or_empty(merge_sets({name}, fset or set(), a2 or set(), ac2 or set()))
        for name, fset, a2, ac2 in zip(
            funders_min["display_name"], funders_min["funder_alts_set"], funders_min["alts_union_set"], funders_min["acronyms_set"]
        )
    ]
    funders_min["acronyms"] = [as_json_or_empty(normalize_acronyms(merge_sets(ac2))) for ac2 in funders_min["acronyms_set"]]
    funders_enriched = funders_min.drop(columns=["funder_alts_set", "alts_union_set", "acronyms_set"])
    funders_enriched["source"] = "funders"

    used_right_ids = set(links["id_right"])
    unmatched = df2[~df2["id"].isin(used_right_ids)].copy()
    alts_unm = unmatched.get("display_name_alternatives", pd.Series([[]] * len(unmatched))).map(to_set)
    acrs_unm = unmatched.get("display_name_acronyms", pd.Series([[]] * len(unmatched))).map(lambda s: normalize_acronyms(to_set(s)))

    alternate_titles_json = [as_json_or_empty(merge_sets({n}, a, ac)) for n, a, ac in zip(unmatched["display_name"], alts_unm, acrs_unm)]
    acronyms_json = [as_json_or_empty(ac) for ac in acrs_unm]

    unmatched_out = pd.DataFrame(
        {
            "id": unmatched["id"],
            "display_name": unmatched["display_name"],
            "country_code": unmatched.get("country_code", unmatched.get("geo:country_code", "")),
            "roles": unmatched.get("roles", ""),
            "ids:wikidata": unmatched.get("ids:wikidata", ""),
            "ids:ror": unmatched.get("ids:ror", ""),
            "ids:crossref": "",
            "ids:doi": "",
            "alternate_titles": alternate_titles_json,
            "acronyms": acronyms_json,
            "description": "",
            "homepage_url": unmatched.get("homepage_url", ""),
            "grants_count": -1,
            "works_count": unmatched.get("summary_stats:works_count", unmatched.get("works_count", -1)).fillna(-1),
            "cited_by_count": unmatched.get("summary_stats:cited_by_count", -1).fillna(-1),
            "source": "institutions",
        }
    )

    final_cols = [
        "id",
        "display_name",
        "country_code",
        "roles",
        "ids:wikidata",
        "ids:ror",
        "ids:crossref",
        "ids:doi",
        "alternate_titles",
        "acronyms",
        "description",
        "homepage_url",
        "grants_count",
        "works_count",
        "cited_by_count",
        "source",
    ]
    for c in final_cols:
        if c not in funders_enriched.columns:
            funders_enriched[c] = "" if c not in ["grants_count", "works_count", "cited_by_count"] else -1
        if c not in unmatched_out.columns:
            unmatched_out[c] = "" if c not in ["grants_count", "works_count", "cited_by_count"] else -1

    final_funders = pd.concat(
        [funders_enriched[final_cols].sort_values(by="grants_count", ascending=False), unmatched_out[final_cols]], ignore_index=True
    )
    return EnrichedFunders(final_funders, unmatched_out, links)


# ---------------------------- collapse ---------------------------- #


def collapse_records(final_funders: pd.DataFrame, ror_path: Path) -> pd.DataFrame:
    ror = pd.read_csv(ror_path)

    inst = final_funders.merge(
        ror[["id", "external_ids.type.grid.preferred", "locations.geonames_details.country_code"]],
        left_on="ids:ror",
        right_on="id",
        how="left",
        suffixes=("", "_ror"),
    )

    def coalesce(*vals, empty: str = ""):
        for v in vals:
            if pd.notna(v) and v not in ("", []):
                return v
        return empty

    inst["country_code_final"] = inst["country_code"].fillna(inst["locations.geonames_details.country_code"])
    inst["homepage_domain"] = inst["homepage_url"].map(reg_domain).fillna("")
    inst["name_norm"] = inst["display_name"].str.casefold().str.strip()
    inst["group_key"] = np.where(
        inst["homepage_domain"].ne(""),
        inst["name_norm"] + " | " + inst["country_code_final"].fillna("") + " | " + inst["homepage_domain"],
        inst["name_norm"] + " | " + inst["country_code_final"].fillna(""),
    )
    inst["is_funder"] = (inst.get("source", "") == "funders").astype(int)
    inst["has_ror"] = inst["ids:ror"].fillna("").astype(bool).astype(int)
    inst["has_wd"] = inst["ids:wikidata"].fillna("").astype(bool).astype(int)
    inst["has_domain"] = inst["homepage_domain"].fillna("").ne("").astype(int)
    for c in ["grants_count", "works_count", "cited_by_count"]:
        if c in inst.columns:
            inst[c + "_nz"] = inst[c].replace({-1: np.nan})
        else:
            inst[c + "_nz"] = np.nan

    sort_cols = ["is_funder", "has_ror", "has_wd", "has_domain", "grants_count_nz", "works_count_nz", "cited_by_count_nz"]
    inst = inst.sort_values(by=sort_cols, ascending=False, kind="stable")

    def reduce_group(g: pd.DataFrame) -> pd.Series:
        canon = g.iloc[0]
        alt_sets = [to_set(x) for x in g.get("alternate_titles", "")]
        alt_sets.append(set(g["display_name"].dropna().astype(str).str.strip()))
        acr_sets = [normalize_acronyms(to_set(x)) for x in g.get("acronyms", "")]
        acr_union = set().union(*acr_sets)
        alt_union = set().union(*alt_sets, acr_union)

        other_ror = set(g["ids:ror"].dropna().astype(str)) - ({canon.get("ids:ror")} if pd.notna(canon.get("ids:ror")) else set())
        other_wd = set(g["ids:wikidata"].dropna().astype(str)) - ({canon.get("ids:wikidata")} if pd.notna(canon.get("ids:wikidata")) else set())

        def max_or_neg1(series: pd.Series) -> int:
            vals = pd.to_numeric(series, errors="coerce").dropna()
            return int(vals.max()) if len(vals) else -1

        return pd.Series(
            {
                "id": canon.get("id"),
                "display_name": canon.get("display_name"),
                "country_code": canon.get("country_code_final") or "",
                "roles": canon.get("roles", ""),
                "ids:wikidata": coalesce(canon.get("ids:wikidata"), ""),
                "ids:ror": coalesce(canon.get("ids:ror"), ""),
                "ids:crossref": coalesce(canon.get("ids:crossref"), ""),
                "ids:doi": coalesce(canon.get("ids:doi"), ""),
                "description": coalesce(canon.get("description"), ""),
                "homepage_url": coalesce(canon.get("homepage_url"), ""),
                "alternate_titles": set_to_json(alt_union),
                "acronyms": set_to_json(acr_union),
                "sources_merged": json.dumps(sorted(set(g.get("source", "institutions").astype(str)))),
                "grants_count": max_or_neg1(g.get("grants_count", pd.Series(dtype=float))),
                "works_count": max_or_neg1(g.get("works_count", pd.Series(dtype=float))),
                "cited_by_count": max_or_neg1(g.get("cited_by_count", pd.Series(dtype=float))),
                "other_ids:ror": set_to_json(other_ror),
                "other_ids:wikidata": set_to_json(other_wd),
                "homepage_domain": canon.get("homepage_domain", ""),
            }
        )

    collapsed = inst.groupby("group_key", sort=False, group_keys=False).apply(reduce_group).reset_index(drop=True)
    return collapsed


def add_normalized_name_lists(collapsed: pd.DataFrame) -> pd.DataFrame:
    out = collapsed.copy()
    out["all_names"] = out.apply(lambda x: [x["display_name"]] + ast.literal_eval(x["alternate_titles"]), axis=1)
    out["all_names"] = out["all_names"].apply(lambda xs: [y.replace("&", "and") for y in xs])
    out["all_names"] = out["all_names"].apply(lambda xs: [y.encode("utf-16", "surrogatepass").decode("utf-16") for y in xs])
    out["all_names"] = out["all_names"].apply(lambda xs: [remove_accents(y) for y in xs])
    out["all_names"] = out["all_names"].apply(lambda xs: [y.replace("’", "'") for y in xs])
    out["all_names_lower"] = out["all_names"].apply(lambda xs: [y.lower() for y in xs])
    out["all_names_lower"] = out["all_names_lower"].apply(lambda xs: [y.encode("utf-16", "surrogatepass").decode("utf-16") for y in xs])
    out["all_names_lower"] = out["all_names_lower"].apply(lambda xs: [y.strip() for y in xs])
    out["display_name"] = out["display_name"].apply(remove_accents)
    out["display_name_lower"] = out["display_name"].str.lower()
    out["display_name_lower"] = out["display_name_lower"].apply(lambda x: x.encode("utf-16", "surrogatepass").decode("utf-16"))
    out["display_name_lower"] = out["display_name_lower"].str.strip()
    out["acronyms"] = out["acronyms"].apply(lambda x: x if x else "[]")
    out["acronyms"] = out["acronyms"].apply(ast.literal_eval)
    out["acronyms"] = out["acronyms"].apply(lambda xs: [y.encode("utf-16", "surrogatepass").decode("utf-16") for y in xs])
    out["acronyms"] = out["acronyms"].apply(lambda xs: [y.strip() for y in xs])
    out["all_names"] = out["all_names"].apply(lambda xs: list(set(xs)))
    out["all_names_lower"] = out["all_names_lower"].apply(lambda xs: list(set(xs)))
    out["acronyms"] = out["acronyms"].apply(lambda xs: list(set(xs)))
    return out


# ---------------------------- accents ---------------------------- #


def remove_accents(text: str) -> str:
    import unicodedata

    normalized_text = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized_text if unicodedata.category(char) != "Mn")


# ---------------------------- CLI ---------------------------- #


def main():
    parser = argparse.ArgumentParser(description="Reproduce OpenAlex prep and first-pass matching.")
    parser.add_argument("--input-dir", default="data/openalex_institutions", type=Path)
    parser.add_argument("--funders-tsv", default=None, type=Path, help="Override funders TSV path.")
    parser.add_argument("--funders-schema", default=None, type=Path, help="Override funders schema JSON path.")
    parser.add_argument("--institutions-tsv", default=None, type=Path, help="Override institutions TSV path.")
    parser.add_argument("--institutions-schema", default=None, type=Path, help="Override institutions schema JSON path.")
    parser.add_argument("--ror-file", default=Path("data/v1.72-2025-10-06-ror-data_schema_v2.csv"), type=Path)
    parser.add_argument("--out-dir", default=Path("artifacts"), type=Path)
    args = parser.parse_args()

    funders_tsv = args.funders_tsv or (args.input_dir / "funders_core+basic+ids+extra+counts.tsv")
    funders_schema = args.funders_schema or (args.input_dir / "funders_core+basic+ids+extra+counts.json")
    inst_tsv = args.institutions_tsv or (args.input_dir / "institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv")
    inst_schema = args.institutions_schema or (args.input_dir / "institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.json")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    funders = load_table(funders_tsv, funders_schema, list_cols=("alternate_titles",))
    institutions = load_table(inst_tsv, inst_schema, list_cols=("display_name_acronyms", "display_name_alternatives"))

    links, _, candidates = first_round_links(funders, institutions)
    enriched = enrich_funders_with_institutions(funders, institutions, links)
    collapsed = collapse_records(enriched.funders, args.ror_file)
    institutions_normalized = add_normalized_name_lists(collapsed)
    institutions_normalized["source"] = institutions_normalized["sources_merged"].apply(
        lambda x: ast.literal_eval(x)[0] if isinstance(x, str) else None
    )
    oa_institutions = institutions_normalized[
        ["id", "display_name", "country_code", "ids:ror", "homepage_url", "source", "alternate_titles", "acronyms"]
    ].copy()

    institutions_normalized.drop(columns='roles').to_parquet(args.out_dir / "institutions_normalized.parquet")
    oa_institutions.to_parquet(args.out_dir / "oa_institutions.parquet")

    print("Wrote:", args.out_dir.resolve())


if __name__ == "__main__":
    main()
