"""
Stage 1 matching pipeline (reproducible, no NER/LSH dedup):
- Load OA institutions (from oa_prep output) and WOS grant agency counts.
- Build document clusters of grant agency strings (LSH near-duplicates) for later propagation.
- Run matching passes: exact names, manual annotations (single JSON map), substring detection, acronyms.
- Augment high-bucket (counts >= high_threshold) missing entries via OpenAlex API search (optional).

Inputs (defaults match repo layout):
  - artifacts/institutions_normalized.parquet   (from scripts/oa_prep.py)
    - data/WOS_TSV_FUNDING_2023.feather            (WOS funding data from preprocessing/export_wos_funding_feather.py)
  - manual_annotations.json

Outputs (written under --out-dir, default artifacts/):
  - u_agencies_stage1.parquet          : grant agencies with IDs/match types and components
  - u_agencies_missing.parquet         : unmatched agencies (not persisted per request; placeholder kept for clarity)
  - u_agencies_high_augmented.parquet  : high-bucket (>= high_threshold) with optional OA API fill
  - grant_agency_edges.csv             : edge list from LSH doc clustering (if run)
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import networkx as nx
import numpy as np
import pandas as pd
import polars as pl
from datasketch import LeanMinHash, MinHash, MinHashLSH
import ahocorasick
import requests
import html

try:
    from tqdm.auto import tqdm  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tqdm = None

# ---------------------------- utils ---------------------------- #


def remove_accents(text: str) -> str:
    import unicodedata

    normalized_text = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized_text if unicodedata.category(char) != "Mn")


def get_shingles(text: str, k: int = 3) -> set:
    if not isinstance(text, str):
        return set()
    text = text.strip()
    if len(text) < k:
        return {text}
    return {text[i : i + k] for i in range(len(text) - k + 1)}


def create_minhash(text: str, num_perm: int = 128) -> LeanMinHash:
    m = MinHash(num_perm=num_perm)
    for sh in get_shingles(text, 3):
        m.update(sh.encode("utf-16", "surrogatepass").decode("utf-16").encode("utf-8"))
    return LeanMinHash(m)

def clean_lower(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.lower()
        .str.encode("utf-16", "surrogatepass")
        .str.decode("utf-16")
        .str.strip()
    )


def clean_id_val(v: str) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    v = str(v).strip()
    if not v:
        return None
    v = v.replace('"', "").strip()
    v = v.split("/")[-1]  # strip openalex URL
    v = v.lstrip("Ff")  # remove leading F
    return v or None


def iter_progress(seq: Iterable, verbose: bool, desc: str):
    """Wrap an iterable with tqdm if available and verbose requested."""
    if verbose and tqdm is not None:
        return tqdm(seq, total=len(seq), desc=desc)
    if verbose:
        print(desc)
    return seq


# ---------------------------- OpenAlex API for high bucket ---------------------------- #


def query_openalex_funders(search_string: str, mailto: str) -> Optional[dict]:
    if not mailto:
        return None
    url = f"https://api.openalex.org/funders?mailto={mailto}&search={search_string}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data.get("results"):
        return None
    res = data["results"][0]
    alt_names = res.get("display_name_alternatives") or res.get("alternate_names") or []
    return {
        "id": clean_id_val(res.get("id")),
        "display_name": res.get("display_name"),
        "alt_names": alt_names if isinstance(alt_names, list) else [alt_names] if alt_names else [],
    }


def augment_high_bucket(df: pd.DataFrame, mailto: Optional[str], sleep: float = 0.1, verbose: bool = False) -> pd.DataFrame:
    if not mailto:
        return df
    df = df.copy()
    resp_data = []
    for txt in iter_progress(df["grant_agency"], verbose=verbose, desc="OA API (high bucket)"):
        res = query_openalex_funders(txt, mailto=mailto)
        resp_data.append(res)
        time.sleep(sleep)

    df["id_api"] = [r.get("id") if r else None for r in resp_data]
    df["display_name_api"] = [r.get("display_name") if r else None for r in resp_data]
    df["alt_names_api"] = [r.get("alt_names") if r else None for r in resp_data]
    df["id"] = df["id"].fillna(df["id_api"])
    df.loc[df["id_api"].notna() & df["match_type"].isna(), "match_type"] = "oa_api"
    df = dedup(df)
    return df


# ---------------------------- Aho-Corasick helpers ---------------------------- #


def build_automaton(words: Sequence[str]) -> ahocorasick.Automaton:
    A = ahocorasick.Automaton()
    for word in words:
        if isinstance(word, str) and word:
            A.add_word(word, word)
    A.make_automaton()
    return A

def find_longest_match(text, automatons):
    matches = []
    for A in automatons:
        for end_index, word in A.iter(text):
            start_index = end_index - len(word) + 1
            # Check for word boundaries
            if (start_index == 0 or not text[start_index - 1].isalnum()) and \
            (end_index == len(text) - 1 or not text[end_index + 1].isalnum()):
                matches.append(word)
        if matches:
            return max(matches, key=len, default=None)
    return None

def find_matches_boundary(text: str, automatons: Sequence[ahocorasick.Automaton], position: str) -> Optional[str]:
    """position: 'start' or 'end'."""
    if not isinstance(text, str):
        return None
    matches = []
    for A in automatons:
        for end_index, word in A.iter(text):
            start_index = end_index - len(word) + 1
            if position == "start":
                if start_index == 0 and (end_index == len(text) - 1 or not text[end_index + 1].isalnum()):
                    matches.append(word)
            else:  # end
                if end_index == len(text) - 1 and (start_index == 0 or not text[start_index - 1].isalnum()):
                    matches.append(word)
        if matches:
            return max(matches, key=len)
    return None


# ---------------------------- doc clustering ---------------------------- #


def build_components(texts: List[str], threshold: float = 0.95, num_perm: int = 128, verbose: bool = False) -> Tuple[List[Tuple[int, int]], dict]:
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    minhashes: List[LeanMinHash] = []
    for idx, text in enumerate(iter_progress(texts, verbose=verbose, desc="LSH minhashes")):
        mh = create_minhash(text or "", num_perm=num_perm)
        minhashes.append(mh)
        lsh.insert(str(idx), mh)

    edges = []
    for idx, mh in enumerate(iter_progress(minhashes, verbose=verbose, desc="LSH queries")):
        result = lsh.query(mh)
        for r in result:
            j = int(r)
            #if j != idx and mh.jaccard(minhashes[j])>= threshold:  # avoid dup/self
            if j != idx:
                edges.append((idx, j))

    G = nx.Graph()
    G.add_nodes_from(range(len(texts)))
    G.add_edges_from(edges)
    components = list(nx.connected_components(G))
    component_map = {node: comp_id for comp_id, comp in enumerate(components) for node in comp}
    return edges, component_map


# ---------------------------- matching passes ---------------------------- #


def run_matching(institutions: pd.DataFrame, u_agencies: pd.DataFrame) -> pd.DataFrame:
    df = u_agencies.copy()
    df["match_type"] = None
    # exact display_name_lower
    df = df.merge(
        institutions[["display_name_lower", "display_name", "id"]], left_on="grant_agency_lower", right_on="display_name_lower", how="left"
    )
    df.loc[df["id"].notna(), "match_type"] = "display_exact"
    df = df.rename(columns={"id": "id_exact", "display_name": "display_exact_name"})

    # exact any alternate/all names
    alt = institutions.explode("all_names_lower")[["all_names_lower", "id"]].dropna(subset=["all_names_lower"])
    df = df.merge(alt, left_on="grant_agency_lower", right_on="all_names_lower", how="left")
    df["id"] = df["id_exact"].fillna(df["id"])
    df.drop(columns=["id_exact"], inplace=True)
    df.loc[df["id"].notna() & df["match_type"].isna(), "match_type"] = "all_exact"

    return df


def apply_manual(df: pd.DataFrame, manual_map: dict) -> pd.DataFrame:
    df = df.copy()
    df["id"] = df["id"].fillna(df["grant_agency"].map(manual_map))
    df.loc[df["id"].notna() & df["match_type"].isna(), "match_type"] = "manual"
    return df


def apply_substring(df: pd.DataFrame, institutions: pd.DataFrame, min_ratio: float = 0.5) -> pd.DataFrame:
    df = df.copy()
    found_agency = institutions["all_names_lower"].explode().dropna().unique()
    A = build_automaton(found_agency)

    df["match_any"] = df["grant_agency_lower"].apply(lambda x: find_longest_match(x, [A]))
    df["match_start"] = df["grant_agency_lower"].apply(lambda x: find_matches_boundary(x, [A], "start"))
    df["match_end"] = df["grant_agency_lower"].apply(lambda x: find_matches_boundary(x, [A], "end"))
    df["agency_in_name"] = df[["match_start", "match_end"]].apply(
        lambda x: max([v for v in x if isinstance(v, str)], key=len) if any(isinstance(v, str) for v in x) else None, axis=1
    )
    df["agency_in_name_search"] = df["match_any"]

    for col in ["agency_in_name", "agency_in_name_search"]:
        df[col] = df.apply(
            lambda row: row[col] if isinstance(row[col], str) and len(row[col]) / max(len(row["grant_agency"]), 1) >= min_ratio else None,
            axis=1,
        )

    for col, label in [("agency_in_name", "substring"), ("agency_in_name_search", "substring_any")]:
        name_to_id = (
            df[[col]]
            .dropna()
            .merge(institutions.explode("all_names_lower")[["all_names_lower", "id"]], left_on=col, right_on="all_names_lower", how="inner")
            .drop_duplicates()
        )
        df = df.merge(name_to_id[[col, "id"]], on=col, how="left", suffixes=("", "_sub"))
        df["id"] = df["id"].fillna(df["id_sub"])
        df.drop(columns=["id_sub"], inplace=True)
        df.loc[df["id"].notna() & df["match_type"].isna(), "match_type"] = label

    return df.drop(columns=["match_any", "match_start", "match_end"])


def apply_acronyms(df: pd.DataFrame, institutions: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    acronyms = institutions.explode("acronyms")[["acronyms", "id"]].dropna()
    df = df.merge(acronyms, left_on="abbrev", right_on="acronyms", how="left", suffixes=("", "_sub"))
    df["id"] = df["id"].fillna(df["id_sub"])
    df.drop(columns=["id_sub"], inplace=True)
    df.loc[df["id"].notna() & df["match_type"].isna(), "match_type"] = "acronyms"
    return df


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["grant_agency_lower", "id"])


def infer_ids_by_component(
    df: pd.DataFrame,
    component_col: str = "component",
    id_col: str = "id",
    new_col: str = "id_from_component",
    fill: bool = False,
) -> pd.DataFrame:
    """
    For rows with missing `id_col`, attach candidate IDs found on other rows sharing the same `component_col`.
    Duplicates are allowed (no dedup of result).
    """
    if component_col not in df.columns or id_col not in df.columns:
        return df

    df = df.copy()
    df_reset = df.reset_index()  # keep original row index in a column named 'index'

    # Rows needing IDs
    missing = df_reset[df_reset[id_col].isna()][["index", component_col, id_col]]

    # Known IDs per component (allow multiple IDs per component)
    donors = (
        df[[component_col, id_col]]
        .dropna(subset=[id_col])
        .drop_duplicates([component_col, id_col])
    )

    # Attach candidate IDs to the missing rows via left-merge on component
    doc_clus = missing.merge(donors, on=component_col, how="left", suffixes=("_x", "_y"))

    # Join back to original df by the saved row index
    df = df.join(doc_clus[["index", f"{id_col}_y"]].set_index("index"), how="left")

    # Rename the attached column to something stable/user-friendly
    df.rename(columns={f"{id_col}_y": new_col}, inplace=True)

    # Optionally fill missing ids with the inferred ones
    if fill:
        df[id_col] = df[id_col].fillna(df[new_col])

    return df


def summarize(df: pd.DataFrame):
    t = df.drop_duplicates(subset=["grant_agency_lower"])
    coverage = t.loc[t.id.notna(), "counts"].sum() / t["counts"].sum()
    print(f"Overall missing id rate: {t.id.isna().mean():.3f}")
    print(f"Share of total counts covered by matched ids: {coverage:.3f}")
    bins = [(1000, float("inf"), ">=1000"), (100, 1000, "100-1000"), (10, 100, "10-100"), (1, 10, "1-10")]
    for lo, hi, label in bins:
        subset = t[(t["counts"] >= lo) & (t["counts"] < hi)]
        if len(subset):
            print(f"{label}: missing rate {subset.id.isna().mean():.3f} (n={len(subset)})")


# ---------------------------- manual maps ---------------------------- #


def load_manual_annotations(manual_path: Path) -> dict:
    if manual_path.exists():
        with manual_path.open() as f:
            data = json.load(f)
        return data
    return {}


# ---------------------------- main ---------------------------- #


def main():
    parser = argparse.ArgumentParser(description="Stage 1 matching through >1000-paper extraction.")
    parser.add_argument("--institutions-parquet", default=Path("artifacts/institutions_normalized.parquet"), type=Path)
    parser.add_argument("--wos-feather", default=Path("data/WOS_TSV_FUNDING_2023.feather"), type=Path)
    parser.add_argument("--manual-annotations", default=Path("manual_annotations.json"), type=Path, help="Single JSON mapping grant_agency -> id")
    parser.add_argument("--out-dir", default=Path("artifacts"), type=Path)
    parser.add_argument("--skip-doc-clustering", action="store_true", help="Skip LSH doc clustering step.")
    parser.add_argument("--high-threshold", default=1000, type=int, help="Counts >= this go to high bucket OA API augmentation.")
    parser.add_argument("--skip-oa-api", action="store_true", help="Skip OpenAlex API augmentation for high bucket.")
    parser.add_argument("--mailto", default=None, help="mailto param for OpenAlex; if absent, OA API is skipped.")
    parser.add_argument("--verbose", action="store_true", help="Print progress indicators for long steps.")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Load institutions
    institutions = pd.read_parquet(args.institutions_parquet)
    for col in ["all_names_lower", "acronyms"]:
        if col in institutions.columns:
            institutions[col] = institutions[col].apply(lambda x: x if isinstance(x, np.ndarray) else ast.literal_eval(x) if isinstance(x, str) else [])
    #institutions["display_name_lower"] = clean_lower(institutions["display_name"])

    # Load WOS grant agency counts
    u_agencies = (
        pl.read_ipc(args.wos_feather)
        .with_columns(pl.col("agency_name").str.split(";"))
        .explode("agency_name")
        .with_columns(pl.col("agency_name").str.split(":").list.first().str.strip_chars())
        .filter(pl.col("agency_name") != "")
    )["agency_name"].value_counts(sort=True).rename({"agency_name": "grant_agency", "count": "counts"}).to_pandas()

    u_agencies['grant_agency'] = u_agencies['grant_agency'].apply(html.unescape)
    u_agencies["grant_agency"] = u_agencies["grant_agency"].apply(lambda x: x.replace("&", "and"))
    u_agencies["grant_agency"] = u_agencies["grant_agency"].str.replace("''", "'")
    u_agencies["grant_agency"] = u_agencies["grant_agency"].apply(lambda x: x if isinstance(x, str) else "")
    u_agencies['grant_agency_lower'] = u_agencies['grant_agency'].str.lower()
    u_agencies['grant_agency_lower'] = u_agencies['grant_agency_lower'].str.strip()
    u_agencies[["abbrev"]] = u_agencies["grant_agency"].str.extract(r"\(([A-Z]*)\)")

    # Doc clustering
    edges, component_map = ([], {})
    edges_path = args.out_dir / "grant_agency_edges.csv"
    if not args.skip_doc_clustering:
        if edges_path.exists():
            with edges_path.open() as f:
                reader = csv.reader(f)
                edges = [(int(a), int(b)) for a, b in reader if a != "" and b != ""]
            G = nx.Graph()
            G.add_nodes_from(range(len(u_agencies)))
            G.add_edges_from(edges)
            components = list(nx.connected_components(G))
            component_map = {node: comp_id for comp_id, comp in enumerate(components) for node in comp}
        else:
            edges, component_map = build_components(u_agencies["grant_agency"].tolist(), verbose=args.verbose)
            with edges_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(edges)
        u_agencies["component"] = u_agencies.index.map(component_map).astype("Int64")

    # Matching passes
    df = run_matching(institutions, u_agencies)
    df = dedup(df)
    manual_map = load_manual_annotations(args.manual_annotations)
    df = apply_manual(df, manual_map)
    df = dedup(df)
    df = apply_substring(df, institutions)
    df = dedup(df)
    df = apply_acronyms(df, institutions)
    df = dedup(df)

    # Propagate IDs within components then dedup again
    df = infer_ids_by_component(df, fill=True)
    if "id_from_component" in df.columns:
        df.loc[df["id_from_component"].notna() & df["match_type"].isna(), "match_type"] = "doc_clustering"
        df = df.drop(columns=["id_from_component"])
    df = dedup(df)

    summarize(df)

    missing = df[df["id"].isna()].copy()

    # High bucket augmentation (>= high_threshold)
    high_missing = missing[missing["counts"] >= args.high_threshold].copy()
    if len(high_missing) and not args.skip_oa_api and args.mailto:
        high_aug = augment_high_bucket(high_missing, mailto=args.mailto, verbose=args.verbose)
        df.loc[high_aug.index, ["id", "match_type"]] = high_aug[["id", "match_type"]]
        missing = df[df["id"].isna()].copy()
    else:
        high_aug = high_missing

    df = df.assign(id = df.id.astype('float64').astype("Int64"))
    df.to_parquet(args.out_dir / "u_agencies_stage1.parquet", index=False)

    high_aug.to_parquet(args.out_dir / "u_agencies_high_augmented.parquet", index=False)

    print("Wrote:")
    print(args.out_dir / "u_agencies_stage1.parquet")
    print(args.out_dir / "u_agencies_high_augmented.parquet")
    if edges:
        print(args.out_dir / "grant_agency_edges.csv")


if __name__ == "__main__":
    main()
