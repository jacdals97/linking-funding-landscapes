"""
Stage 3 final deduplication script:
- Load merged agencies (stage 2) and normalized institutions.
- Optionally propagate IDs within components one last time.
- Run final MinHash/LSH dedup between unmatched agencies and institution names.
- Merge results into final map with ROR enrichment and write outputs.
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import List, Optional, Tuple
import pickle

import numpy as np
import pandas as pd
from datasketch import LeanMinHash, MinHash, MinHashLSH
try:
    from tqdm.auto import tqdm  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tqdm = None

def clean_lower(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.lower()
        .str.encode("utf-16", "surrogatepass")
        .str.decode("utf-16")
        .str.strip()
    )
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


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["grant_agency_lower", "id"])

def iter_progress(seq: Iterable, verbose: bool, desc: str):
    """Wrap an iterable with tqdm if available and verbose requested."""
    if verbose and tqdm is not None:
        return tqdm(seq, desc=desc)
    if verbose:
        print(desc)
    return seq

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


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def run_lsh_dedup(missing: pd.DataFrame, institutions: pd.DataFrame, out: Path, threshold: float = 0.9, num_perm: int = 128, verbose: bool = False) -> pd.DataFrame:
    """Return DataFrame with columns: grant_agency, id (matched institution)."""
    cand_names = missing["grant_agency"].astype(str).str.lower()
    inst_names = institutions["all_names_lower"].explode().dropna().reset_index()

    # Build LSH index on missing grant_agencies
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    cand_mhs: List[Tuple[int, LeanMinHash]] = []
    if out.exists():
        # for idx, text in iter_progress(zip(cand_names.index, cand_names), verbose, 'Read LSH'):
        #     mh = create_minhash(text, num_perm=num_perm)
        #     cand_mhs.append((idx, mh))
        with open(out, "rb") as f:
            lsh = pickle.load(f)
    else:
        for idx, text in iter_progress(zip(cand_names.index, cand_names), verbose, 'Build LSH'):
            mh = create_minhash(text, num_perm=num_perm)
            cand_mhs.append((idx, mh))
            lsh.insert(f"cand_{idx}", mh)
        with open(out, "wb") as f:
           pickle.dump(lsh, f)

    # Query institution names against LSH of missing agencies
    matches = []
    for _, row in iter_progress(inst_names.iterrows(),verbose, 'Query LSH'):
        mh = create_minhash(row["all_names_lower"], num_perm=num_perm)
        res = lsh.query(mh)
        for tag in res:
            cand_idx = int(tag.split("_")[1])
            matches.append({"cand_idx": cand_idx, "inst_idx": row["index"], "inst_name": row["all_names_lower"]})

    if not matches:
        return pd.DataFrame()

    matches_df = pd.DataFrame(matches)
    matches_df = matches_df.merge(missing[["grant_agency"]], left_on="cand_idx", right_index=True, how="left")
    matches_df = matches_df.merge(institutions[["id", "all_names_lower"]].explode("all_names_lower").reset_index(), left_on=["inst_idx", "inst_name"], right_index=False, right_on=[institutions.index.name or "index", "all_names_lower"])
    matches_df = matches_df[["grant_agency", "id"]].drop_duplicates()
    return matches_df


def coalesce(*vals, empty: str = ""):
    for v in vals:
        if pd.notna(v) and v not in ("", []):
            return v
    return empty


def finalize_map(agencies: pd.DataFrame, institutions: pd.DataFrame, ror_path: Path) -> pd.DataFrame:
    ror = pd.read_csv(ror_path)

    final_map = agencies.merge(
        institutions[["id", "display_name", "ids:ror", "source"]],
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_inst"),
    )
    final_map = final_map.rename(columns={"display_name_inst": "display_name"})
    final_map["match_type"] = final_map["match_type"].replace("nan", None)

    return final_map


def main():
    parser = argparse.ArgumentParser(description="Stage 3: final dedup and map writer.")
    parser.add_argument("--agencies", default=Path("artifacts/u_agencies_stage2.parquet"), type=Path)
    parser.add_argument("--institutions-parquet", default=Path("artifacts/institutions_normalized.parquet"), type=Path)
    parser.add_argument("--ror-file", default=Path("v1.72-2025-10-06-ror-data_schema_v2.csv"), type=Path)
    parser.add_argument("--out-dir", default=Path("artifacts"), type=Path)
    parser.add_argument("--lsh-file", default=Path("artifacts/lsh.pkl"), type=Path)
    parser.add_argument("--lsh-threshold", default=0.9, type=float)
    parser.add_argument("--lsh-num-perm", default=128, type=int)
    parser.add_argument("--skip-lsh", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print progress indicators for long steps.")

    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    agencies = pd.read_parquet(args.agencies)
    institutions = pd.read_parquet(args.institutions_parquet)
    for col in ["all_names_lower", "acronyms"]:
        if col in institutions.columns:
            institutions[col] = institutions[col].apply(lambda x: x if isinstance(x, np.ndarray) else ast.literal_eval(x) if isinstance(x, str) else [])

    #agencies["grant_agency_lower"] = clean_lower(agencies["grant_agency"])
    agencies = infer_ids_by_component(agencies, fill=True)
    if "id_from_component" in agencies.columns:
        agencies.loc[agencies["id_from_component"].notna() & agencies["match_type"].isna(), "match_type"] = "doc_clustering"
        agencies = agencies.drop(columns=["id_from_component"])
    agencies = dedup(agencies)

    summarize(agencies)

    # LSH dedup for remaining missing
    missing = agencies[agencies["id"].isna()].copy()
    if len(missing) and not args.skip_lsh:
        lsh_matches = run_lsh_dedup(missing, institutions, out=args.lsh_file, threshold=args.lsh_threshold, num_perm=args.lsh_num_perm, verbose=args.verbose)
        if not lsh_matches.empty:
            agencies = agencies.merge(lsh_matches, left_on="grant_agency", right_on="grant_agency", how="left", suffixes=("", "_lsh"))
            agencies["id"] = agencies["id"].fillna(agencies["id_lsh"])
            agencies.loc[agencies["id_lsh"].notna() & agencies["match_type"].isna(), "match_type"] = "deduplication"
            agencies = agencies.drop(columns=["id_lsh"])
            agencies = dedup(agencies)

    # Final component fill after LSH
    agencies = infer_ids_by_component(agencies, fill=True)
    if "id_from_component" in agencies.columns:
        agencies.loc[agencies["id_from_component"].notna() & agencies["match_type"].isna(), "match_type"] = "doc_clustering"
        agencies = agencies.drop(columns=["id_from_component"])
    agencies = dedup(agencies)

    final_map = finalize_map(agencies, institutions, args.ror_file)

    final_map[final_map["id"].isna()].to_feather(args.out_dir / "missing_agencies.feather")
    final_map[final_map["id"].notna()].to_feather(args.out_dir / "institutions_processed_duplicates.feather")
    final_map[['grant_agency', 'id', 'counts', 'ids:ror', 'source', 'match_type', 'display_name']].to_parquet(args.out_dir / "final_map.parquet", index=False)

    print("Wrote:")
    print(args.out_dir / "final_map.parquet")
    print(args.out_dir / "institutions_processed_duplicates.feather")
    print(args.out_dir / "missing_agencies.feather")


if __name__ == "__main__":
    main()
