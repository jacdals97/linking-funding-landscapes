"""
Stage 2: run NER matching on mid-counts (100 <= papers < 1000) without OpenAlex search.

Pipeline:
1) Load stage1 agencies (u_agencies_stage1.parquet) and institutions_normalized.parquet.
2) For missing entries in 100–1000 bucket, run GLiNER NER on grant_agency, then re-run matching
   (exact display/all-names, substring) on extracted entities (no OpenAlex API search).
3) Write outputs:
   - artifacts/u_agencies_mid_ner.parquet
   - artifacts/u_agencies_mid_missing.parquet
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
import ahocorasick
from gliner import GLiNER
try:
    from tqdm.auto import tqdm  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tqdm = None

# ---------------------------- shared helpers ---------------------------- #


def clean_lower_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )


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


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["key_lower", "id"])

def iter_progress(seq: Iterable, verbose: bool, desc: str):
    """Wrap an iterable with tqdm if available and verbose requested."""
    if verbose and tqdm is not None:
        return tqdm(seq, total=len(seq), desc=desc)
    if verbose:
        print(desc)
    return seq

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

# ---------------------------- matching passes ---------------------------- #


def run_matching(df: pd.DataFrame, institutions: pd.DataFrame, text_col: str, abbrev_col: Optional[str] = None) -> pd.DataFrame:
    df = df.copy()
    df["match_type"] = df.get("match_type", None)
    df["id"] = df.get("id", np.nan)

    # Exact on display_name_lower
    df = df.merge(
        institutions[["display_name_lower", "display_name", "id"]],
        left_on="key_lower",
        right_on="display_name_lower",
        how="left",
        suffixes=("", "_disp"),
    )
    df.loc[df["id_disp"].notna(), "match_type"] = df["match_type"].fillna("display_exact")
    df["id"] = df["id"].fillna(df["id_disp"])
    df.drop(columns=["id_disp", "display_name"], inplace=True)

    # Exact on any alt names
    alt = institutions.explode("all_names_lower")[["all_names_lower", "id"]].dropna(subset=["all_names_lower"])
    df = df.merge(alt, left_on="key_lower", right_on="all_names_lower", how="left", suffixes=("", "_alt"))
    df.loc[df["id_alt"].notna() & df["match_type"].isna(), "match_type"] = "all_exact"
    df["id"] = df["id"].fillna(df["id_alt"])
    df.drop(columns=["id_alt", "all_names_lower"], inplace=True)
    df = dedup(df)

    # Substring (longest anywhere in text)
    found_agency = institutions["all_names_lower"].explode().dropna().unique()
    A = build_automaton(found_agency)
    df["match_substring"] = df["key_lower"].apply(lambda x: find_longest_match(x, [A]))
    df["match_substring"] = df.apply(
        lambda row: row["match_substring"]
        if isinstance(row["match_substring"], str) and len(row["match_substring"]) / max(len(row["key_lower"]), 1) >= 0.5
        else None,
        axis=1,
    )
    name_to_id = (
        df[["match_substring"]]
        .dropna()
        .merge(institutions.explode("all_names_lower")[["all_names_lower", "id"]], left_on="match_substring", right_on="all_names_lower", how="inner")
        .drop_duplicates()
    )
    if not name_to_id.empty:
        df = df.merge(name_to_id[["match_substring", "id"]], on="match_substring", how="left", suffixes=("", "_sub"))
        df["id"] = df["id"].fillna(df["id_sub"])
        df.drop(columns=["id_sub"], inplace=True)
        df.loc[df["id"].notna() & df["match_type"].isna(), "match_type"] = "substring"

    df = dedup(df)
    return df


# ---------------------------- NER on mid bucket ---------------------------- #


def run_ner(texts: List[str], model_name: str, onnx_model: Optional[str], verbose:bool) -> List[List[str]]:
    model = GLiNER.from_pretrained(model_name, load_onnx_model=True, load_tokenizer=True, onnx_model_file=onnx_model)
    model.to("mps")
    entities_out: List[List[str]] = []
    for t in iter_progress(texts, verbose, desc="NER mid bucket"):
        entities = model.predict_entities(t.replace("-", " "), ["organisation", "funder", "funding agency", "funding program"], threshold=0.3, flat_ner=False, multi_label=True)
        entities_out.append([e["text"] for e in entities] or [t])
    return entities_out


# ---------------------------- main ---------------------------- #


def main():
    parser = argparse.ArgumentParser(description="Stage 2: mid bucket NER matching (100-1000) without OpenAlex search.")
    parser.add_argument("--u-agencies", default=Path("artifacts/u_agencies_stage1.parquet"), type=Path)
    parser.add_argument("--institutions-parquet", default=Path("artifacts/institutions_normalized.parquet"), type=Path)
    parser.add_argument("--out-dir", default=Path("artifacts"), type=Path)
    parser.add_argument("--ner-model", default="gliner_news_small")
    parser.add_argument("--ner-onnx", default="model_quantized.onnx")
    parser.add_argument("--mid-threshold-low", default=100, type=int)
    parser.add_argument("--mid-threshold-high", default=1000, type=int)
    parser.add_argument("--verbose", action="store_true", help="Print progress indicators for long steps.")

    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    institutions = pd.read_parquet(args.institutions_parquet)
    for col in ["all_names_lower", "acronyms"]:
        if col in institutions.columns:
            institutions[col] = institutions[col].apply(lambda x: x if isinstance(x, np.ndarray) else ast.literal_eval(x) if isinstance(x, str) else [])
    #institutions["display_name_lower"] = clean_lower_series(institutions["display_name"])

    agencies = pd.read_parquet(args.u_agencies)
    #agencies["grant_agency"] = agencies["grant_agency"].astype(str)
    #agencies["grant_agency_lower"] = clean_lower_series(agencies["grant_agency"])
    agencies["key_lower"] = agencies["grant_agency_lower"]

    # Mid bucket
    mid = agencies[
        (agencies["id"].isna()) & (agencies["counts"] >= args.mid_threshold_low) & (agencies["counts"] < args.mid_threshold_high)
    ].copy()
    if len(mid):
        ner_lists = run_ner(mid["grant_agency"].tolist(), model_name=args.ner_model, onnx_model=args.ner_onnx, verbose=args.verbose)
        mid = mid.assign(NER_agency=ner_lists)
        mid = mid.explode("NER_agency").reset_index()
        mid["NER_agency"] = mid["NER_agency"].fillna(mid["grant_agency"])
        mid["key_lower"] = clean_lower_series(mid["NER_agency"])
        mid["abbrev"] = mid["grant_agency"].str.extract(r"\(([A-Z]*)\)")[0]
        mid = run_matching(mid, institutions, text_col="NER_agency")

    # Merge mid results back into full agencies
    if len(mid):
        mid_cols = ["id", "match_type", "NER_agency"]
        agencies = agencies.merge(mid[["index"] + mid_cols], left_index=True, right_on="index", how="left")
        agencies["id"] = agencies["id_x"].fillna(agencies["id_y"])
        agencies["match_type"] = agencies["match_type_x"].fillna(agencies["match_type_y"])
        agencies = agencies.drop(columns=["id_x", "id_y", "match_type_x", "match_type_y", "index", "NER_agency"])

    agencies = dedup(agencies)

    summarize(agencies)
    
    mid_missing = mid[mid["id"].isna()].copy() if len(mid) else pd.DataFrame()

    agencies.to_parquet(args.out_dir / "u_agencies_stage2.parquet", index=False)

    print("Mid bucket:", len(mid) if len(mid) else 0)
    if len(mid_missing):
        print(f"Mid bucket remaining missing: {len(mid_missing)}")
    print("Combined output:", args.out_dir / "u_agencies_stage2.parquet")


if __name__ == "__main__":
    main()
