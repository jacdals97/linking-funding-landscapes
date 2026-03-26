from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Sequence

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export the legacy WOS funding feather expected by scripts/match_stage1.py from WoS processed parquet."
    )
    parser.add_argument("--wos-extra", type=Path, default=Path("data/WoS_2023_extra.parquet"))
    parser.add_argument("--output", type=Path, default=Path("data/WOS_TSV_FUNDING_2023.feather"))
    parser.add_argument("--workers", type=int, default=-1)
    return parser.parse_args()


def _load_piece_reader():
    try:
        from parcake import PieceReader
        return PieceReader
    except ModuleNotFoundError:
        import sys

        repo_root = Path(__file__).resolve().parents[1]
        vendored_src = repo_root / "preprocessing" / "libraries" / "parcake" / "src"
        if vendored_src.exists() and str(vendored_src) not in sys.path:
            sys.path.insert(0, str(vendored_src))
        from parcake import PieceReader
        return PieceReader


PieceReader = _load_piece_reader()


def default_worker_count(workers: int | None) -> int:
    import os

    detected = os.cpu_count() or 1
    if workers in (None, 0):
        return max(1, detected - 1)
    if workers == -1:
        return detected
    return max(1, int(workers))


def coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if value is None or pd.isna(value):
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError):
                parsed = None
            if isinstance(parsed, (list, tuple, np.ndarray)):
                return list(parsed)
        if ";" in stripped:
            return [item.strip() for item in stripped.split(";") if item.strip()]
        return [stripped]
    return [value]


def join_list(values: Iterable[Any]) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return "; ".join(cleaned)


def derive_agency_name(fp_value: Any, fu_value: Any) -> str:
    agencies = [str(value).strip() for value in coerce_list(fp_value) if str(value).strip()]
    if agencies:
        return "; ".join(agencies)
    fallback = []
    for funding_entry in coerce_list(fu_value):
        agency_name = str(funding_entry).split(":", 1)[0].strip()
        if agency_name:
            fallback.append(agency_name)
    return "; ".join(fallback)


def process_piece(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["UID", "agency_name", "funding", "funding_text", "funding_data"])

    rows = pd.DataFrame(
        {
            "UID": frame["UT"].astype(str),
            "agency_name": [derive_agency_name(fp, fu) for fp, fu in zip(frame["FP"], frame["FU"])],
            "funding": [join_list(coerce_list(value)) for value in frame["FU"]],
            "funding_text": frame["FX"].fillna("").astype(str),
            "funding_data": [join_list(coerce_list(value)) for value in frame["FUR"]],
        }
    )
    return rows[rows["agency_name"] != ""]


def iter_piece_results(source: Path | str, columns: Sequence[str], workers: int) -> Iterator[pd.DataFrame]:
    reader = PieceReader(source, columns=columns)
    with ThreadPoolExecutor(max_workers=default_worker_count(workers)) as executor:
        yield from reader.process(process_piece, executor=executor, keep_order=True, unordered=False)


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    frames = [frame for frame in iter_piece_results(args.wos_extra, ["UT", "FP", "FU", "FX", "FUR"], args.workers) if not frame.empty]
    if not frames:
        raise RuntimeError("No funding rows were produced from the provided WoS extra parquet file.")

    result = pd.concat(frames, ignore_index=True)
    result.to_feather(args.output)
    print(args.output)


if __name__ == "__main__":
    main()