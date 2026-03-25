from __future__ import annotations

import ast
import html
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

import numpy as np
import pandas as pd


def _load_piece_reader():
    try:
        from parcake import PieceReader
        return PieceReader
    except ModuleNotFoundError:
        repo_root = Path(__file__).resolve().parents[1]
        vendored_src = repo_root / "preprocessing" / "libraries" / "parcake" / "src"
        if vendored_src.exists() and str(vendored_src) not in sys.path:
            sys.path.insert(0, str(vendored_src))
        from parcake import PieceReader
        return PieceReader


PieceReader = _load_piece_reader()


def normalize_agency_name(value: Any) -> str:
    if pd.isna(value):
        return ""
    normalized = html.unescape(str(value)).replace("&", "and").replace("''", "'")
    return " ".join(normalized.strip().lower().split())


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


def default_worker_count(workers: Optional[int]) -> int:
    cpu_count = os.cpu_count() or 1
    if workers in (None, 0):
        return max(1, cpu_count - 1)
    if workers == -1:
        return cpu_count
    return max(1, int(workers))


def iter_piece_results(
    source: Path | str,
    *,
    columns: Sequence[str],
    processor: Callable[..., Any],
    workers: Optional[int] = None,
    keep_order: bool = True,
) -> Iterator[Any]:
    reader = PieceReader(source, columns=columns)
    with ThreadPoolExecutor(max_workers=default_worker_count(workers)) as executor:
        yield from reader.process(
            processor,
            executor=executor,
            keep_order=keep_order,
            unordered=not keep_order,
        )