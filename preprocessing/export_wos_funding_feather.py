from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None  # type: ignore[assignment]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export the legacy WOS funding feather expected by scripts/match_stage1.py from WoS processed parquet."
    )
    parser.add_argument(
        "--wos-main",
        type=Path,
        default=Path("data/WoS_2025_main.parquet"),
        help="Processed WOS main parquet. WOSRaw writes UT, FP, FU, and FX here.",
    )
    parser.add_argument(
        "--wos-extra",
        type=Path,
        default=None,
        help="Deprecated compatibility option. FUR lives in extra, but the funding feather is built from main.",
    )
    parser.add_argument("--output", type=Path, default=Path("data/WOS_TSV_FUNDING_2025.feather"))
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
        return min(23, max(1, detected - 1))
    if workers == -1:
        return min(23, detected)
    return min(23, max(1, int(workers)))


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

    fp = frame["FP"] if "FP" in frame.columns else pd.Series("", index=frame.index)
    fu = frame["FU"] if "FU" in frame.columns else pd.Series("", index=frame.index)
    fx = frame["FX"] if "FX" in frame.columns else pd.Series("", index=frame.index)
    fur = frame["FUR"] if "FUR" in frame.columns else pd.Series("", index=frame.index)

    rows = pd.DataFrame(
        {
            "UID": frame["UT"].astype(str),
            "agency_name": [derive_agency_name(fp_value, fu_value) for fp_value, fu_value in zip(fp, fu)],
            "funding": [join_list(coerce_list(value)) for value in fu],
            "funding_text": fx.fillna("").astype(str),
            "funding_data": [join_list(coerce_list(value)) for value in fur],
        }
    )
    return rows[rows["agency_name"] != ""]


def iter_piece_results(source: Path | str, columns: Sequence[str], workers: int) -> Iterator[pd.DataFrame]:
    metadata = pq.ParquetFile(source)
    available_columns = [column for column in columns if column in metadata.schema_arrow.names]
    missing_columns = sorted(set(columns) - set(available_columns))
    if missing_columns:
        print(f"Skipping unavailable columns in {source}: {', '.join(missing_columns)}")

    reader = PieceReader(source, columns=available_columns)
    iterator = reader.process(process_piece, ncpu=default_worker_count(workers), keep_order=False)
    if tqdm is not None:
        iterator = tqdm(
            iterator,
            total=metadata.metadata.num_row_groups,
            desc=f"Exporting {Path(source).name}",
            unit="task",
        )
    yield from iterator


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    source = args.wos_main
    wrote_any = False
    writer: ipc.RecordBatchFileWriter | None = None
    with pa.OSFile(str(args.output), "wb") as sink:
        try:
            for frame in iter_piece_results(source, ["UT", "FP", "FU", "FX"], args.workers):
                if frame.empty:
                    continue
                table = pa.Table.from_pandas(frame, preserve_index=False)
                if writer is None:
                    writer = ipc.new_file(sink, table.schema)
                writer.write_table(table)
                wrote_any = True
        finally:
            if writer is not None:
                writer.close()

    if not wrote_any:
        args.output.unlink(missing_ok=True)
        raise RuntimeError("No funding rows were produced from the provided WoS main parquet file.")

    print(args.output)


if __name__ == "__main__":
    main()
