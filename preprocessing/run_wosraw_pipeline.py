#!/usr/bin/env python3
"""Convert WoS raw deliveries and export the funding Feather with one Python command."""
import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wos_raw_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--base-name", default="WoS_2025")
    parser.add_argument("--ncpu", "--workers", type=int, default=4)
    parser.add_argument("--funding-output", type=Path, default=None)
    args, extra = parser.parse_known_args()
    if args.ncpu < 1:
        parser.error("--ncpu must be positive")
    subprocess.run([
        sys.executable, "-m", "WOSRaw.cli", str(args.wos_raw_dir),
        "--output-dir", str(args.output_dir), "--base-name", args.base_name,
        "--ncpu", str(args.ncpu), "--max-piece-size", "60000",
        "--xml-timeout-seconds", "20000", *extra,
    ], check=True)
    subprocess.run([
        sys.executable, str(Path(__file__).with_name("export_wos_funding_feather.py")),
        "--wos-main", str(args.output_dir / f"{args.base_name}_main.parquet"),
        "--output", str(args.funding_output or args.output_dir / "WOS_TSV_FUNDING_2025.feather"),
        "--workers", str(args.ncpu),
    ], check=True)


if __name__ == "__main__":
    main()
