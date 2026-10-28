#!/usr/bin/env python3
"""Run the ordered workflows in pipeline.json using the active Python environment."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent


def expand_variables(raw: dict) -> dict[str, str]:
    resolved = {"repo": str(ROOT), "python": sys.executable}
    pending = {key: os.path.expanduser(str(value)) for key, value in raw.items()}
    if pending.keys() & resolved.keys():
        raise ValueError("The repo and python variables are provided by the runner.")
    while pending:
        progress = False
        for key, value in list(pending.items()):
            try:
                resolved[key] = value.format_map(resolved)
            except KeyError:
                continue
            del pending[key]
            progress = True
        if not progress:
            raise ValueError(f"Unknown or cyclic variables: {', '.join(pending)}")
    return resolved


def build_plan(config: dict, args: argparse.Namespace) -> list[dict]:
    raw = dict(config["variables"])
    for override in args.overrides:
        key, separator, value = override.partition("=")
        if not separator or key not in raw:
            raise ValueError(f"Expected --set KNOWN_VARIABLE=value, got {override!r}")
        raw[key] = value
    variables = expand_variables(raw)
    if int(variables.get("workers", "1")) < 1:
        raise ValueError("workers must be a positive integer")
    if args.stage:
        if args.from_stage or args.to_stage:
            raise ValueError("Use --stage or --from/--to, not both.")
        names = args.stage
    else:
        names = list(config["workflows"][args.workflow])
        start = names.index(args.from_stage) if args.from_stage else 0
        end = names.index(args.to_stage) + 1 if args.to_stage else len(names)
        if start >= end:
            raise ValueError("--from must precede --to in the selected workflow")
        names = names[start:end]
    unknown_skips = set(args.skip) - set(names)
    if unknown_skips:
        raise ValueError(f"Skipped stages are not in this selection: {sorted(unknown_skips)}")
    plan = []
    for name in names:
        if name in args.skip:
            continue
        stage = config["stages"][name]
        plan.append({"name": name, **{
            key: [str(item).format_map(variables) for item in stage.get(key, [])]
            for key in ("command", "inputs", "outputs")
        }})
    if not plan:
        raise ValueError("No stages selected")
    return plan


def project_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else ROOT / path


def check_inputs(plan: list[dict]) -> None:
    """Check the entire plan, allowing files produced by earlier selected stages."""
    available = set()
    missing = []
    for stage in plan:
        for value in stage["inputs"]:
            path = project_path(value).resolve()
            if path not in available and not path.exists():
                missing.append(f"{stage['name']}: {path}")
        available.update(project_path(p).resolve() for p in stage["outputs"])
    if missing:
        raise FileNotFoundError("Missing inputs (set paths or run their producing stages):\n  " + "\n  ".join(missing))


def execute(plan: list[dict], log_root: Path) -> int:
    log_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_")
    run_dir = Path(tempfile.mkdtemp(prefix=stamp, dir=log_root))
    manifest = {"started_at_utc": datetime.now(timezone.utc).isoformat(),
                "python": sys.executable, "cwd": str(ROOT), "stages": []}
    manifest_path = run_dir / "run.json"
    env = os.environ.copy()
    env.setdefault("MPLBACKEND", "Agg")
    print(f"Logs: {run_dir}", flush=True)
    for index, stage in enumerate(plan, 1):
        # Recheck actual files after each producer; declared outputs alone do not
        # prove that the preceding script successfully created them.
        check_inputs([stage])
        record = {**stage, "status": "running"}
        manifest["stages"].append(record)
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        print(f"[{index}/{len(plan)}] {stage['name']}", flush=True)
        log = run_dir / f"{index:02d}_{stage['name']}.log"
        try:
            with log.open("w", encoding="utf-8") as stream:
                with subprocess.Popen(stage["command"], cwd=ROOT, env=env,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True, encoding="utf-8", errors="replace") as process:
                    try:
                        for line in process.stdout:
                            print(line, end="", flush=True)
                            stream.write(line)
                            stream.flush()
                        code = process.wait()
                    except KeyboardInterrupt:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                        raise
            if code == 0:
                absent = [p for p in stage["outputs"] if not project_path(p).exists()]
                if absent:
                    raise FileNotFoundError(f"{stage['name']} did not produce: {absent}")
            record.update(status="complete" if code == 0 else "failed", returncode=code)
        except (OSError, KeyboardInterrupt) as exc:
            record.update(status="failed", error=str(exc))
            raise
        finally:
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        if code:
            print(f"Stopped at {stage['name']}; see {log}", file=sys.stderr)
            return code if code > 0 else 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "pipeline.json")
    parser.add_argument("--workflow", default="matching")
    parser.add_argument("--stage", action="append", help="Run a named stage; repeat for an explicit sequence.")
    parser.add_argument("--from", dest="from_stage", help="Start at this stage, inclusive.")
    parser.add_argument("--to", dest="to_stage", help="Stop after this stage, inclusive.")
    parser.add_argument("--skip", action="append", default=[], help="Skip a stage whose outputs already exist.")
    parser.add_argument("--set", dest="overrides", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--list", action="store_true", help="List workflow order and configuration variables.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without checking inputs or writing files.")
    parser.add_argument("--check-inputs", action="store_true", help="Check inputs without running or writing files.")
    parser.add_argument("--log-dir", type=Path, default=ROOT / "logs" / "pipeline")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        if args.list:
            for name, stages in config["workflows"].items():
                print(f"{name}: {' -> '.join(stages)}")
            print("\nVariables (override with --set KEY=value):")
            for name, value in config["variables"].items():
                print(f"  {name}={value}")
            return 0
        plan = build_plan(config, args)
        for stage in plan:
            print(f"{stage['name']}: {shlex.join(stage['command'])}", flush=True)
        if args.dry_run and not args.check_inputs:
            return 0
        check_inputs(plan)
        if args.check_inputs:
            print("All external inputs are present; intermediate inputs have producing stages.")
            return 0
        return execute(plan, args.log_dir)
    except (OSError, KeyError, ValueError, TypeError) as exc:
        parser.exit(2, f"Pipeline error: {exc}\n")
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
