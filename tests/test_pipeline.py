"""Exercise the runner with tiny real subprocesses, without research data or models."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def make_config(tmp_path, fail=False):
    script = tmp_path / "stage with spaces.py"
    script.write_text(
        "from pathlib import Path\nimport sys\n"
        "source, output, code = sys.argv[1:]\n"
        "if source != '-':\n    assert Path(source).read_text() == 'done'\n"
        "if int(code):\n    raise SystemExit(int(code))\n"
        "Path(output).write_text('done')\nprint(Path.cwd())\n"
    )
    config = {
        "variables": {"work": str(tmp_path), "script": str(script)},
        "workflows": {"matching": ["first", "second"]},
        "stages": {
            "first": {"command": ["{python}", "{script}", "-", "{work}/first", "7" if fail else "0"],
                      "outputs": ["{work}/first"]},
            "second": {"command": ["{python}", "{script}", "{work}/first", "{work}/second", "0"],
                       "inputs": ["{work}/first"], "outputs": ["{work}/second"]},
        },
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    return path


def run(tmp_path, config, *args):
    return subprocess.run([sys.executable, str(ROOT / "run_pipeline.py"),
                           "--config", str(config), "--log-dir", str(tmp_path / "logs"), *args],
                          cwd=tmp_path, text=True, capture_output=True)


def test_order_logs_and_paths_with_spaces(tmp_path):
    result = run(tmp_path, make_config(tmp_path))
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "second").read_text() == "done"
    manifest = json.loads(next((tmp_path / "logs").glob("*/run.json")).read_text())
    assert [stage["status"] for stage in manifest["stages"]] == ["complete", "complete"]
    assert manifest["cwd"] == str(ROOT)
    assert str(ROOT) in next((tmp_path / "logs").glob("*/01_first.log")).read_text()


def test_failure_stops_downstream_stage(tmp_path):
    result = run(tmp_path, make_config(tmp_path, fail=True))
    assert result.returncode == 7
    assert not (tmp_path / "second").exists()
    manifest = json.loads(next((tmp_path / "logs").glob("*/run.json")).read_text())
    assert len(manifest["stages"]) == 1
    assert manifest["stages"][0]["status"] == "failed"


def test_dry_run_does_not_create_outputs_or_logs(tmp_path):
    result = run(tmp_path, make_config(tmp_path), "--dry-run")
    assert result.returncode == 0
    assert not (tmp_path / "first").exists()
    assert not (tmp_path / "logs").exists()


def test_resume_requires_prior_inputs(tmp_path):
    config = make_config(tmp_path)
    result = run(tmp_path, config, "--from", "second")
    assert result.returncode == 2
    assert "Missing inputs" in result.stderr
    assert not (tmp_path / "logs").exists()
    (tmp_path / "first").write_text("done")
    result = run(tmp_path, config, "--from", "second")
    assert result.returncode == 0, result.stderr


def test_output_contract_checked_even_after_zero_exit(tmp_path):
    config_path = make_config(tmp_path)
    config = json.loads(config_path.read_text())
    config["stages"]["first"]["outputs"].append("{work}/missing")
    config_path.write_text(json.dumps(config))
    result = run(tmp_path, config_path)
    assert result.returncode == 2
    assert "did not produce" in result.stderr
    assert not (tmp_path / "second").exists()


def test_real_config_propagates_custom_paths(tmp_path):
    result = run(tmp_path, ROOT / "pipeline.json", "--dry-run",
                 "--set", "data_dir=custom data", "--set", "wos_basename=NewSnapshot",
                 "--set", "results_dir=custom results", "--set", "artifacts_dir=custom artifacts",
                 "--set", "matches_basename=new")
    assert result.returncode == 0, result.stderr
    assert "custom data/NewSnapshot_main.parquet" in result.stdout
    assert "custom artifacts/u_agencies_stage2.parquet" in result.stdout
    assert "custom results/grant_institutions_new.parquet" in result.stdout
    assert "data/WoS_2025_main.parquet" not in result.stdout


def test_unknown_override_fails_before_running(tmp_path):
    result = run(tmp_path, make_config(tmp_path), "--set", "typo=value")
    assert result.returncode == 2
    assert not (tmp_path / "first").exists()


def test_fresh_subset_preflight_needs_only_external_inputs(tmp_path):
    from argparse import Namespace
    from run_pipeline import build_plan

    config = json.loads((ROOT / "pipeline.json").read_text())
    for key in ["data_dir", "results_dir", "plotting_dir", "source_results",
                "figure_output", "scope_map", "subset_run", "subset_bundle", "matches"]:
        config["variables"][key] = str(tmp_path / key)
    args = Namespace(overrides=[], stage=None, from_stage=None, to_stage=None,
                     skip=[], workflow="scie-ssci")
    for stage in build_plan(config, args):
        for value in stage["inputs"]:
            path = Path(value)
            if not path.is_relative_to(tmp_path / "subset_run"):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
    path = tmp_path / "fresh-subset.json"
    path.write_text(json.dumps(config))
    result = run(tmp_path, path, "--workflow", "scie-ssci", "--check-inputs")
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "subset_run").exists()
    assert not (tmp_path / "logs").exists()


def test_validation_contract_with_custom_matches(tmp_path):
    import pandas as pd

    matches = tmp_path / "grant_institutions_other.parquet"
    pd.DataFrame({"UT": ["WOS:1"], "display_name": ["Example Funder"], "gid": ["OA_1"],
                  "source": ["funders"], "grant_agency": ["Example"]}).to_parquet(matches)
    main = tmp_path / "Custom_main.parquet"
    pd.DataFrame({"UT": ["WOS:1"], "TI": ["Example Paper"], "DI": ["10.1/example"]}).to_parquet(main)
    result = run(tmp_path, ROOT / "pipeline.json", "--stage", "validation",
                 "--set", f"matches={matches}", "--set", f"data_dir={tmp_path}",
                 "--set", "wos_basename=Custom", "--set", f"results_dir={tmp_path / 'results'}",
                 "--set", "workers=1")
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads(next((tmp_path / "logs").glob("*/run.json")).read_text())
    assert manifest["stages"][0]["status"] == "complete"
    output = Path(manifest["stages"][0]["outputs"][0])
    assert pd.read_csv(output)["UT"].tolist() == ["WOS:1"]
