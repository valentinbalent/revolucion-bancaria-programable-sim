import glob
import json
import os
import subprocess
import sys


def run_cmd(args):
    p = subprocess.run(args, capture_output=True, text=True)
    assert p.returncode == 0, (
        f"Command failed: {' '.join(args)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
    )
    return p.stdout


def test_smoke_s0_two_seeds_creates_artifacts(tmp_path):
    runs_root = tmp_path / "runs"
    runs_root.mkdir()

    run_cmd(
        [
            sys.executable,
            "scripts/run_experiment.py",
            "--scenario",
            "S0",
            "--seeds",
            "1",
            "2",
            "--runs-root",
            str(runs_root),
        ]
    )

    index_path = runs_root / "S0" / "index.json"
    assert index_path.exists()

    required = ["tx_log.csv", "event_log.csv", "kpi_run.json", "config_used.json"]

    for seed in [1, 2]:
        for world in ["A", "B"]:
            run_dirs = glob.glob(
                str(runs_root / "S0" / f"seed={seed}" / f"world={world}" / "run_id=*")
            )
            assert len(run_dirs) == 1
            rd = run_dirs[0]
            for f in required:
                assert os.path.exists(os.path.join(rd, f))


def test_kpi_ifs_bounds(tmp_path):
    runs_root = tmp_path / "runs"
    runs_root.mkdir()

    run_cmd(
        [
            sys.executable,
            "scripts/run_experiment.py",
            "--scenario",
            "S0",
            "--seeds",
            "3",
            "4",
            "--runs-root",
            str(runs_root),
        ]
    )

    kpi_paths = glob.glob(
        str(runs_root / "S0" / "seed=*" / "world=*" / "run_id=*" / "kpi_run.json")
    )
    assert len(kpi_paths) == 4

    for kp in kpi_paths:
        with open(kp, encoding="utf-8") as f:
            obj = json.load(f)
        ifs = obj["ifs"]
        assert 0.0 <= float(ifs["IFS_total_100"]) <= 100.0
        for _flow, dd in ifs["flows"].items():
            assert 0.0 <= float(dd["IFS_100"]) <= 100.0
