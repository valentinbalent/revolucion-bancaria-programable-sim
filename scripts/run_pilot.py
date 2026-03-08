#!/usr/bin/env python3
"""
scripts/run_pilot.py

Run a pilot experiment: quick S0 + S2 with 2 seeds, aggregate, build
results pack, and write PILOT_REPORT.md + RUN_META.json.

Defaults:
  config   : configs/thesis_pilot.json  (if it exists, else configs/thesis_smoke.json)
  scenarios: S0 S2
  seeds    : 42 99

Usage:
    python scripts/run_pilot.py
    python scripts/run_pilot.py --config configs/thesis_smoke.json --scenarios S0 S1 S2
    python scripts/run_pilot.py --seeds 42 99 123
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


def _find_python() -> str:
    """Return the current interpreter path."""
    return sys.executable


def _run(args: List[str], label: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"  cmd: {' '.join(args)}")
    print(f"{'=' * 60}\n")
    p = subprocess.run(args, text=True)
    if p.returncode != 0:
        raise SystemExit(f"[FAIL] {label} exited with code {p.returncode}")


def _default_config() -> str:
    pilot = "configs/thesis_pilot.json"
    smoke = "configs/thesis_smoke.json"
    if os.path.exists(pilot):
        return pilot
    if os.path.exists(smoke):
        return smoke
    raise SystemExit(
        "No config found. Provide --config or create "
        "configs/thesis_pilot.json or configs/thesis_smoke.json."
    )


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "UNKNOWN"


def _git_dirty() -> bool:
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"], text=True
        )
        return bool(out.strip())
    except Exception:
        return False


def _code_hash() -> str:
    """Compute a SHA-256 over the source modules (same list as run_experiment)."""
    import hashlib

    h = hashlib.sha256()
    paths = [
        "src/revolucion_bancaria_programable_sim/config.py",
        "src/revolucion_bancaria_programable_sim/rng.py",
        "src/revolucion_bancaria_programable_sim/model_abm.py",
        "src/revolucion_bancaria_programable_sim/model_de.py",
        "src/revolucion_bancaria_programable_sim/metrics.py",
        "scripts/run_experiment.py",
    ]
    for raw in sorted(paths):
        p = Path(raw)
        if p.is_file():
            h.update(p.read_bytes())
    return h.hexdigest()


def _config_hash(config_path: str) -> str:
    from revolucion_bancaria_programable_sim.config import (
        build_config,
        stable_hash_dict,
    )

    cfg = build_config(config_path)
    return stable_hash_dict(cfg)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run a pilot experiment.")
    ap.add_argument(
        "--config",
        default=None,
        help="Config override JSON. Default: thesis_pilot.json or thesis_smoke.json.",
    )
    ap.add_argument(
        "--scenarios",
        nargs="+",
        default=["S0", "S2"],
        help="Scenarios to run (default: S0 S2).",
    )
    ap.add_argument(
        "--seeds",
        nargs="+",
        type=int,
        default=[42, 99],
        help="Seeds (default: 42 99).",
    )
    ap.add_argument(
        "--runs-root",
        default="runs/pilot",
        help="Directory for run artifacts (default: runs/pilot).",
    )
    ap.add_argument(
        "--results-root",
        default="results/pilot",
        help="Directory for aggregated results (default: results/pilot).",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    config_path = args.config or _default_config()
    py = _find_python()
    seeds_str = [str(s) for s in args.seeds]

    print(f"Pilot config : {config_path}")
    print(f"Scenarios    : {args.scenarios}")
    print(f"Seeds        : {args.seeds}")
    print(f"Runs root    : {args.runs_root}")
    print(f"Results root : {args.results_root}")

    # --- 1) Validate config ---
    _run(
        [py, "scripts/validate_config.py", config_path],
        "Validate config",
    )

    # --- 2) Run experiments per scenario ---
    for scen in args.scenarios:
        _run(
            [
                py,
                "scripts/run_experiment.py",
                "--scenario",
                scen,
                "--seeds",
                *seeds_str,
                "--runs-root",
                args.runs_root,
                "--config-override",
                config_path,
            ],
            f"Run experiment {scen}",
        )

    # --- 3) Aggregate results ---
    _run(
        [
            py,
            "scripts/aggregate_results.py",
            "--runs-root",
            args.runs_root,
            "--results-root",
            args.results_root,
        ],
        "Aggregate results",
    )

    # --- 4) Build results pack ---
    _run(
        [
            py,
            "scripts/make_results_pack.py",
            "--results-dir",
            args.results_root,
            "--label",
            "PILOT",
        ],
        "Make results pack",
    )

    # --- 5) Write RUN_META.json ---
    results_dir = Path(args.results_root)
    results_dir.mkdir(parents=True, exist_ok=True)

    meta: Dict[str, Any] = {
        "label": "PILOT",
        "config_path": config_path,
        "scenarios": args.scenarios,
        "seeds": args.seeds,
        "runs_root": args.runs_root,
        "results_root": args.results_root,
        "git_commit": _git_commit(),
        "git_dirty": _git_dirty(),
        "code_hash": _code_hash(),
        "config_hash": _config_hash(config_path),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "python": sys.version.split()[0],
    }

    meta_path = results_dir / "RUN_META.json"
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nWrote: {meta_path}")

    # --- 6) Write PILOT_REPORT.md ---
    report_lines = [
        "# Pilot Report\n",
        "\n",
        f"**Date**: {meta['timestamp']}\n",
        f"**Config**: `{config_path}`\n",
        f"**Git commit**: `{meta['git_commit']}`\n",
        f"**Git dirty**: {meta['git_dirty']}\n",
        f"**Code hash**: `{meta['code_hash'][:16]}...`\n",
        f"**Config hash**: `{meta['config_hash'][:16]}...`\n",
        "\n",
        "## Parameters\n",
        "\n",
        f"- Scenarios: {', '.join(args.scenarios)}\n",
        f"- Seeds: {args.seeds}\n",
        f"- Runs root: `{args.runs_root}`\n",
        f"- Results root: `{args.results_root}`\n",
        "\n",
        "## Outputs\n",
        "\n",
        "| File | Description |\n",
        "|------|-------------|\n",
        "| `kpi_aggregate.csv` | Run-level flat KPIs |\n",
        "| `paired_seed_level.csv` | A/B paired deltas per seed |\n",
        "| `T1_KPIs_core_AvsB.csv` | Core KPIs summary |\n",
        "| `T2_IFS_components.csv` | IFS component scores |\n",
        "| `T3_NI.csv` | No-inferiority verdicts |\n",
        "| `T4_Aux_mechanisms.csv` | Auxiliary mechanism KPIs |\n",
        "| `F1.png` .. `F5.png` | Delta histograms |\n",
        "| `RUN_META.json` | Full reproducibility metadata |\n",
        "| `manifest_sha256.json` | SHA-256 integrity manifest |\n",
        "\n",
        "## Purpose\n",
        "\n",
        "This pilot run validates the end-to-end pipeline and provides\n",
        "initial IFS bounds for the design envelope calibration.\n",
        "Results from pilot runs are **not** used as thesis outputs.\n",
        "\n",
        "## Next Steps\n",
        "\n",
        "1. Review IFS bounds from pilot (p1/p99 + 20% safety margin)\n",
        "2. Freeze bounds into `configs/thesis_v1.json`\n",
        "3. Run full thesis batch (30 seeds, S0+S1+S2)\n",
    ]

    report_path = results_dir / "PILOT_REPORT.md"
    report_path.write_text("".join(report_lines), encoding="utf-8")
    print(f"Wrote: {report_path}")

    print("\n" + "=" * 60)
    print("  PILOT COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
