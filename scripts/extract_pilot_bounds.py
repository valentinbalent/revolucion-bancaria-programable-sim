#!/usr/bin/env python3
"""
scripts/extract_pilot_bounds.py

Extract IFS design-envelope bounds from pilot run kpi_run.json files.

Reads all kpi_run.json under --runs-root, computes g_Z raw values per
flow/component, derives p01/p99 + 20 % expansion, and writes
design_envelope_bounds.json.

Usage:
    python scripts/extract_pilot_bounds.py \
        --runs-root runs_pilot \
        --output calibration/design_envelope_bounds.json \
        --config configs/thesis_pilot.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import sys
from glob import glob
from typing import Any, Dict, List

from revolucion_bancaria_programable_sim.config import build_config
from revolucion_bancaria_programable_sim.metrics import g_Z_scalar

FLOWS = ["XBPAY", "PVP", "DVP"]
COMPS = ["L", "C", "Q", "D", "R", "F"]
DEFAULT_EXPANSION = 0.20


# Pure-Python percentile (same formula as metrics.py — no numpy needed).
def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return xs[int(k)]
    return xs[f] * (c - k) + xs[c] * (k - f)


def _discover_kpi_files(runs_root: str) -> List[str]:
    pattern = os.path.join(runs_root, "**", "kpi_run.json")
    return sorted(glob(pattern, recursive=True))


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Extract design-envelope bounds from pilot kpi_run.json files."
    )
    ap.add_argument(
        "--runs-root",
        required=True,
        help="Root directory containing pilot run artifacts.",
    )
    ap.add_argument(
        "--output",
        default="calibration/design_envelope_bounds.json",
        help="Output path for bounds JSON (default: calibration/design_envelope_bounds.json).",
    )
    ap.add_argument(
        "--config",
        default=None,
        help="Config override for phi weights (default: configs/thesis_pilot.json).",
    )
    ap.add_argument(
        "--expansion",
        type=float,
        default=DEFAULT_EXPANSION,
        help="Expansion factor for bounds (default: 0.20 = 20%%).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    config_path = args.config or "configs/thesis_pilot.json"
    cfg = build_config(config_path)
    expansion = args.expansion

    kpi_paths = _discover_kpi_files(args.runs_root)
    if not kpi_paths:
        print(
            f"[ERROR] No kpi_run.json found under {args.runs_root}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Found {len(kpi_paths)} kpi_run.json files under {args.runs_root}.")

    # Collect raw g_Z values: raw_values[flow][comp] = [float, ...]
    raw_values: Dict[str, Dict[str, List[float]]] = {
        f: {c: [] for c in COMPS} for f in FLOWS
    }

    for kpi_path in kpi_paths:
        with open(kpi_path, encoding="utf-8") as fh:
            obj = json.load(fh)
        kpis = obj["kpis"]
        for flow in FLOWS:
            fk = kpis["flows"].get(flow)
            if fk is None:
                continue
            for comp in COMPS:
                try:
                    val = g_Z_scalar(cfg, flow, comp, fk)
                    raw_values[flow][comp].append(val)
                except Exception as exc:
                    print(
                        f"  WARN: g_Z_scalar({flow},{comp}) failed for "
                        f"{kpi_path}: {exc}"
                    )

    # Compute bounds
    bounds: Dict[str, Dict[str, Any]] = {}
    for flow in FLOWS:
        bounds[flow] = {}
        for comp in COMPS:
            vals = raw_values[flow][comp]
            n = len(vals)
            if n == 0:
                bounds[flow][comp] = {
                    "p01": None,
                    "p99": None,
                    "z_min": 0.0,
                    "z_max": 1.0,
                    "n_samples": 0,
                    "clipping_notes": "NO DATA — using fallback [0, 1]",
                }
                continue

            p01 = _percentile(vals, 0.01)
            p99 = _percentile(vals, 0.99)
            z_min = max(0.0, p01 * (1.0 - expansion))
            z_max = p99 * (1.0 + expansion)

            notes = ""
            if p01 < 0:
                notes = f"p01 was negative ({p01:.6f}), z_min capped at 0.0"

            bounds[flow][comp] = {
                "p01": round(p01, 8),
                "p99": round(p99, 8),
                "z_min": round(z_min, 8),
                "z_max": round(z_max, 8),
                "n_samples": n,
                "clipping_notes": notes,
            }

            print(
                f"  {flow}.{comp}: p01={p01:.6f}  p99={p99:.6f}  "
                f"-> [{z_min:.6f}, {z_max:.6f}]  (n={n})"
            )

    output = {
        "meta": {
            "generated": datetime.datetime.utcnow().isoformat() + "Z",
            "runs_root": args.runs_root,
            "config_path": config_path,
            "n_kpi_files": len(kpi_paths),
            "expansion_factor": expansion,
            "script": "scripts/extract_pilot_bounds.py",
        },
        "bounds": bounds,
    }

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2, sort_keys=False)
    print(f"\nWrote: {args.output}")

    # Print copy-paste summary for thesis_v1.json
    print("\n--- Copy-paste summary (for thesis_v1.json ifs.bounds) ---")
    for flow in FLOWS:
        for comp in COMPS:
            b = bounds[flow][comp]
            print(
                f'  "{flow}"."{comp}": '
                f'{{"min": {b["z_min"]}, "max": {b["z_max"]}}}'
            )


if __name__ == "__main__":
    main()
