#!/usr/bin/env python3
# scripts/aggregate_results.py

from __future__ import annotations

import argparse
import glob
import json
import os
from typing import Any, Dict, List

import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-root", default="runs")
    ap.add_argument("--results-root", default="results")
    ap.add_argument("--scenario", default=None, help="If set, only aggregate this scenario (S0/S1/S2).")
    return ap.parse_args()


def flatten_kpi(run_path: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    # Minimal flatten: scenario/world/seed + IFS_total + per-flow IFS_100 + a few KPIs
    cfg = obj.get("kpis", {})
    ifs = obj.get("ifs", {})
    out: Dict[str, Any] = {}

    # Attempt to infer metadata from path: runs/S0/seed=1/world=A/run_id=.../kpi_run.json
    parts = run_path.replace("\\", "/").split("/")
    try:
        out["scenario"] = parts[1]
        out["seed"] = int(parts[2].split("=")[1])
        out["world"] = parts[3].split("=")[1]
        out["run_id"] = parts[4].split("=")[1]
    except Exception:
        out["scenario"] = cfg.get("scenario")
        out["seed"] = None
        out["world"] = cfg.get("world")
        out["run_id"] = None

    out["IFS_total_100"] = float(ifs.get("IFS_total_100", 0.0))

    flows = (ifs.get("flows") or {})
    for f, dd in flows.items():
        out[f"IFS_{f}_100"] = float(dd.get("IFS_100", 0.0))

    # Example KPI pulls (extend later)
    kpis_flows = (cfg.get("flows") or {})
    for f, dd in kpis_flows.items():
        out[f"{f}_K1_latency_p90"] = float(dd.get("K1_latency_p90", 0.0))
        out[f"{f}_K5_op_fail_rate"] = float(dd.get("K5_op_fail_rate", 0.0))
        out[f"{f}_STP_rate"] = float(dd.get("STP_rate", 0.0))
        out[f"{f}_trace_score_mean"] = float(dd.get("trace_score_mean", 0.0))

    return out


def main() -> None:
    args = parse_args()
    pattern = os.path.join(args.runs_root, "**", "kpi_run.json")
    paths = sorted(glob.glob(pattern, recursive=True))
    if args.scenario:
        paths = [p for p in paths if f"/{args.scenario}/" in p.replace("\\", "/")]

    rows: List[Dict[str, Any]] = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        rows.append(flatten_kpi(p, obj))

    os.makedirs(args.results_root, exist_ok=True)
    df = pd.DataFrame(rows)

    out_csv = os.path.join(args.results_root, "kpi_aggregate.csv")
    df.to_csv(out_csv, index=False)

    print(f"Wrote: {out_csv}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
