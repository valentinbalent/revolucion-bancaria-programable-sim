#!/usr/bin/env python3
"""
Aggregate results for SpecPack v1.0 runs.

Reads per-run artifacts:
  runs/<scenario>/seed=<seed>/world=<A|B>/run_id=<id>/{kpi_run.json,config_used.json}

Outputs (CSV):
  results/kpi_aggregate.csv                 (run-level flat)
  results/paired_seed_level.csv             (A/B paired + deltas per seed)
  results/table_5_2_kpis_xbpay.csv          (scenario-level summaries)
  results/table_5_3_kpis_pvp_dvp.csv
  results/table_5_4_ifs.csv
  results/table_5_5_no_inferiority.csv
"""

import argparse
import glob
import json
import os
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

PATH_RE = re.compile(
    r"(?P<scenario>S[0-2])"
    r"/seed=(?P<seed>\d+)"
    r"/world=(?P<world>[AB])"
    r"/run_id=(?P<run_id>[0-9a-f]+)"
    r"/kpi_run\.json$"
)


FLOWS = ["XBPAY", "PVP", "DVP"]
COMPS = ["L", "C", "Q", "D", "R", "F"]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-root", default="runs")
    ap.add_argument("--results-root", default="results")
    ap.add_argument("--scenario", default=None, help="If set, only aggregate this scenario (S0/S1/S2).")
    return ap.parse_args()


def _percentiles(x: pd.Series) -> dict:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if x.empty:
        return {"median": np.nan, "p10": np.nan, "p90": np.nan, "n": 0}
    return {
        "median": float(np.nanpercentile(x.values, 50)),
        "p10": float(np.nanpercentile(x.values, 10)),
        "p90": float(np.nanpercentile(x.values, 90)),
        "n": int(x.shape[0]),
    }


def _load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def flatten_run(kpi_path: str) -> dict:
    norm = kpi_path.replace("\\", "/")
    m = PATH_RE.search(norm)
    if not m:
        raise ValueError(f"Unrecognized kpi_run.json path layout: {kpi_path}")

    scenario = m.group("scenario")
    seed = int(m.group("seed"))
    world = m.group("world")
    run_id = m.group("run_id")

    obj = _load_json(kpi_path)
    ifs = obj["ifs"]
    kpis = obj["kpis"]
    ni = obj.get("no_inferiority", {})

    cfg_path = kpi_path.replace("kpi_run.json", "config_used.json")
    cfg_used = _load_json(cfg_path)
    cfg = cfg_used.get("config", {})
    v = cfg.get("ifs", {}).get("flow_weights", {})

    row: dict = {
        "scenario": scenario,
        "seed": seed,
        "world": world,
        "run_id": run_id,
        "git_commit": cfg_used.get("git_commit"),
        "git_dirty": cfg_used.get("git_dirty"),
        "python_version": (cfg_used.get("runtime") or {}).get("python_version"),
        "platform": (cfg_used.get("runtime") or {}).get("platform"),
        "IFS_total_100": float(ifs.get("IFS_total_100", np.nan)),
    }

    # Per-flow IFS + component norms (scaled to 0..100 for table-readiness)
    for flow in FLOWS:
        fpack = ifs["flows"][flow]
        row[f"IFS_{flow}_100"] = float(fpack.get("IFS_100", np.nan))
        zn = fpack.get("Z_norm", {})
        for c in COMPS:
            row[f"IFScomp_{flow}_{c}_100"] = 100.0 * float(zn.get(c, np.nan))

    # Aggregate component totals across flows using flow_weights v_*
    # If v_* missing, we still compute with equal weights as a fallback (explicit).
    if all(f"v_{f}" in v for f in FLOWS):
        weights = {f: float(v[f"v_{f}"]) for f in FLOWS}
    else:
        weights = {f: 1.0 / len(FLOWS) for f in FLOWS}
    for c in COMPS:
        tot = 0.0
        for f in FLOWS:
            tot += weights[f] * (row[f"IFScomp_{f}_{c}_100"] / 100.0)
        row[f"IFScomp_TOTAL_{c}_100"] = 100.0 * float(tot)

    # Flow KPIs (prefix by flow)
    for flow, fk in kpis["flows"].items():
        for k, v_ in fk.items():
            # booleans -> int
            if isinstance(v_, bool):
                v_ = int(v_)
            row[f"{flow}_{k}"] = v_

    # Also include NI passes from kpi_run.json (per-world; auxiliary)
    for flow in FLOWS:
        fni = (ni.get("flows") or {}).get(flow, {})
        for ni_key in ["NI-1", "NI-2", "NI-3"]:
            block = fni.get(ni_key, {})
            if "pass" in block:
                row[f"{flow}_{ni_key}_pass_world"] = int(bool(block["pass"]))

    return row


def discover_kpis(runs_root: str, scenario: Optional[str]) -> list[str]:
    pattern = os.path.join(runs_root, "**", "kpi_run.json")
    paths = glob.glob(pattern, recursive=True)
    if scenario:
        paths = [p for p in paths if f"/{scenario}/" in p.replace("\\", "/")]
    return sorted(paths)


def make_paired(run_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Split A/B and align on (scenario, seed)
    a = run_df[run_df["world"] == "A"].copy().set_index(["scenario", "seed"])
    b = run_df[run_df["world"] == "B"].copy().set_index(["scenario", "seed"])

    common = a.index.intersection(b.index)
    a = a.loc[common].sort_index()
    b = b.loc[common].sort_index()

    # keep run_id + provenance
    meta_cols = ["run_id", "git_commit", "git_dirty", "python_version", "platform"]

    # numeric columns for deltas
    drop_cols = ["world"] + meta_cols
    num_cols = [c for c in a.columns if c not in drop_cols and pd.api.types.is_numeric_dtype(a[c])]

    deltas = (b[num_cols] - a[num_cols]).add_suffix("_DELTA")
    paired = pd.concat(
        [
            a[meta_cols].add_suffix("_A"),
            b[meta_cols].add_suffix("_B"),
            a[num_cols].add_suffix("_A"),
            b[num_cols].add_suffix("_B"),
            deltas,
        ],
        axis=1,
    ).reset_index()

    return paired, a.reset_index(), b.reset_index()


def summarize_table(paired: pd.DataFrame, metrics: list[str], out_path: str) -> None:
    rows = []
    for scenario in sorted(paired["scenario"].unique()):
        sub = paired[paired["scenario"] == scenario]
        for metric in metrics:
            a_col = f"{metric}_A"
            b_col = f"{metric}_B"
            d_col = f"{metric}_DELTA"
            if a_col not in sub.columns or b_col not in sub.columns or d_col not in sub.columns:
                continue

            pa = _percentiles(sub[a_col])
            pb = _percentiles(sub[b_col])
            pd_ = _percentiles(sub[d_col])

            flow = "TOTAL"
            name = metric
            if "_" in metric:
                parts = metric.split("_", 1)
                if parts[0] in FLOWS:
                    flow = parts[0]
                    name = parts[1]

            rows.append(
                {
                    "scenario": scenario,
                    "flow": flow,
                    "metric": name,
                    "A_median": pa["median"],
                    "A_p10": pa["p10"],
                    "A_p90": pa["p90"],
                    "B_median": pb["median"],
                    "B_p10": pb["p10"],
                    "B_p90": pb["p90"],
                    "DELTA_median": pd_["median"],
                    "DELTA_p10": pd_["p10"],
                    "DELTA_p90": pd_["p90"],
                    "n_seeds": pa["n"],
                }
            )

    out = pd.DataFrame(rows)
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")


def compute_paired_ni(paired: pd.DataFrame, runs_root: str, scenario: Optional[str]) -> pd.DataFrame:
    """
    Paired NI: evaluates NI criteria on deltas (B-A) per seed.

    Margins read from any config_used.json under the scenario.
    """
    # Find a config_used.json to read margins
    cfg_paths = glob.glob(os.path.join(runs_root, "**", "config_used.json"), recursive=True)
    if scenario:
        cfg_paths = [p for p in cfg_paths if f"/{scenario}/" in p.replace("\\", "/")]
    if not cfg_paths:
        raise ValueError("No config_used.json found to read NI margins.")

    cfg_used = _load_json(sorted(cfg_paths)[0])
    cfg = cfg_used.get("config", {})
    margins = (cfg.get("ni", {}) or {}).get("margins", {})

    d_fin = float(margins.get("Δ_NI_fin_f", np.nan))
    eps_atomic = float(margins.get("ε_atomic_f", np.nan))
    d_ops = float(margins.get("Δ_NI_ops_f", np.nan))
    d_rec = float(margins.get("Δ_NI_rec_f", np.nan))
    d_trace = float(margins.get("Δ_NI_trace_f", np.nan))
    d_backlog = float(margins.get("Δ_NI_backlog_f", np.nan))

    rows = []
    for scenario_id in sorted(paired["scenario"].unique()):
        sub = paired[paired["scenario"] == scenario_id]
        for flow in FLOWS:
            # Required columns
            fin = f"{flow}_FinalityFailureRate"
            atomic = f"{flow}_AtomicityViolationRate"
            drop = f"{flow}_K5_throughput_drop"
            rec = f"{flow}_K5_recovery_time"
            trace = f"{flow}_trace_score_mean"
            stp = f"{flow}_STP_rate"

            needed = [
                f"{fin}_A", f"{fin}_B",
                f"{atomic}_A", f"{atomic}_B",
                f"{drop}_A", f"{drop}_B",
                f"{rec}_A", f"{rec}_B",
                f"{trace}_A", f"{trace}_B",
                f"{stp}_A", f"{stp}_B",
            ]
            if any(c not in sub.columns for c in needed):
                continue

            # deltas
            delta_fin = pd.to_numeric(sub[f"{fin}_B"], errors="coerce") - pd.to_numeric(sub[f"{fin}_A"], errors="coerce")
            delta_atomic = pd.to_numeric(sub[f"{atomic}_B"], errors="coerce") - pd.to_numeric(sub[f"{atomic}_A"], errors="coerce")
            delta_drop = pd.to_numeric(sub[f"{drop}_B"], errors="coerce") - pd.to_numeric(sub[f"{drop}_A"], errors="coerce")
            delta_rec = pd.to_numeric(sub[f"{rec}_B"], errors="coerce") - pd.to_numeric(sub[f"{rec}_A"], errors="coerce")
            delta_trace = pd.to_numeric(sub[f"{trace}_B"], errors="coerce") - pd.to_numeric(sub[f"{trace}_A"], errors="coerce")

            # backlog proxy: hold_rate = 1 - STP
            hold_a = 1.0 - pd.to_numeric(sub[f"{stp}_A"], errors="coerce")
            hold_b = 1.0 - pd.to_numeric(sub[f"{stp}_B"], errors="coerce")
            delta_backlog = hold_b - hold_a

            # NI pass flags per seed
            ni1 = (delta_fin <= d_fin) & (delta_atomic <= eps_atomic)
            ni2 = (delta_drop <= d_ops) & (delta_rec <= d_rec)
            ni3 = (delta_trace >= (-d_trace)) & (delta_backlog <= d_backlog)

            rows.append(
                {
                    "scenario": scenario_id,
                    "flow": flow,
                    "NI-1_pass_rate": float(np.nanmean(ni1.astype(float).values)),
                    "NI-2_pass_rate": float(np.nanmean(ni2.astype(float).values)),
                    "NI-3_pass_rate": float(np.nanmean(ni3.astype(float).values)),
                    "n_seeds": int(sub.shape[0]),
                    "Δ_NI_fin_f": d_fin,
                    "ε_atomic_f": eps_atomic,
                    "Δ_NI_ops_f": d_ops,
                    "Δ_NI_rec_f": d_rec,
                    "Δ_NI_trace_f": d_trace,
                    "Δ_NI_backlog_f": d_backlog,
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    runs_root = args.runs_root
    results_root = args.results_root

    kpi_paths = discover_kpis(runs_root, args.scenario)
    if not kpi_paths:
        raise SystemExit("No kpi_run.json found under runs_root.")

    rows = [flatten_run(p) for p in kpi_paths]
    run_df = pd.DataFrame(rows)

    Path(results_root).mkdir(parents=True, exist_ok=True)
    kpi_agg_path = os.path.join(results_root, "kpi_aggregate.csv")
    run_df.to_csv(kpi_agg_path, index=False)
    print(f"Wrote: {kpi_agg_path}")

    paired, a_df, b_df = make_paired(run_df)
    paired_path = os.path.join(results_root, "paired_seed_level.csv")
    paired.to_csv(paired_path, index=False)
    print(f"Wrote: {paired_path}")

    # ---- Table metric sets (compact but meaningful) ----
    xbpay_metrics = [
        "XBPAY_K1_latency_median",
        "XBPAY_K1_latency_p90",
        "XBPAY_K1_queue_mean",
        "XBPAY_K1_hold_mean",
        "XBPAY_K1_repair_mean",
        "XBPAY_K2_fee_mean",
        "XBPAY_K2_ops_proxy_mean",
        "XBPAY_K2_cost_total_mean",
        "XBPAY_K4_checkpoints_mean",
        "XBPAY_K4_repair_due_missing_rate",
        "XBPAY_K5_op_fail_rate",
        "XBPAY_STP_rate",
        "XBPAY_trace_score_mean",
    ]

    pvp_dvp_metrics = [
        # PvP
        "PVP_K1_latency_median",
        "PVP_K1_latency_p90",
        "PVP_K2_cost_total_mean",
        "PVP_K3_lock_exposure_total_mean",
        "PVP_K4_checkpoints_mean",
        "PVP_K5_op_fail_rate",
        "PVP_AtomicityViolationRate",
        "PVP_STP_rate",
        "PVP_trace_score_mean",
        # DvP
        "DVP_K1_latency_median",
        "DVP_K1_latency_p90",
        "DVP_K2_cost_total_mean",
        "DVP_K3_lock_exposure_total_mean",
        "DVP_K4_checkpoints_mean",
        "DVP_K5_op_fail_rate",
        "DVP_AtomicityViolationRate",
        "DVP_STP_rate",
        "DVP_trace_score_mean",
    ]

    ifs_metrics = [
        "IFS_total_100",
        "IFS_XBPAY_100",
        "IFS_PVP_100",
        "IFS_DVP_100",
    ] + [f"IFScomp_TOTAL_{c}_100" for c in COMPS]

    # Write table-like summaries
    summarize_table(paired, xbpay_metrics, os.path.join(results_root, "table_5_2_kpis_xbpay.csv"))
    summarize_table(paired, pvp_dvp_metrics, os.path.join(results_root, "table_5_3_kpis_pvp_dvp.csv"))
    summarize_table(paired, ifs_metrics, os.path.join(results_root, "table_5_4_ifs.csv"))

    ni_df = compute_paired_ni(paired, runs_root, args.scenario)
    ni_path = os.path.join(results_root, "table_5_5_no_inferiority.csv")
    ni_df.to_csv(ni_path, index=False)
    print(f"Wrote: {ni_path}")


if __name__ == "__main__":
    main()
