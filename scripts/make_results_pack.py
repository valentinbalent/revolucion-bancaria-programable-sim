#!/usr/bin/env python3

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def qstats(series: pd.Series) -> dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"median": None, "p10": None, "p90": None, "n": 0}
    return {
        "median": float(s.median()),
        "p10": float(s.quantile(0.10)),
        "p90": float(s.quantile(0.90)),
        "n": int(s.shape[0]),
    }


def metric_base_from_a(col_a: str) -> str:
    if not col_a.endswith("_A"):
        raise ValueError(f"Column does not end with _A: {col_a}")
    return col_a[:-2]


def agg_triplet(df: pd.DataFrame, cols_a: list) -> pd.DataFrame:
    rows = []
    for col_a in cols_a:
        base = metric_base_from_a(col_a)
        col_b = f"{base}_B"
        col_d = f"{base}_DELTA"

        if col_b not in df.columns or col_d not in df.columns:
            continue

        for scen, sub in df.groupby("scenario"):
            s_a = qstats(sub[col_a])
            s_b = qstats(sub[col_b])
            s_d = qstats(sub[col_d])

            rows.append(
                {
                    "scenario": str(scen),
                    "metric": base,
                    "A_median": s_a["median"],
                    "A_p10": s_a["p10"],
                    "A_p90": s_a["p90"],
                    "B_median": s_b["median"],
                    "B_p10": s_b["p10"],
                    "B_p90": s_b["p90"],
                    "DELTA_median": s_d["median"],
                    "DELTA_p10": s_d["p10"],
                    "DELTA_p90": s_d["p90"],
                    "n_seeds": s_d["n"],
                }
            )
    return pd.DataFrame(rows)


def fig_hist(series: pd.Series, title: str, xlabel: str, outpath: Path) -> None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return
    plt.figure()
    plt.hist(s, bins=20)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def copy_if_exists(src: Path, dst: Path) -> bool:
    if src.exists():
        dst.write_bytes(src.read_bytes())
        return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--label", default="RUN")
    args = ap.parse_args()

    outdir = Path(args.results_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    paired = outdir / "paired_seed_level.csv"
    if not paired.exists():
        raise SystemExit(f"Missing {paired}. Primero corre aggregate_results.py")

    df = pd.read_csv(paired)

    # ---------- T2 / T3 from existing aggregate outputs if present ----------
    t2_path = outdir / "T2_IFS_components.csv"
    t3_path = outdir / "T3_NI.csv"

    copied_t2 = copy_if_exists(outdir / "table_5_4_ifs.csv", t2_path)
    copied_t3 = copy_if_exists(outdir / "table_5_5_no_inferiority.csv", t3_path)

    # fallback T2 if table_5_4_ifs.csv does not exist
    if not copied_t2:
        ifs_cols_a = [
            c
            for c in df.columns
            if c.endswith("_A")
            and (
                c.startswith("IFS_total_100")
                or c.startswith("IFS_XBPAY_100")
                or c.startswith("IFS_PVP_100")
                or c.startswith("IFS_DVP_100")
                or c.startswith("IFScomp_")
            )
        ]
        t2 = agg_triplet(df, ifs_cols_a)

        # split flow / metric when possible
        flow_vals = []
        metric_vals = []
        for m in t2["metric"]:
            if m == "IFS_total_100":
                flow_vals.append("TOTAL")
                metric_vals.append("IFS_total_100")
            elif m.startswith("IFS_XBPAY_100"):
                flow_vals.append("XBPAY")
                metric_vals.append("IFS_XBPAY_100")
            elif m.startswith("IFS_PVP_100"):
                flow_vals.append("PVP")
                metric_vals.append("IFS_PVP_100")
            elif m.startswith("IFS_DVP_100"):
                flow_vals.append("DVP")
                metric_vals.append("IFS_DVP_100")
            elif m.startswith("IFScomp_"):
                parts = m.split("_")
                flow_vals.append(parts[1] if len(parts) > 2 else "TOTAL")
                metric_vals.append(m)
            else:
                flow_vals.append("UNKNOWN")
                metric_vals.append(m)

        t2.insert(1, "flow", flow_vals)
        t2["metric"] = metric_vals
        t2.to_csv(t2_path, index=False)

    # fallback T3 if table_5_5_no_inferiority.csv does not exist
    if not copied_t3:
        ni_cols = [c for c in df.columns if c.endswith("_DELTA") and "_NI-" in c]
        rows = []
        for col in ni_cols:
            for scen, sub in df.groupby("scenario"):
                s = qstats(sub[col])
                rows.append(
                    {
                        "scenario": str(scen),
                        "metric": col[:-6],
                        "DELTA_median": s["median"],
                        "DELTA_p10": s["p10"],
                        "DELTA_p90": s["p90"],
                        "n_seeds": s["n"],
                    }
                )
        pd.DataFrame(rows).to_csv(t3_path, index=False)

    # ---------- T1 core KPIs ----------
    t1_metric_names = [
        "AtomicityViolationRate",
        "FinalityFailureRate",
        "K1_latency_median",
        "K1_latency_p90",
        "K2_cost_total_mean",
        "K3_lock_exposure_total_mean",
        "K4_checkpoints_mean",
        "K5_op_fail_rate",
        "K5_recovery_time",
        "K5_throughput_drop",
        "K6_cycle_time_p90",
        "K6_fail_rate",
        "K6_spread_proxy",
        "STP_rate",
        "trace_score_mean",
    ]
    flows = ["XBPAY", "PVP", "DVP"]
    t1_cols_a = []
    for flow in flows:
        for name in t1_metric_names:
            col_a = f"{flow}_{name}_A"
            if col_a in df.columns:
                t1_cols_a.append(col_a)

    t1 = agg_triplet(df, t1_cols_a)
    if not t1.empty:
        t1.insert(1, "flow", t1["metric"].apply(lambda x: x.split("_", 1)[0]))
    t1.to_csv(outdir / "T1_KPIs_core_AvsB.csv", index=False)

    # ---------- T4 auxiliary mechanisms ----------
    t4_metric_names = [
        "fx_share",
        "K1_queue_mean",
        "K1_repair_mean",
        "K1_hold_mean",
        "K3_coll_lock_exposure_mean",
        "K3_liq_lock_exposure_mean",
        "K4_rescreen_rate",
        "K4_repair_due_missing_rate",
        "trace_score_mean",
    ]
    t4_cols_a = []
    for flow in flows:
        for name in t4_metric_names:
            col_a = f"{flow}_{name}_A"
            if col_a in df.columns:
                t4_cols_a.append(col_a)

    t4 = agg_triplet(df, t4_cols_a)
    if not t4.empty:
        t4.insert(1, "flow", t4["metric"].apply(lambda x: x.split("_", 1)[0]))
    t4.to_csv(outdir / "T4_Aux_mechanisms.csv", index=False)

    # ---------- Figures ----------
    d = df[df["scenario"].astype(str) == "S0"].copy()
    if d.empty:
        d = df.copy()

    fig_hist(
        d["IFS_total_100_DELTA"] if "IFS_total_100_DELTA" in d.columns else pd.Series(dtype=float),
        f"F1 — ΔIFS_total_100 (B−A) | {args.label}",
        "ΔIFS_total_100",
        outdir / "F1.png",
    )
    fig_hist(
        d["IFS_XBPAY_100_DELTA"] if "IFS_XBPAY_100_DELTA" in d.columns else pd.Series(dtype=float),
        f"F2 — ΔIFS_XBPAY_100 (B−A) | {args.label}",
        "ΔIFS_XBPAY_100",
        outdir / "F2.png",
    )
    fig_hist(
        d["IFS_PVP_100_DELTA"] if "IFS_PVP_100_DELTA" in d.columns else pd.Series(dtype=float),
        f"F3 — ΔIFS_PVP_100 (B−A) | {args.label}",
        "ΔIFS_PVP_100",
        outdir / "F3.png",
    )
    fig_hist(
        d["IFS_DVP_100_DELTA"] if "IFS_DVP_100_DELTA" in d.columns else pd.Series(dtype=float),
        f"F4 — ΔIFS_DVP_100 (B−A) | {args.label}",
        "ΔIFS_DVP_100",
        outdir / "F4.png",
    )
    fig_hist(
        d["XBPAY_K1_latency_p90_DELTA"] if "XBPAY_K1_latency_p90_DELTA" in d.columns else pd.Series(dtype=float),
        f"F5 — ΔXBPAY_K1_latency_p90 (B−A) | {args.label}",
        "ΔXBPAY_K1_latency_p90",
        outdir / "F5.png",
    )

    # ---------- RESULTS_PACK.md ----------
    md = []
    md.append(f"# RESULTS_PACK — {args.label}\n\n")
    md.append("## Outputs\n")
    md.append("- `T1_KPIs_core_AvsB.csv`\n")
    md.append("- `T2_IFS_components.csv`\n")
    md.append("- `T3_NI.csv`\n")
    md.append("- `T4_Aux_mechanisms.csv`\n")
    md.append("- `F1.png` .. `F5.png`\n\n")

    md.append("## Quick read\n")
    t2_df = pd.read_csv(t2_path)
    if "metric" in t2_df.columns and "scenario" in t2_df.columns:
        m = t2_df[
            (t2_df["scenario"].astype(str) == "S0")
            & (t2_df["metric"].astype(str) == "IFS_total_100")
        ].head(1)
        if len(m):
            row = m.iloc[0]
            md.append(
                f"- S0: ΔIFS_total_100 median={row['DELTA_median']:.4f} "
                f"(P10={row['DELTA_p10']:.4f}, P90={row['DELTA_p90']:.4f})\n"
            )

    (outdir / "RESULTS_PACK.md").write_text("".join(md), encoding="utf-8")

    # ---------- Manifest ----------
    items = []
    for p in sorted(outdir.glob("*")):
        if p.suffix.lower() in (".csv", ".png", ".md", ".json"):
            items.append(
                {
                    "path": p.name,
                    "sha256": sha256_file(p),
                    "bytes": p.stat().st_size,
                }
            )
    (outdir / "manifest_sha256.json").write_text(json.dumps(items, indent=2), encoding="utf-8")

    print("Wrote Results Pack to:", outdir)


if __name__ == "__main__":
    main()
