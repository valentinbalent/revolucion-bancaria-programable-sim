# metrics.py
"""
Metrics Pack (G):
- KPIs core K1..K6 per flow
- Aux: STP_rate, trace_score
- IFS official {L,C,Q,D,R,F} with g_Z scalar functions + bounds normalization (FIX 3 & FIX 4)
- NI-1/2/3 checks
"""

from __future__ import annotations

import math
from typing import Any, Dict, List


def clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return xs[int(k)]
    return xs[f] * (c - k) + xs[c] * (k - f)


def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def safe_div(a: float, b: float) -> float:
    return a / b if b != 0 else 0.0


def _collect_by_flow(tx_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {"XBPAY": [], "PVP": [], "DVP": []}
    for r in tx_rows:
        out[str(r["flow"])].append(r)
    return out


def compute_kpis(cfg: Dict[str, Any], tx_rows: List[Dict[str, Any]], event_rows: List[Dict[str, Any]], scenario_id: str, world: str) -> Dict[str, Any]:
    by_flow = _collect_by_flow(tx_rows)

    k_cost_exc = float(cfg["params"]["k_cost_exc"])
    k_cost_rep = float(cfg["params"]["k_cost_rep"])
    k_cost_retry = float(cfg["params"]["k_cost_retry"])

    kpis: Dict[str, Any] = {
        "scenario": scenario_id,
        "world": world,
        "flows": {},
        "aux": {},
    }

    # Basic availability proxy: fraction of buckets where component DOWN
    if event_rows:
        down_set = mean([float(r.get("down_settlement_flag", 0.0)) for r in event_rows])
        down_com = mean([float(r.get("down_compliance_flag", 0.0)) for r in event_rows])
        availability = 1.0 - clip01(0.5 * down_set + 0.5 * down_com)
    else:
        availability = 1.0

    # throughput per bucket
    buckets = [(float(r["t_bucket_start"]), float(r["t_bucket_end"])) for r in event_rows] if event_rows else []
    # For throughput_drop & recovery_time, we compare pre-shock vs in-shock using active_shocks label
    pre_rates = []
    shock_rates = []
    post_rates = []
    for r in event_rows:
        settled = float(r.get("settled_XBPAY", 0)) + float(r.get("settled_PVP", 0)) + float(r.get("settled_DVP", 0))
        dt = float(r["t_bucket_end"]) - float(r["t_bucket_start"])
        rate = safe_div(settled, dt)
        active = str(r.get("active_shocks", ""))
        if active.strip() == "":
            pre_rates.append(rate)
            post_rates.append(rate)
        else:
            shock_rates.append(rate)

    pre = mean(pre_rates)
    during = mean(shock_rates)
    throughput_drop = clip01(safe_div(max(0.0, pre - during), max(pre, 1e-12))) if shock_rates else 0.0

    # recovery_time proxy: how long until bucket rates return above phi_rec*pre after shock
    phi_rec = float(cfg["ni"]["phi_rec"])
    rec_time = 0.0
    if shock_rates and pre > 0 and event_rows:
        # find last shock bucket end
        last_shock_end = None
        for r in event_rows:
            if str(r.get("active_shocks", "")).strip() != "":
                last_shock_end = float(r["t_bucket_end"])
        if last_shock_end is not None:
            target = phi_rec * pre
            for r in event_rows:
                if float(r["t_bucket_start"]) < last_shock_end:
                    continue
                settled = float(r.get("settled_XBPAY", 0)) + float(r.get("settled_PVP", 0)) + float(r.get("settled_DVP", 0))
                dt = float(r["t_bucket_end"]) - float(r["t_bucket_start"])
                rate = safe_div(settled, dt)
                if rate >= target:
                    rec_time = float(r["t_bucket_end"]) - last_shock_end
                    break

    for flow, rows in by_flow.items():
        lat = [float(r.get("latency", 0.0)) for r in rows]
        q_time = [float(r.get("queue_time", 0.0)) for r in rows]
        h_time = [float(r.get("hold_time", 0.0)) for r in rows]
        rep_time = [float(r.get("rework_time", 0.0)) for r in rows]

        fees = [float(r.get("fee_total", 0.0)) for r in rows]
        exc = [int(r.get("exc_count", 0)) for r in rows]
        retries = [int(r.get("retry_count", 0)) for r in rows]

        lock_liq = [float(r.get("liq_lock_amt", 0.0)) * float(r.get("liq_lock_dur", 0.0)) for r in rows]
        lock_col = [float(r.get("coll_lock_amt", 0.0)) * float(r.get("coll_lock_dur", 0.0)) for r in rows]

        checkpoints = [int(r.get("n_checkpoints", 0)) for r in rows]
        repair_missing = [1.0 if bool(r.get("repair_due_to_missing_data_flag", False)) else 0.0 for r in rows]

        stp = [1.0 if bool(r.get("stp_flag", False)) else 0.0 for r in rows]
        trace = [float(r.get("trace_score", 0.0)) for r in rows]

        failed = [1.0 if str(r.get("status")) == "FAILED" else 0.0 for r in rows]
        atomic_viol = [1.0 if bool(r.get("atomicity_violation_flag", False)) else 0.0 for r in rows]

        # Ops cost proxy
        ops_proxy = []
        for i in range(len(rows)):
            ops_proxy.append(
                k_cost_exc * float(exc[i]) +
                k_cost_rep * float(rep_time[i]) +
                k_cost_retry * float(retries[i])
            )

        # K6 placeholders – derived proxies
        # spread_proxy: use fee/notional for PVP, fee/amount for XBPAY, fee/cash_amt for DVP
        spread_proxy = 0.0
        cycle_time = percentile(lat, 0.9)
        multi_ccy_liq_req = mean([float(r.get("liq_lock_amt", 0.0)) for r in rows])

        if flow == "PVP":
            notionals = [float(r.get("notional", 0.0)) for r in rows]
            spread_proxy = mean([safe_div(float(rows[i].get("fee_total", 0.0)), max(notionals[i], 1e-12)) for i in range(len(rows))])
        elif flow == "XBPAY":
            amts = [float(r.get("amount", 0.0)) for r in rows]
            spread_proxy = mean([safe_div(float(rows[i].get("fee_total", 0.0)), max(amts[i], 1e-12)) for i in range(len(rows))])
        elif flow == "DVP":
            amts = [float(r.get("cash_amt", 0.0)) for r in rows]
            spread_proxy = mean([safe_div(float(rows[i].get("fee_total", 0.0)), max(amts[i], 1e-12)) for i in range(len(rows))])

        # FX applicability flags
        fx_flags = [int(r.get("fx_flag", 0)) for r in rows if "fx_flag" in r]
        fx_share = mean([1.0 if f == 1 else 0.0 for f in fx_flags]) if fx_flags else 0.0

        kpis["flows"][flow] = {
            # Core KPIs
            "K1_latency_median": percentile(lat, 0.5),
            "K1_latency_p90": percentile(lat, 0.9),
            "K1_queue_mean": mean(q_time),
            "K1_hold_mean": mean(h_time),
            "K1_repair_mean": mean(rep_time),

            "K2_fee_mean": mean(fees),
            "K2_ops_proxy_mean": mean(ops_proxy),
            "K2_cost_total_mean": mean([fees[i] + ops_proxy[i] for i in range(len(rows))]) if rows else 0.0,

            "K3_liq_lock_exposure_mean": mean(lock_liq),
            "K3_coll_lock_exposure_mean": mean(lock_col),
            "K3_lock_exposure_total_mean": mean([lock_liq[i] + lock_col[i] for i in range(len(rows))]) if rows else 0.0,

            "K4_checkpoints_mean": mean([float(x) for x in checkpoints]),
            "K4_rescreen_rate": 0.0,  # reserved; extend when you model rescreen explicitly
            "K4_repair_due_missing_rate": mean(repair_missing),

            "K5_availability": availability,
            "K5_throughput_drop": throughput_drop,
            "K5_recovery_time": rec_time,
            "K5_op_fail_rate": mean(failed),

            "K6_spread_proxy": spread_proxy,
            "K6_fail_rate": mean(failed),
            "K6_cycle_time_p90": cycle_time,
            "K6_multi_ccy_liq_req_mean": multi_ccy_liq_req,

            # Aux
            "STP_rate": mean(stp),
            "trace_score_mean": mean(trace),

            # NI-related observables
            "FinalityFailureRate": mean(failed),
            "AtomicityViolationRate": mean(atomic_viol),

            # flags
            "fx_share": fx_share,
        }

    # Aux global
    kpis["aux"]["availability"] = availability
    kpis["aux"]["throughput_drop_total"] = throughput_drop
    kpis["aux"]["recovery_time_total"] = rec_time
    return kpis


def g_Z_scalar(cfg: Dict[str, Any], flow: str, comp: str, flow_k: Dict[str, Any]) -> float:
    """
    FIX 3: each component returns a scalar, via fixed ex ante phi weights.
    """
    phi = cfg["ifs"]["phi"][comp]
    if comp == "L":
        return float(phi["p90_weight"]) * float(flow_k["K1_latency_p90"]) + float(phi["median_weight"]) * float(flow_k["K1_latency_median"])
    if comp == "C":
        return float(phi["fee_weight"]) * float(flow_k["K2_fee_mean"]) + float(phi["ops_weight"]) * float(flow_k["K2_ops_proxy_mean"])
    if comp == "Q":
        return float(phi["liq_weight"]) * float(flow_k["K3_liq_lock_exposure_mean"]) + float(phi["coll_weight"]) * float(flow_k["K3_coll_lock_exposure_mean"])
    if comp == "D":
        return (
            float(phi["checkpoints_weight"]) * float(flow_k["K4_checkpoints_mean"]) +
            float(phi["rescreen_weight"]) * float(flow_k["K4_rescreen_rate"]) +
            float(phi["repair_weight"]) * float(flow_k["K4_repair_due_missing_rate"])
        )
    if comp == "R":
        unavail = 1.0 - float(flow_k["K5_availability"])
        return (
            float(phi["unavail_weight"]) * unavail +
            float(phi["fail_weight"]) * float(flow_k["K5_op_fail_rate"]) +
            float(phi["drop_weight"]) * float(flow_k["K5_throughput_drop"]) +
            float(phi["rec_weight"]) * float(flow_k["K5_recovery_time"])
        )
    if comp == "F":
        return (
            float(phi["spread_weight"]) * float(flow_k["K6_spread_proxy"]) +
            float(phi["fail_weight"]) * float(flow_k["K6_fail_rate"]) +
            float(phi["cycle_weight"]) * float(flow_k["K6_cycle_time_p90"]) +
            float(phi["liqreq_weight"]) * float(flow_k["K6_multi_ccy_liq_req_mean"])
        )
    raise ValueError(comp)


def normalize(cfg: Dict[str, Any], flow: str, comp: str, raw: float) -> float:
    mn = float(cfg["ifs"]["bounds"][flow][comp]["min"])
    mx = float(cfg["ifs"]["bounds"][flow][comp]["max"])
    if mx <= mn:
        return 0.0
    return clip01((raw - mn) / (mx - mn))


def compute_ifs(cfg: Dict[str, Any], kpis: Dict[str, Any]) -> Dict[str, Any]:
    """
    FIX 2: F applicability:
      - PVP always
      - XBPAY only if fx_share>0 (proxy for fx_flag=1 cases)
      - DVP: if fx_share==0 then set w_F=0 and renormalize
    """
    w = cfg["ifs"]["weights"]
    v = cfg["ifs"]["flow_weights"]

    out = {"flows": {}, "IFS_total": 0.0, "IFS_total_100": 0.0}
    total = 0.0

    for flow, fk in kpis["flows"].items():
        # raw + norm per component
        Z_raw = {}
        Z_norm = {}
        for comp in ["L", "C", "Q", "D", "R", "F"]:
            Z_raw[comp] = g_Z_scalar(cfg, flow, comp, fk)
            Z_norm[comp] = normalize(cfg, flow, comp, Z_raw[comp])

        # Apply FIX 2 logic
        w_eff = {k: float(w[k]) for k in w.keys()}  # w_L..w_F
        if flow == "DVP":
            if float(fk.get("fx_share", 0.0)) == 0.0:
                # convention: F_raw=0 and w_F=0 then renormalize
                Z_raw["F"] = 0.0
                Z_norm["F"] = 0.0
                w_eff["w_F"] = 0.0
        if flow == "XBPAY":
            # apply if fx_share>0; if not, keep but it will be low-impact; you can also set w_F=0 by design.
            if float(fk.get("fx_share", 0.0)) == 0.0:
                Z_raw["F"] = 0.0
                Z_norm["F"] = 0.0
                w_eff["w_F"] = 0.0

        # renormalize if w_F=0
        if w_eff["w_F"] == 0.0:
            denom = w_eff["w_L"] + w_eff["w_C"] + w_eff["w_Q"] + w_eff["w_D"] + w_eff["w_R"]
            if denom > 0:
                for kk in ["w_L", "w_C", "w_Q", "w_D", "w_R"]:
                    w_eff[kk] = w_eff[kk] / denom

        ifs = (
            w_eff["w_L"] * Z_norm["L"] +
            w_eff["w_C"] * Z_norm["C"] +
            w_eff["w_Q"] * Z_norm["Q"] +
            w_eff["w_D"] * Z_norm["D"] +
            w_eff["w_R"] * Z_norm["R"] +
            w_eff["w_F"] * Z_norm["F"]
        )
        ifs = clip01(ifs)

        out["flows"][flow] = {
            "Z_raw": Z_raw,
            "Z_norm": Z_norm,
            "IFS": ifs,
            "IFS_100": 100.0 * ifs,
            "weights_used": w_eff,
        }
        total += float(v[f"v_{flow}"]) * ifs

    out["IFS_total"] = clip01(total)
    out["IFS_total_100"] = 100.0 * out["IFS_total"]
    return out


def compute_no_inferiority(cfg: Dict[str, Any], kpis: Dict[str, Any], ifs_pack: Dict[str, Any]) -> Dict[str, Any]:
    """
    NI-1/2/3 checks (G6):
    This is computed per run per world; A/B pairing is handled by external analysis.
    Here we output the raw observables and compare to margins as "pass" vs fixed thresholds.
    """
    m = cfg["ni"]["margins"]
    out = {"flows": {}}

    for flow, fk in kpis["flows"].items():
        fin_fail = float(fk["FinalityFailureRate"])
        atomic = float(fk["AtomicityViolationRate"])
        drop = float(fk["K5_throughput_drop"])
        rec = float(fk["K5_recovery_time"])
        trace = float(fk["trace_score_mean"])

        # backlog_ratio proxy: use hold_rate (since backlog is in event_log; extend if you store queue sizes)
        # Here: hold_rate = holds_count>0 share is approximated by 1 - STP as conservative proxy
        hold_rate = clip01(1.0 - float(fk["STP_rate"]))
        backlog_ratio = hold_rate

        out["flows"][flow] = {
            "NI-1": {
                "FinalityFailureRate": fin_fail,
                "AtomicityViolationRate": atomic,
                "Δ_NI_fin_f": float(m["Δ_NI_fin_f"]),
                "ε_atomic_f": float(m["ε_atomic_f"]),
                "pass": (fin_fail <= float(m["Δ_NI_fin_f"])) and (atomic <= float(m["ε_atomic_f"])),
            },
            "NI-2": {
                "ThroughputDrop": drop,
                "RecoveryTime": rec,
                "Δ_NI_ops_f": float(m["Δ_NI_ops_f"]),
                "Δ_NI_rec_f": float(m["Δ_NI_rec_f"]),
                "pass": (drop <= float(m["Δ_NI_ops_f"])) and (rec <= float(m["Δ_NI_rec_f"])),
            },
            "NI-3": {
                "trace_score": trace,
                "backlog_ratio": backlog_ratio,
                "hold_rate": hold_rate,
                "Δ_NI_trace_f": float(m["Δ_NI_trace_f"]),
                "Δ_NI_backlog_f": float(m["Δ_NI_backlog_f"]),
                "pass": (trace >= 1.0 - float(m["Δ_NI_trace_f"])) and (backlog_ratio <= float(m["Δ_NI_backlog_f"])),
            },
        }

    return out


def build_kpi_run(cfg: Dict[str, Any], tx_rows: List[Dict[str, Any]], event_rows: List[Dict[str, Any]], scenario_id: str, world: str) -> Dict[str, Any]:
    kpis = compute_kpis(cfg, tx_rows, event_rows, scenario_id, world)
    ifs_pack = compute_ifs(cfg, kpis)
    ni_pack = compute_no_inferiority(cfg, kpis, ifs_pack)

    # Bounds sanity (J)
    for flow, dd in ifs_pack["flows"].items():
        ifs = float(dd["IFS"])
        assert 0.0 <= ifs <= 1.0, f"IFS out of bounds: {flow}"
        ifs100 = float(dd["IFS_100"])
        assert 0.0 <= ifs100 <= 100.0, f"IFS_100 out of bounds: {flow}"

    return {
        "kpis": kpis,
        "ifs": ifs_pack,
        "no_inferiority": ni_pack,
    }
