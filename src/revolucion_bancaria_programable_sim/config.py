# config.py
"""
Spec Pack v1.0 config module.

Key rules:
- Placeholders are expressed as strings like "⟦SET_ME⟧".
- DEBUG_ONLY numeric defaults exist so the project runs immediately.
- For jurado-proof runs: set allow_debug_defaults = False and fill placeholders.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import json
import copy
import hashlib
import os


PLACEHOLDER = "⟦SET_ME⟧"


def is_placeholder(x: Any) -> bool:
    return isinstance(x, str) and ("⟦" in x and "⟧" in x)


def deep_update(base: Dict[str, Any], upd: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in upd.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_update(base[k], v)
        else:
            base[k] = v
    return base


def stable_hash_dict(d: Dict[str, Any]) -> str:
    s = json.dumps(d, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_config_override(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_param(value: Any, debug_default: Any, allow_debug_defaults: bool, path: str) -> Any:
    """
    Resolve placeholder with debug_default if allowed; else raise.
    """
    if is_placeholder(value) or value is None:
        if allow_debug_defaults:
            return debug_default
        raise ValueError(f"Config parameter at '{path}' is placeholder/None and allow_debug_defaults=False")
    return value


def get_default_config() -> Dict[str, Any]:
    """
    Default config uses DEBUG_ONLY values to run.

    For defendible work:
      - set allow_debug_defaults=False
      - fill placeholders in 'params' and 'ifs' and 'ni'
      - keep design envelope bounds fixed across A/B and scenarios
    """
    cfg: Dict[str, Any] = {
        "meta": {
            "spec": "SpecPack v1.0",
            "ifs_components": ["L", "C", "Q", "D", "R", "F"],
            "flows": ["XBPAY", "PVP", "DVP"],
        },

        # Reproducibility
        "repro": {
            "allow_debug_defaults": True,  # set False for jurado-proof
            "code_commit": "UNKNOWN",      # optional; runner will compute a code_hash anyway
            "run_id_salt": "SpecPack-v1.0",
        },

        # Run protocol (F)
        "run": {
            "T_total": PLACEHOLDER,       # total sim horizon
            "T_warm": PLACEHOLDER,        # warm-up time
            "dt_bucket": PLACEHOLDER,     # event aggregation bucket
            "N_runs_min": PLACEHOLDER,    # minimum runs per scenario (protocol)
        },

        # Scenario windows (E): placeholders; DEBUG_ONLY used if allowed
        "scenarios": {
            "S0": {"shocks": []},
            "S1": {
                "shocks": [
                    {"id": "O1", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                    {"id": "O2", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                    {"id": "O3", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                ]
            },
            "S2": {
                "shocks": [
                    {"id": "O1", "severity": "severe", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                    {"id": "O2", "severity": "severe", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                    {"id": "O4", "t_start": PLACEHOLDER, "t_end": PLACEHOLDER},
                    # O3 optional; keep as placeholder add-on if needed
                ]
            },
        },

        # Parameters θ (D)
        "params": {
            # Adoption (B0)
            "p_adopt": PLACEHOLDER,  # runner sets p_adopt=0 for A, 1 for B in Two-worlds mode

            # Heterogeneity / compliance
            "J_het": PLACEHOLDER,
            "ctl_int": PLACEHOLDER,   # AML/KYC intensity

            # Manual ops capacity in DE
            "cap_ops": PLACEHOLDER,

            # Service / latency parameters (DE)
            "tau_srv_A": {"dist": "exp", "mean": PLACEHOLDER},
            "tau_srv_B": {"dist": "exp", "mean": PLACEHOLDER},

            # Fees
            "fee_A": PLACEHOLDER,
            "fee_B": PLACEHOLDER,

            # Semantics / exceptions / repair
            "q_sem": PLACEHOLDER,      # message semantic quality baseline
            "p_exc": PLACEHOLDER,      # exception probability baseline
            "tau_rep": {"dist": "exp", "mean": PLACEHOLDER},

            # Ops proxy costs (metrics)
            "k_cost_exc": PLACEHOLDER,
            "k_cost_rep": PLACEHOLDER,
            "k_cost_retry": PLACEHOLDER,

            # Liquidity / buffers
            "line_intra": PLACEHOLDER,
            "liq_buf_policy": "neutral",     # {neutral, conservative}; shock O2 can force conservative

            # Outages
            "p_out": PLACEHOLDER,
            "tau_out": {"dist": "exp", "mean": PLACEHOLDER},

            # Atomic commit risk for B
            "p_commit_fail": PLACEHOLDER,

            # Sanctions / geopol (O4)
            "san_scope": PLACEHOLDER,

            # Requirements change (O3)
            "req_change": PLACEHOLDER,

            # Demand (ABM) – intensity by flow
            "demand": {
                "XBPAY_rate": PLACEHOLDER,  # arrivals per time unit
                "PVP_rate": PLACEHOLDER,
                "DVP_rate": PLACEHOLDER,
            },

            # Populations
            "population": {
                "n_agents": PLACEHOLDER,
                "n_jurisdictions": PLACEHOLDER,
                "ccys": ["USD", "EUR"],  # can extend
                "assets": ["ASSET1"],    # can extend
            },

            # FX flags shares
            "fx": {
                "p_fx_XBPAY": PLACEHOLDER,
                "p_fx_DVP": PLACEHOLDER,
            },
        },

        # Shocks (E): multipliers / overrides; placeholders; DEBUG_ONLY resolved if allowed
        "shocks": {
            "O1": {
                "desc": "Operativo",
                "mult": {
                    "p_out": PLACEHOLDER,
                    "tau_out_mean": PLACEHOLDER,
                    "tau_srv_mean": PLACEHOLDER,
                },
            },
            "O2": {
                "desc": "Liquidez",
                "mult": {
                    "line_intra": PLACEHOLDER,
                    "locks": PLACEHOLDER,
                },
                "force_liq_buf_policy": "conservative",
            },
            "O3": {
                "desc": "Regulatorio/Semántico",
                "mult": {
                    "req_change": PLACEHOLDER,
                    "q_sem": PLACEHOLDER,
                    "p_exc": PLACEHOLDER,
                    "tau_rep_mean": PLACEHOLDER,
                    "holds": PLACEHOLDER,
                },
            },
            "O4": {
                "desc": "Geopolítico/De-risking",
                "mult": {
                    "san_scope": PLACEHOLDER,
                    "reroute_fail": PLACEHOLDER,
                },
            },
        },

        # Metrics / IFS (G)
        "ifs": {
            # IFS weights (Σ=1) placeholders
            "weights": {
                "w_L": PLACEHOLDER,
                "w_C": PLACEHOLDER,
                "w_Q": PLACEHOLDER,
                "w_D": PLACEHOLDER,
                "w_R": PLACEHOLDER,
                "w_F": PLACEHOLDER,
            },
            # Flow weights (Σ=1) placeholders
            "flow_weights": {
                "v_XBPAY": PLACEHOLDER,
                "v_PVP": PLACEHOLDER,
                "v_DVP": PLACEHOLDER,
            },

            # Design envelope bounds: per flow and component (FIX 4)
            # For jurado-proof: fill these from Tier1/2 or baseline calibration and freeze.
            "bounds": {
                "XBPAY": {z: {"min": PLACEHOLDER, "max": PLACEHOLDER} for z in ["L", "C", "Q", "D", "R", "F"]},
                "PVP":   {z: {"min": PLACEHOLDER, "max": PLACEHOLDER} for z in ["L", "C", "Q", "D", "R", "F"]},
                "DVP":   {z: {"min": PLACEHOLDER, "max": PLACEHOLDER} for z in ["L", "C", "Q", "D", "R", "F"]},
            },

            # Internal g_Z weights placeholders (FIX 3) – keep ex ante constant
            "phi": {
                "L": {"p90_weight": PLACEHOLDER, "median_weight": PLACEHOLDER},
                "C": {"fee_weight": PLACEHOLDER, "ops_weight": PLACEHOLDER},
                "Q": {"liq_weight": PLACEHOLDER, "coll_weight": PLACEHOLDER},
                "D": {"checkpoints_weight": PLACEHOLDER, "rescreen_weight": PLACEHOLDER, "repair_weight": PLACEHOLDER},
                "R": {"unavail_weight": PLACEHOLDER, "fail_weight": PLACEHOLDER, "drop_weight": PLACEHOLDER, "rec_weight": PLACEHOLDER},
                "F": {"spread_weight": PLACEHOLDER, "fail_weight": PLACEHOLDER, "cycle_weight": PLACEHOLDER, "liqreq_weight": PLACEHOLDER},
            },
        },

        # NI (G6)
        "ni": {
            "margins": {
                "Δ_NI_fin_f": PLACEHOLDER,
                "ε_atomic_f": PLACEHOLDER,
                "Δ_NI_ops_f": PLACEHOLDER,
                "Δ_NI_rec_f": PLACEHOLDER,
                "Δ_NI_trace_f": PLACEHOLDER,
                "Δ_NI_backlog_f": PLACEHOLDER,
            },
            "phi_rec": PLACEHOLDER,
            "alpha_ci": PLACEHOLDER,  # reserved for inference plan; runner here computes point estimates only
        },

        # DEBUG_ONLY fallback values (explicit and auditable)
        "DEBUG_ONLY": {
            "run": {
                "T_total": 200.0,
                "T_warm": 0.0,
                "dt_bucket": 10.0,
                "N_runs_min": 2,
            },
            "params": {
                "p_adopt": 0.0,
                "J_het": 0.5,
                "ctl_int": 0.5,
                "cap_ops": 2,
                "tau_srv_A_mean": 5.0,
                "tau_srv_B_mean": 2.0,
                "fee_A": 1.0,
                "fee_B": 0.5,
                "q_sem": 0.8,
                "p_exc": 0.05,
                "tau_rep_mean": 8.0,
                "k_cost_exc": 1.0,
                "k_cost_rep": 0.1,
                "k_cost_retry": 0.5,
                "line_intra": 1000.0,
                "p_out": 0.01,
                "tau_out_mean": 15.0,
                "p_commit_fail": 0.01,
                "san_scope": 0.1,
                "req_change": 0.1,
                "demand": {"XBPAY_rate": 0.2, "PVP_rate": 0.15, "DVP_rate": 0.15},
                "population": {"n_agents": 20, "n_jurisdictions": 3},
                "fx": {"p_fx_XBPAY": 0.7, "p_fx_DVP": 0.2},
            },
            "scenario_windows": {
                "S1": {"t_start": 60.0, "t_end": 120.0},
                "S2": {"t_start": 80.0, "t_end": 160.0},
            },
            "shocks": {
                "O1": {"p_out": 2.0, "tau_out_mean": 1.5, "tau_srv_mean": 1.3},
                "O2": {"line_intra": 0.6, "locks": 1.4},
                "O3": {"req_change": 2.0, "q_sem": 0.85, "p_exc": 1.6, "tau_rep_mean": 1.4, "holds": 1.4},
                "O4": {"san_scope": 3.0, "reroute_fail": 1.4},
            },
            "ifs": {
                "weights": {"w_L": 0.2, "w_C": 0.2, "w_Q": 0.2, "w_D": 0.2, "w_R": 0.1, "w_F": 0.1},
                "flow_weights": {"v_XBPAY": 0.4, "v_PVP": 0.3, "v_DVP": 0.3},
                "bounds": {
                    # DEBUG_ONLY bounds: arbitrary to keep normalization runnable; replace for defendible runs.
                    "XBPAY": {"L": (0.0, 50.0), "C": (0.0, 10.0), "Q": (0.0, 5000.0), "D": (0.0, 10.0), "R": (0.0, 5.0), "F": (0.0, 5.0)},
                    "PVP":   {"L": (0.0, 50.0), "C": (0.0, 10.0), "Q": (0.0, 5000.0), "D": (0.0, 10.0), "R": (0.0, 5.0), "F": (0.0, 5.0)},
                    "DVP":   {"L": (0.0, 50.0), "C": (0.0, 10.0), "Q": (0.0, 5000.0), "D": (0.0, 10.0), "R": (0.0, 5.0), "F": (0.0, 5.0)},
                },
                "phi": {
                    "L": {"p90_weight": 0.7, "median_weight": 0.3},
                    "C": {"fee_weight": 0.6, "ops_weight": 0.4},
                    "Q": {"liq_weight": 0.8, "coll_weight": 0.2},
                    "D": {"checkpoints_weight": 0.5, "rescreen_weight": 0.2, "repair_weight": 0.3},
                    "R": {"unavail_weight": 0.4, "fail_weight": 0.3, "drop_weight": 0.2, "rec_weight": 0.1},
                    "F": {"spread_weight": 0.3, "fail_weight": 0.3, "cycle_weight": 0.2, "liqreq_weight": 0.2},
                },
            },
            "ni": {
                "margins": {
                    "Δ_NI_fin_f": 0.02,
                    "ε_atomic_f": 0.001,
                    "Δ_NI_ops_f": 0.20,
                    "Δ_NI_rec_f": 20.0,
                    "Δ_NI_trace_f": 0.05,
                    "Δ_NI_backlog_f": 0.10,
                },
                "phi_rec": 0.9,
                "alpha_ci": 0.05,
            },
        },
    }
    return cfg


def materialize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replace placeholders with DEBUG_ONLY defaults if allow_debug_defaults=True.

    This keeps the runnable guarantee while making the debug nature explicit.
    """
    cfg = copy.deepcopy(cfg)
    allow = bool(cfg["repro"]["allow_debug_defaults"])
    dbg = cfg["DEBUG_ONLY"]

    # Run protocol
    cfg["run"]["T_total"] = resolve_param(cfg["run"]["T_total"], dbg["run"]["T_total"], allow, "run.T_total")
    cfg["run"]["T_warm"] = resolve_param(cfg["run"]["T_warm"], dbg["run"]["T_warm"], allow, "run.T_warm")
    cfg["run"]["dt_bucket"] = resolve_param(cfg["run"]["dt_bucket"], dbg["run"]["dt_bucket"], allow, "run.dt_bucket")
    cfg["run"]["N_runs_min"] = resolve_param(cfg["run"]["N_runs_min"], dbg["run"]["N_runs_min"], allow, "run.N_runs_min")

    # Scenario windows
    for s_id, s_cfg in cfg["scenarios"].items():
        for shock in s_cfg.get("shocks", []):
            if shock.get("id") in ("O1", "O2", "O3", "O4"):
                # window fallback per scenario
                if s_id in ("S1", "S2"):
                    t0 = dbg["scenario_windows"][s_id]["t_start"]
                    t1 = dbg["scenario_windows"][s_id]["t_end"]
                    shock["t_start"] = resolve_param(shock.get("t_start"), t0, allow, f"scenarios.{s_id}.{shock['id']}.t_start")
                    shock["t_end"] = resolve_param(shock.get("t_end"), t1, allow, f"scenarios.{s_id}.{shock['id']}.t_end")
                else:
                    shock["t_start"] = resolve_param(shock.get("t_start", 0.0), 0.0, True, f"scenarios.{s_id}.{shock['id']}.t_start")
                    shock["t_end"] = resolve_param(shock.get("t_end", cfg['run']['T_total']), cfg["run"]["T_total"], True, f"scenarios.{s_id}.{shock['id']}.t_end")

    # Params θ
    p = cfg["params"]
    p_dbg = dbg["params"]

    p["p_adopt"] = resolve_param(p["p_adopt"], p_dbg["p_adopt"], allow, "params.p_adopt")
    p["J_het"] = resolve_param(p["J_het"], p_dbg["J_het"], allow, "params.J_het")
    p["ctl_int"] = resolve_param(p["ctl_int"], p_dbg["ctl_int"], allow, "params.ctl_int")
    p["cap_ops"] = resolve_param(p["cap_ops"], p_dbg["cap_ops"], allow, "params.cap_ops")

    p["tau_srv_A"]["mean"] = resolve_param(p["tau_srv_A"]["mean"], p_dbg["tau_srv_A_mean"], allow, "params.tau_srv_A.mean")
    p["tau_srv_B"]["mean"] = resolve_param(p["tau_srv_B"]["mean"], p_dbg["tau_srv_B_mean"], allow, "params.tau_srv_B.mean")

    p["fee_A"] = resolve_param(p["fee_A"], p_dbg["fee_A"], allow, "params.fee_A")
    p["fee_B"] = resolve_param(p["fee_B"], p_dbg["fee_B"], allow, "params.fee_B")

    p["q_sem"] = resolve_param(p["q_sem"], p_dbg["q_sem"], allow, "params.q_sem")
    p["p_exc"] = resolve_param(p["p_exc"], p_dbg["p_exc"], allow, "params.p_exc")
    p["tau_rep"]["mean"] = resolve_param(p["tau_rep"]["mean"], p_dbg["tau_rep_mean"], allow, "params.tau_rep.mean")

    p["k_cost_exc"] = resolve_param(p["k_cost_exc"], p_dbg["k_cost_exc"], allow, "params.k_cost_exc")
    p["k_cost_rep"] = resolve_param(p["k_cost_rep"], p_dbg["k_cost_rep"], allow, "params.k_cost_rep")
    p["k_cost_retry"] = resolve_param(p["k_cost_retry"], p_dbg["k_cost_retry"], allow, "params.k_cost_retry")

    p["line_intra"] = resolve_param(p["line_intra"], p_dbg["line_intra"], allow, "params.line_intra")
    p["p_out"] = resolve_param(p["p_out"], p_dbg["p_out"], allow, "params.p_out")
    p["tau_out"]["mean"] = resolve_param(p["tau_out"]["mean"], p_dbg["tau_out_mean"], allow, "params.tau_out.mean")
    p["p_commit_fail"] = resolve_param(p["p_commit_fail"], p_dbg["p_commit_fail"], allow, "params.p_commit_fail")
    p["san_scope"] = resolve_param(p["san_scope"], p_dbg["san_scope"], allow, "params.san_scope")
    p["req_change"] = resolve_param(p["req_change"], p_dbg["req_change"], allow, "params.req_change")

    # Demand
    p["demand"]["XBPAY_rate"] = resolve_param(p["demand"]["XBPAY_rate"], p_dbg["demand"]["XBPAY_rate"], allow, "params.demand.XBPAY_rate")
    p["demand"]["PVP_rate"] = resolve_param(p["demand"]["PVP_rate"], p_dbg["demand"]["PVP_rate"], allow, "params.demand.PVP_rate")
    p["demand"]["DVP_rate"] = resolve_param(p["demand"]["DVP_rate"], p_dbg["demand"]["DVP_rate"], allow, "params.demand.DVP_rate")

    # Population
    p["population"]["n_agents"] = resolve_param(p["population"]["n_agents"], p_dbg["population"]["n_agents"], allow, "params.population.n_agents")
    p["population"]["n_jurisdictions"] = resolve_param(p["population"]["n_jurisdictions"], p_dbg["population"]["n_jurisdictions"], allow, "params.population.n_jurisdictions")

    # FX shares
    p["fx"]["p_fx_XBPAY"] = resolve_param(p["fx"]["p_fx_XBPAY"], p_dbg["fx"]["p_fx_XBPAY"], allow, "params.fx.p_fx_XBPAY")
    p["fx"]["p_fx_DVP"] = resolve_param(p["fx"]["p_fx_DVP"], p_dbg["fx"]["p_fx_DVP"], allow, "params.fx.p_fx_DVP")

    # Shocks multipliers
    for oid, odef in cfg["shocks"].items():
        mult = odef.get("mult", {})
        dbg_mult = dbg["shocks"].get(oid, {})
        for mk in list(mult.keys()):
            mult[mk] = resolve_param(mult[mk], dbg_mult.get(mk, mult[mk]), allow, f"shocks.{oid}.mult.{mk}")

    # IFS weights
    ifs_dbg = dbg["ifs"]
    w = cfg["ifs"]["weights"]
    w_dbg = ifs_dbg["weights"]
    for k in list(w.keys()):
        w[k] = resolve_param(w[k], w_dbg[k], allow, f"ifs.weights.{k}")

    # Flow weights
    vw = cfg["ifs"]["flow_weights"]
    vw_dbg = ifs_dbg["flow_weights"]
    for k in list(vw.keys()):
        vw[k] = resolve_param(vw[k], vw_dbg[k], allow, f"ifs.flow_weights.{k}")

    # IFS bounds (design envelope) – DEBUG_ONLY fallback
    b = cfg["ifs"]["bounds"]
    b_dbg = ifs_dbg["bounds"]
    for flow, comps in b.items():
        for z, mm in comps.items():
            # accept tuple in debug-only; normalize to dict
            dbg_min, dbg_max = b_dbg[flow][z]
            mm["min"] = resolve_param(mm["min"], dbg_min, allow, f"ifs.bounds.{flow}.{z}.min")
            mm["max"] = resolve_param(mm["max"], dbg_max, allow, f"ifs.bounds.{flow}.{z}.max")

    # g_Z internal weights phi
    phi = cfg["ifs"]["phi"]
    phi_dbg = ifs_dbg["phi"]
    for z, dd in phi.items():
        for k in list(dd.keys()):
            dd[k] = resolve_param(dd[k], phi_dbg[z][k], allow, f"ifs.phi.{z}.{k}")

    # NI
    ni_dbg = dbg["ni"]
    m = cfg["ni"]["margins"]
    m_dbg = ni_dbg["margins"]
    for k in list(m.keys()):
        m[k] = resolve_param(m[k], m_dbg[k], allow, f"ni.margins.{k}")
    cfg["ni"]["phi_rec"] = resolve_param(cfg["ni"]["phi_rec"], ni_dbg["phi_rec"], allow, "ni.phi_rec")
    cfg["ni"]["alpha_ci"] = resolve_param(cfg["ni"]["alpha_ci"], ni_dbg["alpha_ci"], allow, "ni.alpha_ci")

    return cfg


def validate_weights_sum_to_one(weights: Dict[str, float], keys: list[str], name: str) -> None:
    s = 0.0
    for k in keys:
        if k not in weights:
            raise ValueError(f"Missing weight {k} in {name}")
        s += float(weights[k])
    # allow tiny floating error
    if not (abs(s - 1.0) <= 1e-6):
        raise ValueError(f"Weights in {name} must sum to 1. Got {s}")


def validate_config(cfg: Dict[str, Any]) -> None:
    # Weight sums (G5)
    validate_weights_sum_to_one(cfg["ifs"]["weights"], ["w_L", "w_C", "w_Q", "w_D", "w_R", "w_F"], "ifs.weights")
    validate_weights_sum_to_one(cfg["ifs"]["flow_weights"], ["v_XBPAY", "v_PVP", "v_DVP"], "ifs.flow_weights")

    # Bounds ordering
    for flow in cfg["meta"]["flows"]:
        for z in cfg["meta"]["ifs_components"]:
            mn = float(cfg["ifs"]["bounds"][flow][z]["min"])
            mx = float(cfg["ifs"]["bounds"][flow][z]["max"])
            if not (mx > mn):
                raise ValueError(f"IFS bounds invalid for {flow}.{z}: max must be > min.")

    # Shock directionality sanity (J)
    # O1 should worsen p_out and tau_out_mean and tau_srv_mean (multipliers >=1)
    o1 = cfg["shocks"]["O1"]["mult"]
    for k in ["p_out", "tau_out_mean", "tau_srv_mean"]:
        if float(o1[k]) < 1.0:
            raise ValueError(f"Shock O1 directionality violated: {k} mult < 1.")
    # O2 should reduce line_intra (mult <=1) and increase locks (>=1)
    o2 = cfg["shocks"]["O2"]["mult"]
    if float(o2["line_intra"]) > 1.0:
        raise ValueError("Shock O2 directionality violated: line_intra mult > 1 (should decrease).")
    if float(o2["locks"]) < 1.0:
        raise ValueError("Shock O2 directionality violated: locks mult < 1 (should increase).")
    # O3 should worsen req_change (>=1), reduce q_sem (<=1), increase p_exc (>=1), tau_rep (>=1), holds (>=1)
    o3 = cfg["shocks"]["O3"]["mult"]
    if float(o3["req_change"]) < 1.0:
        raise ValueError("Shock O3 directionality violated: req_change mult < 1.")
    if float(o3["q_sem"]) > 1.0:
        raise ValueError("Shock O3 directionality violated: q_sem mult > 1 (should decrease).")
    if float(o3["p_exc"]) < 1.0:
        raise ValueError("Shock O3 directionality violated: p_exc mult < 1.")
    if float(o3["tau_rep_mean"]) < 1.0:
        raise ValueError("Shock O3 directionality violated: tau_rep_mean mult < 1.")
    if float(o3["holds"]) < 1.0:
        raise ValueError("Shock O3 directionality violated: holds mult < 1.")
    # O4 should increase san_scope (>=1) and reroute_fail (>=1)
    o4 = cfg["shocks"]["O4"]["mult"]
    if float(o4["san_scope"]) < 1.0:
        raise ValueError("Shock O4 directionality violated: san_scope mult < 1.")
    if float(o4["reroute_fail"]) < 1.0:
        raise ValueError("Shock O4 directionality violated: reroute_fail mult < 1.")


def build_config(override_path: Optional[str] = None) -> Dict[str, Any]:
    cfg = get_default_config()
    if override_path:
        deep_update(cfg, load_config_override(override_path))
    cfg = materialize_config(cfg)
    validate_config(cfg)
    return cfg
