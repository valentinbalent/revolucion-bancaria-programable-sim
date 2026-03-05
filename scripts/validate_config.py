#!/usr/bin/env python3
"""
scripts/validate_config.py

CLI helper: loads a config override, materialises it, runs all built-in
consistency checks, and scans for any surviving placeholder strings.

Exits 0 on success, 1 on any failure.

Usage:
    python scripts/validate_config.py configs/thesis_v1.json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, List

from revolucion_bancaria_programable_sim.config import build_config, is_placeholder

FORBIDDEN_PLACEHOLDER_TOKENS = (
    "⟦SET_ME⟧",
    "SET_ME",
    "TBD",
    "TODO",
    "CHANGEME",
    "<SET_ME>",
)


def _is_forbidden_placeholder(s: str) -> bool:
    ss = s.strip()
    return is_placeholder(ss) or any(tok in ss for tok in FORBIDDEN_PLACEHOLDER_TOKENS)


def _collect_placeholders(obj: Any, path: str, found: List[str]) -> None:
    """Recursively collect paths of any surviving placeholder values."""
    if isinstance(obj, str):
        if _is_forbidden_placeholder(obj):
            found.append(path)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _collect_placeholders(v, f"{path}.{k}", found)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _collect_placeholders(v, f"{path}[{i}]", found)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Validate a SpecPack v1.0 config override — fails if any placeholder survives."
    )
    ap.add_argument("config_path", help="Path to JSON config override file.")
    args = ap.parse_args()

    print(f"Validating: {args.config_path}")

    try:
        cfg = build_config(args.config_path)
    except FileNotFoundError:
        print(f"[FAIL] File not found: {args.config_path}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print(f"[FAIL] JSON parse error:\n  {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"[FAIL] Materialisation / validation error:\n  {exc}", file=sys.stderr)
        return 1

    # --- hard consistency checks (thesis guardrails) ---
    run = cfg.get("run", {})
    T_total = float(run.get("T_total", 0.0))
    T_warm = float(run.get("T_warm", 0.0))
    dt_bucket = float(run.get("dt_bucket", 0.0))
    N_runs_min = int(run.get("N_runs_min", 0))
    seeds = run.get("seeds", [])

    if not (T_total > 0 and 0 <= T_warm < T_total):
        print(f"[FAIL] Invalid run horizon: T_total={T_total}, T_warm={T_warm}", file=sys.stderr)
        return 1
    if dt_bucket <= 0:
        print(f"[FAIL] Invalid dt_bucket: {dt_bucket}", file=sys.stderr)
        return 1
    if not isinstance(seeds, list) or len(seeds) == 0:
        print("[FAIL] run.seeds must be a non-empty list", file=sys.stderr)
        return 1
    if len(seeds) < N_runs_min:
        print(f"[FAIL] len(seeds)={len(seeds)} < N_runs_min={N_runs_min}", file=sys.stderr)
        return 1

    # shock windows sanity (if present)
    scenarios = cfg.get("scenarios", {})
    for scen_id in ("S1", "S2"):
        scen = scenarios.get(scen_id, {})
        win = scen.get("shock_window", scen.get("shock_windows"))
        if isinstance(win, dict):
            t_start = float(win.get("t_start", -1))
            t_end = float(win.get("t_end", -1))
            if not (0 <= t_start < t_end <= T_total):
                print(
                    f"[FAIL] {scen_id} invalid shock window: [{t_start},{t_end}] vs T_total={T_total}",
                    file=sys.stderr,
                )
                return 1
            if t_start < T_warm:
                print(
                    f"[FAIL] {scen_id} shock starts before warm-up ends (T_warm={T_warm})",
                    file=sys.stderr,
                )
                return 1

    # Belt-and-suspenders: scan every node in the resolved config.
    found: List[str] = []
    _collect_placeholders(cfg, "cfg", found)

    if found:
        print("[FAIL] Placeholder(s) survived materialisation:", file=sys.stderr)
        for p in found:
            print(f"  {p}", file=sys.stderr)
        return 1

    # Summary
    run = cfg["run"]
    repro = cfg["repro"]
    print("[OK] Config is valid — no placeholders, all constraints satisfied.")
    print(f"  spec                  : {cfg['meta']['spec']}")
    print(f"  allow_debug_defaults  : {repro['allow_debug_defaults']}")
    print(f"  run_id_salt           : {repro.get('run_id_salt', '(not set)')}")
    print(f"  T_total / T_warm      : {run['T_total']} / {run['T_warm']}")
    print(f"  dt_bucket             : {run['dt_bucket']}")
    print(f"  N_runs_min            : {run['N_runs_min']}")
    seeds = run.get("seeds")
    if seeds is not None:
        print(f"  seeds (ref)           : {seeds}")
    w = cfg["ifs"]["weights"]
    print(
        f"  IFS weights           : "
        f"L={w['w_L']} C={w['w_C']} Q={w['w_Q']} D={w['w_D']} R={w['w_R']} F={w['w_F']}"
    )
    fw = cfg["ifs"]["flow_weights"]
    print(f"  flow weights          : XBPAY={fw['v_XBPAY']} PVP={fw['v_PVP']} DVP={fw['v_DVP']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
