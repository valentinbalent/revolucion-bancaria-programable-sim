#!/usr/bin/env python3
# scripts/run_experiment.py

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from typing import Any, Dict, List, Tuple

from revolucion_bancaria_programable_sim.config import build_config, stable_hash_dict
from revolucion_bancaria_programable_sim.metrics import build_kpi_run
from revolucion_bancaria_programable_sim.model_abm import build_agents, generate_tx_intents
from revolucion_bancaria_programable_sim.model_de import DESim, qa_gates
from revolucion_bancaria_programable_sim.rng import CRN


def compute_code_hash(paths: List[str]) -> str:
    h = hashlib.sha256()
    for p in sorted(paths):
        if not os.path.exists(p):
            continue
        with open(p, "rb") as f:
            h.update(f.read())
    return h.hexdigest()


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(os.path.dirname(path))
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    flat_rows: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        tp = rr.pop("trace_pack", None)
        if isinstance(tp, dict):
            for k, v in tp.items():
                rr[f"trace_pack_{k}"] = v
        flat_rows.append(rr)

    fieldnames = sorted({k for r in flat_rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in flat_rows:
            w.writerow(r)


def write_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def make_run_id(cfg: Dict[str, Any], scenario: str, seed: int, world: str, theta_hash: str, code_hash: str) -> str:
    salt = cfg["repro"].get("run_id_salt", "")
    s = f"{salt}|{scenario}|{seed}|{world}|{theta_hash}|{code_hash}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def config_used_payload(
    cfg: Dict[str, Any],
    scenario: str,
    seed: int,
    world: str,
    theta_hash: str,
    code_hash: str,
    run_id: str,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "scenario": scenario,
        "seed": seed,
        "world": world,
        "theta_hash": theta_hash,
        "code_hash": code_hash,
        "spec": cfg["meta"]["spec"],
        "ifs_components": cfg["meta"]["ifs_components"],
        "flows": cfg["meta"]["flows"],
        "config": cfg,
    }


def run_single(cfg: Dict[str, Any], scenario: str, seed: int, world: str, runs_root: str, agent_snapshot: bool) -> Dict[str, Any]:
    crn = CRN(base_seed=seed)

    agents = build_agents(cfg, crn)
    intents = generate_tx_intents(cfg, crn, agents, scenario)

    # Two-worlds adoption rule (A=0, B=1)
    cfg_run = json.loads(json.dumps(cfg))
    cfg_run["params"]["p_adopt"] = 0.0 if world == "A" else 1.0

    theta_hash = stable_hash_dict(cfg_run["params"])

    # Hash only our package modules + this runner for provenance
    code_hash = compute_code_hash(
        [
            "src/revolucion_bancaria_programable_sim/config.py",
            "src/revolucion_bancaria_programable_sim/rng.py",
            "src/revolucion_bancaria_programable_sim/model_abm.py",
            "src/revolucion_bancaria_programable_sim/model_de.py",
            "src/revolucion_bancaria_programable_sim/metrics.py",
            "scripts/run_experiment.py",
        ]
    )

    run_id = make_run_id(cfg_run, scenario, seed, world, theta_hash, code_hash)

    run_dir = os.path.join(runs_root, scenario, f"seed={seed}", f"world={world}", f"run_id={run_id}")
    ensure_dir(run_dir)

    sim = DESim(cfg_run, scenario, world, seed, crn, agents, intents)
    tx_rows, event_rows = sim.run()

    qa_gates(tx_rows, cfg_run, world)

    write_csv(os.path.join(run_dir, "tx_log.csv"), tx_rows)
    write_csv(os.path.join(run_dir, "event_log.csv"), event_rows)
    write_json(os.path.join(run_dir, "kpi_run.json"), build_kpi_run(cfg_run, tx_rows, event_rows, scenario, world))
    write_json(os.path.join(run_dir, "config_used.json"), config_used_payload(cfg_run, scenario, seed, world, theta_hash, code_hash, run_id))

    if agent_snapshot:
        write_csv(os.path.join(run_dir, "agent_snapshot.csv"), sim.agent_snapshot_rows())

    return {"seed": seed, "world": world, "run_id": run_id, "run_dir": run_dir}


def run_pair(cfg: Dict[str, Any], scenario: str, seed: int, runs_root: str, agent_snapshot: bool) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    a = run_single(cfg, scenario, seed, "A", runs_root, agent_snapshot)
    b = run_single(cfg, scenario, seed, "B", runs_root, agent_snapshot)
    return a, b


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True, choices=["S0", "S1", "S2"])
    ap.add_argument("--seeds", nargs="+", type=int, required=True)
    ap.add_argument("--runs-root", default="runs")
    ap.add_argument("--config-override", default=None, help="Path to JSON override file merged into default config.")
    ap.add_argument("--agent-snapshot", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    cfg = build_config(args.config_override)

    n_min = int(cfg["run"]["N_runs_min"])
    if len(args.seeds) < n_min:
        raise ValueError(f"SeedSet size {len(args.seeds)} < N_runs_min {n_min} (protocol).")

    index: List[Dict[str, Any]] = []
    for s in args.seeds:
        a, b = run_pair(cfg, args.scenario, s, args.runs_root, args.agent_snapshot)
        index.append({"seed": s, "A": a, "B": b})

    index_path = os.path.join(args.runs_root, args.scenario, "index.json")
    ensure_dir(os.path.dirname(index_path))
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, sort_keys=True)

    print(f"Done. Index written to: {index_path}")
    for r in index:
        print(f"  seed={r['seed']} A={r['A']['run_dir']} B={r['B']['run_dir']}")


if __name__ == "__main__":
    main()
