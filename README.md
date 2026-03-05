# Revolución Bancaria Programable — Simulation (ABM/DE)

Repo reproducible para simulación A/B (Mundo A vs Mundo B) con:
- DE (SimPy) + policies tipo ABM
- CRN (Common Random Numbers) para A/B pareado por seed
- Artifacts por corrida: tx_log.csv, event_log.csv, kpi_run.json, config_used.json

## Quickstart (macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## Running the Thesis Experiment (thesis_v1)

All commands below assume the virtualenv is active and you are at the repo root.
The config override `configs/thesis_v1.json` sets `allow_debug_defaults=false` and
fully specifies every parameter (T_total=500, T_warm=50, N_runs_min=30, 30 seeds).
Note: `thesis_v1` enforces `N_runs_min=30`; for fast end-to-end runs use `configs/thesis_smoke.json`.

### 1 — Validate the config before running

```bash
python scripts/validate_config.py configs/thesis_v1.json
```

Expected output: `[OK] Config is valid — no placeholders, all constraints satisfied.`
Exit 1 means a placeholder or constraint violation was found; fix before proceeding.

### 2 — Run S0 (baseline, no shocks)

```bash
python scripts/run_experiment.py \
  --scenario S0 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json
```

### 3 — Run S1 (concurrent operational + liquidity + regulatory stress, t∈[120,220])

```bash
python scripts/run_experiment.py \
  --scenario S1 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json
```

### 4 — Run S2 (severe operational + liquidity + geopolitical stress, t∈[300,430])

```bash
python scripts/run_experiment.py \
  --scenario S2 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json
```

### 5 — Aggregate all scenarios into results tables

```bash
python scripts/aggregate_results.py \
  --runs-root runs \
  --results-root results
```

Output files written to `results/`:

| File | Contents |
|------|----------|
| `kpi_aggregate.csv` | Run-level flat KPIs (all scenarios, both worlds) |
| `paired_seed_level.csv` | A/B paired deltas per seed |
| `table_5_2_kpis_xbpay.csv` | XBPAY KPIs — median / p10 / p90 per scenario |
| `table_5_3_kpis_pvp_dvp.csv` | PVP + DVP KPIs — median / p10 / p90 per scenario |
| `table_5_4_ifs.csv` | IFS scores per flow and component per scenario |
| `table_5_5_no_inferiority.csv` | NI-1 / NI-2 / NI-3 pass rates per flow and scenario |

To aggregate a single scenario only (e.g. during iterative runs):

```bash
python scripts/aggregate_results.py \
  --runs-root runs \
  --results-root results \
  --scenario S1
```

### Optional: agent snapshot

Pass `--agent-snapshot` to any `run_experiment.py` call to also write
`agent_snapshot.csv` in each run directory.

```bash
python scripts/run_experiment.py \
  --scenario S0 \
  --seeds 42 \
  --runs-root runs \
  --config-override configs/thesis_v1.json \
  --agent-snapshot
```

## Development

Run linter and tests:

```bash
ruff check .
pytest tests/
```

## License
MIT — ver LICENSE.
