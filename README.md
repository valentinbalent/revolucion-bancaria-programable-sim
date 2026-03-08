# Revolucion Bancaria Programable — Simulation (ABM/DE)

Reproducible A/B simulation (Mundo A vs Mundo B) with:
- DE (SimPy) + ABM-style policies
- CRN (Common Random Numbers) for paired A/B variance reduction
- Artifacts per run: `tx_log.csv`, `event_log.csv`, `kpi_run.json`, `config_used.json`

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Verify installation:

```bash
ruff check .
pytest -q
```

## Smoke Test (< 1 min)

Fast end-to-end validation with 2 seeds, T_total=120, compressed shock windows.

```bash
# Validate config
python scripts/validate_config.py configs/thesis_smoke.json

# Run S0 (baseline) + S2 (severe stress)
python scripts/run_experiment.py \
  --scenario S0 --seeds 42 99 \
  --runs-root runs_smoke \
  --config-override configs/thesis_smoke.json

python scripts/run_experiment.py \
  --scenario S2 --seeds 42 99 \
  --runs-root runs_smoke \
  --config-override configs/thesis_smoke.json

# Aggregate
python scripts/aggregate_results.py \
  --runs-root runs_smoke --results-root results/smoke
```

## Pilot Run

Automated pipeline that runs experiments, aggregates, and builds a results pack
with `PILOT_REPORT.md` and `RUN_META.json`.

```bash
# Default: thesis_pilot.json (or thesis_smoke.json), S0+S2, seeds 42 99
python scripts/run_pilot.py

# Custom options
python scripts/run_pilot.py \
  --config configs/thesis_smoke.json \
  --scenarios S0 S1 S2 \
  --seeds 42 99 \
  --runs-root runs/pilot \
  --results-root results/pilot
```

Outputs in `results/pilot/`:

| File | Contents |
|------|----------|
| `kpi_aggregate.csv` | Run-level flat KPIs |
| `paired_seed_level.csv` | A/B paired deltas per seed |
| `T1_KPIs_core_AvsB.csv` | Core KPIs (A vs B) |
| `T2_IFS_components.csv` | IFS component scores |
| `T3_NI.csv` | No-inferiority verdicts |
| `PILOT_REPORT.md` | Summary report |
| `RUN_META.json` | Full reproducibility metadata |
| `manifest_sha256.json` | SHA-256 integrity checksums |

## Thesis Batch (thesis_v1)

Full run: 30 seeds, T_total=500, 3 scenarios. Uses `configs/thesis_v1.json`
with `allow_debug_defaults=false` (all parameters explicitly set).

```bash
# 1 — Validate
python scripts/validate_config.py configs/thesis_v1.json

# 2 — Run S0 (baseline)
python scripts/run_experiment.py \
  --scenario S0 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json

# 3 — Run S1 (concurrent O1+O2+O3, t in [120,220])
python scripts/run_experiment.py \
  --scenario S1 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json

# 4 — Run S2 (severe O1+O2+O4, t in [300,430])
python scripts/run_experiment.py \
  --scenario S2 \
  --seeds 42 123 456 789 1337 2026 7 11 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97 101 103 107 109 \
  --runs-root runs \
  --config-override configs/thesis_v1.json

# 5 — Aggregate
python scripts/aggregate_results.py \
  --runs-root runs --results-root results

# 6 — Results pack
python scripts/make_results_pack.py \
  --results-dir results --label thesis_v1
```

## Exact Reproducibility

Every run is fingerprinted for full traceability:

| Artifact | Description |
|----------|-------------|
| `run_id` | `SHA256(salt \| scenario \| seed \| world \| theta_hash \| git_commit \| code_hash)[:16]` |
| `run_id_salt` | Config-level salt (e.g. `thesis-v1.0`) — changing it changes all run_ids |
| `git_commit` | Recorded in `config_used.json` per run |
| `code_hash` | SHA-256 of source modules (`config.py`, `rng.py`, `model_abm.py`, `model_de.py`, `metrics.py`, `run_experiment.py`) |
| `theta_hash` | SHA-256 of the `params` dict (stable JSON serialization) |
| `manifest_sha256.json` | File-level checksums in results pack |

To verify a run was produced by a specific code + config combination:

1. Check `config_used.json` in the run directory for `git_commit`, `code_hash`, `run_id`
2. Recompute `run_id` from the same inputs — must match
3. Check `manifest_sha256.json` in results for file integrity

Config fields `run_id_salt` and `code_hash` together form the **provenance chain**.
If any source file or parameter changes, the `run_id` changes, preventing silent drift.

## Development

```bash
ruff check .
pytest -q
```

## License

MIT — see LICENSE.
