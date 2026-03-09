# Calibration notes

## Pilot envelope run (for bounds only)

Goal: derive provisional Z_min/Z_max per component/flow from a quick pilot run.

### Prerequisites

- All frozen parameters (protocol, shocks, weights, NI margins) are set in
  `configs/thesis_pilot.json` (identical to `thesis_v1.json` except 2 seeds).
- Pilot config validated: `python scripts/validate_config.py configs/thesis_pilot.json`

### Procedure

1. Run pilot:
   ```bash
   python scripts/run_pilot.py \
     --config configs/thesis_pilot.json \
     --scenarios S0 S2 \
     --seeds 42 99 \
     --runs-root runs_pilot \
     --results-root results_pilot
   ```

2. Extract design envelope bounds:
   ```bash
   python scripts/extract_pilot_bounds.py \
     --runs-root runs_pilot \
     --output calibration/design_envelope_bounds.json \
     --config configs/thesis_pilot.json
   ```

3. Review `calibration/design_envelope_bounds.json`:
   - Check that p01/p99 ranges are physically plausible.
   - Check that no z_max is excessively large (indicates outlier or bug).
   - Apply manual caps if needed (document reason in `docs/DECISIONS.md`).

4. Copy bounds into `configs/thesis_v1.json`:
   - For each flow {XBPAY, PVP, DVP} and component {L, C, Q, D, R, F}:
     set `ifs.bounds.<flow>.<comp>.min` = z_min, `.max` = z_max.

5. Validate final config:
   ```bash
   python scripts/validate_config.py configs/thesis_v1.json
   ```

### Envelope extraction algorithm

For each flow f in {XBPAY, PVP, DVP} and component Z in {L, C, Q, D, R, F}:

1. Collect `raw_values = [g_Z_scalar(cfg, f, Z, kpi) for kpi in all_pilot_kpi_runs]`
2. `p01 = percentile(raw_values, 0.01)`
3. `p99 = percentile(raw_values, 0.99)`
4. `z_min = max(0.0, p01 * 0.8)` — shrink p01 by 20%
5. `z_max = p99 * 1.2` — expand p99 by 20%
6. Record `{p01, p99, z_min, z_max, n_samples, clipping_notes}`

### Output schema (design_envelope_bounds.json)

```json
{
  "meta": {
    "generated": "ISO-8601 timestamp",
    "runs_root": "runs_pilot",
    "config_path": "configs/thesis_pilot.json",
    "n_kpi_files": 8,
    "expansion_factor": 0.20
  },
  "bounds": {
    "XBPAY": {
      "L": {"p01": 1.2, "p99": 35.0, "z_min": 0.96, "z_max": 42.0, "n_samples": 8},
      "...": "..."
    }
  }
}
```

## Calibration tiers

- **Frozen**: protocol, windows, shocks, weights, NI margins (thesis_v1.1).
- **Calibrated**: design envelope bounds (Z_min/Z_max) after pilot envelope run.
- **Sensitivity**: any exploratory sweeps outside frozen settings.

## Important

The pilot envelope run is not the final experiment and must not be reported as the thesis result.
See `docs/DECISIONS.md` "Non-final results notice" for the full policy.
