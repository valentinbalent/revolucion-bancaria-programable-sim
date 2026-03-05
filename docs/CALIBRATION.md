# Calibration notes

## Pilot envelope run (for bounds only)

Goal: derive provisional Z_min/Z_max per component/flow from a quick pilot run.

Procedure:
1) Use 2 seeds, scenarios S0 + S2, both worlds (A/B), per-flow outputs.
2) Collect p1/p99 of the raw components (L, C, Q, D, R, F) for each flow.
3) Inflate each bound by +20% to build a safety margin.
4) Freeze the resulting Z_min/Z_max into `configs/thesis_v1.json`.

## Calibration tiers

- Frozen: protocol, windows, shocks, weights, NI margins (thesis_v1.1).
- Calibrated: design envelope bounds (Z_min/Z_max) after pilot envelope run.
- Sensitivity: any exploratory sweeps outside frozen settings.

## Important

The pilot envelope run is not the final experiment and must not be reported as the thesis result.
