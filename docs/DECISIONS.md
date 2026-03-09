# Decisions (thesis_v1.1)

This file freezes protocol, windows, shocks, and scoring parameters for thesis_v1.1.

## Frozen settings

- Protocol/horizon: run.T_total=500.0, run.T_warm=50.0, run.dt_bucket=10.0, run.N_runs_min=30, seeds list fixed (30 seeds).
- Shock windows:
  - S1: t_start=120.0, t_end=220.0
  - S2: t_start=300.0, t_end=430.0
- Shock magnitudes (multipliers on baseline):
  - O1 operativo: p_out x4; tau_out_mean x2; tau_srv_mean x1.4; cap_ops x0.8
  - O2 liquidez: line_intra x0.5; liq_buf_policy=conservative (closest to hoarding); locks x1.3
  - O3 regul/semantic: req_change x2; q_sem delta -0.25 mapped to multiplier; p_exc x2; tau_rep_mean x1.8; holds x1.5
  - O4 geo/de-risking: san_scope x2; route_remove_frac=0.25; reroute_cost x1.2; reroute_latency x1.2
  - ADV5 adversarial: p_commit_fail x5 (not applied to S0/S1/S2)
- Baseline p_commit_fail: params.p_commit_fail=0.002
- IFS weights: w_L=w_C=w_Q=w_D=w_R=w_F=1/6 each; flow weights v_XBPAY=v_PVP=v_DVP=1/3 each.
- Missing-component policy: ifs.missing_component_policy=drop_and_renormalize (when F does not apply, set w_F=0 and renormalize).
- NI margins:
  - epsilon_atomic=0
  - Delta NI fin (FinalityFailureRate) = +0.25 pp
  - Delta NI ops (ThroughputDrop) = +5 pp
  - Delta NI rec (RecoveryTime) = +1 bucket
  - Delta NI trace (trace_score) = -0.05
  - Delta NI backlog = +10% (backlog_ratio)

## Mapping (config_key -> meaning -> value -> notes)

- run.T_total -> total horizon -> 500.0 -> frozen.
- run.T_warm -> warmup duration -> 50.0 -> shocks start strictly after warmup.
- run.dt_bucket -> aggregation bucket -> 10.0 -> frozen.
- run.N_runs_min -> minimum runs per scenario -> 30 -> enforced.
- scenarios.S1.shocks[*].t_start/t_end -> S1 window -> 120.0/220.0 -> schema uses per-shock windows (no scenario-level key).
- scenarios.S2.shocks[*].t_start/t_end -> S2 window -> 300.0/430.0 -> schema uses per-shock windows (no scenario-level key).
- shocks.O1.mult.p_out -> outage intensity multiplier -> 4.0 -> direct mapping.
- shocks.O1.mult.tau_out_mean -> outage duration multiplier -> 2.0 -> direct mapping.
- shocks.O1.mult.tau_srv_mean -> service time multiplier -> 1.4 -> direct mapping.
- shocks.O1.mult.cap_ops -> manual ops capacity multiplier -> 0.8 -> stored for completeness.
- shocks.O2.mult.line_intra -> intraday line multiplier -> 0.5 -> direct mapping.
- shocks.O2.force_liq_buf_policy -> liquidity buffer policy -> conservative -> closest existing enum to hoarding.
- shocks.O2.mult.locks -> liquidity lock severity -> 1.3 -> used as proxy for liq_lock_dur.
- shocks.O3.mult.req_change -> requirements change multiplier -> 2.0 -> direct mapping.
- shocks.O3.mult.q_sem -> semantic quality multiplier -> 0.7058823529 -> equals (q_sem - 0.25) / q_sem with baseline q_sem=0.85.
- shocks.O3.mult.p_exc -> exception multiplier -> 2.0 -> direct mapping.
- shocks.O3.mult.tau_rep_mean -> repair time multiplier -> 1.8 -> direct mapping.
- shocks.O3.mult.holds -> hold rate multiplier -> 1.5 -> direct mapping.
- shocks.O4.mult.san_scope -> sanction scope multiplier -> 2.0 -> direct mapping.
- shocks.O4.mult.reroute_fail -> reroute failure proxy -> 1.2 -> closest existing multiplier for reroute friction.
- shocks.O4.mult.route_remove_frac -> route removal fraction -> 0.25 -> stored for completeness.
- shocks.O4.mult.reroute_cost -> reroute cost multiplier -> 1.2 -> stored for completeness.
- shocks.O4.mult.reroute_latency -> reroute latency multiplier -> 1.2 -> stored for completeness.
- shocks.ADV5.mult.p_commit_fail -> adversarial commit failure multiplier -> 5.0 -> stored for adversarial-only use.
- params.p_commit_fail -> baseline commit failure probability -> 0.002 -> non-zero for NI realism.
- ifs.weights.w_* -> IFS component weights -> 1/6 each -> sum to 1.
- ifs.flow_weights.v_* -> IFS flow weights -> 1/3 each -> sum to 1.
- ifs.missing_component_policy -> missing component rule -> drop_and_renormalize -> drop F then renormalize ex ante.
- ni.margins.Δ_NI_fin_f -> FinalityFailureRate margin -> 0.0025 -> 0.25 pp.
- ni.margins.ε_atomic_f -> AtomicityViolationRate margin -> 0.0 -> epsilon_atomic=0.
- ni.margins.Δ_NI_ops_f -> ThroughputDrop margin -> 0.05 -> 5 pp.
- ni.margins.Δ_NI_rec_f -> RecoveryTime margin -> 1.0 -> 1 bucket.
- ni.margins.Δ_NI_trace_f -> trace_score margin -> 0.05 -> represents delta -0.05 in docs.
- ni.margins.Δ_NI_backlog_f -> backlog_ratio margin -> 0.10 -> 10%.

## Known limitations

The following shock keys are currently unused by the simulator and are kept for documentation/future wiring:
- shocks.O1.mult.cap_ops
- shocks.O4.mult.route_remove_frac
- shocks.O4.mult.reroute_cost
- shocks.O4.mult.reroute_latency
- shocks.ADV5.mult.p_commit_fail

## IFS sign-convention audit (2026-03-08)

All six g_Z components {L, C, Q, D, R, F} follow the convention:
**higher raw value = more fragmentation = worse**.

| Component | KPIs used | Orientation | Status |
|-----------|-----------|-------------|--------|
| L | K1_latency_p90, K1_latency_median | higher = worse | CORRECT |
| C | K2_fee_mean, K2_ops_proxy_mean | higher = worse | CORRECT |
| Q | K3_liq_lock_exposure_mean, K3_coll_lock_exposure_mean | higher = worse | CORRECT |
| D | K4_checkpoints_mean, K4_rescreen_rate, K4_repair_due_missing_rate | higher = worse | CORRECT |
| R | 1-availability, K5_op_fail_rate, K5_throughput_drop, K5_recovery_time | higher = worse | CORRECT |
| F | K6_spread_proxy, K6_fail_rate, K6_cycle_time_p90, K6_multi_ccy_liq_req_mean | higher = worse | CORRECT |

**STP_rate and trace_score**: AUX KPIs that do NOT enter any g_Z component.
They appear only in NI-3 checks with correct orientation handling
(trace >= threshold; backlog = 1 - STP_rate).
If they are ever added to a fragmentation component, they must be inverted
(e.g., `1 - STP_rate`) to maintain the convention.

No code changes were required by this audit.  Monotonicity QA tests added
in `tests/test_monotonicity.py`.

## Config usage policy

| Config file | Purpose | Use for thesis results? |
|-------------|---------|------------------------|
| `configs/thesis_smoke.json` | Fast CI/smoke test (T_total=120, 2 seeds) | NO |
| `configs/thesis_pilot.json` | Pilot envelope / bounds calibration (T_total=500, 2 seeds) | NO |
| `configs/thesis_v1.json` | Final thesis batch (T_total=500, 30 seeds) | **YES** |

Only `thesis_v1.json` with calibrated bounds from the pilot envelope run
is used for citable thesis results.

## Bounds calibration procedure (pilot + manual cap)

1. Run pilot: `python scripts/run_pilot.py --config configs/thesis_pilot.json`
2. Extract bounds: `python scripts/extract_pilot_bounds.py --runs-root runs_pilot`
3. Per flow {XBPAY, PVP, DVP} and component Z in {L, C, Q, D, R, F}:
   a. Compute p01 and p99 of Z_raw across all pilot kpi_run.json files.
   b. Expand by 20%: z_min = max(0, p01 * 0.8), z_max = p99 * 1.2.
   c. Set ifs.bounds.<flow>.<Z>.min/max in thesis_v1.json.
4. Minimal manual review: only adjust if clipping is absurd or economically inconsistent.
5. Document every manual revision in the "Manual bounds caps" section below.
6. Validate: `python scripts/validate_config.py configs/thesis_v1.json`
7. Once frozen, do NOT touch bounds again before final batch unless there is a real bug.

## Manual bounds caps (thesis_v1)

| Flow | Component | Original z_max | Capped z_max | Reason |
|------|-----------|---------------|--------------|--------|
| _(to be filled after pilot)_ | | | | |

## Non-final results notice

Results from smoke runs and pilot envelope runs are NOT thesis outputs.
They serve only for pipeline validation and bounds calibration.
Any results produced before the sign-convention audit and bounds freeze
(including any previous `results_thesis_v1_1/` artifacts) are preliminary
and must NOT appear in the thesis document.  Only the final thesis batch
(30 seeds, calibrated bounds, frozen thesis_v1.json) constitutes the
citable result set.
