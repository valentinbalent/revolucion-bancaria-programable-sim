"""
tests/test_monotonicity.py

Monotonicity QA: for each IFS component {L,C,Q,D,R,F}, verify that
worsening the underlying KPIs produces a HIGHER g_Z_scalar and a
HIGHER (or equal) normalized value.

Convention: higher IFS = more fragmentation = worse.
See docs/DECISIONS.md "IFS sign-convention audit" for the full audit.

No simulation needed — pure unit tests on g_Z_scalar and normalize.
"""
from __future__ import annotations

import json

import pytest

from revolucion_bancaria_programable_sim.metrics import g_Z_scalar, normalize


@pytest.fixture()
def cfg():
    """Minimal config loaded from thesis_smoke.json (phi weights + bounds)."""
    with open("configs/thesis_smoke.json", encoding="utf-8") as f:
        return json.load(f)


def _base_kpis():
    """Return a 'good' (low-fragmentation) KPI dict with all keys at benign values."""
    return {
        "K1_latency_p90": 5.0,
        "K1_latency_median": 3.0,
        "K2_fee_mean": 0.5,
        "K2_ops_proxy_mean": 0.1,
        "K3_liq_lock_exposure_mean": 100.0,
        "K3_coll_lock_exposure_mean": 20.0,
        "K4_checkpoints_mean": 1.0,
        "K4_rescreen_rate": 0.0,
        "K4_repair_due_missing_rate": 0.01,
        "K5_availability": 0.99,
        "K5_op_fail_rate": 0.01,
        "K5_throughput_drop": 0.02,
        "K5_recovery_time": 1.0,
        "K6_spread_proxy": 0.001,
        "K6_fail_rate": 0.01,
        "K6_cycle_time_p90": 5.0,
        "K6_multi_ccy_liq_req_mean": 50.0,
    }


FLOWS = ["XBPAY", "PVP", "DVP"]

# (component, list of (kpi_key, delta_to_worsen) pairs)
# Positive delta = increase the KPI value (worse for most).
# Negative delta for K5_availability because lower availability = worse.
COMPONENT_CASES = [
    ("L", [("K1_latency_p90", 10.0), ("K1_latency_median", 5.0)]),
    ("C", [("K2_fee_mean", 2.0), ("K2_ops_proxy_mean", 1.0)]),
    ("Q", [("K3_liq_lock_exposure_mean", 500.0), ("K3_coll_lock_exposure_mean", 100.0)]),
    ("D", [("K4_checkpoints_mean", 3.0), ("K4_repair_due_missing_rate", 0.1)]),
    (
        "R",
        [
            ("K5_availability", -0.1),
            ("K5_op_fail_rate", 0.05),
            ("K5_throughput_drop", 0.1),
            ("K5_recovery_time", 5.0),
        ],
    ),
    (
        "F",
        [
            ("K6_spread_proxy", 0.01),
            ("K6_fail_rate", 0.05),
            ("K6_cycle_time_p90", 10.0),
            ("K6_multi_ccy_liq_req_mean", 200.0),
        ],
    ),
]


@pytest.mark.parametrize(
    "comp,deltas",
    COMPONENT_CASES,
    ids=[c[0] for c in COMPONENT_CASES],
)
def test_monotonicity_g_Z(cfg, comp, deltas):
    """Worsening KPIs for component ``comp`` must increase g_Z_scalar."""
    for flow in FLOWS:
        better = _base_kpis()
        worse = _base_kpis()
        for key, delta in deltas:
            worse[key] = better[key] + delta

        z_better = g_Z_scalar(cfg, flow, comp, better)
        z_worse = g_Z_scalar(cfg, flow, comp, worse)
        assert z_worse > z_better, (
            f"Monotonicity failed for {comp}/{flow}: "
            f"g_Z(worse)={z_worse} <= g_Z(better)={z_better}"
        )


@pytest.mark.parametrize(
    "comp,deltas",
    COMPONENT_CASES,
    ids=[c[0] for c in COMPONENT_CASES],
)
def test_monotonicity_normalize(cfg, comp, deltas):
    """Worsening KPIs must produce higher or equal normalized value."""
    for flow in FLOWS:
        better = _base_kpis()
        worse = _base_kpis()
        for key, delta in deltas:
            worse[key] = better[key] + delta

        z_better = g_Z_scalar(cfg, flow, comp, better)
        z_worse = g_Z_scalar(cfg, flow, comp, worse)

        n_better = normalize(cfg, flow, comp, z_better)
        n_worse = normalize(cfg, flow, comp, z_worse)
        assert n_worse >= n_better, (
            f"Normalize monotonicity failed for {comp}/{flow}: "
            f"norm(worse)={n_worse} < norm(better)={n_better}"
        )
