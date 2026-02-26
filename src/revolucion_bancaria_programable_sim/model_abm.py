# model_abm.py
"""
ABM layer:
- defines Agents (objects)
- generates transaction intents (ABM→DE triggers) using CRN shared demand streams
- sets tx attributes: msg_quality, trace_pack, jurisdictions, fx_flag, etc.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional, Tuple
import math
import numpy as np

from .rng import CRN


@dataclass
class ComplianceProfile:
    risk_tier: str
    kyc_status: str


@dataclass
class Agent:
    id: str
    type: str
    jurisdiction: str
    liquid_balances: Dict[str, float]
    collateral_available: float
    queue_policy: str
    priority_policy: str
    compliance_profile: ComplianceProfile
    credentials_state: Optional[str] = None  # only relevant in World B per spec

    # For DvP assets
    holdings: Optional[Dict[str, float]] = None


@dataclass
class TxIntent:
    tx_id: str
    flow: str
    t_init: float
    sender: str
    receiver: str

    # XBPAY fields
    amount: float = 0.0
    ccy: str = "USD"
    urgency: int = 0
    msg_quality: float = 1.0
    trace_pack: Dict[str, Any] = None
    juris_src: str = ""
    juris_dst: str = ""
    fx_flag: int = 0

    # PvP fields
    partyA: str = ""
    partyB: str = ""
    ccy1: str = ""
    ccy2: str = ""
    notional: float = 0.0
    rate: float = 1.0

    # DvP fields
    buyer: str = ""
    seller: str = ""
    cash_ccy: str = ""
    cash_amt: float = 0.0
    asset_id: str = ""
    qty: float = 0.0
    price: float = 0.0

    def to_trigger(self) -> Dict[str, Any]:
        """
        Return the canonical ABM→DE trigger payload (C2).
        """
        if self.flow == "XBPAY":
            return {
                "type": "XBPAY_INIT",
                "tx_id": self.tx_id,
                "sender": self.sender,
                "receiver": self.receiver,
                "amount": self.amount,
                "ccy": self.ccy,
                "urgency": self.urgency,
                "msg_quality": self.msg_quality,
                "trace_pack": self.trace_pack,
                "juris_src": self.juris_src,
                "juris_dst": self.juris_dst,
                "fx_flag": self.fx_flag,
            }
        if self.flow == "PVP":
            return {
                "type": "PVP_INIT",
                "tx_id": self.tx_id,
                "partyA": self.partyA,
                "partyB": self.partyB,
                "ccy1": self.ccy1,
                "ccy2": self.ccy2,
                "notional": self.notional,
                "rate": self.rate,
                "urgency": self.urgency,
                "msg_quality": self.msg_quality,
                "trace_pack": self.trace_pack,
            }
        if self.flow == "DVP":
            return {
                "type": "DVP_INIT",
                "tx_id": self.tx_id,
                "buyer": self.buyer,
                "seller": self.seller,
                "cash_ccy": self.cash_ccy,
                "cash_amt": self.cash_amt,
                "asset_id": self.asset_id,
                "qty": self.qty,
                "price": self.price,
                "urgency": self.urgency,
                "msg_quality": self.msg_quality,
                "trace_pack": self.trace_pack,
                "fx_flag": self.fx_flag,
            }
        raise ValueError(f"Unknown flow: {self.flow}")


def _exp_interarrival(rng: np.random.Generator, rate: float) -> float:
    if rate <= 0:
        return float("inf")
    u = max(1e-12, float(rng.random()))
    return -math.log(u) / rate


def _bounded(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _make_trace_pack(rng: np.random.Generator, msg_quality: float, world_tag: str) -> Dict[str, Any]:
    """
    trace_pack is an arbitrary structure; trace_score computed deterministically downstream.

    Keep it compact but stable:
      - fields: completeness, handoff_consistency, standardization
    """
    # completeness correlates with msg_quality
    noise = float(rng.normal(0.0, 0.05))
    completeness = _bounded(msg_quality + noise, 0.0, 1.0)
    handoff_consistency = _bounded(msg_quality + float(rng.normal(0.0, 0.07)), 0.0, 1.0)
    standardization = _bounded(msg_quality + float(rng.normal(0.0, 0.06)), 0.0, 1.0)

    return {
        "completeness": completeness,
        "handoff_consistency": handoff_consistency,
        "standardization": standardization,
        "world_hint": world_tag,  # not used by metrics; for debugging
    }


def build_agents(cfg: Dict[str, Any], crn: CRN) -> Dict[str, Agent]:
    """
    Create a synthetic agent population (shared across A/B under CRN).
    """
    n_agents = int(cfg["params"]["population"]["n_agents"])
    n_j = int(cfg["params"]["population"]["n_jurisdictions"])
    ccys = list(cfg["params"]["population"]["ccys"])
    assets = list(cfg["params"]["population"]["assets"])

    rng_pop = crn.stream("population", shared=True)
    agents: Dict[str, Agent] = {}
    for i in range(n_agents):
        aid = f"A{i:03d}"
        juris = f"J{int(rng_pop.integers(0, n_j)):02d}"
        # balances
        bal = {}
        for c in ccys:
            # DEBUG_ONLY-ish: balances are generated but deterministic via CRN.
            bal[c] = float(abs(rng_pop.normal(5000.0, 1000.0)) + 1000.0)
        holdings = {assets[0]: float(abs(rng_pop.normal(100.0, 20.0)) + 10.0)} if assets else {}

        risk_tier = ["low", "med", "high"][int(rng_pop.integers(0, 3))]
        kyc_status = ["verified", "pending"][int(rng_pop.integers(0, 2))]

        agents[aid] = Agent(
            id=aid,
            type="bank",
            jurisdiction=juris,
            liquid_balances=bal,
            collateral_available=float(abs(rng_pop.normal(2000.0, 500.0)) + 500.0),
            queue_policy="fifo",
            priority_policy="urgency",
            compliance_profile=ComplianceProfile(risk_tier=risk_tier, kyc_status=kyc_status),
            credentials_state=None,  # set for World B at runtime if needed
            holdings=holdings,
        )
    return agents


def generate_tx_intents(cfg: Dict[str, Any], crn: CRN, agents: Dict[str, Agent], scenario_id: str) -> List[TxIntent]:
    """
    Generate transaction intents for full horizon T_total using shared CRN streams.

    Controls:
      - same seed => same intents A/B (CRN shared)
    """
    T_total = float(cfg["run"]["T_total"])
    demand = cfg["params"]["demand"]
    rate_x = float(demand["XBPAY_rate"])
    rate_p = float(demand["PVP_rate"])
    rate_d = float(demand["DVP_rate"])

    fx_cfg = cfg["params"]["fx"]
    p_fx_x = float(fx_cfg["p_fx_XBPAY"])
    p_fx_d = float(fx_cfg["p_fx_DVP"])

    q_sem = float(cfg["params"]["q_sem"])

    rng_arr = crn.stream(f"arrivals|{scenario_id}", shared=True)
    rng_amt = crn.stream(f"amounts|{scenario_id}", shared=True)
    rng_part = crn.stream(f"parties|{scenario_id}", shared=True)
    rng_msg = crn.stream(f"msg|{scenario_id}", shared=True)

    agent_ids = list(agents.keys())
    ccys = list(cfg["params"]["population"]["ccys"])
    asset_id = cfg["params"]["population"]["assets"][0] if cfg["params"]["population"]["assets"] else "ASSET1"

    intents: List[TxIntent] = []
    tx_counter = 0

    def pick_two_distinct() -> Tuple[str, str]:
        a = agent_ids[int(rng_part.integers(0, len(agent_ids)))]
        b = agent_ids[int(rng_part.integers(0, len(agent_ids)))]
        while b == a:
            b = agent_ids[int(rng_part.integers(0, len(agent_ids)))]
        return a, b

    # superpose 3 Poisson processes by simulating each separately
    def gen_flow(flow: str, rate: float):
        nonlocal tx_counter
        t = 0.0
        while t < T_total:
            dt = _exp_interarrival(rng_arr, rate)
            t += dt
            if t >= T_total:
                break

            a, b = pick_two_distinct()

            # message quality around q_sem
            msg_quality = _bounded(float(rng_msg.normal(q_sem, 0.1)), 0.0, 1.0)
            trace_pack = _make_trace_pack(rng_msg, msg_quality, world_tag="shared")

            urgency = int(rng_msg.integers(0, 3))

            tx_id = f"{scenario_id}-{flow}-{tx_counter:08d}"
            tx_counter += 1

            if flow == "XBPAY":
                ccy = ccys[int(rng_part.integers(0, len(ccys)))]
                amount = float(abs(rng_amt.normal(1000.0, 500.0)) + 10.0)
                fx_flag = 1 if float(rng_part.random()) < p_fx_x else 0

                intents.append(TxIntent(
                    tx_id=tx_id, flow="XBPAY", t_init=t,
                    sender=a, receiver=b,
                    amount=amount, ccy=ccy, urgency=urgency,
                    msg_quality=msg_quality, trace_pack=trace_pack,
                    juris_src=agents[a].jurisdiction, juris_dst=agents[b].jurisdiction,
                    fx_flag=fx_flag,
                ))

            elif flow == "PVP":
                ccy1, ccy2 = "USD", "EUR"
                notional = float(abs(rng_amt.normal(2000.0, 700.0)) + 10.0)
                rate_fx = float(abs(rng_amt.normal(1.0, 0.05)) + 1e-6)
                intents.append(TxIntent(
                    tx_id=tx_id, flow="PVP", t_init=t,
                    sender=a, receiver=b,
                    partyA=a, partyB=b, ccy1=ccy1, ccy2=ccy2,
                    notional=notional, rate=rate_fx,
                    urgency=urgency, msg_quality=msg_quality, trace_pack=trace_pack,
                ))

            elif flow == "DVP":
                cash_ccy = ccys[int(rng_part.integers(0, len(ccys)))]
                qty = float(abs(rng_amt.normal(10.0, 3.0)) + 0.1)
                price = float(abs(rng_amt.normal(100.0, 20.0)) + 1.0)
                cash_amt = qty * price
                fx_flag = 1 if float(rng_part.random()) < p_fx_d else 0
                intents.append(TxIntent(
                    tx_id=tx_id, flow="DVP", t_init=t,
                    sender=a, receiver=b,
                    buyer=a, seller=b,
                    cash_ccy=cash_ccy, cash_amt=cash_amt,
                    asset_id=asset_id, qty=qty, price=price,
                    urgency=urgency, msg_quality=msg_quality, trace_pack=trace_pack,
                    fx_flag=fx_flag,
                ))

            else:
                raise ValueError(flow)

    gen_flow("XBPAY", rate_x)
    gen_flow("PVP", rate_p)
    gen_flow("DVP", rate_d)

    # Stable ordering by time
    intents.sort(key=lambda x: x.t_init)
    return intents
