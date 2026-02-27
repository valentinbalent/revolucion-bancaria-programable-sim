# model_de.py
"""
DE layer (SimPy):
- Executes queues/latencies/service/cutoffs, legs + commit/rollback, outages, retries, repair/manual backlog.
- Implements DE→ABM events via tx completion records and event sampler logs.
- Produces tx_log rows (contract-based fields), event_log rows (bucketized), and optional agent snapshots.

This is a minimal runnable scaffold; extend pipelines as your thesis requires.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import simpy

from .model_abm import Agent, TxIntent
from .rng import CRN


def clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def trace_score_from(msg_quality: float, trace_pack: Dict[str, Any]) -> float:
    """
    Deterministic h(·) in [0,1] (auxiliary; NOT IFS).
    """
    if not trace_pack:
        return clip01(msg_quality * 0.5)
    c = float(trace_pack.get("completeness", 0.0))
    h = float(trace_pack.get("handoff_consistency", 0.0))
    s = float(trace_pack.get("standardization", 0.0))
    return clip01(0.4 * msg_quality + 0.2 * c + 0.2 * h + 0.2 * s)


def draw_time(dist_def: Dict[str, Any], rng: np.random.Generator, mean_key: str = "mean") -> float:
    """
    Supported: exp with mean.
    """
    dist = dist_def.get("dist", "exp")
    mean = float(dist_def.get(mean_key))
    if mean <= 0:
        return 0.0
    if dist == "exp":
        u = max(1e-12, float(rng.random()))
        return -math.log(u) * mean
    raise ValueError(f"Unsupported distribution: {dist}")


@dataclass
class ShockWindow:
    id: str
    t_start: float
    t_end: float
    severity: str = "base"


class ShockManager:
    """
    Provides time-varying multipliers for parameters during shock windows.
    """
    def __init__(self, cfg: Dict[str, Any], scenario_id: str):
        self.cfg = cfg
        self.scenario_id = scenario_id
        self.windows: List[ShockWindow] = []
        for sh in cfg["scenarios"][scenario_id].get("shocks", []):
            self.windows.append(ShockWindow(
                id=sh["id"],
                t_start=float(sh["t_start"]),
                t_end=float(sh["t_end"]),
                severity=str(sh.get("severity", "base")),
            ))

    def active_ids(self, t: float) -> List[str]:
        return [w.id for w in self.windows if w.t_start <= t <= w.t_end]

    def mult(self, t: float, param: str) -> float:
        """
        Combine multipliers multiplicatively if multiple shocks touch same param.
        """
        m = 1.0
        for w in self.windows:
            if not (w.t_start <= t <= w.t_end):
                continue
            odef = self.cfg["shocks"].get(w.id, {})
            mm = odef.get("mult", {})
            # mapping keys in shocks to param names used in DE
            if param == "p_out" and "p_out" in mm:
                m *= float(mm["p_out"])
            if param == "tau_out_mean" and "tau_out_mean" in mm:
                m *= float(mm["tau_out_mean"])
            if param == "tau_srv_mean" and "tau_srv_mean" in mm:
                m *= float(mm["tau_srv_mean"])
            if param == "line_intra" and "line_intra" in mm:
                m *= float(mm["line_intra"])
            if param == "locks" and "locks" in mm:
                m *= float(mm["locks"])
            if param == "req_change" and "req_change" in mm:
                m *= float(mm["req_change"])
            if param == "q_sem" and "q_sem" in mm:
                m *= float(mm["q_sem"])
            if param == "p_exc" and "p_exc" in mm:
                m *= float(mm["p_exc"])
            if param == "tau_rep_mean" and "tau_rep_mean" in mm:
                m *= float(mm["tau_rep_mean"])
            if param == "holds" and "holds" in mm:
                m *= float(mm["holds"])
            if param == "san_scope" and "san_scope" in mm:
                m *= float(mm["san_scope"])
            if param == "reroute_fail" and "reroute_fail" in mm:
                m *= float(mm["reroute_fail"])
        return m

    def forced_liq_policy(self, t: float) -> Optional[str]:
        for w in self.windows:
            if not (w.t_start <= t <= w.t_end):
                continue
            odef = self.cfg["shocks"].get(w.id, {})
            pol = odef.get("force_liq_buf_policy")
            if pol:
                return str(pol)
        return None


class ComponentOutage:
    """
    Simple outage model:
      - with intensity p_out (per unit time) triggers DOWN for duration ~ tau_out
    """
    def __init__(self, env: simpy.Environment, name: str, cfg: Dict[str, Any], rng: np.random.Generator, shocks: ShockManager):
        self.env = env
        self.name = name
        self.cfg = cfg
        self.rng = rng
        self.shocks = shocks
        self.state = "OK"  # OK/DEGRADED/DOWN
        self.downtime = 0.0
        self.last_change_t = env.now

        # Event that processes can wait on when DOWN
        self._up_event = env.event()
        self._up_event.succeed()  # initially up

        self.process = env.process(self.run())

    def is_up(self) -> bool:
        return self.state != "DOWN"

    def wait_if_down(self):
        if self.state == "DOWN":
            return self._up_event
        # already up -> immediate
        ev = self.env.event()
        ev.succeed()
        return ev

    def run(self):
        while True:
            # hazard per unit time; implement as small step exponential approx
            p_out_base = float(self.cfg["params"]["p_out"])
            p_out_eff = p_out_base * self.shocks.mult(self.env.now, "p_out")

            # Next outage inter-arrival (approx exp with rate p_out_eff)
            if p_out_eff <= 0:
                yield self.env.timeout(1.0)
                continue
            u = max(1e-12, float(self.rng.random()))
            dt = -math.log(u) / p_out_eff
            yield self.env.timeout(dt)

            # go DOWN
            self._set_state("DOWN")
            tau_out_mean = float(self.cfg["params"]["tau_out"]["mean"]) * self.shocks.mult(self.env.now, "tau_out_mean")
            dur = draw_time({"dist": "exp", "mean": tau_out_mean}, self.rng)
            yield self.env.timeout(dur)

            # back up
            self._set_state("OK")

    def _set_state(self, new_state: str):
        t = self.env.now
        # accumulate downtime
        if self.state == "DOWN":
            self.downtime += (t - self.last_change_t)
        self.state = new_state
        self.last_change_t = t

        if new_state != "DOWN":
            # release waiters
            self._up_event = self.env.event()
            self._up_event.succeed()
        else:
            # create an unsatisfied event for waiters
            self._up_event = self.env.event()


class DESim:
    def __init__(
        self,
        cfg: Dict[str, Any],
        scenario_id: str,
        world: str,  # "A" or "B"
        seed: int,
        crn: CRN,
        agents: Dict[str, Agent],
        intents: List[TxIntent],
    ):
        self.cfg = cfg
        self.scenario_id = scenario_id
        self.world = world
        self.seed = int(seed)
        self.crn = crn
        self.agents = agents
        self.intents = intents

        self.env = simpy.Environment()
        self.shocks = ShockManager(cfg, scenario_id)

        # World-specific RNG substreams (risks exclusive to B)
        self.rng_srv = crn.stream(f"service|{scenario_id}", shared=True)  # shared service noise (comparable)
        self.rng_exc = crn.stream(f"exceptions|{scenario_id}", shared=True)  # shared exception draws
        self.rng_world = crn.stream(f"world|{scenario_id}", shared=False, world=world)  # exclusive randomness

        # Ops manual capacity
        cap_ops = int(cfg["params"]["cap_ops"])
        self.manual_ops = simpy.Resource(self.env, capacity=max(1, cap_ops))

        # Shared components with outages
        self.comp_settlement = ComponentOutage(self.env, "settlement", cfg, self.rng_world, self.shocks)
        self.comp_compliance = ComponentOutage(self.env, "compliance", cfg, self.rng_world, self.shocks)

        # Ledgers for conservation checks
        self.fee_account: Dict[str, float] = {c: 0.0 for c in cfg["params"]["population"]["ccys"]}

        # Logs
        self.tx_rows: List[Dict[str, Any]] = []
        self.event_rows: List[Dict[str, Any]] = []

        # For event sampling
        self._bucket_stats = {
            "arrivals": {"XBPAY": 0, "PVP": 0, "DVP": 0},
            "settled": {"XBPAY": 0, "PVP": 0, "DVP": 0},
            "failed": {"XBPAY": 0, "PVP": 0, "DVP": 0},
            "held": {"XBPAY": 0, "PVP": 0, "DVP": 0},
            "queued": {"XBPAY": 0, "PVP": 0, "DVP": 0},
            "backlog_manual": 0,
            "down_settlement": 0.0,
            "down_compliance": 0.0,
        }
        self._bucket_start_t = 0.0

        # Track manual ops queue length approx (SimPy doesn't expose directly robustly)
        self._manual_ops_requests = 0

    def _effective_liq_policy(self) -> str:
        pol = self.cfg["params"].get("liq_buf_policy", "neutral")
        forced = self.shocks.forced_liq_policy(self.env.now)
        return forced or pol

    def _line_intra_eff(self) -> float:
        base = float(self.cfg["params"]["line_intra"])
        return base * self.shocks.mult(self.env.now, "line_intra")

    def _locks_mult(self) -> float:
        return self.shocks.mult(self.env.now, "locks")

    def _p_exc_eff(self, intent: TxIntent) -> float:
        base = float(self.cfg["params"]["p_exc"])
        # heterogeneity, compliance intensity, and semantic shifts
        J_het = float(self.cfg["params"]["J_het"])
        ctl = float(self.cfg["params"]["ctl_int"])
        mult = self.shocks.mult(self.env.now, "p_exc")
        # mild coupling to msg_quality
        qual_penalty = (1.0 - float(intent.msg_quality)) * (0.5 + 0.5 * J_het)
        return clip01(base * mult + 0.05 * ctl + 0.05 * qual_penalty)

    def _tau_srv_mean_eff(self) -> float:
        # base mean depends on world A vs B (treatment)
        base = float(self.cfg["params"]["tau_srv_A"]["mean"] if self.world == "A" else self.cfg["params"]["tau_srv_B"]["mean"])
        return base * self.shocks.mult(self.env.now, "tau_srv_mean")

    def _tau_rep_mean_eff(self) -> float:
        base = float(self.cfg["params"]["tau_rep"]["mean"])
        return base * self.shocks.mult(self.env.now, "tau_rep_mean")

    def _san_scope_eff(self) -> float:
        base = float(self.cfg["params"]["san_scope"])
        return base * self.shocks.mult(self.env.now, "san_scope")

    def _reroute_fail_mult(self) -> float:
        return self.shocks.mult(self.env.now, "reroute_fail")

    def _holds_mult(self) -> float:
        return self.shocks.mult(self.env.now, "holds")

    def _fee_eff(self) -> float:
        return float(self.cfg["params"]["fee_A"] if self.world == "A" else self.cfg["params"]["fee_B"])

    def _commit_fail_p(self) -> float:
        # only meaningful in B; use world-specific randomness
        base = float(self.cfg["params"]["p_commit_fail"])
        return clip01(base)

    def _count_checkpoints(self, intent: TxIntent) -> int:
        # A has more checkpoints; B reuses SSI/VC -> fewer
        J_het = float(self.cfg["params"]["J_het"])
        ctl = float(self.cfg["params"]["ctl_int"])
        base = 2 if self.world == "B" else 4
        # heterogeneity and intensity inflate duplication
        extra = int(round(2 * J_het + 2 * ctl))
        # O3 increases requirements
        req = float(self.cfg["params"]["req_change"]) * self.shocks.mult(self.env.now, "req_change")
        extra += int(round(req))
        return max(1, base + extra)

    def _maybe_hold_reason(self, intent: TxIntent) -> Optional[str]:
        # O3 increases holds; higher ctl_int increases holds.
        ctl = float(self.cfg["params"]["ctl_int"])
        holds_mult = self._holds_mult()
        p_hold = clip01(0.05 * ctl * holds_mult + (1.0 - float(intent.msg_quality)) * 0.1 * holds_mult)
        if float(self.rng_exc.random()) < p_hold:
            return "COMPLIANCE_HOLD"
        return None

    def _maybe_sanction_block(self, intent: TxIntent) -> bool:
        # O4: more de-risking; also depends on jurisdiction mismatch
        san = self._san_scope_eff()
        mismatch = 1.0 if getattr(intent, "juris_src", "") != getattr(intent, "juris_dst", "") else 0.5
        p_block = clip01(0.05 * san * mismatch * self._reroute_fail_mult())
        return float(self.rng_exc.random()) < p_block

    def _manual_repair(self, tx_state: Dict[str, Any]) -> simpy.events.Event:
        """
        Manual ops backlog: request manual_ops resource and wait repair time.
        """
        self._manual_ops_requests += 1
        req = self.manual_ops.request()
        tx_state["manual_review_flag"] = True

        def _proc():
            t0 = self.env.now
            yield req
            # approximate backlog measurement
            self._bucket_stats["backlog_manual"] = max(self._bucket_stats["backlog_manual"], self._manual_ops_requests)
            tau = draw_time({"dist": "exp", "mean": self._tau_rep_mean_eff()}, self.rng_srv)
            yield self.env.timeout(tau)
            t1 = self.env.now
            tx_state["rework_time"] += (t1 - t0)
            self.manual_ops.release(req)
            self._manual_ops_requests -= 1

        return self.env.process(_proc())

    def _wait_component(self, comp: ComponentOutage) -> simpy.events.Event:
        return comp.wait_if_down()

    def _sample_events(self):
        dt = float(self.cfg["run"]["dt_bucket"])
        while True:
            yield self.env.timeout(dt)
            t = self.env.now
            # estimate downtime in last bucket by comparing state at bucket end; coarse but deterministic
            down_set = 1.0 if self.comp_settlement.state == "DOWN" else 0.0
            down_com = 1.0 if self.comp_compliance.state == "DOWN" else 0.0

            row = {
                "t_bucket_start": self._bucket_start_t,
                "t_bucket_end": t,
                "scenario": self.scenario_id,
                "world": self.world,
                "seed": self.seed,
                "arrivals_XBPAY": self._bucket_stats["arrivals"]["XBPAY"],
                "arrivals_PVP": self._bucket_stats["arrivals"]["PVP"],
                "arrivals_DVP": self._bucket_stats["arrivals"]["DVP"],
                "settled_XBPAY": self._bucket_stats["settled"]["XBPAY"],
                "settled_PVP": self._bucket_stats["settled"]["PVP"],
                "settled_DVP": self._bucket_stats["settled"]["DVP"],
                "failed_XBPAY": self._bucket_stats["failed"]["XBPAY"],
                "failed_PVP": self._bucket_stats["failed"]["PVP"],
                "failed_DVP": self._bucket_stats["failed"]["DVP"],
                "held_XBPAY": self._bucket_stats["held"]["XBPAY"],
                "held_PVP": self._bucket_stats["held"]["PVP"],
                "held_DVP": self._bucket_stats["held"]["DVP"],
                "queued_XBPAY": self._bucket_stats["queued"]["XBPAY"],
                "queued_PVP": self._bucket_stats["queued"]["PVP"],
                "queued_DVP": self._bucket_stats["queued"]["DVP"],
                "backlog_manual_peak": self._bucket_stats["backlog_manual"],
                "settlement_state": self.comp_settlement.state,
                "compliance_state": self.comp_compliance.state,
                "down_settlement_flag": down_set,
                "down_compliance_flag": down_com,
                "active_shocks": ",".join(self.shocks.active_ids(t)),
            }
            self.event_rows.append(row)

            # reset bucket stats
            self._bucket_stats["arrivals"] = {"XBPAY": 0, "PVP": 0, "DVP": 0}
            self._bucket_stats["settled"] = {"XBPAY": 0, "PVP": 0, "DVP": 0}
            self._bucket_stats["failed"] = {"XBPAY": 0, "PVP": 0, "DVP": 0}
            self._bucket_stats["held"] = {"XBPAY": 0, "PVP": 0, "DVP": 0}
            self._bucket_stats["queued"] = {"XBPAY": 0, "PVP": 0, "DVP": 0}
            self._bucket_stats["backlog_manual"] = 0
            self._bucket_start_t = t

    def _schedule_intents(self):
        """
        Schedule each intent as a process starting at its t_init.
        """
        for intent in self.intents:
            self.env.process(self._tx_process(intent))

    def _tx_process(self, intent: TxIntent):
        """
        Process a tx end-to-end, log TX_SETTLED / TX_FAILED / TX_HELD as per C2.
        """
        # wait until init time
        yield self.env.timeout(max(0.0, float(intent.t_init) - self.env.now))
        self._bucket_stats["arrivals"][intent.flow] += 1

        # tx state
        tx = {
            "tx_id": intent.tx_id,
            "flow": intent.flow,
            "scenario": self.scenario_id,
            "world": self.world,
            "seed": self.seed,
            "t_init": float(intent.t_init),
            "status": "INIT",
            "msg_quality": float(intent.msg_quality),
            "trace_score": trace_score_from(float(intent.msg_quality), intent.trace_pack),
            "trace_pack": intent.trace_pack or {},
            "exc_count": 0,
            "rework_time": 0.0,
            "retry_count": 0,
            "manual_review_flag": False,
            "holds_count": 0,
            "queue_time": 0.0,
            "hold_time": 0.0,
            "repair_due_to_missing_data_flag": False,
            "atomicity_violation_flag": False,
            "fee_total": 0.0,
            "liq_lock_amt": 0.0,
            "liq_lock_dur": 0.0,
            "coll_lock_amt": 0.0,
            "coll_lock_dur": 0.0,
            "stp_flag": True,  # may flip false on manual repair/holds/exc
            "n_checkpoints": self._count_checkpoints(intent),
        }

        # Populate flow-specific identity fields (contract C2)
        if intent.flow == "XBPAY":
            tx.update({
                "sender": intent.sender,
                "receiver": intent.receiver,
                "amount": float(intent.amount),
                "ccy": str(intent.ccy),
                "urgency": int(intent.urgency),
                "juris_src": str(intent.juris_src),
                "juris_dst": str(intent.juris_dst),
                "fx_flag": int(intent.fx_flag),
            })
        elif intent.flow == "PVP":
            tx.update({
                "partyA": intent.partyA,
                "partyB": intent.partyB,
                "ccy1": intent.ccy1,
                "ccy2": intent.ccy2,
                "notional": float(intent.notional),
                "rate": float(intent.rate),
            })
        elif intent.flow == "DVP":
            tx.update({
                "buyer": intent.buyer,
                "seller": intent.seller,
                "cash_ccy": intent.cash_ccy,
                "cash_amt": float(intent.cash_amt),
                "asset_id": intent.asset_id,
                "qty": float(intent.qty),
                "price": float(intent.price),
                "fx_flag": int(intent.fx_flag),
            })

        # compliance stage (may hold, may sanction-block)
        t0 = self.env.now
        yield self._wait_component(self.comp_compliance)

        hold_reason = self._maybe_hold_reason(intent)
        if hold_reason is not None:
            tx["holds_count"] += 1
            tx["stp_flag"] = False
            self._bucket_stats["held"][intent.flow] += 1
            # hold time depends on manual ops / repair mean
            hold_tau = 0.5 * self._tau_rep_mean_eff()
            yield self.env.timeout(hold_tau)
            tx["hold_time"] += (self.env.now - t0)

        if intent.flow == "XBPAY":
            if self._maybe_sanction_block(intent):
                tx["status"] = "FAILED"
                tx["t_fail"] = self.env.now
                tx["reason"] = "SANCTION_BLOCK"
                tx["stp_flag"] = False
                self._bucket_stats["failed"][intent.flow] += 1
                self._finalize_tx(tx, settled=False)
                return

        # process flow pipeline
        if intent.flow == "XBPAY":
            ok = yield self._pipe_xbpay(tx)
        elif intent.flow == "PVP":
            ok = yield self._pipe_pvp(tx)
        elif intent.flow == "DVP":
            ok = yield self._pipe_dvp(tx)
        else:
            raise ValueError(intent.flow)

        if ok:
            tx["status"] = "SETTLED"
            tx["t_final"] = self.env.now
            self._bucket_stats["settled"][intent.flow] += 1
        else:
            tx["status"] = "FAILED"
            tx["t_fail"] = self.env.now
            self._bucket_stats["failed"][intent.flow] += 1

        self._finalize_tx(tx, settled=ok)

    def _finalize_tx(self, tx: Dict[str, Any], settled: bool):
        # fees (explicit)
        fee = self._fee_eff()
        tx["fee_total"] += fee

        # Apply fee to fee_account in tx currency if possible
        fee_ccy = tx.get("ccy") or tx.get("cash_ccy") or tx.get("ccy1") or "USD"
        if fee_ccy not in self.fee_account:
            self.fee_account[fee_ccy] = 0.0
        self.fee_account[fee_ccy] += fee

        # latency
        t_init = float(tx["t_init"])
        t_end = float(tx.get("t_final", tx.get("t_fail", self.env.now)))
        tx["latency"] = t_end - t_init

        # idempotence: ensure unique tx_id in logs
        # (hard QA gate also checks)
        self.tx_rows.append(tx)

    # ---------------------------
    # Pipelines per flow
    # ---------------------------

    def _pipe_xbpay(self, tx: Dict[str, Any]) -> simpy.events.Event:
        """
        XBPAY:
          - A: more steps, higher exc/repair; possible cutoffs modeled by queue delay
          - B: fewer touches, faster
        """
        def _proc():
            # queue/cutoff proxy: in A add extra delay for "correspondent chain"
            if self.world == "A":
                q_delay = 0.5 * self._tau_srv_mean_eff()
                tx["queue_time"] += q_delay
                self._bucket_stats["queued"]["XBPAY"] += 1
                yield self.env.timeout(q_delay)

            # service stage
            yield self._wait_component(self.comp_settlement)
            srv = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
            yield self.env.timeout(srv)

            # exception?
            p_exc = self._p_exc_eff(TxIntent(tx_id=tx["tx_id"], flow="XBPAY", t_init=tx["t_init"], sender=tx["sender"], receiver=tx["receiver"],
                                             msg_quality=tx["msg_quality"], trace_pack=tx["trace_pack"]))
            if float(self.rng_exc.random()) < p_exc:
                tx["exc_count"] += 1
                tx["stp_flag"] = False
                # missing data more likely in A and with O3 (req_change)
                req = float(self.cfg["params"]["req_change"]) * self.shocks.mult(self.env.now, "req_change")
                if self.world == "A" or req > 1.0:
                    tx["repair_due_to_missing_data_flag"] = True
                yield self._manual_repair(tx)

            # liquidity debit/credit (simple)
            ccy = str(tx["ccy"])
            amt = float(tx["amount"])
            sender = self.agents[str(tx["sender"])]
            receiver = self.agents[str(tx["receiver"])]

            # liquidity buffer policy affects effective line
            line = self._line_intra_eff()
            pol = self._effective_liq_policy()
            pol_factor = 0.7 if pol == "conservative" else 1.0

            if sender.liquid_balances.get(ccy, 0.0) + pol_factor * line < amt:
                # hold for liquidity (counts as hold)
                tx["holds_count"] += 1
                tx["stp_flag"] = False
                self._bucket_stats["held"]["XBPAY"] += 1
                hold_tau = 0.3 * self._tau_rep_mean_eff()
                yield self.env.timeout(hold_tau)
                tx["hold_time"] += hold_tau
                # if still insufficient -> fail
                if sender.liquid_balances.get(ccy, 0.0) + pol_factor * line < amt:
                    tx["reason"] = "INSUFFICIENT_LIQ"
                    return False

            sender.liquid_balances[ccy] = sender.liquid_balances.get(ccy, 0.0) - amt
            receiver.liquid_balances[ccy] = receiver.liquid_balances.get(ccy, 0.0) + amt
            return True

        return self.env.process(_proc())

    def _pipe_pvp(self, tx: Dict[str, Any]) -> simpy.events.Event:
        """
        PvP:
          - B: atomic commit with explicit locks
          - A: legs sequential with potential mismatch/unwind (atomicity_violation_flag may appear)
        """
        def _proc():
            partyA = self.agents[str(tx["partyA"])]
            partyB = self.agents[str(tx["partyB"])]
            ccy1 = str(tx["ccy1"])
            ccy2 = str(tx["ccy2"])
            notional = float(tx["notional"])
            rate = float(tx["rate"])
            amt1 = notional
            amt2 = notional * rate

            line = self._line_intra_eff()
            pol = self._effective_liq_policy()
            pol_factor = 0.7 if pol == "conservative" else 1.0
            locks_mult = self._locks_mult()

            yield self._wait_component(self.comp_settlement)

            if self.world == "B":
                # lock both legs
                t_lock = self.env.now
                tx["liq_lock_amt"] += locks_mult * (amt1 + amt2)
                # if insufficient, hold
                if partyA.liquid_balances.get(ccy1, 0.0) + pol_factor * line < amt1 or partyB.liquid_balances.get(ccy2, 0.0) + pol_factor * line < amt2:
                    tx["holds_count"] += 1
                    tx["stp_flag"] = False
                    self._bucket_stats["held"]["PVP"] += 1
                    hold_tau = 0.4 * self._tau_rep_mean_eff()
                    yield self.env.timeout(hold_tau)
                    tx["hold_time"] += hold_tau
                    if partyA.liquid_balances.get(ccy1, 0.0) + pol_factor * line < amt1 or partyB.liquid_balances.get(ccy2, 0.0) + pol_factor * line < amt2:
                        tx["reason"] = "INSUFFICIENT_LIQ"
                        # unlock
                        tx["liq_lock_dur"] += (self.env.now - t_lock)
                        return False

                # atomic commit attempt
                srv = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv)

                if float(self.rng_world.random()) < self._commit_fail_p():
                    tx["retry_count"] += 1
                    tx["stp_flag"] = False
                    # rollback – no leg settles
                    tx["reason"] = "COMMIT_FAIL"
                    tx["liq_lock_dur"] += (self.env.now - t_lock)
                    return False

                # commit: exchange
                partyA.liquid_balances[ccy1] -= amt1
                partyA.liquid_balances[ccy2] = partyA.liquid_balances.get(ccy2, 0.0) + amt2
                partyB.liquid_balances[ccy2] -= amt2
                partyB.liquid_balances[ccy1] = partyB.liquid_balances.get(ccy1, 0.0) + amt1

                tx["liq_lock_dur"] += (self.env.now - t_lock)
                return True

            else:
                # World A: sequential legs + potential mismatch
                # leg1 A pays ccy1 to B
                if partyA.liquid_balances.get(ccy1, 0.0) + pol_factor * line < amt1:
                    tx["holds_count"] += 1
                    tx["stp_flag"] = False
                    self._bucket_stats["held"]["PVP"] += 1
                    hold_tau = 0.3 * self._tau_rep_mean_eff()
                    yield self.env.timeout(hold_tau)
                    tx["hold_time"] += hold_tau
                    if partyA.liquid_balances.get(ccy1, 0.0) + pol_factor * line < amt1:
                        tx["reason"] = "INSUFFICIENT_LIQ_LEG1"
                        return False

                srv1 = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv1)
                partyA.liquid_balances[ccy1] -= amt1
                partyB.liquid_balances[ccy1] = partyB.liquid_balances.get(ccy1, 0.0) + amt1

                # gap
                gap = 0.2 * self._tau_srv_mean_eff()
                yield self.env.timeout(gap)

                # leg2 B pays ccy2 to A, may fail
                if partyB.liquid_balances.get(ccy2, 0.0) + pol_factor * line < amt2:
                    tx["atomicity_violation_flag"] = True  # one leg already executed
                    tx["stp_flag"] = False
                    # unwind leg1 (repair)
                    yield self._manual_repair(tx)
                    # unwind: reverse leg1
                    partyA.liquid_balances[ccy1] += amt1
                    partyB.liquid_balances[ccy1] -= amt1
                    tx["reason"] = "LEG2_FAIL_UNWIND"
                    return False

                srv2 = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv2)
                partyB.liquid_balances[ccy2] -= amt2
                partyA.liquid_balances[ccy2] = partyA.liquid_balances.get(ccy2, 0.0) + amt2
                return True

        return self.env.process(_proc())

    def _pipe_dvp(self, tx: Dict[str, Any]) -> simpy.events.Event:
        """
        DvP:
          - B: atomic escrow commit
          - A: sequential cash then asset (or viceversa) with potential mismatch/unwind
        """
        def _proc():
            buyer = self.agents[str(tx["buyer"])]
            seller = self.agents[str(tx["seller"])]
            cash_ccy = str(tx["cash_ccy"])
            cash_amt = float(tx["cash_amt"])
            asset_id = str(tx["asset_id"])
            qty = float(tx["qty"])

            line = self._line_intra_eff()
            pol = self._effective_liq_policy()
            pol_factor = 0.7 if pol == "conservative" else 1.0
            locks_mult = self._locks_mult()

            # Ensure holdings dict
            if buyer.holdings is None:
                buyer.holdings = {}
            if seller.holdings is None:
                seller.holdings = {}
            seller_qty = float(seller.holdings.get(asset_id, 0.0))

            yield self._wait_component(self.comp_settlement)

            if self.world == "B":
                # lock cash + asset
                t_lock = self.env.now
                tx["liq_lock_amt"] += locks_mult * cash_amt
                tx["coll_lock_amt"] += locks_mult * qty  # treat asset lock as collateral lock for KPI K3
                if buyer.liquid_balances.get(cash_ccy, 0.0) + pol_factor * line < cash_amt or seller_qty < qty:
                    tx["holds_count"] += 1
                    tx["stp_flag"] = False
                    self._bucket_stats["held"]["DVP"] += 1
                    hold_tau = 0.4 * self._tau_rep_mean_eff()
                    yield self.env.timeout(hold_tau)
                    tx["hold_time"] += hold_tau
                    seller_qty = float(seller.holdings.get(asset_id, 0.0))
                    if buyer.liquid_balances.get(cash_ccy, 0.0) + pol_factor * line < cash_amt or seller_qty < qty:
                        tx["reason"] = "INSUFFICIENT_CASH_OR_ASSET"
                        tx["liq_lock_dur"] += (self.env.now - t_lock)
                        tx["coll_lock_dur"] += (self.env.now - t_lock)
                        return False

                srv = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv)

                if float(self.rng_world.random()) < self._commit_fail_p():
                    tx["retry_count"] += 1
                    tx["stp_flag"] = False
                    tx["reason"] = "COMMIT_FAIL"
                    tx["liq_lock_dur"] += (self.env.now - t_lock)
                    tx["coll_lock_dur"] += (self.env.now - t_lock)
                    return False

                # commit: transfer cash and asset
                buyer.liquid_balances[cash_ccy] -= cash_amt
                seller.liquid_balances[cash_ccy] = seller.liquid_balances.get(cash_ccy, 0.0) + cash_amt
                seller.holdings[asset_id] = seller_qty - qty
                buyer.holdings[asset_id] = float(buyer.holdings.get(asset_id, 0.0)) + qty

                tx["liq_lock_dur"] += (self.env.now - t_lock)
                tx["coll_lock_dur"] += (self.env.now - t_lock)
                return True

            else:
                # World A: sequential (cash then asset)
                # cash leg
                if buyer.liquid_balances.get(cash_ccy, 0.0) + pol_factor * line < cash_amt:
                    tx["holds_count"] += 1
                    tx["stp_flag"] = False
                    self._bucket_stats["held"]["DVP"] += 1
                    hold_tau = 0.3 * self._tau_rep_mean_eff()
                    yield self.env.timeout(hold_tau)
                    tx["hold_time"] += hold_tau
                    if buyer.liquid_balances.get(cash_ccy, 0.0) + pol_factor * line < cash_amt:
                        tx["reason"] = "INSUFFICIENT_CASH"
                        return False

                srv1 = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv1)
                buyer.liquid_balances[cash_ccy] -= cash_amt
                seller.liquid_balances[cash_ccy] = seller.liquid_balances.get(cash_ccy, 0.0) + cash_amt

                # gap
                gap = 0.2 * self._tau_srv_mean_eff()
                yield self.env.timeout(gap)

                # asset leg may fail
                seller_qty = float(seller.holdings.get(asset_id, 0.0))
                if seller_qty < qty:
                    tx["atomicity_violation_flag"] = True
                    tx["stp_flag"] = False
                    # unwind cash leg
                    yield self._manual_repair(tx)
                    buyer.liquid_balances[cash_ccy] += cash_amt
                    seller.liquid_balances[cash_ccy] -= cash_amt
                    tx["reason"] = "ASSET_LEG_FAIL_UNWIND"
                    return False

                srv2 = draw_time({"dist": "exp", "mean": self._tau_srv_mean_eff()}, self.rng_srv)
                yield self.env.timeout(srv2)
                seller.holdings[asset_id] = seller_qty - qty
                buyer.holdings[asset_id] = float(buyer.holdings.get(asset_id, 0.0)) + qty
                return True

        return self.env.process(_proc())

    # ---------------------------
    # Run / QA helpers
    # ---------------------------

    def run(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        # Start event sampler
        self.env.process(self._sample_events())
        # Schedule tx intents
        self._schedule_intents()
        # Run env
        T_total = float(self.cfg["run"]["T_total"])
        self.env.run(until=T_total)
        return self.tx_rows, self.event_rows

    def agent_snapshot_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for aid, a in self.agents.items():
            row = {
                "agent_id": aid,
                "jurisdiction": a.jurisdiction,
                "type": a.type,
                "collateral_available": a.collateral_available,
            }
            for c, v in a.liquid_balances.items():
                row[f"bal_{c}"] = float(v)
            if a.holdings:
                for k, v in a.holdings.items():
                    row[f"hold_{k}"] = float(v)
            rows.append(row)
        return rows


def qa_gates(tx_rows: List[Dict[str, Any]], cfg: Dict[str, Any], world: str) -> None:
    """
    QA gates (J):
    - idempotence: unique tx_id
    - finality: settled => unique t_final ; failed => has t_fail
    - atomicity B: no one-leg-only (represented as atomicity_violation_flag must be False)
    - bounds: trace_score in [0,1], rates <=1 computed later; here basic checks
    """
    seen = set()
    for tx in tx_rows:
        txid = tx["tx_id"]
        assert txid not in seen, f"Idempotence violated: duplicate tx_id {txid}"
        seen.add(txid)

        st = tx["status"]
        if st == "SETTLED":
            assert "t_final" in tx, f"Finality violated: SETTLED missing t_final tx_id={txid}"
        if st == "FAILED":
            assert "t_fail" in tx or "reason" in tx, f"FAILED missing t_fail/reason tx_id={txid}"

        ts = float(tx.get("trace_score", 0.0))
        assert 0.0 <= ts <= 1.0, f"trace_score out of bounds tx_id={txid}"

        if world == "B" and tx["flow"] in ("PVP", "DVP"):
            assert not bool(tx.get("atomicity_violation_flag", False)), f"Atomicity violated in B tx_id={txid}"
