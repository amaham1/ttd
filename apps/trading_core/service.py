from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from libs.contracts.messages import OrderSubmitCommand, RiskGateDecision, TradeCandidate, TradeIntent
from libs.db.base import SessionLocal
from libs.db.repositories import TradingRepository
from libs.risk.gate import evaluate_hard_block


@dataclass(slots=True)
class TradingCoreSnapshot:
    order_count: int
    open_breaks: int
    projected_positions: int
    projected_cash_krw: int
    operation_mode: str


class TradingCoreService:
    def __init__(self) -> None:
        self.repository = TradingRepository(SessionLocal)

    def snapshot(self) -> TradingCoreSnapshot:
        return TradingCoreSnapshot(
            order_count=12,
            open_breaks=1,
            projected_positions=3,
            projected_cash_krw=9_850_000,
            operation_mode="NORMAL",
        )

    def evaluate_candidate(
        self,
        candidate: TradeCandidate,
        *,
        market_data_ok: bool = True,
        account_entry_enabled: bool = True,
        has_kill_switch: bool = False,
        has_reconciliation_break: bool = False,
        has_risk_flag: bool = False,
    ) -> RiskGateDecision:
        result = evaluate_hard_block(
            market_data_ok=market_data_ok,
            account_entry_enabled=account_entry_enabled,
            has_kill_switch=has_kill_switch,
            has_reconciliation_break=has_reconciliation_break,
            has_risk_flag=has_risk_flag,
        )
        return RiskGateDecision(
            candidate_id=candidate.candidate_id,
            account_id=candidate.account_scope,
            passed_gate_set_version="risk-gate-v1",
            penalty_bps_total=result.penalty_bps_total,
            final_allowed_notional_hint=None if result.hard_block else candidate.target_notional_krw,
            hard_block=result.hard_block,
            failed_gate_codes=result.failed_gate_codes,
            reason_codes=result.reason_codes,
        )

    def build_trade_intent(self, candidate: TradeCandidate, decision: RiskGateDecision) -> TradeIntent | None:
        if decision.hard_block:
            return None
        target_qty = max(candidate.target_notional_krw // 80_000, 1)
        return TradeIntent(
            intent_id=f"intent-{uuid4().hex[:12]}",
            candidate_id=candidate.candidate_id,
            account_id=candidate.account_scope,
            instrument_id=candidate.instrument_id,
            side=candidate.side,
            target_qty=target_qty,
            target_notional_krw=candidate.target_notional_krw,
            max_slippage_bps=35.0,
            urgency="NORMAL",
            route_policy="VENUE_HINT_THEN_FALLBACK",
            tif="DAY",
            expire_ts_utc=datetime.now(UTC) + timedelta(minutes=10),
        )

    def build_order_submit_command(
        self,
        *,
        intent: TradeIntent,
        strategy_id: str,
        price_krw: int,
        venue_hint: str | None = None,
        order_type: str = "LIMIT",
    ) -> OrderSubmitCommand:
        qty = max(intent.target_notional_krw // max(price_krw, 1), 1)
        return OrderSubmitCommand(
            internal_order_id=f"order-{uuid4().hex[:12]}",
            client_order_id=f"client-{uuid4().hex[:12]}",
            account_id=intent.account_id,
            instrument_id=intent.instrument_id,
            side=intent.side,
            qty=qty,
            price=price_krw,
            order_type=order_type,
            tif=intent.tif,
            venue_hint=venue_hint,
            route_policy=intent.route_policy,
            urgency=intent.urgency,
            submitted_by_strategy=strategy_id,
            correlation_id=intent.intent_id,
        )

    def persist_pipeline(
        self,
        *,
        candidate: TradeCandidate,
        decision: RiskGateDecision,
        intent: TradeIntent | None,
    ) -> dict:
        result = {
            "candidate": candidate.model_dump(mode="json"),
            "decision": decision.model_dump(mode="json"),
            "intent": intent.model_dump(mode="json") if intent is not None else None,
            "persistence": {},
        }
        result["persistence"]["candidate_pk"] = self.repository.store_trade_candidate(candidate).primary_key
        result["persistence"]["decision_pk"] = self.repository.store_risk_decision(
            decision.model_dump(mode="json")
        ).primary_key
        if intent is not None:
            result["persistence"]["intent_pk"] = self.repository.store_trade_intent(
                intent.model_dump(mode="json")
            ).primary_key
        return result


trading_core_service = TradingCoreService()
