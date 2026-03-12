from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from libs.contracts.messages import (
    CandidateDecisionRecord,
    ExecutionReadiness,
    OrderSubmitCommand,
    RiskGateDecision,
    TradeCandidate,
    TradeIntent,
)
from libs.config.settings import get_settings
from libs.db.base import SessionLocal
from libs.db.repositories import TradingRepository
from libs.domain.enums import OrderSide
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
        self.settings = get_settings()
        self.repository = TradingRepository(SessionLocal)

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(value, upper))

    def _target_notional_bounds(self, base_notional_krw: int) -> tuple[int, int]:
        max_notional = base_notional_krw
        if self.settings.trading_max_target_notional_krw > 0:
            max_notional = min(max_notional, self.settings.trading_max_target_notional_krw)
        min_notional = 0
        if self.settings.trading_min_target_notional_krw > 0:
            min_notional = min(base_notional_krw, self.settings.trading_min_target_notional_krw)
        return min_notional, max_notional

    def _quality_scaled_target(self, candidate: TradeCandidate, base_notional_krw: int) -> tuple[int, str]:
        edge_reference = max(self.settings.trading_reference_edge_bps, 1.0)
        penalty_reference = max(self.settings.trading_reference_penalty_bps, 1.0)
        total_penalties = max(candidate.expected_slippage_bps or 0.0, 0.0) + max(
            candidate.tail_risk_penalty_bps or 0.0,
            0.0,
        ) + max(candidate.crowding_penalty_bps or 0.0, 0.0)
        edge_score = self._clamp(candidate.expected_edge_bps / edge_reference, 0.25, 1.0)
        confidence_score = self._clamp(
            candidate.selection_confidence
            if candidate.selection_confidence is not None
            else self.settings.selector_confidence_floor,
            0.25,
            1.0,
        )
        risk_score = self._clamp(1.0 - (total_penalties / penalty_reference), 0.25, 1.0)
        quality_score = self._clamp(
            (edge_score * 0.4) + (confidence_score * 0.35) + (risk_score * 0.25),
            0.2,
            1.0,
        )
        selected_rank = max(candidate.selected_rank or 1, 1)
        rank_discount = self._clamp(1.0 - ((selected_rank - 1) * 0.04), 0.72, 1.0)
        min_notional, max_notional = self._target_notional_bounds(base_notional_krw)
        scaled_notional_krw = int(round(base_notional_krw * quality_score * rank_discount))
        target_notional_krw = max(min_notional, min(max_notional, scaled_notional_krw))
        sizing_reason = (
            f"base={base_notional_krw:,}KRW, edge_score={edge_score:.2f}, "
            f"confidence={confidence_score:.2f}, risk_score={risk_score:.2f}, "
            f"rank_discount={rank_discount:.2f} -> target={target_notional_krw:,}KRW"
        )
        return target_notional_krw, sizing_reason

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
        execution_readiness: ExecutionReadiness | None = None,
        market_data_ok: bool = True,
        account_entry_enabled: bool = True,
        has_kill_switch: bool = False,
        has_reconciliation_break: bool = False,
        has_risk_flag: bool = False,
    ) -> RiskGateDecision:
        side = candidate.side
        if execution_readiness is not None:
            market_data_ok = execution_readiness.market_data_ok and execution_readiness.data_freshness_ok
            account_permission_enabled = (
                execution_readiness.account_exit_enabled
                if side == OrderSide.SELL
                else execution_readiness.account_entry_enabled
            )
            session_permission_allowed = (
                execution_readiness.session_exit_allowed
                if side == OrderSide.SELL
                else execution_readiness.session_entry_allowed
            )
            account_entry_enabled = (
                account_permission_enabled
                and session_permission_allowed
                and not execution_readiness.symbol_blocked
                and execution_readiness.confidence_ok
                and execution_readiness.vendor_healthy
            )
            has_kill_switch = execution_readiness.kill_switch_active
            has_reconciliation_break = execution_readiness.reconciliation_break_active
            has_risk_flag = execution_readiness.risk_flag_active or execution_readiness.symbol_blocked
        result = evaluate_hard_block(
            market_data_ok=market_data_ok,
            account_entry_enabled=account_entry_enabled,
            has_kill_switch=has_kill_switch,
            has_reconciliation_break=has_reconciliation_break,
            has_risk_flag=has_risk_flag,
        )
        if side == OrderSide.SELL:
            result.failed_gate_codes = [
                "ACCOUNT_EXIT_DISABLED" if code == "ACCOUNT_ENTRY_DISABLED" else code
                for code in result.failed_gate_codes
            ]
            result.reason_codes = [
                "ACCOUNT_EXIT_DISABLED" if code == "ACCOUNT_ENTRY_DISABLED" else code
                for code in result.reason_codes
            ]
        allowed_notional_hint: int | None
        if result.hard_block:
            allowed_notional_hint = None
        elif candidate.side == OrderSide.SELL:
            allowed_notional_hint = candidate.target_notional_krw
        elif execution_readiness is not None and execution_readiness.max_allowed_notional_krw is not None:
            allowed_notional_hint = min(candidate.target_notional_krw, execution_readiness.max_allowed_notional_krw)
        else:
            allowed_notional_hint = candidate.target_notional_krw
        return RiskGateDecision(
            candidate_id=candidate.candidate_id,
            account_id=candidate.account_scope,
            passed_gate_set_version="risk-gate-v2",
            penalty_bps_total=result.penalty_bps_total,
            final_allowed_notional_hint=allowed_notional_hint,
            hard_block=result.hard_block,
            failed_gate_codes=result.failed_gate_codes,
            reason_codes=list(
                dict.fromkeys(
                    result.reason_codes
                    + (execution_readiness.reason_codes if execution_readiness is not None else [])
                )
            ),
        )

    def build_trade_intent(self, candidate: TradeCandidate, decision: RiskGateDecision) -> TradeIntent | None:
        if decision.hard_block:
            return None
        base_notional_krw = candidate.target_notional_krw
        if decision.final_allowed_notional_hint is not None:
            base_notional_krw = min(base_notional_krw, decision.final_allowed_notional_hint)
        if candidate.side == OrderSide.SELL and (candidate.target_qty_override or 0) > 0:
            target_notional_krw = max(base_notional_krw, 0)
            target_qty = max(int(candidate.target_qty_override or 0), 0)
            sizing_reason = (
                candidate.selection_reason
                or f"exit_target_qty={target_qty}, base_notional={target_notional_krw:,}KRW"
            )
        else:
            target_notional_krw, sizing_reason = self._quality_scaled_target(candidate, base_notional_krw)
            target_qty = max(target_notional_krw // max(self.settings.trading_proxy_price_krw, 1), 0)
        return TradeIntent(
            intent_id=f"intent-{uuid4().hex[:12]}",
            candidate_id=candidate.candidate_id,
            account_id=candidate.account_scope,
            instrument_id=candidate.instrument_id,
            side=candidate.side,
            target_qty=target_qty,
            target_notional_krw=target_notional_krw,
            base_notional_krw=base_notional_krw,
            max_slippage_bps=35.0,
            urgency="NORMAL",
            route_policy="VENUE_HINT_THEN_FALLBACK",
            tif="DAY",
            expire_ts_utc=datetime.now(UTC) + timedelta(minutes=10),
            sizing_reason=sizing_reason,
        )

    def build_order_submit_command(
        self,
        *,
        intent: TradeIntent,
        strategy_id: str,
        price_krw: int,
        venue_hint: str | None = None,
        order_type: str = "LIMIT",
        max_order_value_krw: int | None = None,
        enforce_hard_value_cap: bool = False,
    ) -> OrderSubmitCommand:
        normalized_price_krw = max(price_krw, 1)
        if intent.side == OrderSide.SELL and intent.target_qty > 0:
            qty = intent.target_qty
        else:
            effective_notional_krw = intent.target_notional_krw
            if max_order_value_krw is not None and max_order_value_krw > 0:
                effective_notional_krw = min(effective_notional_krw, max_order_value_krw)
            qty = effective_notional_krw // normalized_price_krw
            if qty <= 0:
                if enforce_hard_value_cap and max_order_value_krw is not None and max_order_value_krw > 0:
                    raise ValueError(
                        f"single share price {normalized_price_krw:,}KRW exceeds micro test cap "
                        f"{max_order_value_krw:,}KRW"
                    )
                overshoot_tolerance = max(self.settings.trading_single_share_overshoot_tolerance_pct, 0.0) / 100.0
                affordable_limit = effective_notional_krw * (1.0 + overshoot_tolerance)
                if normalized_price_krw > affordable_limit:
                    raise ValueError(
                        f"single share price {normalized_price_krw:,}KRW exceeds sizing limit "
                        f"{effective_notional_krw:,}KRW"
                    )
                qty = 1
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
        candidate_decision = CandidateDecisionRecord(
            decision_id=f"decision-{candidate.candidate_id}",
            candidate_id=candidate.candidate_id,
            source_receipt_no=candidate.source_receipt_no,
            source_report_name=candidate.source_report_name,
            source_symbol=candidate.instrument_id,
            matched_positive_rule_id=candidate.matched_rule_id,
            candidate_status="REJECTED_RISK_GATE" if decision.hard_block else (candidate.candidate_status or "SELECTED"),
            selection_reason=candidate.selection_reason,
            rejection_reason=candidate.rejection_reason
            or ("리스크 게이트에서 신규 진입이 차단되었습니다." if decision.hard_block else None),
            ranking_score=candidate.ranking_score,
            ranking_reason=candidate.ranking_reason,
            decision_payload_json={
                "candidate": candidate.model_dump(mode="json"),
                "risk_gate": decision.model_dump(mode="json"),
            },
        )
        result["persistence"]["candidate_decision_pk"] = self.repository.store_candidate_decision(
            candidate_decision
        ).primary_key
        result["persistence"]["decision_pk"] = self.repository.store_risk_decision(
            decision.model_dump(mode="json")
        ).primary_key
        if intent is not None:
            result["persistence"]["intent_pk"] = self.repository.store_trade_intent(
                intent.model_dump(mode="json")
            ).primary_key
        return result


trading_core_service = TradingCoreService()
