from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Callable

from sqlalchemy.orm import Session

from libs.contracts.messages import FillEvent, OrderAckEvent, TradeCandidate
from libs.db.models import (
    CoreOrderTicket,
    CoreReconciliationBreak,
    CoreRiskGateDecision,
    CoreTradeCandidate,
    CoreTradeIntent,
    EvtExecutionFill,
    EvtOrderEvent,
    EvtRawSourceEvent,
)


@dataclass(slots=True)
class PersistenceResult:
    primary_key: int


class TradingRepository:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def store_raw_event(
        self,
        *,
        source_system_code: str,
        channel_code: str,
        endpoint_code: str,
        payload_json: dict,
        source_object_id: str | None = None,
        account_pk: int | None = None,
        instrument_pk: int | None = None,
        venue_code: str | None = None,
    ) -> PersistenceResult:
        encoded = str(payload_json).encode("utf-8")
        row = EvtRawSourceEvent(
            source_system_code=source_system_code,
            channel_code=channel_code,
            endpoint_code=endpoint_code,
            source_object_id=source_object_id,
            account_pk=account_pk,
            instrument_pk=instrument_pk,
            venue_code=venue_code,
            payload_sha256=sha256(encoded).hexdigest(),
            payload_json=payload_json,
        )
        with self.session_factory() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.raw_pk)

    def store_order_ack(self, event: OrderAckEvent, payload_json: dict) -> PersistenceResult:
        with self.session_factory() as session:
            ticket = session.query(CoreOrderTicket).filter_by(internal_order_id=event.internal_order_id).one_or_none()
            if ticket is None:
                ticket = CoreOrderTicket(
                    internal_order_id=event.internal_order_id,
                    client_order_id=event.client_order_id,
                    broker_order_no=event.broker_order_no,
                    account_uid=payload_json.get("account_id", "default"),
                    instrument_pk=payload_json.get("instrument_pk"),
                    side_code=payload_json.get("side_code", "BUY"),
                    order_state_code="ACKED",
                    order_type_code=payload_json.get("order_type_code", "LIMIT"),
                    tif_code=payload_json.get("tif_code", "DAY"),
                    last_event_at_utc=event.ack_ts_utc.replace(tzinfo=None),
                    payload_json=payload_json,
                )
                session.add(ticket)
            else:
                ticket.broker_order_no = event.broker_order_no
                ticket.order_state_code = "ACKED"
                ticket.last_event_at_utc = event.ack_ts_utc.replace(tzinfo=None)
                ticket.payload_json = payload_json

            order_event = EvtOrderEvent(
                internal_order_id=event.internal_order_id,
                client_order_id=event.client_order_id,
                broker_order_no=event.broker_order_no,
                event_name="OrderAcked",
                event_state_code="ACKED",
                event_ts_utc=event.ack_ts_utc.replace(tzinfo=None),
                source_system_code="BROKER_GATEWAY",
                raw_ref=event.raw_ref,
                payload_json=payload_json,
            )
            session.add(order_event)
            session.commit()
            session.refresh(order_event)
            return PersistenceResult(primary_key=order_event.order_event_pk)

    def store_fill(self, event: FillEvent, payload_json: dict) -> PersistenceResult:
        with self.session_factory() as session:
            fill = EvtExecutionFill(
                internal_order_id=event.internal_order_id,
                broker_order_no=event.broker_order_no,
                broker_trade_id=event.broker_trade_id,
                account_uid=event.account_id,
                instrument_pk=payload_json.get("instrument_pk"),
                side_code=event.side.value.upper(),
                venue_code=event.venue,
                fill_ts_utc=event.fill_ts_utc.replace(tzinfo=None),
                fill_price=event.price,
                fill_qty=event.qty,
                fee_krw=event.fee,
                tax_krw=event.tax,
                raw_ref=event.raw_ref,
                payload_json=payload_json,
            )
            session.add(fill)

            ticket = session.query(CoreOrderTicket).filter_by(internal_order_id=event.internal_order_id).one_or_none()
            if ticket is not None:
                ticket.broker_order_no = event.broker_order_no
                ticket.filled_qty = (ticket.filled_qty or 0) + event.qty
                remaining = max((ticket.working_qty or 0) - event.qty, 0)
                ticket.working_qty = remaining
                ticket.avg_fill_price = event.price
                ticket.order_state_code = "FILLED" if remaining == 0 else "PARTIALLY_FILLED"
                ticket.last_event_at_utc = event.fill_ts_utc.replace(tzinfo=None)

            session.commit()
            session.refresh(fill)
            return PersistenceResult(primary_key=fill.execution_fill_pk)

    def store_trade_candidate(self, candidate: TradeCandidate) -> PersistenceResult:
        with self.session_factory() as session:
            row = session.query(CoreTradeCandidate).filter_by(candidate_id=candidate.candidate_id).one_or_none()
            if row is None:
                row = CoreTradeCandidate(
                    candidate_id=candidate.candidate_id,
                    strategy_id=candidate.strategy_id,
                    account_scope=candidate.account_scope,
                    instrument_pk=None,
                    side_code=candidate.side.value.upper(),
                    expected_edge_bps=candidate.expected_edge_bps,
                    target_notional_krw=candidate.target_notional_krw,
                    entry_style_code=candidate.entry_style,
                    expire_ts_utc=candidate.expire_ts_utc.replace(tzinfo=None),
                    meta_model_version=candidate.meta_model_version,
                    payload_json=candidate.model_dump(mode="json"),
                )
                session.add(row)
            else:
                row.strategy_id = candidate.strategy_id
                row.account_scope = candidate.account_scope
                row.side_code = candidate.side.value.upper()
                row.expected_edge_bps = candidate.expected_edge_bps
                row.target_notional_krw = candidate.target_notional_krw
                row.entry_style_code = candidate.entry_style
                row.expire_ts_utc = candidate.expire_ts_utc.replace(tzinfo=None)
                row.meta_model_version = candidate.meta_model_version
                row.payload_json = candidate.model_dump(mode="json")
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.trade_candidate_pk)

    def store_risk_decision(self, decision_payload: dict) -> PersistenceResult:
        row = CoreRiskGateDecision(
            candidate_id=decision_payload["candidate_id"],
            account_uid=decision_payload["account_id"],
            gate_set_version=decision_payload["passed_gate_set_version"],
            hard_block=decision_payload["hard_block"],
            penalty_bps_total=decision_payload["penalty_bps_total"],
            final_allowed_notional_hint=decision_payload["final_allowed_notional_hint"],
            failed_gate_codes=decision_payload["failed_gate_codes"],
            reason_codes=decision_payload["reason_codes"],
            decided_at_utc=datetime.now(UTC).replace(tzinfo=None),
        )
        with self.session_factory() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.risk_gate_decision_pk)

    def store_trade_intent(self, intent_payload: dict) -> PersistenceResult:
        with self.session_factory() as session:
            row = session.query(CoreTradeIntent).filter_by(intent_id=intent_payload["intent_id"]).one_or_none()
            expire_ts = (
                datetime.fromisoformat(intent_payload["expire_ts_utc"]).replace(tzinfo=None)
                if isinstance(intent_payload["expire_ts_utc"], str)
                else intent_payload["expire_ts_utc"].replace(tzinfo=None)
            )
            side_code = intent_payload["side"].upper() if isinstance(intent_payload["side"], str) else str(intent_payload["side"])
            if row is None:
                row = CoreTradeIntent(
                    intent_id=intent_payload["intent_id"],
                    candidate_id=intent_payload["candidate_id"],
                    account_uid=intent_payload["account_id"],
                    instrument_pk=None,
                    side_code=side_code,
                    target_qty=intent_payload["target_qty"],
                    target_notional_krw=intent_payload["target_notional_krw"],
                    max_slippage_bps=intent_payload["max_slippage_bps"],
                    urgency_code=intent_payload["urgency"],
                    route_policy_code=intent_payload["route_policy"],
                    tif_code=intent_payload["tif"],
                    expire_ts_utc=expire_ts,
                )
                session.add(row)
            else:
                row.candidate_id = intent_payload["candidate_id"]
                row.account_uid = intent_payload["account_id"]
                row.side_code = side_code
                row.target_qty = intent_payload["target_qty"]
                row.target_notional_krw = intent_payload["target_notional_krw"]
                row.max_slippage_bps = intent_payload["max_slippage_bps"]
                row.urgency_code = intent_payload["urgency"]
                row.route_policy_code = intent_payload["route_policy"]
                row.tif_code = intent_payload["tif"]
                row.expire_ts_utc = expire_ts
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.trade_intent_pk)

    def create_reconciliation_break(
        self,
        *,
        break_id: str,
        scope_type: str,
        scope_id: str,
        severity_code: str,
        expected_payload: dict,
        actual_payload: dict,
        notes: str | None = None,
    ) -> PersistenceResult:
        row = CoreReconciliationBreak(
            break_id=break_id,
            scope_type=scope_type,
            scope_id=scope_id,
            severity_code=severity_code,
            status_code="OPEN",
            expected_payload=expected_payload,
            actual_payload=actual_payload,
            notes=notes,
        )
        with self.session_factory() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.reconciliation_break_pk)
