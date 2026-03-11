from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
import re
from typing import Callable

from sqlalchemy.orm import Session

from libs.contracts.messages import CandidateDecisionRecord, FillEvent, OrderAckEvent, TradeCandidate
from libs.db.models import (
    EvtCandidateDecision,
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
            working_qty = int(payload_json.get("qty") or payload_json.get("ord_qty") or 0)
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
                    working_qty=working_qty,
                    last_event_at_utc=event.ack_ts_utc.replace(tzinfo=None),
                    payload_json=payload_json,
                )
                session.add(ticket)
            else:
                ticket.broker_order_no = event.broker_order_no
                ticket.order_state_code = "ACKED"
                if working_qty > 0:
                    ticket.working_qty = max(working_qty - (ticket.filled_qty or 0), 0)
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
            if ticket is None:
                ticket = CoreOrderTicket(
                    internal_order_id=event.internal_order_id,
                    client_order_id=payload_json.get("client_order_id", event.internal_order_id),
                    broker_order_no=event.broker_order_no,
                    account_uid=event.account_id,
                    instrument_pk=payload_json.get("instrument_pk"),
                    side_code=event.side.value.upper(),
                    order_state_code="FILLED",
                    order_type_code=payload_json.get("order_type_code", "LIMIT"),
                    tif_code=payload_json.get("tif_code", "DAY"),
                    working_qty=0,
                    filled_qty=event.qty,
                    avg_fill_price=event.price,
                    last_event_at_utc=event.fill_ts_utc.replace(tzinfo=None),
                    payload_json=payload_json,
                )
                session.add(ticket)
            else:
                previous_filled_qty = ticket.filled_qty or 0
                previous_avg_fill_price = float(ticket.avg_fill_price) if ticket.avg_fill_price is not None else None
                ticket.broker_order_no = event.broker_order_no
                ticket.filled_qty = previous_filled_qty + event.qty
                remaining = max((ticket.working_qty or 0) - event.qty, 0)
                ticket.working_qty = remaining
                if previous_avg_fill_price is None or previous_filled_qty <= 0:
                    ticket.avg_fill_price = event.price
                else:
                    total_value = (previous_avg_fill_price * previous_filled_qty) + (event.price * event.qty)
                    ticket.avg_fill_price = total_value / max(ticket.filled_qty, 1)
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

    def store_candidate_decision(self, decision: CandidateDecisionRecord) -> PersistenceResult:
        with self.session_factory() as session:
            lookup = None
            if decision.candidate_id:
                lookup = session.query(EvtCandidateDecision).filter_by(candidate_id=decision.candidate_id).one_or_none()
            if lookup is None:
                lookup = session.query(EvtCandidateDecision).filter_by(decision_id=decision.decision_id).one_or_none()

            if lookup is None:
                lookup = EvtCandidateDecision(
                    decision_id=decision.decision_id,
                    candidate_id=decision.candidate_id,
                    source_receipt_no=decision.source_receipt_no,
                    source_report_name=decision.source_report_name,
                    source_symbol=decision.source_symbol,
                    matched_positive_rule_id=decision.matched_positive_rule_id,
                    matched_block_rule_id=decision.matched_block_rule_id,
                    candidate_status=decision.candidate_status,
                    selection_reason=decision.selection_reason,
                    rejection_reason=decision.rejection_reason,
                    ranking_score=decision.ranking_score,
                    ranking_reason=decision.ranking_reason,
                    decision_payload_json=decision.decision_payload_json,
                    created_at_utc=decision.created_at_utc.replace(tzinfo=None),
                )
                session.add(lookup)
            else:
                lookup.candidate_id = decision.candidate_id
                lookup.source_receipt_no = decision.source_receipt_no
                lookup.source_report_name = decision.source_report_name
                lookup.source_symbol = decision.source_symbol
                lookup.matched_positive_rule_id = decision.matched_positive_rule_id
                lookup.matched_block_rule_id = decision.matched_block_rule_id
                lookup.candidate_status = decision.candidate_status
                lookup.selection_reason = decision.selection_reason
                lookup.rejection_reason = decision.rejection_reason
                lookup.ranking_score = decision.ranking_score
                lookup.ranking_reason = decision.ranking_reason
                lookup.decision_payload_json = decision.decision_payload_json
                lookup.created_at_utc = decision.created_at_utc.replace(tzinfo=None)
            session.commit()
            session.refresh(lookup)
            return PersistenceResult(primary_key=lookup.candidate_decision_pk)

    def list_candidate_decisions(self, *, limit: int = 100) -> list[dict]:
        with self.session_factory() as session:
            rows = (
                session.query(EvtCandidateDecision)
                .order_by(EvtCandidateDecision.created_at_utc.desc())
                .limit(limit)
                .all()
            )
            return [self._candidate_decision_row_to_dict(row) for row in rows]

    def get_candidate_decision_by_candidate_id(self, candidate_id: str) -> dict | None:
        with self.session_factory() as session:
            row = session.query(EvtCandidateDecision).filter_by(candidate_id=candidate_id).one_or_none()
            if row is None:
                return None
            return self._candidate_decision_row_to_dict(row)

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

    def upsert_reconciliation_break(
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
        with self.session_factory() as session:
            row = session.query(CoreReconciliationBreak).filter_by(break_id=break_id).one_or_none()
            if row is None:
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
                session.add(row)
            else:
                row.scope_type = scope_type
                row.scope_id = scope_id
                row.severity_code = severity_code
                row.status_code = "OPEN"
                row.resolved_at_utc = None
                row.expected_payload = expected_payload
                row.actual_payload = actual_payload
                row.notes = notes
            session.commit()
            session.refresh(row)
            return PersistenceResult(primary_key=row.reconciliation_break_pk)

    def resolve_reconciliation_breaks(self, *, scope_type: str, scope_id: str) -> int:
        with self.session_factory() as session:
            rows = (
                session.query(CoreReconciliationBreak)
                .filter_by(scope_type=scope_type, scope_id=scope_id, status_code="OPEN")
                .all()
            )
            resolved_at_utc = datetime.now(UTC).replace(tzinfo=None)
            for row in rows:
                row.status_code = "RESOLVED"
                row.resolved_at_utc = resolved_at_utc
            session.commit()
            return len(rows)

    def list_order_tickets(self, *, limit: int = 100) -> list[dict]:
        with self.session_factory() as session:
            rows = (
                session.query(CoreOrderTicket)
                .order_by(CoreOrderTicket.last_event_at_utc.desc())
                .limit(limit)
                .all()
            )
            return [self._order_ticket_row_to_dict(row) for row in rows]

    def get_order_ticket_by_internal_order_id(self, internal_order_id: str) -> dict | None:
        with self.session_factory() as session:
            row = session.query(CoreOrderTicket).filter_by(internal_order_id=internal_order_id).one_or_none()
            if row is None:
                return None
            return self._order_ticket_row_to_dict(row)

    def list_execution_fills(
        self,
        *,
        limit: int = 100,
        internal_order_id: str | None = None,
        broker_order_no: str | None = None,
    ) -> list[dict]:
        with self.session_factory() as session:
            query = session.query(EvtExecutionFill)
            if internal_order_id:
                query = query.filter_by(internal_order_id=internal_order_id)
            if broker_order_no:
                query = query.filter_by(broker_order_no=broker_order_no)
            rows = (
                query.order_by(EvtExecutionFill.fill_ts_utc.desc())
                .limit(limit)
                .all()
            )
            return [self._execution_fill_row_to_dict(row) for row in rows]

    def list_reconciliation_breaks(
        self,
        *,
        limit: int = 100,
        status_code: str | None = None,
    ) -> list[dict]:
        with self.session_factory() as session:
            query = session.query(CoreReconciliationBreak)
            if status_code:
                query = query.filter_by(status_code=status_code)
            rows = (
                query.order_by(CoreReconciliationBreak.detected_at_utc.desc())
                .limit(limit)
                .all()
            )
            return [self._reconciliation_break_row_to_dict(row) for row in rows]

    def purge_nonlive_order_artifacts(self) -> dict[str, int | list[str]]:
        live_order_pattern = re.compile(r"^order-[0-9a-f]{12}$")
        purge_order_ids: set[str] = set()

        with self.session_factory() as session:
            ticket_rows = session.query(CoreOrderTicket).all()
            for row in ticket_rows:
                internal_order_id = str(row.internal_order_id or "")
                if internal_order_id.startswith("order-") and not live_order_pattern.fullmatch(internal_order_id):
                    purge_order_ids.add(internal_order_id)

            break_rows = session.query(CoreReconciliationBreak).filter_by(scope_type="ORDER").all()
            for row in break_rows:
                scope_id = str(row.scope_id or "")
                if scope_id.startswith("order-") and not live_order_pattern.fullmatch(scope_id):
                    purge_order_ids.add(scope_id)

            return self._purge_order_artifacts(session=session, purge_order_ids=purge_order_ids)

    def purge_order_artifacts_by_internal_order_ids(self, internal_order_ids: list[str]) -> dict[str, int | list[str]]:
        purge_order_ids = {str(item).strip() for item in internal_order_ids if str(item).strip()}
        with self.session_factory() as session:
            return self._purge_order_artifacts(session=session, purge_order_ids=purge_order_ids)

    @staticmethod
    def _purge_order_artifacts(*, session: Session, purge_order_ids: set[str]) -> dict[str, int | list[str]]:
        if not purge_order_ids:
            return {
                "order_ticket_count": 0,
                "order_event_count": 0,
                "execution_fill_count": 0,
                "reconciliation_break_count": 0,
                "purged_order_ids": [],
            }

        order_ticket_count = (
            session.query(CoreOrderTicket)
            .filter(CoreOrderTicket.internal_order_id.in_(purge_order_ids))
            .delete(synchronize_session=False)
        )
        order_event_count = (
            session.query(EvtOrderEvent)
            .filter(EvtOrderEvent.internal_order_id.in_(purge_order_ids))
            .delete(synchronize_session=False)
        )
        execution_fill_count = (
            session.query(EvtExecutionFill)
            .filter(EvtExecutionFill.internal_order_id.in_(purge_order_ids))
            .delete(synchronize_session=False)
        )
        reconciliation_break_count = (
            session.query(CoreReconciliationBreak)
            .filter(
                CoreReconciliationBreak.scope_type == "ORDER",
                CoreReconciliationBreak.scope_id.in_(purge_order_ids),
            )
            .delete(synchronize_session=False)
        )
        session.commit()

        return {
            "order_ticket_count": int(order_ticket_count),
            "order_event_count": int(order_event_count),
            "execution_fill_count": int(execution_fill_count),
            "reconciliation_break_count": int(reconciliation_break_count),
            "purged_order_ids": sorted(purge_order_ids),
        }

    @staticmethod
    def _candidate_decision_row_to_dict(row: EvtCandidateDecision) -> dict:
        return {
            "decision_id": row.decision_id,
            "candidate_id": row.candidate_id,
            "source_receipt_no": row.source_receipt_no,
            "source_report_name": row.source_report_name,
            "source_symbol": row.source_symbol,
            "matched_positive_rule_id": row.matched_positive_rule_id,
            "matched_block_rule_id": row.matched_block_rule_id,
            "candidate_status": row.candidate_status,
            "selection_reason": row.selection_reason,
            "rejection_reason": row.rejection_reason,
            "ranking_score": float(row.ranking_score) if row.ranking_score is not None else None,
            "ranking_reason": row.ranking_reason,
            "decision_payload_json": row.decision_payload_json,
            "created_at_utc": row.created_at_utc.isoformat() if row.created_at_utc is not None else None,
        }

    @staticmethod
    def _order_ticket_row_to_dict(row: CoreOrderTicket) -> dict:
        payload_json = row.payload_json or {}
        return {
            "internal_order_id": row.internal_order_id,
            "client_order_id": row.client_order_id,
            "broker_order_no": row.broker_order_no,
            "account_uid": row.account_uid,
            "instrument_id": payload_json.get("instrument_id") or payload_json.get("pdno"),
            "side_code": row.side_code,
            "order_state_code": row.order_state_code,
            "order_type_code": row.order_type_code,
            "tif_code": row.tif_code,
            "working_qty": row.working_qty,
            "filled_qty": row.filled_qty,
            "avg_fill_price": float(row.avg_fill_price) if row.avg_fill_price is not None else None,
            "last_event_at_utc": row.last_event_at_utc.isoformat() if row.last_event_at_utc is not None else None,
            "payload_json": payload_json,
        }

    @staticmethod
    def _execution_fill_row_to_dict(row: EvtExecutionFill) -> dict:
        payload_json = row.payload_json or {}
        return {
            "internal_order_id": row.internal_order_id,
            "broker_order_no": row.broker_order_no,
            "broker_trade_id": row.broker_trade_id,
            "account_uid": row.account_uid,
            "instrument_id": payload_json.get("instrument_id") or payload_json.get("pdno"),
            "side_code": row.side_code,
            "venue_code": row.venue_code,
            "fill_ts_utc": row.fill_ts_utc.isoformat() if row.fill_ts_utc is not None else None,
            "fill_price": float(row.fill_price),
            "fill_qty": row.fill_qty,
            "fee_krw": float(row.fee_krw),
            "tax_krw": float(row.tax_krw),
            "raw_ref": row.raw_ref,
            "payload_json": payload_json,
        }

    @staticmethod
    def _reconciliation_break_row_to_dict(row: CoreReconciliationBreak) -> dict:
        return {
            "break_id": row.break_id,
            "scope_type": row.scope_type,
            "scope_id": row.scope_id,
            "severity_code": row.severity_code,
            "status_code": row.status_code,
            "detected_at_utc": row.detected_at_utc.isoformat() if row.detected_at_utc is not None else None,
            "resolved_at_utc": row.resolved_at_utc.isoformat() if row.resolved_at_utc is not None else None,
            "expected_payload": row.expected_payload or {},
            "actual_payload": row.actual_payload or {},
            "notes": row.notes,
        }
