from datetime import UTC, date, datetime
from uuid import uuid4

from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from libs.db.base import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class RefInstrument(Base):
    __tablename__ = "ref_instrument"

    instrument_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_uid: Mapped[str] = mapped_column(String(36), default=lambda: str(uuid4()), unique=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    kis_symbol: Mapped[str | None] = mapped_column(String(32))
    dart_corp_code: Mapped[str | None] = mapped_column(String(32))
    market_code: Mapped[str | None] = mapped_column(String(16))
    listing_status_code: Mapped[str | None] = mapped_column(String(32))
    nxt_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class RefInstrumentProfile(Base):
    __tablename__ = "ref_instrument_profile"

    instrument_profile_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    issuer_name: Mapped[str | None] = mapped_column(String(255))
    sector_name: Mapped[str | None] = mapped_column(String(128))
    oil_up_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    usdkrw_up_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    rates_up_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    china_growth_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    domestic_demand_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    export_beta: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    thematic_tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    rationale: Mapped[str | None] = mapped_column(Text)
    confidence_score: Mapped[float | None] = mapped_column(Numeric(8, 4))
    used_fallback: Mapped[bool] = mapped_column(Boolean, default=False)
    source_event_family: Mapped[str | None] = mapped_column(String(64), index=True)
    source_event_type: Mapped[str | None] = mapped_column(String(64))
    source_report_name: Mapped[str | None] = mapped_column(String(255))
    source_receipt_no: Mapped[str | None] = mapped_column(String(64), index=True)
    source_summary_text: Mapped[str | None] = mapped_column(Text)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    updated_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class RefBrokerStatusMap(Base):
    __tablename__ = "ref_broker_status_map"

    status_map_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    broker_code: Mapped[str] = mapped_column(String(32), index=True)
    channel_code: Mapped[str] = mapped_column(String(32))
    raw_code: Mapped[str] = mapped_column(String(64))
    raw_text: Mapped[str | None] = mapped_column(String(255))
    canonical_event_code: Mapped[str | None] = mapped_column(String(64))
    canonical_state_code: Mapped[str | None] = mapped_column(String(64))


class RefKisRestErrorMap(Base):
    __tablename__ = "ref_kis_rest_error_map"

    rest_error_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tr_id: Mapped[str | None] = mapped_column(String(32), index=True)
    rt_cd: Mapped[str | None] = mapped_column(String(16))
    msg_cd: Mapped[str] = mapped_column(String(64), index=True)
    msg_text: Mapped[str | None] = mapped_column(String(255))
    error_category: Mapped[str | None] = mapped_column(String(64))
    retryable: Mapped[bool] = mapped_column(Boolean, default=False)
    canonical_outcome: Mapped[str | None] = mapped_column(String(64))


class RefKisWsNoticeMap(Base):
    __tablename__ = "ref_kis_ws_notice_map"

    ws_notice_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tr_id: Mapped[str] = mapped_column(String(32), index=True)
    notice_field: Mapped[str] = mapped_column(String(64))
    raw_code: Mapped[str] = mapped_column(String(64))
    raw_text: Mapped[str | None] = mapped_column(String(255))
    canonical_event_code: Mapped[str | None] = mapped_column(String(64))
    canonical_state_code: Mapped[str | None] = mapped_column(String(64))


class RefKisMarketStatusMap(Base):
    __tablename__ = "ref_kis_market_status_map"

    market_status_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    venue_code: Mapped[str] = mapped_column(String(16), index=True)
    raw_code: Mapped[str] = mapped_column(String(64))
    session_code: Mapped[str] = mapped_column(String(64))
    entry_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    cancel_allowed: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[str | None] = mapped_column(Text)


class CoreAccount(Base):
    __tablename__ = "core_account"

    account_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_uid: Mapped[str] = mapped_column(String(36), default=lambda: str(uuid4()), unique=True)
    broker_code: Mapped[str] = mapped_column(String(32))
    account_status_code: Mapped[str] = mapped_column(String(32), default="ACTIVE")
    entry_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    exit_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    max_gross_exposure_krw: Mapped[float | None] = mapped_column(Numeric(20, 2))
    max_daily_loss_krw: Mapped[float | None] = mapped_column(Numeric(20, 2))


class CoreMarketSessionState(Base):
    __tablename__ = "core_market_session_state"

    market_session_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    venue_code: Mapped[str] = mapped_column(String(16), index=True)
    session_code: Mapped[str] = mapped_column(String(32))
    effective_from_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    effective_to_ts_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    entry_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    cancel_allowed: Mapped[bool] = mapped_column(Boolean, default=True)
    market_data_ok: Mapped[bool] = mapped_column(Boolean, default=True)
    degraded_flag: Mapped[bool] = mapped_column(Boolean, default=False)


class EvtRawSourceEvent(Base):
    __tablename__ = "evt_raw_source_event"

    raw_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    received_date: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow, index=True)
    source_system_code: Mapped[str] = mapped_column(String(64))
    channel_code: Mapped[str] = mapped_column(String(32))
    endpoint_code: Mapped[str] = mapped_column(String(64))
    source_object_id: Mapped[str | None] = mapped_column(String(128))
    account_pk: Mapped[int | None] = mapped_column(BigInteger)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger)
    venue_code: Mapped[str | None] = mapped_column(String(16))
    exchange_ts_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    received_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    parse_status_code: Mapped[str] = mapped_column(String(32), default="RAW_ONLY")
    payload_sha256: Mapped[str | None] = mapped_column(String(128))
    payload_json: Mapped[dict] = mapped_column(JSON)


class EvtDisclosureEvent(Base):
    __tablename__ = "evt_disclosure_event"

    disclosure_event_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    disclosure_id: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    source_system_code: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(64))
    direction_code: Mapped[str | None] = mapped_column(String(32))
    confidence_score: Mapped[float | None] = mapped_column(Numeric(8, 4))
    tradeability_code: Mapped[str | None] = mapped_column(String(32))
    hard_block_candidate: Mapped[bool] = mapped_column(Boolean, default=False)
    parser_version: Mapped[str | None] = mapped_column(String(64))
    occurred_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    payload_json: Mapped[dict] = mapped_column(JSON)


class EvtRiskFlag(Base):
    __tablename__ = "evt_risk_flag"

    risk_flag_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    account_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    source_system_code: Mapped[str] = mapped_column(String(32))
    flag_type: Mapped[str] = mapped_column(String(64))
    severity_code: Mapped[str] = mapped_column(String(32))
    hard_block: Mapped[bool] = mapped_column(Boolean, default=False)
    effective_from_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    effective_to_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    reason_code: Mapped[str | None] = mapped_column(String(64))
    reason_text: Mapped[str | None] = mapped_column(Text)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)


class CoreTradeCandidate(Base):
    __tablename__ = "core_trade_candidate"

    trade_candidate_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    candidate_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    strategy_id: Mapped[str] = mapped_column(String(64), index=True)
    account_scope: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    side_code: Mapped[str] = mapped_column(String(16))
    expected_edge_bps: Mapped[float] = mapped_column(Numeric(12, 4))
    target_notional_krw: Mapped[float] = mapped_column(Numeric(20, 2))
    entry_style_code: Mapped[str] = mapped_column(String(32))
    expire_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    meta_model_version: Mapped[str | None] = mapped_column(String(64))
    status_code: Mapped[str] = mapped_column(String(32), default="CREATED")
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)


class EvtCandidateDecision(Base):
    __tablename__ = "evt_candidate_decision"

    candidate_decision_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    decision_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    candidate_id: Mapped[str | None] = mapped_column(String(64), index=True)
    source_receipt_no: Mapped[str | None] = mapped_column(String(64), index=True)
    source_report_name: Mapped[str | None] = mapped_column(String(255))
    source_symbol: Mapped[str | None] = mapped_column(String(32), index=True)
    matched_positive_rule_id: Mapped[str | None] = mapped_column(String(128), index=True)
    matched_block_rule_id: Mapped[str | None] = mapped_column(String(128), index=True)
    candidate_status: Mapped[str] = mapped_column(String(64), index=True)
    selection_reason: Mapped[str | None] = mapped_column(Text)
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    ranking_score: Mapped[float | None] = mapped_column(Numeric(12, 4))
    ranking_reason: Mapped[str | None] = mapped_column(Text)
    decision_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class CoreRiskGateDecision(Base):
    __tablename__ = "core_risk_gate_decision"

    risk_gate_decision_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    candidate_id: Mapped[str] = mapped_column(String(64), index=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    gate_set_version: Mapped[str] = mapped_column(String(32))
    hard_block: Mapped[bool] = mapped_column(Boolean, default=False)
    penalty_bps_total: Mapped[float] = mapped_column(Numeric(12, 4), default=0)
    final_allowed_notional_hint: Mapped[float | None] = mapped_column(Numeric(20, 2))
    failed_gate_codes: Mapped[dict] = mapped_column(JSON, default=list)
    reason_codes: Mapped[dict] = mapped_column(JSON, default=list)
    decided_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class CoreTradeIntent(Base):
    __tablename__ = "core_trade_intent"

    trade_intent_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    intent_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    candidate_id: Mapped[str] = mapped_column(String(64), index=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    side_code: Mapped[str] = mapped_column(String(16))
    target_qty: Mapped[int] = mapped_column(Integer)
    target_notional_krw: Mapped[float] = mapped_column(Numeric(20, 2))
    max_slippage_bps: Mapped[float] = mapped_column(Numeric(12, 4))
    urgency_code: Mapped[str] = mapped_column(String(32))
    route_policy_code: Mapped[str] = mapped_column(String(64))
    tif_code: Mapped[str] = mapped_column(String(16))
    expire_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class CoreOrderTicket(Base):
    __tablename__ = "core_order_ticket"

    order_ticket_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    internal_order_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    client_order_id: Mapped[str] = mapped_column(String(64), index=True)
    broker_order_no: Mapped[str | None] = mapped_column(String(64), index=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    side_code: Mapped[str] = mapped_column(String(16))
    order_state_code: Mapped[str] = mapped_column(String(32), index=True)
    order_type_code: Mapped[str] = mapped_column(String(32))
    tif_code: Mapped[str] = mapped_column(String(16))
    working_qty: Mapped[int] = mapped_column(Integer, default=0)
    filled_qty: Mapped[int] = mapped_column(Integer, default=0)
    avg_fill_price: Mapped[float | None] = mapped_column(Numeric(20, 4))
    last_event_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)


class EvtOrderEvent(Base):
    __tablename__ = "evt_order_event"

    order_event_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    internal_order_id: Mapped[str] = mapped_column(String(64), index=True)
    client_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    broker_order_no: Mapped[str | None] = mapped_column(String(64), index=True)
    event_name: Mapped[str] = mapped_column(String(64), index=True)
    event_state_code: Mapped[str | None] = mapped_column(String(32))
    event_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    source_system_code: Mapped[str] = mapped_column(String(32))
    raw_ref: Mapped[str | None] = mapped_column(String(128))
    payload_json: Mapped[dict] = mapped_column(JSON)


class EvtExecutionFill(Base):
    __tablename__ = "evt_execution_fill"

    execution_fill_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    internal_order_id: Mapped[str] = mapped_column(String(64), index=True)
    broker_order_no: Mapped[str] = mapped_column(String(64), index=True)
    broker_trade_id: Mapped[str | None] = mapped_column(String(64), index=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int | None] = mapped_column(BigInteger, index=True)
    side_code: Mapped[str] = mapped_column(String(16))
    venue_code: Mapped[str | None] = mapped_column(String(16))
    fill_ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), index=True)
    fill_price: Mapped[float] = mapped_column(Numeric(20, 4))
    fill_qty: Mapped[int] = mapped_column(Integer)
    fee_krw: Mapped[float] = mapped_column(Numeric(20, 2), default=0)
    tax_krw: Mapped[float] = mapped_column(Numeric(20, 2), default=0)
    raw_ref: Mapped[str | None] = mapped_column(String(128))
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)


class CorePositionCurrent(Base):
    __tablename__ = "core_position_current"

    position_current_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    instrument_pk: Mapped[int] = mapped_column(BigInteger, index=True)
    net_qty: Mapped[int] = mapped_column(Integer, default=0)
    avg_cost_krw: Mapped[float | None] = mapped_column(Numeric(20, 4))
    market_value_krw: Mapped[float | None] = mapped_column(Numeric(20, 2))
    unrealized_pnl_krw: Mapped[float | None] = mapped_column(Numeric(20, 2))
    updated_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class EvtCashLedger(Base):
    __tablename__ = "evt_cash_ledger"

    cash_ledger_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_uid: Mapped[str] = mapped_column(String(64), index=True)
    ledger_event_type: Mapped[str] = mapped_column(String(64))
    trading_date: Mapped[date] = mapped_column(Date)
    amount_krw: Mapped[float] = mapped_column(Numeric(20, 2))
    balance_after_krw: Mapped[float | None] = mapped_column(Numeric(20, 2))
    related_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    occurred_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)


class CoreCashCurrent(Base):
    __tablename__ = "core_cash_current"

    cash_current_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_uid: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    settled_cash_krw: Mapped[float] = mapped_column(Numeric(20, 2), default=0)
    available_to_order_krw: Mapped[float] = mapped_column(Numeric(20, 2), default=0)
    pending_settlement_krw: Mapped[float] = mapped_column(Numeric(20, 2), default=0)
    updated_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)


class CoreReconciliationBreak(Base):
    __tablename__ = "core_reconciliation_break"

    reconciliation_break_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    break_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    scope_type: Mapped[str] = mapped_column(String(32))
    scope_id: Mapped[str] = mapped_column(String(64), index=True)
    severity_code: Mapped[str] = mapped_column(String(32))
    status_code: Mapped[str] = mapped_column(String(32))
    detected_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    resolved_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    expected_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    actual_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    notes: Mapped[str | None] = mapped_column(Text)


class PolicyRegistry(Base):
    __tablename__ = "policy_registry"

    policy_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    policy_id: Mapped[str] = mapped_column(String(64), index=True)
    policy_name: Mapped[str] = mapped_column(String(128))
    policy_scope: Mapped[str] = mapped_column(String(64))
    policy_type: Mapped[str] = mapped_column(String(64))
    policy_version: Mapped[str] = mapped_column(String(32))
    effective_from_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    effective_to_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    owner: Mapped[str | None] = mapped_column(String(128))
    approval_required: Mapped[bool] = mapped_column(Boolean, default=False)
    change_reason: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str | None] = mapped_column(String(128))
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    updated_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utcnow)
    payload: Mapped[dict] = mapped_column(JSON)
