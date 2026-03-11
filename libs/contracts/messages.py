from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from libs.domain.enums import MessageType, OrderSide


class MessageEnvelope(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    message_type: MessageType
    message_name: str
    message_version: str = "1.0"
    producer: str
    occurred_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    observed_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    trading_date: date
    correlation_id: str
    causation_id: str | None = None
    idempotency_key: str
    account_scope: str | None = None
    instrument_scope: str | None = None
    trace_scope: dict[str, Any] = Field(default_factory=dict)
    schema_version: str = "1.0"
    payload: dict[str, Any]


class OrderSubmitCommand(BaseModel):
    internal_order_id: str
    client_order_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    qty: int
    price: int | None = None
    order_type: str
    tif: str
    venue_hint: str | None = None
    route_policy: str
    urgency: str
    submitted_by_strategy: str
    correlation_id: str


class OrderAckEvent(BaseModel):
    internal_order_id: str
    client_order_id: str
    broker_order_no: str
    broker_status_code: str | None = None
    ack_ts_utc: datetime
    venue: str | None = None
    raw_ref: str | None = None


class FillEvent(BaseModel):
    internal_order_id: str
    broker_order_no: str
    broker_trade_id: str | None = None
    account_id: str
    instrument_id: str
    side: OrderSide
    venue: str | None = None
    fill_ts_utc: datetime
    price: int
    qty: int
    fee: int = 0
    tax: int = 0
    raw_ref: str | None = None


class MarketTick(BaseModel):
    instrument_id: str
    venue: str
    exchange_ts_utc: datetime
    received_ts_utc: datetime
    last_price: int
    last_qty: int
    cum_volume: int | None = None
    trade_strength: float | None = None
    raw_ref: str | None = None


class QuoteL1(BaseModel):
    instrument_id: str
    venue: str
    exchange_ts_utc: datetime
    best_bid_px: int
    best_bid_qty: int
    best_ask_px: int
    best_ask_qty: int
    total_bid_qty: int | None = None
    total_ask_qty: int | None = None
    spread_bps: float | None = None
    imbalance_l1: float | None = None
    raw_ref: str | None = None


class DisclosureEvent(BaseModel):
    disclosure_id: str
    instrument_id: str
    event_type: str
    direction: str
    magnitude: float | None = None
    dilution_pct: float | None = None
    confidence: float
    tradeability: str
    hard_block_candidate: bool
    parser_version: str


class RiskFlag(BaseModel):
    instrument_id: str
    source_system: str
    flag_type: str
    severity: str
    hard_block: bool
    effective_from_utc: datetime
    effective_to_utc: datetime | None = None
    reason_code: str | None = None
    reason_text: str | None = None


class TradeCandidate(BaseModel):
    candidate_id: str
    strategy_id: str
    account_scope: str
    instrument_id: str
    side: OrderSide
    expected_edge_bps: float
    target_notional_krw: int
    entry_style: str
    expire_ts_utc: datetime
    meta_model_version: str
    source_signal_refs: list[str] = Field(default_factory=list)


class RiskGateDecision(BaseModel):
    candidate_id: str
    account_id: str
    passed_gate_set_version: str
    penalty_bps_total: float
    final_allowed_notional_hint: int | None = None
    hard_block: bool
    failed_gate_codes: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)


class TradeIntent(BaseModel):
    intent_id: str
    candidate_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    target_qty: int
    target_notional_krw: int
    max_slippage_bps: float
    urgency: str
    route_policy: str
    tif: str
    expire_ts_utc: datetime

