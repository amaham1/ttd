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


class PriceBarRecord(BaseModel):
    instrument_id: str
    venue: str
    bar_start_ts_utc: datetime
    bar_end_ts_utc: datetime
    open_px: float
    high_px: float
    low_px: float
    close_px: float
    volume: float
    turnover_krw: float | None = None
    vwap: float | None = None
    source: str = "KIS"


class TradeTickRecord(BaseModel):
    instrument_id: str
    venue: str
    trade_ts_utc: datetime
    price: float
    qty: float
    side_hint: str | None = None
    raw_ref: str | None = None


class OrderBookSnapshotRecord(BaseModel):
    instrument_id: str
    venue: str
    snapshot_ts_utc: datetime
    best_bid_px: float
    best_bid_qty: float
    best_ask_px: float
    best_ask_qty: float
    total_bid_qty: float | None = None
    total_ask_qty: float | None = None
    depth_payload: dict[str, Any] = Field(default_factory=dict)


class NewsItemRecord(BaseModel):
    news_id: str
    instrument_id: str | None = None
    source: str
    headline: str
    body: str | None = None
    published_at_utc: datetime
    sentiment_score: float | None = None
    relevance_score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MacroSeriesPointRecord(BaseModel):
    series_id: str
    observation_date: date
    value: float | None = None
    realtime_start: date | None = None
    realtime_end: date | None = None
    source: str = "FRED"
    metadata: dict[str, Any] = Field(default_factory=dict)


class InvestorFlowRecord(BaseModel):
    instrument_id: str
    venue: str
    trade_date: date
    foreign_net_buy_krw: float | None = None
    institution_net_buy_krw: float | None = None
    retail_net_buy_krw: float | None = None
    other_corp_net_buy_krw: float | None = None
    foreign_net_buy_shares: float | None = None
    institution_net_buy_shares: float | None = None
    retail_net_buy_shares: float | None = None
    other_corp_net_buy_shares: float | None = None
    source: str = "PYKRX"
    metadata: dict[str, Any] = Field(default_factory=dict)


class InstrumentFundamentalsRecord(BaseModel):
    instrument_id: str
    venue: str
    trade_date: date
    bps: float | None = None
    per: float | None = None
    pbr: float | None = None
    eps: float | None = None
    div_yield_pct: float | None = None
    dps: float | None = None
    source: str = "PYKRX"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ShortInterestRecord(BaseModel):
    instrument_id: str
    venue: str
    trade_date: date
    short_volume_shares: float | None = None
    short_volume_ratio_pct: float | None = None
    short_balance_shares: float | None = None
    short_balance_ratio_pct: float | None = None
    borrow_balance_shares: float | None = None
    borrow_balance_ratio_pct: float | None = None
    source: str = "MANUAL"
    metadata: dict[str, Any] = Field(default_factory=dict)


class SectorFlowRecord(BaseModel):
    trade_date: date
    sector_name: str
    net_flow_krw: float | None = None
    foreign_net_flow_krw: float | None = None
    institution_net_flow_krw: float | None = None
    retail_net_flow_krw: float | None = None
    sector_return_pct: float | None = None
    relative_strength_pct: float | None = None
    futures_basis_bps: float | None = None
    source: str = "MANUAL"
    metadata: dict[str, Any] = Field(default_factory=dict)


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


class StructuredEvent(BaseModel):
    event_id: str
    source_type: str = "DISCLOSURE"
    source_receipt_no: str | None = None
    instrument_id: str
    issuer_id: str | None = None
    issuer_name: str | None = None
    market: str = "KR"
    venue: str = "KRX"
    event_family: str
    event_type: str
    direction: str
    severity: float
    novelty: float
    magnitude: float | None = None
    parser_confidence: float
    model_confidence: float
    tradeability: str
    hard_block_candidate: bool
    time_sensitivity_minutes: int | None = None
    cash_impact_score: float | None = None
    earnings_impact_score: float | None = None
    dilution_risk_score: float | None = None
    peer_spillover_score: float | None = None
    extracted_summary: str | None = None
    extraction_payload: dict[str, Any] = Field(default_factory=dict)
    event_ts_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class EventCluster(BaseModel):
    cluster_id: str
    cluster_key: str
    instrument_id: str
    issuer_id: str | None = None
    event_family: str
    event_type: str
    event_direction: str
    source_receipt_nos: list[str] = Field(default_factory=list)
    source_event_ids: list[str] = Field(default_factory=list)
    severity: float
    novelty: float
    representative_summary: str | None = None
    event_count: int = 1
    latest_event_ts_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WatchlistTrigger(BaseModel):
    trigger_id: str
    instrument_id: str
    event_cluster_id: str | None = None
    trigger_type: str
    reason_code: str
    priority: int
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at_utc: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlphaFeatureVector(BaseModel):
    vector_id: str
    instrument_id: str
    event_cluster_id: str
    selection_horizon_days: int
    feature_version: str
    event_severity: float
    event_novelty: float
    surprise_score: float
    valuation_context_score: float
    gap_bps: float
    volume_anomaly_score: float
    sector_sympathy_score: float
    macro_regime_score: float
    spread_bps: float
    depth_imbalance: float
    opening_state_score: float
    closing_state_score: float
    follow_through_score: float
    liquidity_score: float
    slippage_estimate_bps: float
    tail_risk_score: float
    crowding_score: float
    data_freshness_score: float
    confidence_score: float
    feature_payload: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ExpertModelScore(BaseModel):
    expert_name: str
    horizon_days: int
    expected_return_bps: float
    expected_slippage_bps: float
    tail_risk_penalty_bps: float
    crowding_penalty_bps: float
    confidence: float
    rationale: str | None = None
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class RegimeState(BaseModel):
    regime_id: str
    regime_name: str
    risk_mode: str
    confidence: float
    market_data_fresh: bool
    event_density_score: float
    macro_score: float
    liquidity_score: float
    volatility_score: float
    expert_weights: dict[str, float] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


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
    source_event_ids: list[str] = Field(default_factory=list)
    event_cluster_id: str | None = None
    issuer_id: str | None = None
    sector_name: str | None = None
    source_report_name: str | None = None
    source_receipt_no: str | None = None
    matched_rule_id: str | None = None
    selection_reason: str | None = None
    rejection_reason: str | None = None
    candidate_status: str | None = None
    ranking_score: float | None = None
    ranking_reason: str | None = None
    selected_rank: int | None = None
    selection_confidence: float | None = None
    expected_slippage_bps: float | None = None
    tail_risk_penalty_bps: float | None = None
    crowding_penalty_bps: float | None = None
    cooldown_key: str | None = None
    decision_id: str | None = None
    candidate_family: str | None = None
    exit_reason_code: str | None = None
    target_qty_override: int | None = None
    position_qty: int | None = None
    position_avg_cost_krw: float | None = None
    position_return_pct: float | None = None
    holding_days: int | None = None
    thematic_tags: list[str] = Field(default_factory=list)
    cross_asset_impact_score: float | None = None
    thematic_alignment_score: float | None = None
    macro_headwind_score: float | None = None


class DisclosureRuleDefinition(BaseModel):
    rule_id: str
    rule_type: str
    rule_name: str
    match_field: str
    match_pattern: str
    decision_effect: str
    reason_template: str
    enabled: bool = True
    priority: int = 100


class CandidateDecisionRecord(BaseModel):
    decision_id: str
    candidate_id: str | None = None
    source_type: str = "DISCLOSURE"
    source_receipt_no: str | None = None
    source_report_name: str | None = None
    source_symbol: str | None = None
    matched_positive_rule_id: str | None = None
    matched_block_rule_id: str | None = None
    candidate_status: str
    selection_reason: str | None = None
    rejection_reason: str | None = None
    confidence_summary: str | None = None
    ranking_score: float | None = None
    ranking_reason: str | None = None
    selected_rank: int | None = None
    decision_payload_json: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


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
    base_notional_krw: int
    max_slippage_bps: float
    urgency: str
    route_policy: str
    tif: str
    expire_ts_utc: datetime
    sizing_reason: str | None = None


class SelectionDecision(BaseModel):
    selection_id: str
    candidate_id: str
    strategy_id: str
    account_scope: str
    instrument_id: str
    side: OrderSide
    event_cluster_id: str
    regime_id: str
    decision_status: str
    selected_rank: int | None = None
    net_alpha_score: float
    expected_return_bps: float
    expected_slippage_bps: float
    tail_risk_penalty_bps: float
    crowding_penalty_bps: float
    model_confidence: float
    target_notional_krw: int
    max_holding_days: int
    explanation: str
    reject_reason: str | None = None
    source_receipt_nos: list[str] = Field(default_factory=list)
    source_event_ids: list[str] = Field(default_factory=list)
    feature_vectors: list[AlphaFeatureVector] = Field(default_factory=list)
    expert_scores: list[ExpertModelScore] = Field(default_factory=list)
    selection_payload: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ModelScoreRecord(BaseModel):
    score_id: str
    selection_id: str
    instrument_id: str
    event_cluster_id: str
    expert_name: str
    horizon_days: int
    expected_return_bps: float
    expected_slippage_bps: float
    tail_risk_penalty_bps: float
    crowding_penalty_bps: float
    confidence: float
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ForwardLabelRecord(BaseModel):
    label_id: str
    instrument_id: str
    event_cluster_id: str
    vector_id: str
    horizon_days: int
    label_ts_utc: datetime
    forward_return_bps: float
    residual_return_bps: float
    max_drawdown_bps: float
    payoff_score: float
    label_payload: dict[str, Any] = Field(default_factory=dict)


class SelectionAuditRecord(BaseModel):
    audit_id: str
    selection_id: str
    candidate_id: str
    instrument_id: str
    event_cluster_id: str
    decision_status: str
    net_alpha_score: float
    model_confidence: float
    execution_ready: bool
    audit_payload: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PortfolioSelection(BaseModel):
    portfolio_id: str
    trading_date: date
    selection_ids: list[str] = Field(default_factory=list)
    weights_by_selection_id: dict[str, float] = Field(default_factory=dict)
    target_notionals_by_selection_id: dict[str, int] = Field(default_factory=dict)
    selected_symbols: list[str] = Field(default_factory=list)
    constraint_notes: list[str] = Field(default_factory=list)
    rebalance_reason: str
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ExecutionReadiness(BaseModel):
    account_id: str
    strategy_id: str
    instrument_id: str
    execution_side: OrderSide = OrderSide.BUY
    market_data_ok: bool = True
    account_entry_enabled: bool = True
    account_exit_enabled: bool = True
    kill_switch_active: bool = False
    reconciliation_break_active: bool = False
    risk_flag_active: bool = False
    symbol_blocked: bool = False
    data_freshness_ok: bool = True
    confidence_ok: bool = True
    vendor_healthy: bool = True
    session_entry_allowed: bool = True
    session_exit_allowed: bool = True
    max_allowed_notional_krw: int | None = None
    reason_codes: list[str] = Field(default_factory=list)
    as_of_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WalkForwardFoldReport(BaseModel):
    fold_id: str
    train_start_utc: datetime
    train_end_utc: datetime
    validation_start_utc: datetime
    validation_end_utc: datetime
    train_count: int
    validation_count: int
    mean_return_bps: float
    mean_payoff_score: float
    hit_rate: float
    net_sharpe_proxy: float
    max_drawdown_bps: float


class ModelTrainingReport(BaseModel):
    training_id: str
    model_version: str
    algorithm_family: str
    trained_experts: list[str] = Field(default_factory=list)
    training_row_count: int = 0
    fold_reports: list[WalkForwardFoldReport] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    artifact_refs: dict[str, str] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PromotionReadiness(BaseModel):
    promotion_id: str
    current_stage: str
    target_stage: str
    approved: bool
    reason_codes: list[str] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    baseline_metrics: dict[str, float] = Field(default_factory=dict)
    shadow_metrics: dict[str, float] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ShadowLiveRunAuditRecord(BaseModel):
    run_id: str
    candidate_id: str | None = None
    instrument_id: str | None = None
    execute_live: bool
    persisted: bool
    status: str
    reason: str | None = None
    promotion_required: bool = False
    promotion_approved: bool | None = None
    stale_data_incident: bool = False
    duplicate_order_incident: bool = False
    selector_mismatch_incident: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MicroTestCandidatePreview(BaseModel):
    candidate_id: str
    instrument_id: str
    side: OrderSide
    eligible_now: bool
    eligible_if_allowlisted: bool = False
    allowed_by_config: bool = True
    on_cooldown: bool = False
    selection_reason: str | None = None
    matched_rule_id: str | None = None
    quote_basis: str | None = None
    selected_price_krw: int | None = None
    best_bid_krw: int | None = None
    best_ask_krw: int | None = None
    last_price_krw: int | None = None
    spread_bps: float | None = None
    proposed_qty: int | None = None
    proposed_order_value_krw: int | None = None
    readiness_reason_codes: list[str] = Field(default_factory=list)
    block_reason_codes: list[str] = Field(default_factory=list)


class MicroTestCandidatePreviewResponse(BaseModel):
    generated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    configured_allowed_symbols: list[str] = Field(default_factory=list)
    suggested_allowed_symbols: list[str] = Field(default_factory=list)
    effective_max_order_value_krw: int | None = None
    live_risk_state: dict[str, Any] | None = None
    warnings: list[str] = Field(default_factory=list)
    candidates: list[MicroTestCandidatePreview] = Field(default_factory=list)


class PostTradeVerificationReport(BaseModel):
    verified_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status: str
    reason: str | None = None
    candidate_id: str | None = None
    account_id: str | None = None
    instrument_id: str | None = None
    internal_order_id: str | None = None
    broker_order_no: str | None = None
    planned_qty: int | None = None
    planned_price_krw: int | None = None
    matched_order_count: int = 0
    matched_fill_qty: int = 0
    matched_avg_fill_price_krw: float | None = None
    balance_position_qty: int | None = None
    balance_available_cash_krw: float | None = None
    matched_orders: list[dict[str, Any]] = Field(default_factory=list)
    matched_positions: list[dict[str, Any]] = Field(default_factory=list)
    daily_ccld_payload: dict[str, Any] | None = None
    balance_payload: dict[str, Any] | None = None


class ResearchInstrumentReport(BaseModel):
    instrument_id: str
    event_family: str
    sample_count: int
    avg_forward_return_bps: float
    avg_payoff_score: float
    worst_drawdown_bps: float


class ShadowLiveMetricsSummary(BaseModel):
    run_count: int
    no_trade_count: int
    blocked_count: int
    submitted_count: int
    live_attempt_count: int
    stale_data_incident_count: int
    duplicate_order_incident_count: int
    selector_mismatch_incident_count: int
    promotion_block_count: int
    latest_run_at_utc: datetime | None = None
    recent_runs: list[ShadowLiveRunAuditRecord] = Field(default_factory=list)


class ResearchReportSnapshot(BaseModel):
    generated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))
    latest_training_report: ModelTrainingReport | None = None
    training_history: list[ModelTrainingReport] = Field(default_factory=list)
    instrument_leaderboard: list[ResearchInstrumentReport] = Field(default_factory=list)
    event_family_leaderboard: list[dict[str, Any]] = Field(default_factory=list)
    fold_reports: list[WalkForwardFoldReport] = Field(default_factory=list)
    data_counts: dict[str, int] = Field(default_factory=dict)
