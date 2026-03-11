from datetime import UTC, date, datetime

from pydantic import BaseModel, Field

from libs.domain.enums import OperationMode


class KillSwitchRequest(BaseModel):
    reason_code: str
    activated_by: str
    exit_policy: str = "EXIT_ONLY"


class StrategyState(BaseModel):
    strategy_id: str
    enabled: bool
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AccountState(BaseModel):
    account_id: str
    entry_enabled: bool
    exit_enabled: bool
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SymbolBlockState(BaseModel):
    symbol: str
    blocked: bool
    reason_code: str | None = None
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class BreakState(BaseModel):
    break_id: str
    scope: str
    severity: str
    status: str
    detected_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RiskFlagState(BaseModel):
    symbol: str
    flag_type: str
    severity: str
    hard_block: bool
    source_system: str
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SessionState(BaseModel):
    venue: str
    session_code: str
    market_data_ok: bool
    degraded: bool
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PositionState(BaseModel):
    symbol: str
    net_qty: int
    avg_cost_krw: float
    market_value_krw: float
    unrealized_pnl_krw: float
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OrderTrace(BaseModel):
    order_id: str
    state: str
    client_order_id: str
    broker_order_no: str | None = None
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CandidateState(BaseModel):
    candidate_id: str
    strategy_id: str
    instrument_id: str
    status: str
    expected_edge_bps: float | None = None
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ReplayJobState(BaseModel):
    replay_job_id: str
    trading_date: str
    status: str
    scenario: str = "baseline"
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ReplayJobCreateRequest(BaseModel):
    trading_date: date
    scenario: str = "baseline"
    notes: str | None = None


class OperationModeState(BaseModel):
    mode: OperationMode
    reason: str | None = None
    updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DashboardSummary(BaseModel):
    mode: OperationModeState
    strategy_enabled_count: int
    blocked_symbol_count: int
    open_break_count: int
    replay_job_count: int
    risk_flag_count: int
    active_position_count: int
