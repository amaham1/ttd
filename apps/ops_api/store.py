from datetime import UTC, datetime

from libs.domain.enums import OperationMode
from libs.replay.service import replay_job_service

from apps.ops_api.schemas import (
    AccountState,
    BreakState,
    CandidateState,
    DashboardSummary,
    KillSwitchRequest,
    OperationModeState,
    OrderTrace,
    PositionState,
    ReplayJobCreateRequest,
    ReplayJobState,
    RiskFlagState,
    SessionState,
    StrategyState,
    SymbolBlockState,
)


class InMemoryOpsStore:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self.replay_jobs_service = replay_job_service
        self.mode = OperationModeState(mode=OperationMode.NORMAL, updated_at_utc=now)
        self.strategies = {
            "disclosure-alpha": StrategyState(strategy_id="disclosure-alpha", enabled=True),
            "micro-flow": StrategyState(strategy_id="micro-flow", enabled=True),
            "close-only-defense": StrategyState(strategy_id="close-only-defense", enabled=False),
        }
        self.accounts = {
            "default": AccountState(account_id="default", entry_enabled=True, exit_enabled=True)
        }
        self.symbol_blocks: dict[str, SymbolBlockState] = {}
        self.breaks = {
            "break-demo": BreakState(
                break_id="break-demo",
                scope="account:default",
                severity="LOW",
                status="OPEN",
            )
        }
        self.orders = {
            "order-demo": OrderTrace(
                order_id="order-demo",
                state="REST_ACCEPTED",
                client_order_id="client-demo",
                broker_order_no="8300012345",
            )
        }
        self.candidates = {
            "candidate-demo": CandidateState(
                candidate_id="candidate-demo",
                strategy_id="disclosure-alpha",
                instrument_id="005930",
                status="CREATED",
                expected_edge_bps=28.5,
            )
        }
        self.risk_flags = {
            "005930:WATCH": RiskFlagState(
                symbol="005930",
                flag_type="DISCLOSURE_WATCH",
                severity="INFO",
                hard_block=False,
                source_system="DART",
            ),
            "035420:HALT": RiskFlagState(
                symbol="035420",
                flag_type="VOLATILITY_WARNING",
                severity="HIGH",
                hard_block=True,
                source_system="KRX",
            ),
        }
        self.sessions = {
            "KRX": SessionState(venue="KRX", session_code="CONTINUOUS", market_data_ok=True, degraded=False),
            "NXT": SessionState(venue="NXT", session_code="CONTINUOUS", market_data_ok=True, degraded=False),
        }
        self.positions = {
            "005930": PositionState(
                symbol="005930",
                net_qty=12,
                avg_cost_krw=79_500,
                market_value_krw=964_800,
                unrealized_pnl_krw=10_800,
            ),
            "000660": PositionState(
                symbol="000660",
                net_qty=4,
                avg_cost_krw=182_000,
                market_value_krw=736_000,
                unrealized_pnl_krw=8_000,
            ),
        }

    def summary(self) -> DashboardSummary:
        return DashboardSummary(
            mode=self.mode,
            strategy_enabled_count=len([state for state in self.strategies.values() if state.enabled]),
            blocked_symbol_count=len([state for state in self.symbol_blocks.values() if state.blocked]),
            open_break_count=len([state for state in self.breaks.values() if state.status == "OPEN"]),
            replay_job_count=len(self.replay_jobs_service.list_jobs()),
            risk_flag_count=len(self.risk_flags),
            active_position_count=len([state for state in self.positions.values() if state.net_qty != 0]),
        )

    def activate_kill_switch(self, request: KillSwitchRequest) -> OperationModeState:
        self.mode = OperationModeState(
            mode=OperationMode.KILL_SWITCH,
            reason=f"{request.reason_code}:{request.activated_by}",
        )
        return self.mode

    def set_strategy_enabled(self, strategy_id: str, enabled: bool) -> StrategyState:
        state = StrategyState(strategy_id=strategy_id, enabled=enabled)
        self.strategies[strategy_id] = state
        return state

    def set_account_entry_enabled(self, account_id: str, enabled: bool) -> AccountState:
        current = self.accounts.get(
            account_id,
            AccountState(account_id=account_id, entry_enabled=True, exit_enabled=True),
        )
        current.entry_enabled = enabled
        current.updated_at_utc = datetime.now(UTC)
        self.accounts[account_id] = current
        return current

    def set_symbol_block(self, symbol: str, blocked: bool, reason_code: str | None) -> SymbolBlockState:
        state = SymbolBlockState(symbol=symbol, blocked=blocked, reason_code=reason_code)
        self.symbol_blocks[symbol] = state
        return state

    def create_replay_job(self, request: ReplayJobCreateRequest) -> ReplayJobState:
        replay_job = self.replay_jobs_service.create_job(
            trading_date=request.trading_date,
            scenario=request.scenario,
            notes=request.notes,
        )
        return ReplayJobState(
            replay_job_id=replay_job.replay_job_id,
            trading_date=replay_job.trading_date.isoformat(),
            status=replay_job.status,
            scenario=replay_job.scenario,
            created_at_utc=replay_job.created_at_utc,
        )

    def list_replay_jobs(self) -> list[ReplayJobState]:
        return [
            ReplayJobState(
                replay_job_id=job.replay_job_id,
                trading_date=job.trading_date.isoformat(),
                status=job.status,
                scenario=job.scenario,
                created_at_utc=job.created_at_utc,
            )
            for job in self.replay_jobs_service.list_jobs()
        ]


store = InMemoryOpsStore()
