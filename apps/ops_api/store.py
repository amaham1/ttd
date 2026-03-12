from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from libs.config.settings import get_settings
from libs.contracts.messages import ExecutionReadiness
from libs.db.base import SessionLocal
from libs.db.repositories import TradingRepository
from libs.domain.enums import OperationMode, OrderSide
from libs.replay.service import replay_job_service

from apps.ops_api.schemas import (
    AccountState,
    BreakState,
    CandidateState,
    ControlPlaneAuditEvent,
    DashboardSummary,
    KillSwitchRequest,
    LiveControlState,
    LoopSchedulerState,
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
        self.settings = get_settings()
        self.replay_jobs_service = replay_job_service
        self.repository = TradingRepository(SessionLocal)
        self.audit_events: list[ControlPlaneAuditEvent] = []
        self._reset_defaults()
        self.reload_state()

    def _reset_defaults(self) -> None:
        now = datetime.now(UTC)
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
        self.breaks: dict[str, BreakState] = {}
        self.orders: dict[str, OrderTrace] = {}
        self.candidates: dict[str, CandidateState] = {}
        self.risk_flags: dict[str, RiskFlagState] = {}
        self.sessions: dict[str, SessionState] = {}
        self.positions: dict[str, PositionState] = {}
        self.loop_states: dict[str, LoopSchedulerState] = {}
        self.live_control = LiveControlState()
        self.instrument_name_cache: dict[str, dict[str, Any]] = {}
        self._broker_balance_name_cache: dict[str, str] = {}
        self._broker_balance_name_cache_updated_at_utc: datetime | None = None
        self._dart_symbol_name_cache: dict[str, str] = {}
        self._dart_symbol_name_cache_updated_at_utc: datetime | None = None
        self._pykrx_symbol_name_cache: dict[str, str] = {}
        self._pykrx_symbol_name_cache_updated_at_utc: datetime | None = None
        self.audit_events = []

    def _state_path(self) -> Path:
        return Path(self.settings.ops_state_path)

    @staticmethod
    def _serialize_model(model: Any) -> dict[str, Any] | None:
        if model is None:
            return None
        return model.model_dump(mode="json")

    @staticmethod
    def _serialize_mapping(mapping: dict[str, Any]) -> list[dict[str, Any]]:
        return [value.model_dump(mode="json") for value in mapping.values()]

    def _state_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "mode": self.mode.model_dump(mode="json"),
            "strategies": self._serialize_mapping(self.strategies),
            "accounts": self._serialize_mapping(self.accounts),
            "symbol_blocks": self._serialize_mapping(self.symbol_blocks),
            "breaks": self._serialize_mapping(self.breaks),
            "orders": self._serialize_mapping(self.orders),
            "candidates": self._serialize_mapping(self.candidates),
            "risk_flags": self._serialize_mapping(self.risk_flags),
            "sessions": self._serialize_mapping(self.sessions),
            "positions": self._serialize_mapping(self.positions),
            "loop_states": self._serialize_mapping(self.loop_states),
            "live_control": self.live_control.model_dump(mode="json"),
            "audit_events": [event.model_dump(mode="json") for event in self.audit_events],
        }

    @staticmethod
    def _load_mapping(items: list[dict[str, Any]], model_cls: Any, key_field: str) -> dict[str, Any]:
        mapping: dict[str, Any] = {}
        for item in items:
            model = model_cls.model_validate(item)
            mapping[str(getattr(model, key_field))] = model
        return mapping

    def _persist_state(self) -> None:
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self._state_payload(), ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

    def _persist_without_audit(self) -> None:
        self._persist_state()

    def _record_audit_event(
        self,
        *,
        action: str,
        resource_type: str,
        resource_id: str,
        actor: str | None = None,
        reason_code: str | None = None,
        before: Any = None,
        after: Any = None,
    ) -> ControlPlaneAuditEvent:
        event = ControlPlaneAuditEvent(
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            actor=actor,
            reason_code=reason_code,
            before=self._serialize_model(before),
            after=self._serialize_model(after),
        )
        self.audit_events.insert(0, event)
        audit_limit = max(int(self.settings.ops_state_audit_limit or 0), 1)
        if len(self.audit_events) > audit_limit:
            self.audit_events = self.audit_events[:audit_limit]
        return event

    def _commit(
        self,
        *,
        action: str,
        resource_type: str,
        resource_id: str,
        actor: str | None = None,
        reason_code: str | None = None,
        before: Any = None,
        after: Any = None,
    ) -> None:
        self._record_audit_event(
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            actor=actor,
            reason_code=reason_code,
            before=before,
            after=after,
        )
        self._persist_state()

    def reload_state(self) -> None:
        self._reset_defaults()
        path = self._state_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        try:
            self.mode = OperationModeState.model_validate(payload.get("mode") or {})
            self.strategies = self._load_mapping(
                payload.get("strategies") or [],
                StrategyState,
                "strategy_id",
            )
            self.accounts = self._load_mapping(
                payload.get("accounts") or [],
                AccountState,
                "account_id",
            )
            self.symbol_blocks = self._load_mapping(
                payload.get("symbol_blocks") or [],
                SymbolBlockState,
                "symbol",
            )
            self.breaks = self._load_mapping(
                payload.get("breaks") or [],
                BreakState,
                "break_id",
            )
            self.orders = self._load_mapping(
                payload.get("orders") or [],
                OrderTrace,
                "order_id",
            )
            self.candidates = self._load_mapping(
                payload.get("candidates") or [],
                CandidateState,
                "candidate_id",
            )
            self.risk_flags = self._load_mapping(
                payload.get("risk_flags") or [],
                RiskFlagState,
                "symbol",
            )
            self.sessions = self._load_mapping(
                payload.get("sessions") or [],
                SessionState,
                "venue",
            )
            self.positions = self._load_mapping(
                payload.get("positions") or [],
                PositionState,
                "symbol",
            )
            self.loop_states = self._load_mapping(
                payload.get("loop_states") or [],
                LoopSchedulerState,
                "loop_id",
            )
            self.live_control = LiveControlState.model_validate(payload.get("live_control") or {})
            self.audit_events = [
                ControlPlaneAuditEvent.model_validate(item)
                for item in (payload.get("audit_events") or [])
            ]
        except Exception:
            self._reset_defaults()

    def reset_state(self, *, delete_persisted: bool = False) -> None:
        self.replay_jobs_service.reset()
        self._reset_defaults()
        path = self._state_path()
        if delete_persisted and path.exists():
            path.unlink(missing_ok=True)
            return
        self._persist_state()

    def list_audit_events(self, *, limit: int = 50) -> list[ControlPlaneAuditEvent]:
        capped_limit = max(limit, 1)
        return self.audit_events[:capped_limit]

    @staticmethod
    def _normalize_symbol_list(symbols: list[str]) -> list[str]:
        normalized_symbols: list[str] = []
        seen: set[str] = set()
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
        return normalized_symbols

    def cache_broker_balance_names(self, name_by_symbol: dict[str, str]) -> None:
        now = datetime.now(UTC)
        normalized: dict[str, str] = {}
        for raw_symbol, raw_name in name_by_symbol.items():
            symbol = str(raw_symbol or "").strip()
            name = str(raw_name or "").strip()
            if not symbol or not name:
                continue
            normalized[symbol] = name
            self.instrument_name_cache[symbol] = {
                "symbol": symbol,
                "name": name,
                "source": "BROKER_BALANCE",
                "updated_at_utc": now.isoformat(),
            }
        if normalized:
            self._broker_balance_name_cache.update(normalized)
            self._broker_balance_name_cache_updated_at_utc = now

    def broker_balance_name_cache_stale(self, *, ttl_seconds: int = 120) -> bool:
        if self._broker_balance_name_cache_updated_at_utc is None:
            return True
        effective_ttl_seconds = max(int(ttl_seconds), 1)
        return datetime.now(UTC) - self._broker_balance_name_cache_updated_at_utc > timedelta(
            seconds=effective_ttl_seconds,
        )

    def cached_broker_balance_names(self) -> dict[str, str]:
        return dict(self._broker_balance_name_cache)

    def cache_dart_symbol_names(self, name_by_symbol: dict[str, str]) -> None:
        now = datetime.now(UTC)
        normalized: dict[str, str] = {}
        for raw_symbol, raw_name in name_by_symbol.items():
            symbol = str(raw_symbol or "").strip()
            name = str(raw_name or "").strip()
            if not symbol or not name:
                continue
            normalized[symbol] = name
            self.instrument_name_cache[symbol] = {
                "symbol": symbol,
                "name": name,
                "source": "DART_CORP_CODE",
                "updated_at_utc": now.isoformat(),
            }
        if normalized:
            self._dart_symbol_name_cache.update(normalized)
            self._dart_symbol_name_cache_updated_at_utc = now

    def dart_symbol_name_cache_stale(self, *, ttl_seconds: int = 86400) -> bool:
        if self._dart_symbol_name_cache_updated_at_utc is None:
            return True
        effective_ttl_seconds = max(int(ttl_seconds), 1)
        return datetime.now(UTC) - self._dart_symbol_name_cache_updated_at_utc > timedelta(
            seconds=effective_ttl_seconds,
        )

    def cached_dart_symbol_names(self) -> dict[str, str]:
        return dict(self._dart_symbol_name_cache)

    def cache_pykrx_symbol_names(self, name_by_symbol: dict[str, str]) -> None:
        now = datetime.now(UTC)
        normalized: dict[str, str] = {}
        for raw_symbol, raw_name in name_by_symbol.items():
            symbol = str(raw_symbol or "").strip()
            name = str(raw_name or "").strip()
            if not symbol or not name:
                continue
            normalized[symbol] = name
            self.instrument_name_cache[symbol] = {
                "symbol": symbol,
                "name": name,
                "source": "PYKRX",
                "updated_at_utc": now.isoformat(),
            }
        if normalized:
            self._pykrx_symbol_name_cache.update(normalized)
            self._pykrx_symbol_name_cache_updated_at_utc = now

    def pykrx_symbol_name_cache_stale(self, *, ttl_seconds: int = 86400) -> bool:
        if self._pykrx_symbol_name_cache_updated_at_utc is None:
            return True
        effective_ttl_seconds = max(int(ttl_seconds), 1)
        return datetime.now(UTC) - self._pykrx_symbol_name_cache_updated_at_utc > timedelta(
            seconds=effective_ttl_seconds,
        )

    def cached_pykrx_symbol_names(self) -> dict[str, str]:
        return dict(self._pykrx_symbol_name_cache)

    def resolve_instrument_names(
        self,
        *,
        symbols: list[str],
        balance_name_by_symbol: dict[str, str] | None = None,
        dart_name_by_symbol: dict[str, str] | None = None,
        pykrx_name_by_symbol: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_symbols = self._normalize_symbol_list(symbols)
        if balance_name_by_symbol:
            self.cache_broker_balance_names(balance_name_by_symbol)
        if dart_name_by_symbol:
            self.cache_dart_symbol_names(dart_name_by_symbol)
        if pykrx_name_by_symbol:
            self.cache_pykrx_symbol_names(pykrx_name_by_symbol)

        entries: list[dict[str, Any]] = []
        for symbol in normalized_symbols:
            cached = self.instrument_name_cache.get(symbol)
            if cached and cached.get("name"):
                entries.append(dict(cached))
                continue

            profile = None
            try:
                profile = self.repository.get_instrument_profile(symbol)
            except Exception:
                profile = None

            if profile is not None and profile.issuer_name:
                entry = {
                    "symbol": symbol,
                    "name": str(profile.issuer_name).strip(),
                    "source": "INSTRUMENT_PROFILE",
                    "updated_at_utc": (
                        profile.updated_at_utc.isoformat()
                        if profile.updated_at_utc is not None
                        else None
                    ),
                }
                self.instrument_name_cache[symbol] = entry
                entries.append(dict(entry))
                continue

            cached_balance_name = self._broker_balance_name_cache.get(symbol)
            if cached_balance_name:
                entry = {
                    "symbol": symbol,
                    "name": cached_balance_name,
                    "source": "BROKER_BALANCE",
                    "updated_at_utc": (
                        self._broker_balance_name_cache_updated_at_utc.isoformat()
                        if self._broker_balance_name_cache_updated_at_utc is not None
                        else None
                    ),
                }
                self.instrument_name_cache[symbol] = entry
                entries.append(dict(entry))
                continue

            cached_dart_name = self._dart_symbol_name_cache.get(symbol)
            if cached_dart_name:
                entry = {
                    "symbol": symbol,
                    "name": cached_dart_name,
                    "source": "DART_CORP_CODE",
                    "updated_at_utc": (
                        self._dart_symbol_name_cache_updated_at_utc.isoformat()
                        if self._dart_symbol_name_cache_updated_at_utc is not None
                        else None
                    ),
                }
                self.instrument_name_cache[symbol] = entry
                entries.append(dict(entry))
                continue

            cached_pykrx_name = self._pykrx_symbol_name_cache.get(symbol)
            if cached_pykrx_name:
                entry = {
                    "symbol": symbol,
                    "name": cached_pykrx_name,
                    "source": "PYKRX",
                    "updated_at_utc": (
                        self._pykrx_symbol_name_cache_updated_at_utc.isoformat()
                        if self._pykrx_symbol_name_cache_updated_at_utc is not None
                        else None
                    ),
                }
                self.instrument_name_cache[symbol] = entry
                entries.append(dict(entry))
                continue

            unresolved_entry = {
                "symbol": symbol,
                "name": None,
                "source": None,
                "updated_at_utc": None,
            }
            entries.append(unresolved_entry)
        return entries

    def list_reconciliation_breaks(self, *, limit: int = 100) -> list[BreakState]:
        try:
            repository_rows = self.repository.list_reconciliation_breaks(limit=limit, status_code="OPEN")
        except Exception:
            repository_rows = None
        if repository_rows is not None:
            self.breaks = {
                str(row["break_id"]): BreakState(
                    break_id=str(row["break_id"]),
                    scope=str(row["scope_type"]),
                    severity=str(row["severity_code"]),
                    status=str(row["status_code"]),
                    detected_at_utc=datetime.fromisoformat(str(row["detected_at_utc"]).replace("Z", "+00:00")),
                )
                for row in repository_rows
            }
        return list(self.breaks.values())[:limit]

    def list_loop_states(self) -> list[LoopSchedulerState]:
        return list(self.loop_states.values())

    def get_loop_state(self, loop_id: str) -> LoopSchedulerState | None:
        return self.loop_states.get(loop_id)

    def get_live_control(self) -> LiveControlState:
        return self.live_control

    def set_live_control(
        self,
        *,
        max_order_value_krw: int | None = None,
        auto_loop_interval_seconds: int | None = None,
        autonomous_loop_enabled: bool | None = None,
        actor: str | None = None,
        reason_code: str | None = None,
    ) -> LiveControlState:
        before = self.live_control.model_copy(deep=True)
        next_max_order_value = self.live_control.max_order_value_krw
        next_interval_seconds = self.live_control.auto_loop_interval_seconds
        next_autonomous_loop_enabled = self.live_control.autonomous_loop_enabled

        if max_order_value_krw is not None:
            next_max_order_value = max(int(max_order_value_krw), 1)
        if auto_loop_interval_seconds is not None:
            next_interval_seconds = max(int(auto_loop_interval_seconds), 1)
        if autonomous_loop_enabled is not None:
            next_autonomous_loop_enabled = bool(autonomous_loop_enabled)

        self.live_control = LiveControlState(
            max_order_value_krw=next_max_order_value,
            auto_loop_interval_seconds=next_interval_seconds,
            autonomous_loop_enabled=next_autonomous_loop_enabled,
        )
        self._commit(
            action="SET_LIVE_CONTROL",
            resource_type="live_control",
            resource_id="shadow-live",
            actor=actor,
            reason_code=reason_code,
            before=before,
            after=self.live_control,
        )
        return self.live_control

    def _save_loop_state(
        self,
        state: LoopSchedulerState,
        *,
        action: str | None = None,
        actor: str | None = None,
        reason_code: str | None = None,
        before: LoopSchedulerState | None = None,
    ) -> LoopSchedulerState:
        self.loop_states[state.loop_id] = state
        if action is None:
            self._persist_without_audit()
            return state
        self._commit(
            action=action,
            resource_type="loop_scheduler",
            resource_id=state.loop_id,
            actor=actor,
            reason_code=reason_code,
            before=before,
            after=state,
        )
        return state

    def acquire_loop_lease(
        self,
        *,
        loop_id: str,
        service_name: str,
        owner_id: str,
        interval_seconds: int,
        execute_live: bool,
        persist: bool,
        ttl_seconds: float,
        actor: str | None = None,
        reason_code: str | None = None,
    ) -> LoopSchedulerState:
        now = datetime.now(UTC)
        current = self.loop_states.get(loop_id)
        if (
            current is not None
            and current.owner_id
            and current.owner_id != owner_id
            and current.lease_expires_at_utc is not None
            and current.lease_expires_at_utc > now
        ):
            raise RuntimeError(f"loop lease is held by {current.owner_id}")

        state = LoopSchedulerState(
            loop_id=loop_id,
            service_name=service_name,
            desired_running=True,
            execute_live=execute_live,
            persist=persist,
            interval_seconds=interval_seconds,
            owner_id=owner_id,
            lease_expires_at_utc=now + timedelta(seconds=max(ttl_seconds, 1.0)),
            heartbeat_at_utc=now,
            last_started_at_utc=now,
            last_stopped_at_utc=None if current is None else current.last_stopped_at_utc,
            last_result_status=None if current is None else current.last_result_status,
            last_error=None,
            restart_count=(0 if current is None else current.restart_count) + 1,
        )
        return self._save_loop_state(
            state,
            action="ACQUIRE_LOOP_LEASE",
            actor=actor or owner_id,
            reason_code=reason_code,
            before=current,
        )

    def renew_loop_lease(
        self,
        *,
        loop_id: str,
        owner_id: str,
        ttl_seconds: float,
        last_result_status: str | None = None,
        last_error: str | None = None,
    ) -> LoopSchedulerState:
        current = self.loop_states.get(loop_id)
        if current is None:
            raise RuntimeError("loop state not found")
        if current.owner_id not in {None, owner_id}:
            if current.lease_expires_at_utc and current.lease_expires_at_utc > datetime.now(UTC):
                raise RuntimeError(f"loop lease belongs to {current.owner_id}")
        now = datetime.now(UTC)
        updated = current.model_copy(
            update={
                "owner_id": owner_id,
                "lease_expires_at_utc": now + timedelta(seconds=max(ttl_seconds, 1.0)),
                "heartbeat_at_utc": now,
                "last_result_status": (
                    current.last_result_status if last_result_status is None else last_result_status
                ),
                "last_error": last_error,
                "updated_at_utc": now,
            },
            deep=True,
        )
        return self._save_loop_state(updated)

    def release_loop_lease(
        self,
        *,
        loop_id: str,
        owner_id: str,
        desired_running: bool,
        last_error: str | None = None,
        reason_code: str | None = None,
    ) -> LoopSchedulerState | None:
        current = self.loop_states.get(loop_id)
        if current is None:
            return None
        if current.owner_id not in {None, owner_id}:
            raise RuntimeError(f"loop lease belongs to {current.owner_id}")
        now = datetime.now(UTC)
        updated = current.model_copy(
            update={
                "desired_running": desired_running,
                "owner_id": None,
                "lease_expires_at_utc": None,
                "heartbeat_at_utc": now,
                "last_stopped_at_utc": now,
                "last_error": last_error,
                "updated_at_utc": now,
            },
            deep=True,
        )
        return self._save_loop_state(
            updated,
            action="RELEASE_LOOP_LEASE" if not desired_running else "SUSPEND_LOOP_RUNTIME",
            actor=owner_id,
            reason_code=reason_code,
            before=current,
        )

    def set_session_state(
        self,
        *,
        venue: str,
        session_code: str,
        market_data_ok: bool,
        degraded: bool,
        entry_allowed: bool,
        reason_codes: list[str] | None = None,
        last_quote_at_utc: datetime | None = None,
        last_trade_at_utc: datetime | None = None,
    ) -> SessionState:
        state = SessionState(
            venue=venue,
            session_code=session_code,
            market_data_ok=market_data_ok,
            degraded=degraded,
            entry_allowed=entry_allowed,
            reason_codes=list(reason_codes or []),
            last_quote_at_utc=last_quote_at_utc,
            last_trade_at_utc=last_trade_at_utc,
        )
        self.sessions[venue] = state
        self._persist_without_audit()
        return state

    def summary(self) -> DashboardSummary:
        open_breaks = self.list_reconciliation_breaks(limit=200)
        return DashboardSummary(
            mode=self.mode,
            strategy_enabled_count=len([state for state in self.strategies.values() if state.enabled]),
            blocked_symbol_count=len([state for state in self.symbol_blocks.values() if state.blocked]),
            open_break_count=len([state for state in open_breaks if state.status == "OPEN"]),
            replay_job_count=len(self.replay_jobs_service.list_jobs()),
            risk_flag_count=len(self.risk_flags),
            active_position_count=len([state for state in self.positions.values() if state.net_qty != 0]),
        )

    def activate_kill_switch(self, request: KillSwitchRequest) -> OperationModeState:
        before = self.mode.model_copy(deep=True)
        self.mode = OperationModeState(
            mode=OperationMode.KILL_SWITCH,
            reason=f"{request.reason_code}:{request.activated_by}",
        )
        self._commit(
            action="ACTIVATE_KILL_SWITCH",
            resource_type="operation_mode",
            resource_id="global",
            actor=request.activated_by,
            reason_code=request.reason_code,
            before=before,
            after=self.mode,
        )
        return self.mode

    def set_strategy_enabled(self, strategy_id: str, enabled: bool) -> StrategyState:
        before = self.strategies.get(strategy_id)
        state = StrategyState(strategy_id=strategy_id, enabled=enabled)
        self.strategies[strategy_id] = state
        self._commit(
            action="SET_STRATEGY_ENABLED",
            resource_type="strategy",
            resource_id=strategy_id,
            before=before,
            after=state,
        )
        return state

    def set_account_permissions(
        self,
        account_id: str,
        *,
        entry_enabled: bool | None = None,
        exit_enabled: bool | None = None,
    ) -> AccountState:
        current = self.accounts.get(
            account_id,
            AccountState(account_id=account_id, entry_enabled=True, exit_enabled=True),
        )
        before = current.model_copy(deep=True)
        if entry_enabled is not None:
            current.entry_enabled = bool(entry_enabled)
        if exit_enabled is not None:
            current.exit_enabled = bool(exit_enabled)
        current.updated_at_utc = datetime.now(UTC)
        self.accounts[account_id] = current
        self._commit(
            action="SET_ACCOUNT_PERMISSIONS",
            resource_type="account",
            resource_id=account_id,
            before=before,
            after=current,
        )
        return current

    def set_account_entry_enabled(self, account_id: str, enabled: bool) -> AccountState:
        return self.set_account_permissions(account_id, entry_enabled=enabled)

    def set_account_exit_enabled(self, account_id: str, enabled: bool) -> AccountState:
        return self.set_account_permissions(account_id, exit_enabled=enabled)

    def set_symbol_block(self, symbol: str, blocked: bool, reason_code: str | None) -> SymbolBlockState:
        before = self.symbol_blocks.get(symbol)
        state = SymbolBlockState(symbol=symbol, blocked=blocked, reason_code=reason_code)
        self.symbol_blocks[symbol] = state
        self._commit(
            action="SET_SYMBOL_BLOCK",
            resource_type="symbol_block",
            resource_id=symbol,
            reason_code=reason_code,
            before=before,
            after=state,
        )
        return state

    def create_replay_job(self, request: ReplayJobCreateRequest) -> ReplayJobState:
        replay_job = self.replay_jobs_service.create_job(
            trading_date=request.trading_date,
            scenario=request.scenario,
            notes=request.notes,
        )
        state = ReplayJobState(
            replay_job_id=replay_job.replay_job_id,
            trading_date=replay_job.trading_date.isoformat(),
            status=replay_job.status,
            scenario=replay_job.scenario,
            created_at_utc=replay_job.created_at_utc,
        )
        self._commit(
            action="CREATE_REPLAY_JOB",
            resource_type="replay_job",
            resource_id=replay_job.replay_job_id,
            reason_code=request.scenario,
            after=state,
        )
        return state

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

    def resolve_execution_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        instrument_id: str,
        execution_side: OrderSide = OrderSide.BUY,
        confidence_ok: bool = True,
        market_data_ok: bool = True,
        data_freshness_ok: bool = True,
        vendor_healthy: bool = True,
        session_entry_allowed: bool = True,
        session_exit_allowed: bool = True,
        max_allowed_notional_krw: int | None = None,
    ) -> ExecutionReadiness:
        account = self.accounts.get(account_id)
        strategy = self.strategies.get(strategy_id)
        symbol_block = self.symbol_blocks.get(instrument_id)
        risk_flag = self.risk_flags.get(instrument_id)
        session_state = self.sessions.get("KRX")
        open_break = any(state.status == "OPEN" for state in self.list_reconciliation_breaks(limit=200))
        kill_switch_active = self.mode.mode == OperationMode.KILL_SWITCH
        effective_market_data_ok = market_data_ok
        effective_session_entry_allowed = session_entry_allowed
        effective_session_exit_allowed = session_exit_allowed
        reason_codes: list[str] = []
        effective_account_entry_enabled = account.entry_enabled if account is not None else True
        effective_account_exit_enabled = account.exit_enabled if account is not None else True
        if execution_side == OrderSide.BUY and self.mode.mode in {OperationMode.ENTRY_FROZEN, OperationMode.EXIT_ONLY}:
            effective_account_entry_enabled = False
            reason_codes.append(f"OPERATION_MODE_{self.mode.mode.value}")
        if strategy is not None and not strategy.enabled:
            reason_codes.append("STRATEGY_DISABLED")
        if symbol_block is not None and symbol_block.blocked:
            reason_codes.append(symbol_block.reason_code or "SYMBOL_BLOCKED")
        if risk_flag is not None and risk_flag.hard_block:
            reason_codes.append(risk_flag.flag_type)
        if execution_side == OrderSide.BUY and not effective_account_entry_enabled:
            reason_codes.append("ACCOUNT_ENTRY_DISABLED")
        if execution_side == OrderSide.SELL and not effective_account_exit_enabled:
            reason_codes.append("ACCOUNT_EXIT_DISABLED")
        if not confidence_ok:
            reason_codes.append("CONFIDENCE_FLOOR")
        if not vendor_healthy:
            reason_codes.append("VENDOR_UNHEALTHY")
        if not data_freshness_ok:
            reason_codes.append("STALE_DATA")
        if session_state is not None:
            effective_market_data_ok = effective_market_data_ok and session_state.market_data_ok
            effective_session_entry_allowed = effective_session_entry_allowed and session_state.entry_allowed
            effective_session_exit_allowed = effective_session_exit_allowed and session_state.entry_allowed
            if not session_state.market_data_ok:
                reason_codes.append("SESSION_MARKET_DATA_UNAVAILABLE")
            if session_state.degraded:
                reason_codes.extend(session_state.reason_codes)
        if execution_side == OrderSide.BUY and not effective_session_entry_allowed:
            reason_codes.append("SESSION_ENTRY_BLOCKED")
        if execution_side == OrderSide.SELL and not effective_session_exit_allowed:
            reason_codes.append("SESSION_EXIT_BLOCKED")
        reason_codes = list(dict.fromkeys(code for code in reason_codes if code))
        return ExecutionReadiness(
            account_id=account_id,
            strategy_id=strategy_id,
            instrument_id=instrument_id,
            execution_side=execution_side,
            market_data_ok=effective_market_data_ok,
            account_entry_enabled=effective_account_entry_enabled,
            account_exit_enabled=effective_account_exit_enabled,
            kill_switch_active=kill_switch_active,
            reconciliation_break_active=open_break,
            risk_flag_active=bool(risk_flag.hard_block) if risk_flag is not None else False,
            symbol_blocked=bool(symbol_block.blocked) if symbol_block is not None else False,
            data_freshness_ok=data_freshness_ok,
            confidence_ok=confidence_ok and (strategy.enabled if strategy is not None else True),
            vendor_healthy=vendor_healthy,
            session_entry_allowed=effective_session_entry_allowed,
            session_exit_allowed=effective_session_exit_allowed,
            max_allowed_notional_krw=max_allowed_notional_krw,
            reason_codes=reason_codes,
        )


store = InMemoryOpsStore()
