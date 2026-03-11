import asyncio
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import re
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from libs.adapters.kis import KISHttpBrokerGateway
from libs.adapters.kis_mapper import map_fill_notice, map_order_ack, map_quote_l1, map_trade_tick
from libs.adapters.minio_store import MinioObjectStore
from libs.adapters.nats import NatsEventBus
from libs.config.settings import get_settings
from libs.db.base import SessionLocal
from libs.db.repositories import TradingRepository
from libs.domain.enums import MessageType
from libs.domain.enums import Environment
from libs.services.common_stock_universe import CommonStockUniverseError
from libs.services.common_stock_universe import CommonStockUniverseService
from libs.services.event_pipeline import EventPipelineService
from libs.services.raw_event_service import RawEventService


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _normalize_order_no(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.lstrip("0")
    return normalized or "0"


def _today_for_timezone(timezone_name: str) -> date:
    try:
        return datetime.now(ZoneInfo(timezone_name)).date()
    except Exception:
        return datetime.now().date()


def _now_in_timezone(timezone_name: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(timezone_name))
    except Exception:
        return datetime.now()


def _parse_local_hhmm(value: str, *, default_hour: int, default_minute: int) -> tuple[int, int]:
    text = str(value or "").strip()
    if not text:
        return default_hour, default_minute
    try:
        hour_text, minute_text = text.split(":", maxsplit=1)
        hour = min(max(int(hour_text), 0), 23)
        minute = min(max(int(minute_text), 0), 59)
        return hour, minute
    except Exception:
        return default_hour, default_minute


def _safe_field(fields: list[str], index: int) -> str:
    if index >= len(fields):
        return ""
    value = fields[index]
    return "" if value is None else str(value).strip()


def _is_truthy_flag(value: Any) -> bool:
    return str(value or "").strip().upper() in {"1", "Y", "T", "TRUE"}


_KIS_WS_QUOTE_TR_IDS = {
    "H0STASP0": "KRX",
    "H0NXASP0": "NXT",
    "H0UNASP0": "TOTAL",
}
_KIS_WS_TRADE_TR_IDS = {
    "H0STCNT0": "KRX",
    "H0NXCNT0": "NXT",
    "H0UNCNT0": "TOTAL",
}
_KIS_WS_FILL_NOTICE_TR_IDS = {
    "H0STCNI0",
    "H0STCNI9",
    "K0STCNI0",
    "K0STCNI9",
}
_KIS_WS_MARKET_STATUS_TR_IDS = {
    "H0STMKO0": "KRX",
    "H0NXMKO0": "NXT",
    "H0UNMKO0": "TOTAL",
}


@dataclass(slots=True)
class BrokerSessionState:
    rest_token_ready: bool
    ws_approval_ready: bool
    last_rest_auth_utc: datetime | None
    last_ws_auth_utc: datetime | None
    current_mode: str
    pending_rate_budget: int
    degraded_reason: str | None = None
    allowed_envs: list[str] | None = None
    live_trading_enabled: bool = False
    live_trading_armed: bool = False
    live_trading_armed_by: str | None = None
    live_trading_armed_at_utc: datetime | None = None
    last_total_equity_krw: int | None = None
    baseline_total_equity_krw: int | None = None
    daily_loss_pct: float | None = None
    entry_paused: bool = False
    live_pause_reason: str | None = None
    common_stock_universe_count: int | None = None
    oms_order_count: int = 0
    execution_fill_count: int = 0
    open_reconciliation_break_count: int = 0
    ws_consumer_running: bool = False
    ws_symbol_count: int = 0
    ws_last_message_at_utc: datetime | None = None
    ws_market_data_stale: bool = False
    ws_reconnect_count: int = 0
    ws_last_error: str | None = None


@dataclass(slots=True)
class ReconciliationRunSummary:
    trading_date: str
    order_ticket_count: int
    execution_fill_count: int
    break_opened_count: int
    break_resolved_count: int
    open_break_count: int
    issues: list[str] = field(default_factory=list)
    created_breaks: list[dict[str, Any]] = field(default_factory=list)
    resolved_scopes: list[str] = field(default_factory=list)
    broker_daily_ccld_rows: int = 0
    broker_balance_rows: int = 0
    generated_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class BrokerWebSocketSnapshot:
    running: bool
    env: str | None
    venue: str | None
    symbol_count: int
    symbols: list[str]
    include_fill_notice: bool
    include_market_status: bool
    started_at_utc: datetime | None
    connected_at_utc: datetime | None
    last_message_at_utc: datetime | None
    last_quote_at_utc: datetime | None
    last_trade_at_utc: datetime | None
    last_fill_notice_at_utc: datetime | None
    last_order_notice_at_utc: datetime | None
    last_market_status_at_utc: datetime | None
    last_error: str | None
    last_disconnect_reason: str | None
    connect_count: int
    reconnect_count: int
    message_count: int
    control_count: int
    quote_count: int
    trade_count: int
    fill_notice_count: int
    order_notice_count: int
    market_status_count: int
    market_data_stale: bool
    latest_quote_symbol_count: int
    latest_trade_symbol_count: int
    latest_market_status_count: int


class LiveTradingGuardError(RuntimeError):
    pass


@dataclass(slots=True)
class BrokerGatewayRuntime:
    adapter: KISHttpBrokerGateway = field(default_factory=KISHttpBrokerGateway)
    repository: TradingRepository = field(default_factory=lambda: TradingRepository(SessionLocal))
    raw_event_service: RawEventService = field(
        default_factory=lambda: RawEventService(
            repository=TradingRepository(SessionLocal),
            object_store=MinioObjectStore(),
        )
    )
    event_pipeline: EventPipelineService = field(
        default_factory=lambda: EventPipelineService(event_bus=NatsEventBus())
    )
    common_stock_universe: CommonStockUniverseService = field(default_factory=CommonStockUniverseService)
    last_rest_auth_utc: datetime | None = None
    last_ws_auth_utc: datetime | None = None
    live_trading_armed: bool = False
    live_trading_armed_by: str | None = None
    live_trading_armed_at_utc: datetime | None = None
    last_total_equity_krw: int | None = None
    baseline_total_equity_krw: int | None = None
    daily_loss_pct: float | None = None
    entry_paused: bool = False
    live_pause_reason: str | None = None
    oms_order_tickets: dict[str, dict[str, Any]] = field(default_factory=dict)
    execution_fills: list[dict[str, Any]] = field(default_factory=list)
    reconciliation_breaks: dict[str, dict[str, Any]] = field(default_factory=dict)
    _fill_seen_keys: set[str] = field(default_factory=set)
    last_reconciliation_run_at_utc: datetime | None = None
    latest_quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_trades: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_market_status: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_order_notices: list[dict[str, Any]] = field(default_factory=list)
    _ws_loop_task: asyncio.Task[None] | None = None
    _ws_stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    _ws_env: Environment | None = None
    _ws_venue: str | None = None
    _ws_symbols: list[str] = field(default_factory=list)
    _ws_include_fill_notice: bool = True
    _ws_include_market_status: bool = True
    _ws_started_at_utc: datetime | None = None
    _ws_connected_at_utc: datetime | None = None
    _ws_last_message_at_utc: datetime | None = None
    _ws_last_quote_at_utc: datetime | None = None
    _ws_last_trade_at_utc: datetime | None = None
    _ws_last_fill_notice_at_utc: datetime | None = None
    _ws_last_order_notice_at_utc: datetime | None = None
    _ws_last_market_status_at_utc: datetime | None = None
    _ws_last_error: str | None = None
    _ws_last_disconnect_reason: str | None = None
    _ws_connect_count: int = 0
    _ws_reconnect_count: int = 0
    _ws_message_count: int = 0
    _ws_control_count: int = 0
    _ws_quote_count: int = 0
    _ws_trade_count: int = 0
    _ws_fill_notice_count: int = 0
    _ws_order_notice_count: int = 0
    _ws_market_status_count: int = 0

    def snapshot(self) -> BrokerSessionState:
        settings = get_settings()
        common_stock_snapshot = self.common_stock_universe.snapshot()
        open_break_count = len([item for item in self.list_reconciliation_breaks(open_only=True) if item["status_code"] == "OPEN"])
        ws_snapshot = self.ws_snapshot()
        return BrokerSessionState(
            rest_token_ready=self.last_rest_auth_utc is not None,
            ws_approval_ready=self.last_ws_auth_utc is not None,
            last_rest_auth_utc=self.last_rest_auth_utc,
            last_ws_auth_utc=self.last_ws_auth_utc,
            current_mode="NORMAL",
            pending_rate_budget=42,
            allowed_envs=["prod"] if not settings.kis_enable_paper else ["prod", "vps"],
            live_trading_enabled=settings.kis_live_trading_enabled,
            live_trading_armed=self.live_trading_armed,
            live_trading_armed_by=self.live_trading_armed_by,
            live_trading_armed_at_utc=self.live_trading_armed_at_utc,
            last_total_equity_krw=self.last_total_equity_krw,
            baseline_total_equity_krw=self.baseline_total_equity_krw,
            daily_loss_pct=self.daily_loss_pct,
            entry_paused=self.entry_paused,
            live_pause_reason=self.live_pause_reason,
            common_stock_universe_count=common_stock_snapshot.symbol_count or None,
            oms_order_count=len(self.list_order_tickets(limit=500)),
            execution_fill_count=len(self.list_execution_fills(limit=500)),
            open_reconciliation_break_count=open_break_count,
            ws_consumer_running=ws_snapshot.running,
            ws_symbol_count=ws_snapshot.symbol_count,
            ws_last_message_at_utc=ws_snapshot.last_message_at_utc,
            ws_market_data_stale=ws_snapshot.market_data_stale,
            ws_reconnect_count=ws_snapshot.reconnect_count,
            ws_last_error=ws_snapshot.last_error,
        )

    def reset_runtime(self) -> None:
        task = self._ws_loop_task
        if task is not None and not task.done():
            self._ws_stop_event.set()
            try:
                task.cancel()
            except RuntimeError:
                pass
        self.last_rest_auth_utc = None
        self.last_ws_auth_utc = None
        self.live_trading_armed = False
        self.live_trading_armed_by = None
        self.live_trading_armed_at_utc = None
        self.last_total_equity_krw = None
        self.baseline_total_equity_krw = None
        self.daily_loss_pct = None
        self.entry_paused = False
        self.live_pause_reason = None
        self.oms_order_tickets = {}
        self.execution_fills = []
        self.reconciliation_breaks = {}
        self._fill_seen_keys = set()
        self.last_reconciliation_run_at_utc = None
        self.latest_quotes = {}
        self.latest_trades = {}
        self.latest_market_status = {}
        self.recent_order_notices = []
        self._ws_loop_task = None
        self._ws_stop_event = asyncio.Event()
        self._ws_env = None
        self._ws_venue = None
        self._ws_symbols = []
        self._ws_include_fill_notice = True
        self._ws_include_market_status = True
        self._ws_started_at_utc = None
        self._ws_connected_at_utc = None
        self._ws_last_message_at_utc = None
        self._ws_last_quote_at_utc = None
        self._ws_last_trade_at_utc = None
        self._ws_last_fill_notice_at_utc = None
        self._ws_last_order_notice_at_utc = None
        self._ws_last_market_status_at_utc = None
        self._ws_last_error = None
        self._ws_last_disconnect_reason = None
        self._ws_connect_count = 0
        self._ws_reconnect_count = 0
        self._ws_message_count = 0
        self._ws_control_count = 0
        self._ws_quote_count = 0
        self._ws_trade_count = 0
        self._ws_fill_notice_count = 0
        self._ws_order_notice_count = 0
        self._ws_market_status_count = 0

    def _ensure_allowed_env(self, env: Environment) -> None:
        settings = get_settings()
        if env == Environment.VPS and not settings.kis_enable_paper:
            raise LiveTradingGuardError("paper trading is disabled; use env=prod only")

    def _ensure_expected_account(self, payload: dict[str, Any]) -> None:
        settings = get_settings()
        cano = str(payload.get("cano", ""))
        product_code = str(payload.get("acnt_prdt_cd", ""))
        if settings.kis_account_no and cano and cano != settings.kis_account_no:
            raise LiveTradingGuardError("payload cano does not match configured live account")
        if settings.kis_account_product_code and product_code and product_code != settings.kis_account_product_code:
            raise LiveTradingGuardError("payload acnt_prdt_cd does not match configured live account")

    def _balance_query_payload(self) -> dict[str, Any]:
        settings = get_settings()
        return {
            "env": Environment.PROD,
            "cano": settings.kis_account_no,
            "acnt_prdt_cd": settings.kis_account_product_code,
            "afhr_flpr_yn": "N",
            "ofl_yn": "",
            "inqr_dvsn": "02",
            "unpr_dvsn": "01",
            "fund_sttl_icld_yn": "N",
            "fncg_amt_auto_rdpt_yn": "N",
            "prcs_dvsn": "00",
            "ctx_area_fk100": "",
            "ctx_area_nk100": "",
        }

    def _parse_live_risk_metrics(self, payload: dict[str, Any]) -> tuple[int, int, float]:
        summary = (payload.get("output2") or [{}])[0]
        current_total = int(float(summary.get("tot_evlu_amt") or summary.get("nass_amt") or 0))
        baseline_total = int(float(summary.get("bfdy_tot_asst_evlu_amt") or current_total or 0))
        if baseline_total <= 0:
            daily_loss_pct = 0.0
        else:
            daily_loss_pct = max(((baseline_total - current_total) / baseline_total) * 100.0, 0.0)
        return current_total, baseline_total, daily_loss_pct

    def _daily_ccld_query_payload(
        self,
        *,
        trading_date: date,
        symbol: str | None = None,
        broker_order_no: str | None = None,
    ) -> dict[str, Any]:
        trading_date_text = trading_date.strftime("%Y%m%d")
        settings = get_settings()
        return {
            "env": Environment.PROD,
            "pd_dv": "inner",
            "cano": settings.kis_account_no,
            "acnt_prdt_cd": settings.kis_account_product_code,
            "inqr_strt_dt": trading_date_text,
            "inqr_end_dt": trading_date_text,
            "sll_buy_dvsn_cd": "00",
            "pdno": symbol or "",
            "ccld_dvsn": "00",
            "inqr_dvsn": "00",
            "inqr_dvsn_3": "00",
            "ord_gno_brno": "",
            "odno": broker_order_no or "",
            "inqr_dvsn_1": "",
            "ctx_area_fk100": "",
            "ctx_area_nk100": "",
            "excg_id_dvsn_cd": "KRX",
        }

    @staticmethod
    def _fill_dedupe_key(event: Any) -> str:
        return (
            event.broker_trade_id
            or f"{event.internal_order_id}:{event.broker_order_no}:{event.fill_ts_utc.isoformat()}:{event.qty}:{event.price}"
        )

    def _record_order_ticket(self, ticket: dict[str, Any]) -> dict[str, Any]:
        internal_order_id = str(ticket["internal_order_id"])
        existing = self.oms_order_tickets.get(internal_order_id, {})
        merged = {**existing, **ticket}
        self.oms_order_tickets[internal_order_id] = merged
        return merged

    def _record_order_ack(self, event: Any, payload_json: dict[str, Any]) -> dict[str, Any]:
        existing = self.oms_order_tickets.get(event.internal_order_id, {})
        filled_qty = _safe_int(existing.get("filled_qty"))
        working_qty = _safe_int(payload_json.get("qty") or payload_json.get("ord_qty") or existing.get("working_qty"))
        if working_qty > 0:
            working_qty = max(working_qty - filled_qty, 0)
        ticket = {
            "internal_order_id": event.internal_order_id,
            "client_order_id": event.client_order_id,
            "broker_order_no": event.broker_order_no,
            "account_uid": payload_json.get("account_id", existing.get("account_uid", "default")),
            "instrument_id": payload_json.get("instrument_id") or payload_json.get("pdno") or existing.get("instrument_id"),
            "side_code": payload_json.get("side_code", existing.get("side_code", "BUY")),
            "order_state_code": "ACKED",
            "order_type_code": payload_json.get("order_type_code", existing.get("order_type_code", "LIMIT")),
            "tif_code": payload_json.get("tif_code", existing.get("tif_code", "DAY")),
            "working_qty": working_qty,
            "filled_qty": filled_qty,
            "avg_fill_price": existing.get("avg_fill_price"),
            "last_event_at_utc": event.ack_ts_utc.isoformat(),
            "payload_json": payload_json,
        }
        return self._record_order_ticket(ticket)

    def _record_fill(self, event: Any, payload_json: dict[str, Any]) -> dict[str, Any]:
        dedupe_key = self._fill_dedupe_key(event)
        fill_payload = {
            "internal_order_id": event.internal_order_id,
            "broker_order_no": event.broker_order_no,
            "broker_trade_id": event.broker_trade_id,
            "account_uid": event.account_id,
            "instrument_id": event.instrument_id,
            "side_code": event.side.value.upper(),
            "venue_code": event.venue,
            "fill_ts_utc": event.fill_ts_utc.isoformat(),
            "fill_price": float(event.price),
            "fill_qty": int(event.qty),
            "fee_krw": float(event.fee),
            "tax_krw": float(event.tax),
            "raw_ref": event.raw_ref,
            "payload_json": payload_json,
        }
        if dedupe_key not in self._fill_seen_keys:
            self.execution_fills.insert(0, fill_payload)
            self._fill_seen_keys.add(dedupe_key)

        existing = self.oms_order_tickets.get(event.internal_order_id, {})
        previous_filled_qty = _safe_int(existing.get("filled_qty"))
        previous_avg_fill_price = _safe_float(existing.get("avg_fill_price")) if existing.get("avg_fill_price") is not None else None
        new_filled_qty = previous_filled_qty + event.qty
        if previous_avg_fill_price is None or previous_filled_qty <= 0:
            avg_fill_price = float(event.price)
        else:
            total_value = (previous_avg_fill_price * previous_filled_qty) + (float(event.price) * event.qty)
            avg_fill_price = total_value / max(new_filled_qty, 1)
        working_qty = max(_safe_int(existing.get("working_qty")) - event.qty, 0)
        ticket = {
            "internal_order_id": event.internal_order_id,
            "client_order_id": existing.get("client_order_id") or payload_json.get("client_order_id") or event.internal_order_id,
            "broker_order_no": event.broker_order_no,
            "account_uid": event.account_id,
            "instrument_id": event.instrument_id,
            "side_code": event.side.value.upper(),
            "order_state_code": "FILLED" if working_qty == 0 else "PARTIALLY_FILLED",
            "order_type_code": payload_json.get("order_type_code", existing.get("order_type_code", "LIMIT")),
            "tif_code": payload_json.get("tif_code", existing.get("tif_code", "DAY")),
            "working_qty": working_qty,
            "filled_qty": new_filled_qty,
            "avg_fill_price": round(avg_fill_price, 4),
            "last_event_at_utc": event.fill_ts_utc.isoformat(),
            "payload_json": {**existing.get("payload_json", {}), **payload_json},
        }
        self._record_order_ticket(ticket)
        return fill_payload

    @staticmethod
    def _reconciliation_break_id(scope_type: str, scope_id: str) -> str:
        digest = sha256(f"{scope_type}:{scope_id}".encode("utf-8")).hexdigest()[:12]
        return f"recon-{scope_type.lower()}-{digest}"

    def _upsert_runtime_break(
        self,
        *,
        break_id: str,
        scope_type: str,
        scope_id: str,
        severity_code: str,
        expected_payload: dict[str, Any],
        actual_payload: dict[str, Any],
        notes: str | None,
    ) -> dict[str, Any]:
        entry = {
            "break_id": break_id,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "severity_code": severity_code,
            "status_code": "OPEN",
            "detected_at_utc": datetime.now(UTC).isoformat(),
            "resolved_at_utc": None,
            "expected_payload": expected_payload,
            "actual_payload": actual_payload,
            "notes": notes,
        }
        current = self.reconciliation_breaks.get(break_id, {})
        merged = {**current, **entry}
        self.reconciliation_breaks[break_id] = merged
        return merged

    def _resolve_runtime_breaks(self, *, scope_type: str, scope_id: str) -> int:
        resolved_at_utc = datetime.now(UTC).isoformat()
        resolved_count = 0
        for break_id, item in self.reconciliation_breaks.items():
            if item.get("scope_type") == scope_type and item.get("scope_id") == scope_id and item.get("status_code") == "OPEN":
                item["status_code"] = "RESOLVED"
                item["resolved_at_utc"] = resolved_at_utc
                self.reconciliation_breaks[break_id] = item
                resolved_count += 1
        return resolved_count

    def list_order_tickets(self, *, limit: int = 100) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = dict(self.oms_order_tickets)
        try:
            for ticket in self.repository.list_order_tickets(limit=limit):
                merged.setdefault(ticket["internal_order_id"], ticket)
        except Exception:
            pass
        items = sorted(
            merged.values(),
            key=lambda item: item.get("last_event_at_utc") or "",
            reverse=True,
        )
        return items[:limit]

    def get_order_ticket(self, internal_order_id: str) -> dict[str, Any] | None:
        try:
            ticket = self.repository.get_order_ticket_by_internal_order_id(internal_order_id)
            if ticket is not None:
                return ticket
        except Exception:
            pass
        return self.oms_order_tickets.get(internal_order_id)

    def list_execution_fills(
        self,
        *,
        limit: int = 100,
        internal_order_id: str | None = None,
        broker_order_no: str | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = list(self.execution_fills)
        try:
            repository_items = self.repository.list_execution_fills(
                limit=limit,
                internal_order_id=internal_order_id,
                broker_order_no=broker_order_no,
            )
            dedupe: dict[str, dict[str, Any]] = {}
            for item in repository_items + items:
                key = item.get("broker_trade_id") or (
                    f"{item.get('internal_order_id')}:{item.get('broker_order_no')}:{item.get('fill_ts_utc')}:{item.get('fill_qty')}"
                )
                dedupe[key] = item
            items = list(dedupe.values())
        except Exception:
            pass
        if internal_order_id:
            items = [item for item in items if item.get("internal_order_id") == internal_order_id]
        if broker_order_no:
            normalized = _normalize_order_no(broker_order_no)
            items = [
                item
                for item in items
                if _normalize_order_no(item.get("broker_order_no")) == normalized
            ]
        items.sort(key=lambda item: item.get("fill_ts_utc") or "", reverse=True)
        return items[:limit]

    def list_reconciliation_breaks(self, *, limit: int = 100, open_only: bool = False) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = dict(self.reconciliation_breaks)
        try:
            repository_items = self.repository.list_reconciliation_breaks(
                limit=limit,
                status_code="OPEN" if open_only else None,
            )
            for item in repository_items:
                merged[item["break_id"]] = item
        except Exception:
            pass
        items = list(merged.values())
        if open_only:
            items = [item for item in items if item.get("status_code") == "OPEN"]
        items.sort(key=lambda item: item.get("detected_at_utc") or "", reverse=True)
        return items[:limit]

    @staticmethod
    def _is_nonlive_order_id(order_id: str | None) -> bool:
        if not order_id:
            return False
        normalized = str(order_id).strip()
        if not normalized.startswith("order-"):
            return False
        return re.fullmatch(r"order-[0-9a-f]{12}", normalized) is None

    def purge_nonlive_order_artifacts(self) -> dict[str, Any]:
        purge_order_ids: set[str] = set()

        for internal_order_id in self.oms_order_tickets:
            if self._is_nonlive_order_id(internal_order_id):
                purge_order_ids.add(str(internal_order_id))

        try:
            for ticket in self.repository.list_order_tickets(limit=1000):
                internal_order_id = str(ticket.get("internal_order_id") or "")
                if self._is_nonlive_order_id(internal_order_id):
                    purge_order_ids.add(internal_order_id)
        except Exception:
            pass

        for item in self.list_reconciliation_breaks(limit=1000, open_only=False):
            if str(item.get("scope_type") or "") != "ORDER":
                continue
            scope_id = str(item.get("scope_id") or "")
            if self._is_nonlive_order_id(scope_id):
                purge_order_ids.add(scope_id)

        runtime_order_ticket_count = 0
        runtime_fill_count = 0
        runtime_break_count = 0
        if purge_order_ids:
            runtime_order_ticket_count = sum(
                1 for internal_order_id in list(self.oms_order_tickets) if internal_order_id in purge_order_ids
            )
            for internal_order_id in purge_order_ids:
                self.oms_order_tickets.pop(internal_order_id, None)

            retained_fills: list[dict[str, Any]] = []
            for fill in self.execution_fills:
                if str(fill.get("internal_order_id") or "") in purge_order_ids:
                    runtime_fill_count += 1
                    continue
                retained_fills.append(fill)
            self.execution_fills = retained_fills

            retained_breaks: dict[str, dict[str, Any]] = {}
            for break_id, item in self.reconciliation_breaks.items():
                scope_id = str(item.get("scope_id") or "")
                if str(item.get("scope_type") or "") == "ORDER" and scope_id in purge_order_ids:
                    runtime_break_count += 1
                    continue
                retained_breaks[break_id] = item
            self.reconciliation_breaks = retained_breaks

            retained_seen_keys: set[str] = set()
            for fill_seen_key in self._fill_seen_keys:
                if any(fill_seen_key.startswith(f"{internal_order_id}:") for internal_order_id in purge_order_ids):
                    continue
                retained_seen_keys.add(fill_seen_key)
            self._fill_seen_keys = retained_seen_keys

        repository_result: dict[str, Any] = {
            "order_ticket_count": 0,
            "order_event_count": 0,
            "execution_fill_count": 0,
            "reconciliation_break_count": 0,
            "purged_order_ids": [],
        }
        try:
            repository_result = self.repository.purge_nonlive_order_artifacts()
        except Exception:
            pass

        purged_order_ids = sorted(set(repository_result.get("purged_order_ids") or []) | purge_order_ids)
        return {
            "purged_order_ids": purged_order_ids,
            "runtime_order_ticket_count": runtime_order_ticket_count,
            "runtime_fill_count": runtime_fill_count,
            "runtime_break_count": runtime_break_count,
            "repository_order_ticket_count": int(repository_result.get("order_ticket_count") or 0),
            "repository_order_event_count": int(repository_result.get("order_event_count") or 0),
            "repository_execution_fill_count": int(repository_result.get("execution_fill_count") or 0),
            "repository_reconciliation_break_count": int(repository_result.get("reconciliation_break_count") or 0),
        }

    async def recover_oms_state(self, *, limit: int = 200) -> dict[str, Any]:
        order_tickets = self.list_order_tickets(limit=limit)
        execution_fills = self.list_execution_fills(limit=limit)
        open_breaks = self.list_reconciliation_breaks(limit=limit, open_only=True)
        self.last_reconciliation_run_at_utc = datetime.now(UTC)
        return {
            "recovered_at_utc": self.last_reconciliation_run_at_utc,
            "order_ticket_count": len(order_tickets),
            "execution_fill_count": len(execution_fills),
            "open_break_count": len(open_breaks),
            "latest_order_ticket": order_tickets[0] if order_tickets else None,
        }

    @staticmethod
    def _match_daily_ccld_rows(
        rows: list[dict[str, Any]],
        *,
        ticket: dict[str, Any],
    ) -> list[dict[str, Any]]:
        normalized_ticket_order_no = _normalize_order_no(ticket.get("broker_order_no"))
        instrument_id = str(ticket.get("instrument_id") or "").strip()
        matches: list[dict[str, Any]] = []
        for row in rows:
            row_order_no = _normalize_order_no(_first_present(row, "odno", "ODNO", "ord_no", "ordno"))
            row_symbol = str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip()
            if normalized_ticket_order_no and row_order_no == normalized_ticket_order_no:
                matches.append(row)
                continue
            if not normalized_ticket_order_no and instrument_id and row_symbol == instrument_id:
                matches.append(row)
        return matches

    @staticmethod
    def _aggregate_broker_fill_qty(rows: list[dict[str, Any]]) -> tuple[int, float | None]:
        total_qty = 0
        total_value = 0.0
        fallback_price: float | None = None
        for row in rows:
            qty = _safe_int(_first_present(row, "tot_ccld_qty", "ccld_qty", "exec_qty", "ord_qty"))
            price = _safe_float(_first_present(row, "avg_prvs", "avg_ccld_unpr", "ccld_avg_prc", "ord_unpr"))
            if price > 0 and fallback_price is None:
                fallback_price = price
            if qty > 0 and price > 0:
                total_qty += qty
                total_value += qty * price
            elif qty > 0:
                total_qty += qty
        if total_qty > 0 and total_value > 0:
            return total_qty, round(total_value / total_qty, 4)
        return total_qty, fallback_price

    @staticmethod
    def _broker_position_qty_map(balance_rows: list[dict[str, Any]]) -> dict[str, int]:
        positions: dict[str, int] = {}
        for row in balance_rows:
            symbol = str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip()
            if not symbol:
                continue
            positions[symbol] = positions.get(symbol, 0) + _safe_int(
                _first_present(row, "hldg_qty", "hold_qty", "cblc_qty", "ord_psbl_qty")
            )
        return positions

    @staticmethod
    def _internal_position_qty_map(fills: list[dict[str, Any]]) -> dict[str, int]:
        positions: dict[str, int] = {}
        for fill in fills:
            symbol = str(fill.get("instrument_id") or "").strip()
            if not symbol:
                continue
            qty = _safe_int(fill.get("fill_qty"))
            side_code = str(fill.get("side_code") or "BUY").upper()
            signed_qty = qty if side_code == "BUY" else -qty
            positions[symbol] = positions.get(symbol, 0) + signed_qty
        return positions

    def _upsert_reconciliation_break(
        self,
        *,
        scope_type: str,
        scope_id: str,
        severity_code: str,
        expected_payload: dict[str, Any],
        actual_payload: dict[str, Any],
        notes: str | None,
    ) -> dict[str, Any]:
        break_id = self._reconciliation_break_id(scope_type, scope_id)
        try:
            self.repository.upsert_reconciliation_break(
                break_id=break_id,
                scope_type=scope_type,
                scope_id=scope_id,
                severity_code=severity_code,
                expected_payload=expected_payload,
                actual_payload=actual_payload,
                notes=notes,
            )
        except Exception:
            pass
        return self._upsert_runtime_break(
            break_id=break_id,
            scope_type=scope_type,
            scope_id=scope_id,
            severity_code=severity_code,
            expected_payload=expected_payload,
            actual_payload=actual_payload,
            notes=notes,
        )

    def _resolve_reconciliation_break_scope(self, *, scope_type: str, scope_id: str) -> int:
        runtime_resolved_count = self._resolve_runtime_breaks(scope_type=scope_type, scope_id=scope_id)
        repository_resolved_count = 0
        try:
            repository_resolved_count = self.repository.resolve_reconciliation_breaks(
                scope_type=scope_type,
                scope_id=scope_id,
            )
        except Exception:
            pass
        return max(runtime_resolved_count, repository_resolved_count)

    async def run_intraday_reconciliation(self, *, trading_date: date | None = None) -> ReconciliationRunSummary:
        effective_trading_date = trading_date or _today_for_timezone(get_settings().app_timezone)
        order_tickets = self.list_order_tickets(limit=500)
        execution_fills = self.list_execution_fills(limit=2000)
        daily_ccld_payload = await self.query_daily_ccld(self._daily_ccld_query_payload(trading_date=effective_trading_date))
        balance_payload = await self.query_balance(self._balance_query_payload())

        daily_rows = daily_ccld_payload.get("output1") or []
        if isinstance(daily_rows, dict):
            daily_rows = [daily_rows]
        if not isinstance(daily_rows, list):
            daily_rows = []
        balance_rows = balance_payload.get("output1") or []
        if isinstance(balance_rows, dict):
            balance_rows = [balance_rows]
        if not isinstance(balance_rows, list):
            balance_rows = []

        issues: list[str] = []
        created_breaks: list[dict[str, Any]] = []
        resolved_scopes: list[str] = []
        break_opened_count = 0
        break_resolved_count = 0

        for ticket in order_tickets:
            scope_type = "ORDER"
            scope_id = str(ticket["internal_order_id"])
            matched_rows = self._match_daily_ccld_rows(daily_rows, ticket=ticket)
            internal_filled_qty = _safe_int(ticket.get("filled_qty"))
            broker_filled_qty, broker_avg_fill_price = self._aggregate_broker_fill_qty(matched_rows)
            order_state_code = str(ticket.get("order_state_code") or "")

            if order_state_code in {"ACKED", "PARTIALLY_FILLED", "FILLED"} and not matched_rows:
                created_breaks.append(
                    self._upsert_reconciliation_break(
                        scope_type=scope_type,
                        scope_id=scope_id,
                        severity_code="HIGH",
                        expected_payload={"ticket": ticket},
                        actual_payload={"broker_rows": []},
                        notes="Broker daily-ccld did not return the internal order.",
                    )
                )
                break_opened_count += 1
                issues.append(f"ORDER_MISSING:{scope_id}")
                continue

            if matched_rows and broker_filled_qty != internal_filled_qty:
                created_breaks.append(
                    self._upsert_reconciliation_break(
                        scope_type=scope_type,
                        scope_id=scope_id,
                        severity_code="MEDIUM",
                        expected_payload={
                            "internal_filled_qty": internal_filled_qty,
                            "internal_avg_fill_price": ticket.get("avg_fill_price"),
                            "ticket": ticket,
                        },
                        actual_payload={
                            "broker_filled_qty": broker_filled_qty,
                            "broker_avg_fill_price": broker_avg_fill_price,
                            "broker_rows": matched_rows,
                        },
                        notes="Internal fill state does not match broker daily-ccld.",
                    )
                )
                break_opened_count += 1
                issues.append(f"FILLED_QTY_MISMATCH:{scope_id}")
                continue

            resolved_count = self._resolve_reconciliation_break_scope(scope_type=scope_type, scope_id=scope_id)
            if resolved_count > 0:
                break_resolved_count += resolved_count
                resolved_scopes.append(f"{scope_type}:{scope_id}")

        internal_positions = self._internal_position_qty_map(execution_fills)
        broker_positions = self._broker_position_qty_map(balance_rows)
        all_symbols = sorted(set(internal_positions) | set(broker_positions))
        for symbol in all_symbols:
            scope_type = "POSITION"
            scope_id = symbol
            internal_qty = internal_positions.get(symbol, 0)
            broker_qty = broker_positions.get(symbol, 0)
            if internal_qty != broker_qty:
                created_breaks.append(
                    self._upsert_reconciliation_break(
                        scope_type=scope_type,
                        scope_id=scope_id,
                        severity_code="HIGH",
                        expected_payload={"internal_net_qty": internal_qty},
                        actual_payload={"broker_net_qty": broker_qty},
                        notes="Internal fill-derived position does not match broker balance.",
                    )
                )
                break_opened_count += 1
                issues.append(f"POSITION_QTY_MISMATCH:{symbol}")
                continue

            resolved_count = self._resolve_reconciliation_break_scope(scope_type=scope_type, scope_id=scope_id)
            if resolved_count > 0:
                break_resolved_count += resolved_count
                resolved_scopes.append(f"{scope_type}:{scope_id}")

        self.last_reconciliation_run_at_utc = datetime.now(UTC)
        open_break_count = len(self.list_reconciliation_breaks(limit=500, open_only=True))
        return ReconciliationRunSummary(
            trading_date=effective_trading_date.isoformat(),
            order_ticket_count=len(order_tickets),
            execution_fill_count=len(execution_fills),
            break_opened_count=break_opened_count,
            break_resolved_count=break_resolved_count,
            open_break_count=open_break_count,
            issues=issues,
            created_breaks=created_breaks,
            resolved_scopes=resolved_scopes,
            broker_daily_ccld_rows=len(daily_rows),
            broker_balance_rows=len(balance_rows),
        )

    def _ws_market_data_is_stale(self) -> bool:
        settings = get_settings()
        threshold_seconds = max(settings.kis_ws_stale_after_seconds, 0.0)
        if threshold_seconds <= 0:
            return False
        if not self._ws_symbols:
            return False
        latest_market_ts = max(
            [item for item in [self._ws_last_quote_at_utc, self._ws_last_trade_at_utc] if item is not None],
            default=None,
        )
        reference_ts = latest_market_ts or self._ws_started_at_utc
        if reference_ts is None:
            return False
        return datetime.now(UTC) - reference_ts > timedelta(seconds=threshold_seconds)

    def ws_snapshot(self) -> BrokerWebSocketSnapshot:
        return BrokerWebSocketSnapshot(
            running=self._ws_loop_task is not None and not self._ws_loop_task.done(),
            env=self._ws_env.value if self._ws_env is not None else None,
            venue=self._ws_venue,
            symbol_count=len(self._ws_symbols),
            symbols=list(self._ws_symbols),
            include_fill_notice=self._ws_include_fill_notice,
            include_market_status=self._ws_include_market_status,
            started_at_utc=self._ws_started_at_utc,
            connected_at_utc=self._ws_connected_at_utc,
            last_message_at_utc=self._ws_last_message_at_utc,
            last_quote_at_utc=self._ws_last_quote_at_utc,
            last_trade_at_utc=self._ws_last_trade_at_utc,
            last_fill_notice_at_utc=self._ws_last_fill_notice_at_utc,
            last_order_notice_at_utc=self._ws_last_order_notice_at_utc,
            last_market_status_at_utc=self._ws_last_market_status_at_utc,
            last_error=self._ws_last_error,
            last_disconnect_reason=self._ws_last_disconnect_reason,
            connect_count=self._ws_connect_count,
            reconnect_count=self._ws_reconnect_count,
            message_count=self._ws_message_count,
            control_count=self._ws_control_count,
            quote_count=self._ws_quote_count,
            trade_count=self._ws_trade_count,
            fill_notice_count=self._ws_fill_notice_count,
            order_notice_count=self._ws_order_notice_count,
            market_status_count=self._ws_market_status_count,
            market_data_stale=self._ws_market_data_is_stale(),
            latest_quote_symbol_count=len(self.latest_quotes),
            latest_trade_symbol_count=len(self.latest_trades),
            latest_market_status_count=len(self.latest_market_status),
        )

    def list_latest_quotes(self, *, limit: int = 100, symbol: str | None = None) -> list[dict[str, Any]]:
        items = list(self.latest_quotes.values())
        if symbol:
            items = [item for item in items if item.get("instrument_id") == symbol]
        items.sort(key=lambda item: item.get("exchange_ts_utc") or "", reverse=True)
        return items[:limit]

    def list_latest_trades(self, *, limit: int = 100, symbol: str | None = None) -> list[dict[str, Any]]:
        items = list(self.latest_trades.values())
        if symbol:
            items = [item for item in items if item.get("instrument_id") == symbol]
        items.sort(
            key=lambda item: item.get("received_ts_utc") or item.get("exchange_ts_utc") or "",
            reverse=True,
        )
        return items[:limit]

    def list_market_status_snapshots(self, *, limit: int = 20) -> list[dict[str, Any]]:
        items = list(self.latest_market_status.values())
        items.sort(key=lambda item: item.get("received_at_utc") or "", reverse=True)
        return items[:limit]

    @staticmethod
    def _parse_snapshot_dt(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    def _entry_session_open(self) -> bool:
        settings = get_settings()
        now_local = _now_in_timezone(settings.app_timezone)
        if now_local.weekday() >= 5:
            return False
        start_hour, start_minute = _parse_local_hhmm(
            settings.trading_live_entry_session_start_local_time,
            default_hour=9,
            default_minute=5,
        )
        end_hour, end_minute = _parse_local_hhmm(
            settings.trading_live_entry_session_end_local_time,
            default_hour=15,
            default_minute=15,
        )
        start_local = now_local.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
        end_local = now_local.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
        return start_local <= now_local <= end_local

    def live_market_guard(self, *, symbol: str, venue: str = "KRX") -> dict[str, Any]:
        ws_status = self.ws_snapshot()
        requested_venue = venue.upper()
        latest_quote = next(iter(self.list_latest_quotes(limit=1, symbol=symbol)), None)
        latest_trade = next(iter(self.list_latest_trades(limit=1, symbol=symbol)), None)
        latest_market_status = next(
            iter(
                [
                    item
                    for item in self.list_market_status_snapshots(limit=20)
                    if str(item.get("venue") or "").upper() in {requested_venue, "TOTAL"}
                ]
            ),
            None,
        )
        latest_quote_at = self._parse_snapshot_dt(latest_quote.get("exchange_ts_utc")) if latest_quote else None
        latest_trade_at = self._parse_snapshot_dt(
            (latest_trade or {}).get("received_ts_utc") or (latest_trade or {}).get("exchange_ts_utc")
        )

        reason_codes: list[str] = []
        if not self._entry_session_open():
            reason_codes.append("SESSION_OUTSIDE_ENTRY_WINDOW")
        if not ws_status.running:
            reason_codes.append("WS_FEED_NOT_RUNNING")
        if ws_status.running and ws_status.venue not in {None, requested_venue, "TOTAL"}:
            reason_codes.append(f"WS_VENUE_MISMATCH:{ws_status.venue}")
        if ws_status.market_data_stale:
            reason_codes.append("WS_MARKET_DATA_STALE")
        if ws_status.running and symbol not in ws_status.symbols:
            reason_codes.append(f"WS_SYMBOL_NOT_SUBSCRIBED:{symbol}")
        if latest_quote is None:
            reason_codes.append("WS_QUOTE_UNAVAILABLE")
        halt_detected = _is_truthy_flag((latest_trade or {}).get("halt_yn"))
        if halt_detected:
            reason_codes.append("TRADING_HALT_ACTIVE")

        entry_allowed = not reason_codes
        return {
            "instrument_id": symbol,
            "venue": venue.upper(),
            "entry_allowed": entry_allowed,
            "reason_codes": reason_codes,
            "ws_running": ws_status.running,
            "ws_market_data_stale": ws_status.market_data_stale,
            "session_open": "SESSION_OUTSIDE_ENTRY_WINDOW" not in reason_codes,
            "halt_detected": halt_detected,
            "latest_quote_at_utc": latest_quote_at,
            "latest_trade_at_utc": latest_trade_at,
            "quote_snapshot": latest_quote,
            "trade_snapshot": latest_trade,
            "market_status_snapshot": latest_market_status,
        }

    async def start_ws_consumer(
        self,
        *,
        symbols: list[str] | None = None,
        venue: str = "KRX",
        env: Environment | None = None,
        include_fill_notice: bool = True,
        include_market_status: bool = True,
    ) -> BrokerWebSocketSnapshot:
        settings = get_settings()
        effective_symbols = list(
            dict.fromkeys(
                [
                    str(symbol).strip()
                    for symbol in (symbols or settings.kis_live_allowed_symbol_list)
                    if str(symbol).strip()
                ]
            )
        )
        effective_venue = venue.upper()
        effective_env = env or (Environment.VPS if settings.kis_enable_paper else Environment.PROD)
        self._ensure_allowed_env(effective_env)
        if not effective_symbols and not include_fill_notice and not include_market_status:
            raise ValueError("websocket consumer needs at least one symbol or broker notice subscription")

        await self.stop_ws_consumer()
        self.latest_quotes = {}
        self.latest_trades = {}
        self.latest_market_status = {}
        self.recent_order_notices = []
        self._ws_env = effective_env
        self._ws_venue = effective_venue
        self._ws_symbols = effective_symbols
        self._ws_include_fill_notice = include_fill_notice
        self._ws_include_market_status = include_market_status
        self._ws_started_at_utc = datetime.now(UTC)
        self._ws_connected_at_utc = None
        self._ws_last_message_at_utc = None
        self._ws_last_quote_at_utc = None
        self._ws_last_trade_at_utc = None
        self._ws_last_fill_notice_at_utc = None
        self._ws_last_order_notice_at_utc = None
        self._ws_last_market_status_at_utc = None
        self._ws_last_error = None
        self._ws_last_disconnect_reason = None
        self._ws_connect_count = 0
        self._ws_reconnect_count = 0
        self._ws_message_count = 0
        self._ws_control_count = 0
        self._ws_quote_count = 0
        self._ws_trade_count = 0
        self._ws_fill_notice_count = 0
        self._ws_order_notice_count = 0
        self._ws_market_status_count = 0
        self._ws_stop_event = asyncio.Event()
        self._ws_loop_task = asyncio.create_task(self._ws_loop_worker(), name="broker-gateway-ws")
        return self.ws_snapshot()

    async def stop_ws_consumer(self) -> BrokerWebSocketSnapshot:
        self._ws_stop_event.set()
        task = self._ws_loop_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._ws_loop_task = None
        self._ws_connected_at_utc = None
        if self._ws_started_at_utc is not None and self._ws_last_disconnect_reason is None:
            self._ws_last_disconnect_reason = "MANUAL_STOP"
        try:
            await self.adapter.close_ws()
        except Exception as exc:
            self._ws_last_error = str(exc)
        return self.ws_snapshot()

    async def _subscribe_ws_streams(self, *, env: Environment, venue: str, symbols: list[str]) -> None:
        approval = await self.adapter.issue_ws_approval(env)
        self.last_ws_auth_utc = approval.issued_at_utc
        for symbol in symbols:
            await self.adapter.subscribe_quote(symbol, venue=venue, env=env)
            await self.adapter.subscribe_trade(symbol, venue=venue, env=env)
        if self._ws_include_fill_notice:
            await self.adapter.subscribe_fill_notice(env=env)
        if self._ws_include_market_status:
            await self.adapter.subscribe_market_status(venue=venue, env=env)

    async def _ws_loop_worker(self) -> None:
        settings = get_settings()
        try:
            while not self._ws_stop_event.is_set():
                try:
                    await self._subscribe_ws_streams(
                        env=self._ws_env or Environment.PROD,
                        venue=self._ws_venue or "KRX",
                        symbols=self._ws_symbols,
                    )
                    self._ws_connected_at_utc = datetime.now(UTC)
                    self._ws_connect_count += 1
                    self._ws_last_error = None
                    while not self._ws_stop_event.is_set():
                        message = await asyncio.wait_for(
                            self.adapter.recv_ws_message(),
                            timeout=max(settings.kis_ws_heartbeat_timeout_seconds, 0.1),
                        )
                        await self.consume_ws_message(message)
                except asyncio.CancelledError:
                    raise
                except TimeoutError:
                    self._ws_last_error = (
                        f"websocket heartbeat timeout after {settings.kis_ws_heartbeat_timeout_seconds:.1f}s"
                    )
                    self._ws_last_disconnect_reason = "HEARTBEAT_TIMEOUT"
                except Exception as exc:
                    self._ws_last_error = str(exc)
                    self._ws_last_disconnect_reason = str(exc)
                finally:
                    self._ws_connected_at_utc = None
                    try:
                        await self.adapter.close_ws()
                    except Exception:
                        pass
                if self._ws_stop_event.is_set():
                    break
                self._ws_reconnect_count += 1
                try:
                    await asyncio.wait_for(
                        self._ws_stop_event.wait(),
                        timeout=max(settings.kis_ws_reconnect_delay_seconds, 0.1),
                    )
                except TimeoutError:
                    continue
        finally:
            self._ws_loop_task = None

    @staticmethod
    def _split_ws_records(payload: str, count: int) -> list[list[str]]:
        fields = str(payload or "").split("^")
        effective_count = max(int(count or 1), 1)
        if effective_count <= 1 or len(fields) <= effective_count:
            return [fields]
        if len(fields) % effective_count != 0:
            return [fields]
        chunk_size = len(fields) // effective_count
        if chunk_size <= 0:
            return [fields]
        return [
            fields[index:index + chunk_size]
            for index in range(0, len(fields), chunk_size)
        ]

    @staticmethod
    def _quote_payload_from_fields(fields: list[str]) -> dict[str, Any]:
        return {
            "MKSC_SHRN_ISCD": _safe_field(fields, 0),
            "STCK_CNTG_HOUR": _safe_field(fields, 1),
            "ASKP1": _safe_field(fields, 3),
            "BIDP1": _safe_field(fields, 13),
            "ASKP_RSQN1": _safe_field(fields, 23),
            "BIDP_RSQN1": _safe_field(fields, 33),
            "TOTAL_ASKP_RSQN": _safe_field(fields, 43),
            "TOTAL_BIDP_RSQN": _safe_field(fields, 44),
        }

    @staticmethod
    def _trade_payload_from_fields(fields: list[str]) -> dict[str, Any]:
        return {
            "MKSC_SHRN_ISCD": _safe_field(fields, 0),
            "STCK_CNTG_HOUR": _safe_field(fields, 1),
            "STCK_PRPR": _safe_field(fields, 2),
            "ASKP1": _safe_field(fields, 10),
            "BIDP1": _safe_field(fields, 11),
            "CNTG_VOL": _safe_field(fields, 12),
            "ACML_VOL": _safe_field(fields, 13),
            "CTTR": _safe_field(fields, 18),
            "BUSINESS_DATE": _safe_field(fields, 34),
            "HALT_YN": _safe_field(fields, 36),
            "TIME_CLS_CODE": _safe_field(fields, 43),
        }

    @staticmethod
    def _market_status_payload_from_fields(fields: list[str], *, tr_id: str, venue: str) -> dict[str, Any]:
        return {
            "tr_id": tr_id,
            "venue": venue,
            "scope_key": _safe_field(fields, 0) or venue,
            "session_time": _safe_field(fields, 1),
            "status_code": _safe_field(fields, 2) or _safe_field(fields, 3),
            "raw_payload": "^".join(fields),
            "field_count": len(fields),
            "received_at_utc": datetime.now(UTC).isoformat(),
        }

    @staticmethod
    def _notice_payload_from_fields(fields: list[str]) -> dict[str, Any]:
        is_fill = _safe_field(fields, 13) == "2"
        return {
            "account_id": _safe_field(fields, 1),
            "ODER_NO": _safe_field(fields, 2),
            "ORGN_ODNO": _safe_field(fields, 3),
            "SELN_BYOV_CLS": _safe_field(fields, 4),
            "RVSE_CNCL_DVSN_CD": _safe_field(fields, 5),
            "ORD_DVSN": _safe_field(fields, 6),
            "ORD_COND": _safe_field(fields, 7),
            "STCK_SHRN_ISCD": _safe_field(fields, 8),
            "CNTG_QTY": _safe_field(fields, 9) if is_fill else "0",
            "CNTG_UNPR": _safe_field(fields, 10) if is_fill else "0",
            "STCK_CNTG_HOUR": _safe_field(fields, 11),
            "REJECT_YN": _safe_field(fields, 12),
            "NOTICE_KIND": _safe_field(fields, 13),
            "RCPT_YN": _safe_field(fields, 14),
            "BRANCH_NO": _safe_field(fields, 15),
            "ORD_QTY": _safe_field(fields, 16) or _safe_field(fields, 9),
            "ORD_EXG_GB": _safe_field(fields, 19),
            "ORD_UNPR": _safe_field(fields, 25) or _safe_field(fields, 10),
            "raw_payload": "^".join(fields),
            "is_fill": is_fill,
        }

    async def _publish_quote_event(self, event: Any) -> None:
        try:
            await self.event_pipeline.publish_quote_l1(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            await self.event_pipeline.publish_quote_l1(event)

    async def _publish_trade_event(self, event: Any) -> None:
        try:
            await self.event_pipeline.publish_market_tick(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            await self.event_pipeline.publish_market_tick(event)

    def _store_raw_ws_notice(
        self,
        *,
        endpoint_code: str,
        payload_json: dict[str, Any],
        source_object_id: str | None,
        venue_code: str | None = None,
    ) -> dict[str, Any]:
        try:
            receipt = self.raw_event_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code=endpoint_code,
                payload_json=payload_json,
                source_object_id=source_object_id or endpoint_code,
                venue_code=venue_code,
            )
        except Exception:
            fallback_raw_service = RawEventService()
            receipt = fallback_raw_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code=endpoint_code,
                payload_json=payload_json,
                source_object_id=source_object_id or endpoint_code,
                venue_code=venue_code,
            )
        return {
            "checksum": receipt.checksum,
            "stored_at_utc": receipt.stored_at_utc.isoformat(),
            "raw_ref": f"raw:{receipt.checksum[:12]}",
        }

    def _find_internal_order_id_by_broker_order_no(self, *order_numbers: Any) -> str | None:
        normalized_order_numbers = {
            normalized
            for normalized in (_normalize_order_no(item) for item in order_numbers)
            if normalized is not None
        }
        if not normalized_order_numbers:
            return None
        for ticket in self.list_order_tickets(limit=500):
            if _normalize_order_no(ticket.get("broker_order_no")) in normalized_order_numbers:
                return str(ticket["internal_order_id"])
        return None

    def _record_recent_order_notice(self, notice: dict[str, Any]) -> None:
        notice_record = {
            **notice,
            "received_at_utc": datetime.now(UTC).isoformat(),
        }
        self.recent_order_notices.insert(0, notice_record)
        self.recent_order_notices = self.recent_order_notices[:20]

    def _resolve_order_notice_state(self, notice: dict[str, Any], existing: dict[str, Any]) -> str:
        existing_state = str(existing.get("order_state_code") or "")
        if _is_truthy_flag(notice.get("REJECT_YN")):
            return "REJECTED"
        if _is_truthy_flag(notice.get("RCPT_YN")) and existing_state not in {"FILLED", "PARTIALLY_FILLED"}:
            return "RECEIVED"
        return existing_state or "ORDER_NOTICE"

    def _record_order_notice(self, notice: dict[str, Any]) -> dict[str, Any]:
        internal_order_id = str(notice["internal_order_id"])
        existing = self.get_order_ticket(internal_order_id) or {}
        order_qty = _safe_int(notice.get("ORD_QTY")) or (
            _safe_int(existing.get("working_qty")) + _safe_int(existing.get("filled_qty"))
        )
        filled_qty = _safe_int(existing.get("filled_qty"))
        working_qty = max(order_qty - filled_qty, 0) if order_qty > 0 else _safe_int(existing.get("working_qty"))
        ticket = {
            "internal_order_id": internal_order_id,
            "client_order_id": existing.get("client_order_id") or internal_order_id,
            "broker_order_no": notice.get("ODER_NO") or existing.get("broker_order_no"),
            "account_uid": notice.get("account_id") or existing.get("account_uid") or "default",
            "instrument_id": notice.get("STCK_SHRN_ISCD") or existing.get("instrument_id"),
            "side_code": "BUY" if notice.get("SELN_BYOV_CLS") == "02" else "SELL",
            "order_state_code": self._resolve_order_notice_state(notice, existing),
            "order_type_code": existing.get("order_type_code", "LIMIT"),
            "tif_code": existing.get("tif_code", "DAY"),
            "working_qty": working_qty,
            "filled_qty": filled_qty,
            "avg_fill_price": existing.get("avg_fill_price"),
            "last_event_at_utc": map_order_ack(
                payload={"output": {"ODNO": notice.get("ODER_NO"), "ORD_TMD": notice.get("STCK_CNTG_HOUR")}},
                internal_order_id=internal_order_id,
                client_order_id=existing.get("client_order_id") or internal_order_id,
            ).ack_ts_utc.isoformat(),
            "payload_json": {**existing.get("payload_json", {}), **notice},
        }
        return self._record_order_ticket(ticket)

    async def consume_ws_message(self, message: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        self._ws_message_count += 1
        self._ws_last_message_at_utc = now

        message_type = str(message.get("type") or "").strip().lower()
        if message_type == "control":
            self._ws_control_count += 1
            payload = message.get("payload") or {}
            if payload.get("is_pingpong"):
                try:
                    await self.adapter.send_ws_pong("PINGPONG")
                except Exception:
                    pass
            if str(payload.get("rt_cd") or "").strip() == "1":
                self._ws_last_error = (
                    f"{payload.get('tr_id') or 'WS'}:{payload.get('msg1') or payload.get('msg_cd') or 'control-error'}"
                )
            return {"handled": "control", "payload": payload}

        if message_type != "stream":
            return {"handled": "ignored", "payload": message}

        tr_id = str(message.get("tr_id") or "").strip()
        record_count = max(_safe_int(message.get("count")), 1)
        payload_text = str(message.get("payload") or "")

        if tr_id in _KIS_WS_QUOTE_TR_IDS:
            venue = _KIS_WS_QUOTE_TR_IDS[tr_id]
            processed = 0
            for fields in self._split_ws_records(payload_text, record_count):
                quote = map_quote_l1(
                    payload=self._quote_payload_from_fields(fields),
                    instrument_id=_safe_field(fields, 0),
                    venue=venue,
                    raw_ref=f"ws:{tr_id}",
                )
                self.latest_quotes[quote.instrument_id] = quote.model_dump(mode="json")
                self._ws_quote_count += 1
                self._ws_last_quote_at_utc = now
                processed += 1
                await self._publish_quote_event(quote)
            return {"handled": "quote", "count": processed}

        if tr_id in _KIS_WS_TRADE_TR_IDS:
            venue = _KIS_WS_TRADE_TR_IDS[tr_id]
            processed = 0
            for fields in self._split_ws_records(payload_text, record_count):
                trade = map_trade_tick(
                    payload=self._trade_payload_from_fields(fields),
                    instrument_id=_safe_field(fields, 0),
                    venue=venue,
                    raw_ref=f"ws:{tr_id}",
                )
                trade_snapshot = trade.model_dump(mode="json")
                trade_snapshot.update(
                    {
                        "halt_yn": _safe_field(fields, 36),
                        "time_cls_code": _safe_field(fields, 43),
                        "best_ask_px": _safe_int(_safe_field(fields, 10)),
                        "best_bid_px": _safe_int(_safe_field(fields, 11)),
                    }
                )
                self.latest_trades[trade.instrument_id] = trade_snapshot
                self._ws_trade_count += 1
                self._ws_last_trade_at_utc = now
                processed += 1
                await self._publish_trade_event(trade)
            return {"handled": "trade", "count": processed}

        if tr_id in _KIS_WS_MARKET_STATUS_TR_IDS:
            venue = _KIS_WS_MARKET_STATUS_TR_IDS[tr_id]
            processed = 0
            for fields in self._split_ws_records(payload_text, record_count):
                payload = self._market_status_payload_from_fields(fields, tr_id=tr_id, venue=venue)
                market_status_key = f"{venue}:{payload['scope_key']}"
                self.latest_market_status[market_status_key] = payload
                self._ws_market_status_count += 1
                self._ws_last_market_status_at_utc = now
                processed += 1
            return {"handled": "market-status", "count": processed}

        if tr_id in _KIS_WS_FILL_NOTICE_TR_IDS:
            fill_notice_count = 0
            order_notice_count = 0
            for fields in self._split_ws_records(payload_text, record_count):
                notice = self._notice_payload_from_fields(fields)
                if not notice["is_fill"]:
                    notice.update(
                        self._store_raw_ws_notice(
                            endpoint_code="order-notice",
                            payload_json=notice,
                            source_object_id=notice.get("ODER_NO") or notice.get("ORGN_ODNO") or "order-notice",
                            venue_code=notice.get("ORD_EXG_GB"),
                        )
                    )
                internal_order_id = self._find_internal_order_id_by_broker_order_no(
                    notice.get("ODER_NO"),
                    notice.get("ORGN_ODNO"),
                )
                if internal_order_id is None:
                    scope_id = (
                        _normalize_order_no(notice.get("ODER_NO"))
                        or _normalize_order_no(notice.get("ORGN_ODNO"))
                        or str(uuid4())
                    )
                    self._upsert_reconciliation_break(
                        scope_type="BROKER_NOTICE",
                        scope_id=scope_id,
                        severity_code="HIGH",
                        expected_payload={},
                        actual_payload=notice,
                        notes="Could not resolve websocket broker notice to an internal order.",
                    )
                    self._ws_last_error = (
                        "unmatched websocket broker notice for broker_order_no="
                        f"{notice.get('ODER_NO') or notice.get('ORGN_ODNO') or 'unknown'}"
                    )
                    continue

                notice["internal_order_id"] = internal_order_id
                notice["account_id"] = (
                    notice.get("account_id")
                    or (self.get_order_ticket(internal_order_id) or {}).get("account_uid")
                    or "default"
                )
                if notice["is_fill"]:
                    await self.normalize_fill_notice(notice)
                    self._ws_fill_notice_count += 1
                    self._ws_last_fill_notice_at_utc = now
                    fill_notice_count += 1
                else:
                    self._record_order_notice(notice)
                    self._record_recent_order_notice(notice)
                    self._ws_order_notice_count += 1
                    self._ws_last_order_notice_at_utc = now
                    order_notice_count += 1
            return {
                "handled": "broker-notice",
                "fill_notice_count": fill_notice_count,
                "order_notice_count": order_notice_count,
            }

        return {"handled": "unmapped-stream", "tr_id": tr_id}

    async def refresh_live_risk_state(self) -> dict[str, Any]:
        settings = get_settings()
        payload = await self.adapter.query_balance(self._balance_query_payload())
        current_total, baseline_total, daily_loss_pct = self._parse_live_risk_metrics(payload)
        self.last_total_equity_krw = current_total
        self.baseline_total_equity_krw = baseline_total
        self.daily_loss_pct = daily_loss_pct

        pause_reasons: list[str] = []
        if settings.kis_live_min_total_equity_krw > 0 and current_total <= settings.kis_live_min_total_equity_krw:
            pause_reasons.append(f"TOTAL_EQUITY_AT_OR_BELOW_{settings.kis_live_min_total_equity_krw}")
        if settings.kis_live_daily_loss_limit_pct > 0 and daily_loss_pct >= settings.kis_live_daily_loss_limit_pct:
            pause_reasons.append(f"DAILY_LOSS_LIMIT_{settings.kis_live_daily_loss_limit_pct:.2f}_PCT_BREACHED")

        self.entry_paused = bool(pause_reasons)
        self.live_pause_reason = ",".join(pause_reasons) if pause_reasons else None
        if self.entry_paused:
            self.live_trading_armed = False

        return {
            "current_total_equity_krw": current_total,
            "baseline_total_equity_krw": baseline_total,
            "daily_loss_pct": round(daily_loss_pct, 4),
            "entry_paused": self.entry_paused,
            "live_pause_reason": self.live_pause_reason,
        }

    async def _ensure_common_stock_symbol(self, symbol: str) -> None:
        settings = get_settings()
        if not settings.kis_live_common_stock_only:
            return
        try:
            is_common = await self.common_stock_universe.is_common_stock(symbol)
        except CommonStockUniverseError as exc:
            raise LiveTradingGuardError(f"failed to verify common stock universe: {exc}") from exc
        if not is_common:
            raise LiveTradingGuardError(f"symbol {symbol} is not recognized as a regular listed stock")

    async def _ensure_live_order_allowed(self, payload: dict[str, Any]) -> None:
        settings = get_settings()
        if not settings.kis_live_trading_enabled:
            raise LiveTradingGuardError("live trading is disabled in configuration")
        risk_state = await self.refresh_live_risk_state()
        if risk_state["entry_paused"]:
            raise LiveTradingGuardError(f"live entry paused: {risk_state['live_pause_reason']}")
        if settings.kis_live_require_arm and not self.live_trading_armed:
            raise LiveTradingGuardError("live trading is not armed")
        self._ensure_expected_account(payload)

        symbol = str(payload.get("pdno", "")).strip()
        allowed_symbols = settings.kis_live_allowed_symbol_list
        if settings.trading_micro_test_mode_enabled and settings.trading_micro_test_require_allowed_symbols and not allowed_symbols:
            raise LiveTradingGuardError("micro test mode requires KIS_LIVE_ALLOWED_SYMBOLS")
        if allowed_symbols and symbol and symbol not in allowed_symbols:
            raise LiveTradingGuardError(f"symbol {symbol} is not in KIS_LIVE_ALLOWED_SYMBOLS")
        if symbol:
            await self._ensure_common_stock_symbol(symbol)

        qty = int(payload.get("ord_qty", 0))
        price = int(float(payload.get("ord_unpr", 0)))
        order_value = qty * price
        if (
            settings.trading_micro_test_mode_enabled
            and settings.trading_micro_test_max_order_value_krw > 0
            and order_value > settings.trading_micro_test_max_order_value_krw
        ):
            raise LiveTradingGuardError(
                "order value "
                f"{order_value} exceeds TRADING_MICRO_TEST_MAX_ORDER_VALUE_KRW="
                f"{settings.trading_micro_test_max_order_value_krw}"
            )
        if settings.kis_live_max_order_value_krw > 0 and order_value > settings.kis_live_max_order_value_krw:
            raise LiveTradingGuardError(
                f"order value {order_value} exceeds KIS_LIVE_MAX_ORDER_VALUE_KRW={settings.kis_live_max_order_value_krw}"
            )

    async def arm_live_trading(self, armed_by: str, reason: str | None = None) -> dict[str, Any]:
        settings = get_settings()
        if not settings.kis_live_trading_enabled:
            raise LiveTradingGuardError("set KIS_LIVE_TRADING_ENABLED=true before arming live trading")
        await self.refresh_live_risk_state()
        if self.entry_paused and self.live_pause_reason:
            raise LiveTradingGuardError(f"cannot arm while entry is paused: {self.live_pause_reason}")
        self.live_trading_armed = True
        self.live_trading_armed_by = armed_by if reason is None else f"{armed_by}:{reason}"
        self.live_trading_armed_at_utc = datetime.now(UTC)
        return {
            "live_trading_enabled": settings.kis_live_trading_enabled,
            "live_trading_armed": self.live_trading_armed,
            "live_trading_armed_by": self.live_trading_armed_by,
            "live_trading_armed_at_utc": self.live_trading_armed_at_utc,
        }

    def disarm_live_trading(self, disarmed_by: str, reason: str | None = None) -> dict[str, Any]:
        self.live_trading_armed = False
        self.live_trading_armed_by = disarmed_by if reason is None else f"{disarmed_by}:{reason}"
        self.live_trading_armed_at_utc = datetime.now(UTC)
        return {
            "live_trading_enabled": get_settings().kis_live_trading_enabled,
            "live_trading_armed": self.live_trading_armed,
            "live_trading_armed_by": self.live_trading_armed_by,
            "live_trading_armed_at_utc": self.live_trading_armed_at_utc,
        }

    async def issue_rest_token(self, env: Environment) -> dict[str, Any]:
        self._ensure_allowed_env(env)
        token = await self.adapter.issue_rest_token(env)
        self.last_rest_auth_utc = datetime.now(UTC)
        return {
            "access_token_prefix": token.access_token[:8],
            "expires_at_utc": token.expires_at_utc,
            "token_type": token.token_type,
        }

    async def issue_ws_approval(self, env: Environment) -> dict[str, Any]:
        self._ensure_allowed_env(env)
        approval = await self.adapter.issue_ws_approval(env)
        self.last_ws_auth_utc = approval.issued_at_utc
        return {
            "approval_key_prefix": approval.approval_key[:8],
            "issued_at_utc": approval.issued_at_utc,
        }

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        if env == Environment.PROD:
            await self._ensure_live_order_allowed(payload)
        return await self.adapter.submit_cash_order(payload)

    async def submit_cancel_replace(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        rvse_cncl_dvsn_cd = str(payload.get("rvse_cncl_dvsn_cd", ""))
        if env == Environment.PROD and rvse_cncl_dvsn_cd != "02":
            await self._ensure_live_order_allowed(payload)
        return await self.adapter.submit_cancel_replace(payload)

    async def query_psbl_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        return await self.adapter.query_psbl_order(payload)

    async def query_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        return await self.adapter.query_price(payload)

    async def query_asking_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        return await self.adapter.query_asking_price(payload)

    async def query_balance(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        response = await self.adapter.query_balance(payload)
        if env == Environment.PROD:
            try:
                current_total, baseline_total, daily_loss_pct = self._parse_live_risk_metrics(response)
                self.last_total_equity_krw = current_total
                self.baseline_total_equity_krw = baseline_total
                self.daily_loss_pct = daily_loss_pct
                settings = get_settings()
                self.entry_paused = (
                    (settings.kis_live_min_total_equity_krw > 0 and current_total <= settings.kis_live_min_total_equity_krw)
                    or (
                        settings.kis_live_daily_loss_limit_pct > 0
                        and daily_loss_pct >= settings.kis_live_daily_loss_limit_pct
                    )
                )
                if self.entry_paused:
                    reasons = []
                    if current_total <= settings.kis_live_min_total_equity_krw:
                        reasons.append(f"TOTAL_EQUITY_AT_OR_BELOW_{settings.kis_live_min_total_equity_krw}")
                    if daily_loss_pct >= settings.kis_live_daily_loss_limit_pct:
                        reasons.append(
                            f"DAILY_LOSS_LIMIT_{settings.kis_live_daily_loss_limit_pct:.2f}_PCT_BREACHED"
                        )
                    self.live_pause_reason = ",".join(reasons)
                    self.live_trading_armed = False
                else:
                    self.live_pause_reason = None
            except Exception:
                pass
        return response

    async def query_daily_ccld(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        return await self.adapter.query_daily_ccld(payload)

    async def normalize_order_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            receipt = self.raw_event_service.store(
                source_system_code="KIS",
                channel_code="REST",
                endpoint_code="order-cash",
                payload_json=payload,
                source_object_id=str(payload.get("output", {}).get("ODNO") or payload.get("msg_cd") or "order-ack"),
                venue_code=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
            )
        except Exception:
            fallback_raw_service = RawEventService()
            receipt = fallback_raw_service.store(
                source_system_code="KIS",
                channel_code="REST",
                endpoint_code="order-cash",
                payload_json=payload,
                source_object_id=str(payload.get("output", {}).get("ODNO") or payload.get("msg_cd") or "order-ack"),
                venue_code=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
            )
        event = map_order_ack(
            payload=payload,
            internal_order_id=payload["internal_order_id"],
            client_order_id=payload["client_order_id"],
            raw_ref=f"raw:{receipt.checksum[:12]}",
            venue=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
        )
        try:
            envelope = await self.event_pipeline.publish_order_ack(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            envelope = await self.event_pipeline.publish_order_ack(event)
        persistence: dict[str, Any] = {"repository": None, "fallback": "runtime-memory"}
        try:
            repository_result = self.repository.store_order_ack(event, payload)
            persistence["repository"] = {"order_event_pk": repository_result.primary_key}
        except Exception as exc:
            persistence["repository_error"] = str(exc)
        order_ticket = self._record_order_ack(event, payload)
        return {
            "raw_receipt": {
                "checksum": receipt.checksum,
                "stored_at_utc": receipt.stored_at_utc,
            },
            "event": event,
            "order_ticket": order_ticket,
            "persistence": persistence,
            "envelope": envelope.envelope,
            "message_type": MessageType.EVENT,
        }

    async def normalize_fill_notice(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            receipt = self.raw_event_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code="fill-notice",
                payload_json=payload,
                source_object_id=str(payload.get("ODER_NO") or payload.get("oder_no") or "fill-notice"),
            )
        except Exception:
            fallback_raw_service = RawEventService()
            receipt = fallback_raw_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code="fill-notice",
                payload_json=payload,
                source_object_id=str(payload.get("ODER_NO") or payload.get("oder_no") or "fill-notice"),
            )
        event = map_fill_notice(
            payload=payload,
            internal_order_id=payload["internal_order_id"],
            account_id=payload["account_id"],
            raw_ref=f"raw:{receipt.checksum[:12]}",
        )
        try:
            envelope = await self.event_pipeline.publish_fill(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            envelope = await self.event_pipeline.publish_fill(event)
        persistence: dict[str, Any] = {"repository": None, "fallback": "runtime-memory"}
        try:
            repository_result = self.repository.store_fill(event, payload)
            persistence["repository"] = {"execution_fill_pk": repository_result.primary_key}
        except Exception as exc:
            persistence["repository_error"] = str(exc)
        fill_record = self._record_fill(event, payload)
        return {
            "raw_receipt": {
                "checksum": receipt.checksum,
                "stored_at_utc": receipt.stored_at_utc,
            },
            "event": event,
            "fill_record": fill_record,
            "persistence": persistence,
            "envelope": envelope.envelope,
        }

    async def close(self) -> None:
        await self.stop_ws_consumer()
        await self.adapter.close()
        await self.common_stock_universe.close()
        if self.event_pipeline.event_bus is not None:
            await self.event_pipeline.event_bus.close()


runtime = BrokerGatewayRuntime()
