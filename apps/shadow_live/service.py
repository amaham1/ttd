from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from apps.data_ingest.service import data_ingest_service
from apps.ops_api.store import store
from apps.trading_core.service import trading_core_service
from libs.config.settings import get_settings
from libs.contracts.messages import (
    ExecutionReadiness,
    MicroTestCandidatePreview,
    MicroTestCandidatePreviewResponse,
    OrderSubmitCommand,
    PostTradeVerificationReport,
    ShadowLiveRunAuditRecord,
    TradeCandidate,
)
from libs.domain.enums import Environment, OrderSide


@dataclass(slots=True)
class ShadowLiveSnapshot:
    mode: str
    candidate_count: int
    fill_match_rate: float
    last_sync_utc: datetime
    last_candidate_id: str | None = None
    last_intent_id: str | None = None
    last_internal_order_id: str | None = None
    last_broker_order_no: str | None = None
    last_execution_status: str | None = None
    last_execution_reason: str | None = None
    no_trade_count: int = 0
    blocked_count: int = 0
    submitted_count: int = 0
    stale_data_incident_count: int = 0
    duplicate_order_incident_count: int = 0
    selector_mismatch_incident_count: int = 0


@dataclass(slots=True)
class ExecutionPlan:
    candidate_id: str
    intent_id: str | None
    planned_order: dict[str, Any] | None
    selected_price_krw: int | None
    quote_basis: str | None
    price_source_value: int | None
    execute_live: bool
    persisted: bool
    selection_reason: str | None = None
    matched_rule_id: str | None = None
    source_report_name: str | None = None
    source_receipt_no: str | None = None
    price_reason: str | None = None
    quantity_reason: str | None = None
    risk_reason_summary: str | None = None
    risk_state: dict[str, Any] | None = None
    broker_response: dict[str, Any] | None = None
    normalized_ack: dict[str, Any] | None = None
    promotion_readiness: dict[str, Any] | None = None
    persistence_error: str | None = None
    status: str = "PLANNED"
    reason: str | None = None


@dataclass(slots=True)
class ShadowLoopSnapshot:
    running: bool
    execute_live: bool
    persist: bool
    interval_seconds: int
    run_count: int
    desired_running: bool = False
    owner_id: str | None = None
    lease_expires_at_utc: datetime | None = None
    heartbeat_at_utc: datetime | None = None
    lease_stale: bool = False
    restored_from_durable: bool = False
    last_started_at_utc: datetime | None = None
    last_finished_at_utc: datetime | None = None
    last_result_status: str | None = None
    last_error: str | None = None


def _venue_code_to_market_div(venue_hint: str | None) -> str:
    venue = (venue_hint or "KRX").upper()
    if venue == "NXT":
        return "NX"
    if venue in {"UN", "SOR"}:
        return "UN"
    return "J"


def _pick_limit_price(
    side: OrderSide,
    asking_price_payload: dict[str, Any],
    current_price_payload: dict[str, Any],
) -> tuple[int, str]:
    output1 = asking_price_payload.get("output1") or {}
    current_output = current_price_payload.get("output") or {}
    bid = output1.get("bidp1") or output1.get("bidp_rsqn1") or output1.get("stck_bidp")
    ask = output1.get("askp1") or output1.get("askp_rsqn1") or output1.get("stck_askp")
    last_price = current_output.get("stck_prpr") or current_output.get("stck_sdpr") or current_output.get("stck_clpr")

    if side == OrderSide.BUY and ask:
        return int(float(ask)), "BEST_ASK"
    if side == OrderSide.SELL and bid:
        return int(float(bid)), "BEST_BID"
    if last_price:
        return int(float(last_price)), "LAST_PRICE"
    raise ValueError("unable to determine execution price from KIS quote payload")


def _build_price_reason(quote_basis: str, selected_price_krw: int) -> str:
    if quote_basis in {"BEST_ASK", "WS_BEST_ASK"}:
        return f"매수 지정가 기준으로 한국투자증권 최우선 매도호가 {selected_price_krw:,}원을 사용했습니다."
    if quote_basis in {"BEST_BID", "WS_BEST_BID"}:
        return f"매도 지정가 기준으로 한국투자증권 최우선 매수호가 {selected_price_krw:,}원을 사용했습니다."
    return f"대체 경로로 최근 체결가 {selected_price_krw:,}원을 사용했습니다."


def _build_quantity_reason(
    target_notional_krw: int,
    selected_price_krw: int,
    qty: int,
    *,
    sizing_reason: str | None = None,
) -> str:
    detail = (
        f"목표 투자금 {target_notional_krw:,}원을 선택 가격 {selected_price_krw:,}원에 적용해 "
        f"{qty:,}주를 산출했습니다."
    )
    if not sizing_reason:
        return detail
    return f"{sizing_reason}. {detail}"


def _build_risk_reason_summary(
    *,
    decision_reason_codes: list[str],
    live_risk_state: dict[str, Any] | None,
) -> str:
    if decision_reason_codes:
        return f"리스크 게이트 사유: {', '.join(decision_reason_codes)}"
    if live_risk_state and live_risk_state.get("entry_paused"):
        return f"실거래 진입 일시 중지: {live_risk_state.get('live_pause_reason')}"
    return "모든 리스크 게이트를 통과했습니다."


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _calculate_spread_bps(best_bid_krw: int | None, best_ask_krw: int | None) -> float | None:
    if best_bid_krw is None or best_ask_krw is None:
        return None
    if best_bid_krw <= 0 or best_ask_krw <= 0:
        return None
    mid = (best_bid_krw + best_ask_krw) / 2.0
    if mid <= 0:
        return None
    return round(((best_ask_krw - best_bid_krw) / mid) * 10000.0, 2)


def _build_quote_context(
    side: OrderSide,
    asking_price_payload: dict[str, Any],
    current_price_payload: dict[str, Any],
) -> dict[str, Any]:
    output1 = asking_price_payload.get("output1") or {}
    current_output = current_price_payload.get("output") or {}
    best_bid_krw = _safe_int(_first_present(output1, "bidp1", "bidp_rsqn1", "stck_bidp"))
    best_ask_krw = _safe_int(_first_present(output1, "askp1", "askp_rsqn1", "stck_askp"))
    last_price_krw = _safe_int(_first_present(current_output, "stck_prpr", "stck_sdpr", "stck_clpr"))
    selected_price_krw: int | None = None
    quote_basis: str | None = None
    try:
        selected_price_krw, quote_basis = _pick_limit_price(
            side,
            asking_price_payload,
            current_price_payload,
        )
    except ValueError:
        selected_price_krw = None
        quote_basis = None
    return {
        "best_bid_krw": best_bid_krw,
        "best_ask_krw": best_ask_krw,
        "last_price_krw": last_price_krw,
        "selected_price_krw": selected_price_krw,
        "quote_basis": quote_basis,
        "spread_bps": _calculate_spread_bps(best_bid_krw, best_ask_krw),
    }


def _unique_reason_codes(reason_codes: list[str]) -> list[str]:
    return list(dict.fromkeys(code for code in reason_codes if code))


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


def _parse_snapshot_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _build_quote_context_from_ws_guard(
    side: OrderSide,
    market_guard: dict[str, Any],
) -> dict[str, Any]:
    quote_snapshot = market_guard.get("quote_snapshot") or {}
    trade_snapshot = market_guard.get("trade_snapshot") or {}
    best_bid_krw = _safe_int(quote_snapshot.get("best_bid_px"))
    best_ask_krw = _safe_int(quote_snapshot.get("best_ask_px"))
    last_price_krw = _safe_int(trade_snapshot.get("last_price"))
    selected_price_krw: int | None = None
    quote_basis: str | None = None
    if side == OrderSide.BUY and best_ask_krw is not None:
        selected_price_krw = best_ask_krw
        quote_basis = "WS_BEST_ASK"
    elif side == OrderSide.SELL and best_bid_krw is not None:
        selected_price_krw = best_bid_krw
        quote_basis = "WS_BEST_BID"
    elif last_price_krw is not None:
        selected_price_krw = last_price_krw
        quote_basis = "WS_LAST_PRICE"
    return {
        "best_bid_krw": best_bid_krw,
        "best_ask_krw": best_ask_krw,
        "last_price_krw": last_price_krw,
        "selected_price_krw": selected_price_krw,
        "quote_basis": quote_basis,
        "spread_bps": _calculate_spread_bps(best_bid_krw, best_ask_krw),
    }


class BrokerGatewayClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = httpx.AsyncClient(base_url=self.settings.broker_gateway_url, timeout=30.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = await self.client.get(path, params=params or {})
        if response.is_error:
            parsed_payload: Any | None = None
            detail = response.text.strip() or response.reason_phrase
            try:
                parsed_payload = response.json()
                if isinstance(parsed_payload, dict):
                    detail = str(parsed_payload.get("detail") or parsed_payload)
                elif parsed_payload is not None:
                    detail = str(parsed_payload)
            except Exception:
                parsed_payload = None
            raise BrokerGatewayRequestError(
                status_code=response.status_code,
                detail=detail or "broker-gateway request failed",
                payload=parsed_payload,
            )
        return response.json()

    async def _post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self.client.post(path, json=payload or {})
        if response.is_error:
            parsed_payload: Any | None = None
            detail = response.text.strip() or response.reason_phrase
            try:
                parsed_payload = response.json()
                if isinstance(parsed_payload, dict):
                    detail = str(parsed_payload.get("detail") or parsed_payload)
                elif parsed_payload is not None:
                    detail = str(parsed_payload)
            except Exception:
                parsed_payload = None
            raise BrokerGatewayRequestError(
                status_code=response.status_code,
                detail=detail or "broker-gateway request failed",
                payload=parsed_payload,
            )
        return response.json()

    async def refresh_live_risk_state(self) -> dict[str, Any]:
        return await self._post("/live/risk-check")

    async def query_price(self, *, symbol: str, market_div: str) -> dict[str, Any]:
        return await self._post(
            "/query/price",
            {
                "payload": {
                    "env": Environment.PROD.value,
                    "fid_cond_mrkt_div_code": market_div,
                    "fid_input_iscd": symbol,
                }
            },
        )

    async def query_asking_price(self, *, symbol: str, market_div: str) -> dict[str, Any]:
        return await self._post(
            "/query/asking-price",
            {
                "payload": {
                    "env": Environment.PROD.value,
                    "fid_cond_mrkt_div_code": market_div,
                    "fid_input_iscd": symbol,
                }
            },
        )

    async def query_balance(self) -> dict[str, Any]:
        return await self._post(
            "/query/balance",
            {
                "payload": {
                    "env": Environment.PROD.value,
                    "cano": self.settings.kis_account_no,
                    "acnt_prdt_cd": self.settings.kis_account_product_code,
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
            },
        )

    async def query_daily_ccld(
        self,
        *,
        symbol: str | None,
        broker_order_no: str | None,
        venue_code: str = "KRX",
        trading_date: date | None = None,
    ) -> dict[str, Any]:
        query_date = trading_date or _today_for_timezone(self.settings.app_timezone)
        query_date_text = query_date.strftime("%Y%m%d")
        return await self._post(
            "/query/daily-ccld",
            {
                "payload": {
                    "env": Environment.PROD.value,
                    "pd_dv": "inner",
                    "cano": self.settings.kis_account_no,
                    "acnt_prdt_cd": self.settings.kis_account_product_code,
                    "inqr_strt_dt": query_date_text,
                    "inqr_end_dt": query_date_text,
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
                    "excg_id_dvsn_cd": venue_code,
                }
            },
        )

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/order/cash", {"payload": payload})

    async def normalize_order_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/normalize/order-ack", {"payload": payload})

    async def ws_status(self) -> dict[str, Any]:
        return await self._get("/ws/status")

    async def ws_quotes(self, *, symbol: str) -> list[dict[str, Any]]:
        payload = await self._get("/ws/quotes", {"symbol": symbol, "limit": 1})
        return payload if isinstance(payload, list) else []

    async def ws_trades(self, *, symbol: str) -> list[dict[str, Any]]:
        payload = await self._get("/ws/trades", {"symbol": symbol, "limit": 1})
        return payload if isinstance(payload, list) else []

    async def market_guard(self, *, symbol: str, venue: str = "KRX") -> dict[str, Any]:
        return await self._get(f"/market/guard/{symbol}", {"venue": venue})

    async def start_ws(
        self,
        *,
        symbols: list[str] | None = None,
        venue: str = "KRX",
        include_fill_notice: bool = True,
        include_market_status: bool = True,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "venue": venue,
            "include_fill_notice": include_fill_notice,
            "include_market_status": include_market_status,
        }
        if symbols is not None:
            payload["symbols"] = symbols
        return await self._post("/ws/start", payload)


class BrokerGatewayRequestError(RuntimeError):
    def __init__(
        self,
        *,
        status_code: int,
        detail: str,
        payload: Any | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload


def _build_kis_order_payload(command: OrderSubmitCommand) -> dict[str, Any]:
    settings = get_settings()
    order_dvsn = "00" if command.order_type.upper() == "LIMIT" else "01"
    price = command.price or 0
    return {
        "env": Environment.PROD.value,
        "ord_dv": "buy" if command.side == OrderSide.BUY else "sell",
        "cano": settings.kis_account_no,
        "acnt_prdt_cd": settings.kis_account_product_code,
        "pdno": command.instrument_id,
        "ord_dvsn": order_dvsn,
        "ord_qty": str(command.qty),
        "ord_unpr": str(price),
        "excg_id_dvsn_cd": (command.venue_hint or "KRX").upper(),
        "internal_order_id": command.internal_order_id,
        "client_order_id": command.client_order_id,
        "account_id": command.account_id,
        "route_policy": command.route_policy,
        "urgency": command.urgency,
        "submitted_by_strategy": command.submitted_by_strategy,
        "correlation_id": command.correlation_id,
    }


class ShadowLiveService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.loop_id = "shadow-live-main"
        self.loop_owner_id = f"shadow-live-{uuid4().hex[:8]}"
        self.loop_restored_from_durable = False
        self.gateway_client = BrokerGatewayClient()
        self.ops_client = httpx.AsyncClient(base_url=self.settings.ops_api_url, timeout=15.0)
        self.selector_client = httpx.AsyncClient(base_url=self.settings.selector_engine_url, timeout=15.0)
        self.portfolio_client = httpx.AsyncClient(
            base_url=self.settings.portfolio_engine_url,
            timeout=20.0,
        )
        self.last_sync_utc = datetime.now(UTC) - timedelta(seconds=12)
        self.last_candidate_id: str | None = None
        self.last_intent_id: str | None = None
        self.last_internal_order_id: str | None = None
        self.last_broker_order_no: str | None = None
        self.last_execution_status: str | None = None
        self.last_execution_reason: str | None = None
        self.loop_interval_seconds = 60
        self.loop_execute_live = False
        self.loop_persist = True
        self.loop_run_count = 0
        self.loop_last_started_at_utc: datetime | None = None
        self.loop_last_finished_at_utc: datetime | None = None
        self.loop_last_result_status: str | None = None
        self.loop_last_error: str | None = None
        self.last_execution_plan: ExecutionPlan | None = None
        self._loop_task: asyncio.Task[None] | None = None
        self._loop_stop_event = asyncio.Event()
        self._cooldown_until_by_key: dict[str, datetime] = {}
        self.no_trade_count = 0
        self.blocked_count = 0
        self.submitted_count = 0
        self.stale_data_incident_count = 0
        self.duplicate_order_incident_count = 0
        self.selector_mismatch_incident_count = 0
        self.latest_portfolio_selected_count = 0

    async def close(self) -> None:
        durable_state = store.get_loop_state(self.loop_id)
        await self._stop_loop_internal(
            desired_running=bool(durable_state.desired_running) if durable_state is not None else False,
            reason_code="PROCESS_SHUTDOWN",
        )
        await self.gateway_client.close()
        await self.ops_client.aclose()
        await self.selector_client.aclose()
        await self.portfolio_client.aclose()

    def _cancel_loop_task(self) -> None:
        task = self._loop_task
        if task is None or task.done():
            return
        self._loop_stop_event.set()
        try:
            task.cancel()
        except RuntimeError:
            return

    def reset_runtime(self) -> None:
        self._cancel_loop_task()
        self.last_sync_utc = datetime.now(UTC) - timedelta(seconds=12)
        self.last_candidate_id = None
        self.last_intent_id = None
        self.last_internal_order_id = None
        self.last_broker_order_no = None
        self.last_execution_status = None
        self.last_execution_reason = None
        self.loop_owner_id = f"shadow-live-{uuid4().hex[:8]}"
        self.loop_restored_from_durable = False
        self.loop_interval_seconds = 60
        self.loop_execute_live = False
        self.loop_persist = True
        self.loop_run_count = 0
        self.loop_last_started_at_utc = None
        self.loop_last_finished_at_utc = None
        self.loop_last_result_status = None
        self.loop_last_error = None
        self.last_execution_plan = None
        self._loop_task = None
        self._loop_stop_event = asyncio.Event()
        self._cooldown_until_by_key = {}
        self.no_trade_count = 0
        self.blocked_count = 0
        self.submitted_count = 0
        self.stale_data_incident_count = 0
        self.duplicate_order_incident_count = 0
        self.selector_mismatch_incident_count = 0
        self.latest_portfolio_selected_count = 0

    def snapshot(self) -> ShadowLiveSnapshot:
        try:
            persistent_summary = data_ingest_service.shadow_live_metrics_summary()
        except Exception:
            persistent_summary = None
        return ShadowLiveSnapshot(
            mode="ACTIVE",
            candidate_count=self.latest_portfolio_selected_count,
            fill_match_rate=0.78,
            last_sync_utc=self.last_sync_utc,
            last_candidate_id=self.last_candidate_id,
            last_intent_id=self.last_intent_id,
            last_internal_order_id=self.last_internal_order_id,
            last_broker_order_no=self.last_broker_order_no,
            last_execution_status=self.last_execution_status,
            last_execution_reason=self.last_execution_reason,
            no_trade_count=(
                persistent_summary.no_trade_count
                if persistent_summary is not None
                else self.no_trade_count
            ),
            blocked_count=(
                persistent_summary.blocked_count
                if persistent_summary is not None
                else self.blocked_count
            ),
            submitted_count=(
                persistent_summary.submitted_count
                if persistent_summary is not None
                else self.submitted_count
            ),
            stale_data_incident_count=(
                persistent_summary.stale_data_incident_count
                if persistent_summary is not None
                else self.stale_data_incident_count
            ),
            duplicate_order_incident_count=(
                persistent_summary.duplicate_order_incident_count
                if persistent_summary is not None
                else self.duplicate_order_incident_count
            ),
            selector_mismatch_incident_count=(
                persistent_summary.selector_mismatch_incident_count
                if persistent_summary is not None
                else self.selector_mismatch_incident_count
            ),
        )

    def _loop_lease_ttl_seconds(self, interval_seconds: int | None = None) -> float:
        effective_interval = float(interval_seconds or self.loop_interval_seconds or 1)
        configured_ttl = max(float(self.settings.shadow_live_loop_lease_ttl_seconds or 0.0), 1.0)
        return max(configured_ttl, (effective_interval * 2.0) + 5.0)

    def _loop_watchdog_stale(self, durable_state: Any | None) -> bool:
        if durable_state is None or not durable_state.desired_running:
            return False
        now = datetime.now(UTC)
        if durable_state.lease_expires_at_utc is not None and durable_state.lease_expires_at_utc < now:
            return True
        if durable_state.heartbeat_at_utc is None:
            return True
        grace_seconds = max(float(self.settings.shadow_live_loop_watchdog_grace_seconds or 0.0), 1.0)
        return now - durable_state.heartbeat_at_utc > timedelta(seconds=grace_seconds)

    @staticmethod
    def _autonomous_live_loop_enabled() -> bool:
        try:
            store.reload_state()
            live_control = store.get_live_control()
        except Exception:
            return False
        return bool(live_control.autonomous_loop_enabled)

    def loop_snapshot(self) -> ShadowLoopSnapshot:
        durable_state = store.get_loop_state(self.loop_id)
        return ShadowLoopSnapshot(
            running=self._loop_task is not None and not self._loop_task.done(),
            execute_live=self.loop_execute_live,
            persist=self.loop_persist,
            interval_seconds=self.loop_interval_seconds,
            run_count=self.loop_run_count,
            desired_running=bool(durable_state.desired_running) if durable_state is not None else False,
            owner_id=None if durable_state is None else durable_state.owner_id,
            lease_expires_at_utc=None if durable_state is None else durable_state.lease_expires_at_utc,
            heartbeat_at_utc=None if durable_state is None else durable_state.heartbeat_at_utc,
            lease_stale=self._loop_watchdog_stale(durable_state),
            restored_from_durable=self.loop_restored_from_durable,
            last_started_at_utc=self.loop_last_started_at_utc,
            last_finished_at_utc=self.loop_last_finished_at_utc,
            last_result_status=(
                self.loop_last_result_status
                if durable_state is None or durable_state.last_result_status is None
                else durable_state.last_result_status
            ),
            last_error=(
                self.loop_last_error
                if durable_state is None or durable_state.last_error is None
                else durable_state.last_error
            ),
        )

    async def start_loop(self, *, interval_seconds: int, execute_live: bool, persist: bool) -> ShadowLoopSnapshot:
        return await self._start_loop_internal(
            interval_seconds=interval_seconds,
            execute_live=execute_live,
            persist=persist,
            restored_from_durable=False,
            reason_code="MANUAL_START",
        )

    async def restore_loop_if_needed(self) -> ShadowLoopSnapshot:
        durable_state = store.get_loop_state(self.loop_id)
        if durable_state is None or not durable_state.desired_running:
            return self.loop_snapshot()
        if not self.settings.shadow_live_loop_auto_resume:
            return self.loop_snapshot()
        if (
            durable_state.owner_id
            and durable_state.owner_id != self.loop_owner_id
            and durable_state.lease_expires_at_utc is not None
            and durable_state.lease_expires_at_utc > datetime.now(UTC)
        ):
            self.loop_last_error = f"LEASE_HELD_BY:{durable_state.owner_id}"
            return self.loop_snapshot()
        return await self._start_loop_internal(
            interval_seconds=durable_state.interval_seconds,
            execute_live=durable_state.execute_live,
            persist=durable_state.persist,
            restored_from_durable=True,
            reason_code="AUTO_RESUME",
        )

    async def _start_loop_internal(
        self,
        *,
        interval_seconds: int,
        execute_live: bool,
        persist: bool,
        restored_from_durable: bool,
        reason_code: str,
    ) -> ShadowLoopSnapshot:
        if interval_seconds < 1:
            raise ValueError("interval_seconds must be at least 1")
        if (
            execute_live
            and self.settings.trading_micro_test_mode_enabled
            and self.settings.trading_micro_test_run_once_only
            and not self._autonomous_live_loop_enabled()
        ):
            raise ValueError("micro live test mode allows execute_live only via /run/sample")
        await self._stop_loop_internal(desired_running=False, reason_code="RESTART_LOOP")
        store.acquire_loop_lease(
            loop_id=self.loop_id,
            service_name="shadow-live",
            owner_id=self.loop_owner_id,
            interval_seconds=interval_seconds,
            execute_live=execute_live,
            persist=persist,
            ttl_seconds=self._loop_lease_ttl_seconds(interval_seconds),
            actor=self.loop_owner_id,
            reason_code=reason_code,
        )
        self.loop_interval_seconds = interval_seconds
        self.loop_execute_live = execute_live
        self.loop_persist = persist
        self.loop_last_error = None
        self.loop_restored_from_durable = restored_from_durable
        self._loop_stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop_worker(), name="shadow-live-loop")
        return self.loop_snapshot()

    async def stop_loop(self) -> ShadowLoopSnapshot:
        return await self._stop_loop_internal(desired_running=False, reason_code="MANUAL_STOP")

    async def _stop_loop_internal(
        self,
        *,
        desired_running: bool,
        reason_code: str,
    ) -> ShadowLoopSnapshot:
        self._loop_stop_event.set()
        task = self._loop_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._loop_task = None
        try:
            store.release_loop_lease(
                loop_id=self.loop_id,
                owner_id=self.loop_owner_id,
                desired_running=desired_running,
                last_error=self.loop_last_error,
                reason_code=reason_code,
            )
        except Exception as exc:
            self.loop_last_error = f"LEASE_RELEASE_FAILED:{exc}"
        if not desired_running:
            self.loop_restored_from_durable = False
        return self.loop_snapshot()

    async def _loop_worker(self) -> None:
        try:
            while not self._loop_stop_event.is_set():
                durable_state = store.get_loop_state(self.loop_id)
                if (
                    durable_state is not None
                    and durable_state.owner_id not in {None, self.loop_owner_id}
                    and durable_state.lease_expires_at_utc is not None
                    and durable_state.lease_expires_at_utc > datetime.now(UTC)
                ):
                    self.loop_last_result_status = "LEASE_LOST"
                    self.loop_last_error = f"LEASE_LOST:{durable_state.owner_id}"
                    break
                store.renew_loop_lease(
                    loop_id=self.loop_id,
                    owner_id=self.loop_owner_id,
                    ttl_seconds=self._loop_lease_ttl_seconds(),
                    last_result_status=self.loop_last_result_status,
                    last_error=self.loop_last_error,
                )
                self.loop_last_started_at_utc = datetime.now(UTC)
                self.loop_last_error = None
                try:
                    plan = await self.run_once(
                        execute_live=self.loop_execute_live,
                        persist=self.loop_persist,
                    )
                    self.loop_last_result_status = plan.status
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.loop_last_result_status = "ERROR"
                    self.loop_last_error = str(exc)
                    self.last_execution_status = "ERROR"
                    self.last_execution_reason = str(exc)
                finally:
                    self.loop_run_count += 1
                    self.loop_last_finished_at_utc = datetime.now(UTC)
                    try:
                        store.renew_loop_lease(
                            loop_id=self.loop_id,
                            owner_id=self.loop_owner_id,
                            ttl_seconds=self._loop_lease_ttl_seconds(),
                            last_result_status=self.loop_last_result_status,
                            last_error=self.loop_last_error,
                        )
                    except Exception as exc:
                        self.loop_last_error = str(exc)
                        self.loop_last_result_status = "LEASE_UPDATE_FAILED"
                        break
                try:
                    await asyncio.wait_for(self._loop_stop_event.wait(), timeout=self.loop_interval_seconds)
                except TimeoutError:
                    continue
        finally:
            if not self._loop_stop_event.is_set():
                try:
                    store.release_loop_lease(
                        loop_id=self.loop_id,
                        owner_id=self.loop_owner_id,
                        desired_running=True,
                        last_error=self.loop_last_error,
                        reason_code="LOOP_EXITED",
                    )
                except Exception as exc:
                    self.loop_last_error = f"LEASE_RELEASE_FAILED:{exc}"
            self._loop_task = None

    def _is_on_cooldown(self, candidate: TradeCandidate) -> bool:
        cooldown_key = candidate.cooldown_key
        if not cooldown_key:
            return False
        until = self._cooldown_until_by_key.get(cooldown_key)
        return until is not None and until > datetime.now(UTC)

    def _mark_submitted_cooldown(self, candidate: TradeCandidate) -> None:
        cooldown_key = candidate.cooldown_key
        if not cooldown_key:
            return
        self._cooldown_until_by_key[cooldown_key] = datetime.now(UTC) + timedelta(
            minutes=self.settings.selector_post_entry_cooldown_minutes
        )

    async def _fetch_portfolio_candidates(
        self,
        *,
        force_refresh: bool = False,
    ) -> list[TradeCandidate]:
        try:
            response = await self.portfolio_client.get(
                "/portfolio/candidates",
                params={"force_refresh": str(force_refresh).lower()},
            )
            response.raise_for_status()
            candidates = [
                TradeCandidate.model_validate(item)
                for item in response.json()
            ]
            self.latest_portfolio_selected_count = len(candidates)
            return candidates
        except Exception:
            return []

    async def _fetch_portfolio_selected_count(self) -> int:
        try:
            response = await self.portfolio_client.get("/snapshot")
            response.raise_for_status()
            payload = response.json()
            return int(payload.get("selected_count") or 0)
        except Exception:
            return self.latest_portfolio_selected_count

    async def _select_candidate(self) -> TradeCandidate | None:
        live_candidates = await self._fetch_portfolio_candidates(force_refresh=False)
        if not live_candidates and await self._fetch_portfolio_selected_count():
            self.selector_mismatch_incident_count += 1
        for candidate in live_candidates:
            if self._is_on_cooldown(candidate):
                self.duplicate_order_incident_count += 1
                continue
            return candidate
        return None

    async def _select_micro_test_candidate(self) -> tuple[TradeCandidate | None, str | None]:
        live_candidates = await self._fetch_portfolio_candidates(force_refresh=False)
        if not live_candidates and await self._fetch_portfolio_selected_count():
            self.selector_mismatch_incident_count += 1

        allowed_symbols = set(self.settings.kis_live_allowed_symbol_list)
        effective_cap = self._effective_order_value_cap_krw(execute_live=True)
        rejected_symbol_count = 0
        rejected_price_count = 0
        rejected_spread_count = 0
        spread_limit_bps = max(float(self.settings.trading_micro_test_max_spread_bps or 0.0), 0.0)

        for candidate in live_candidates:
            if self._is_on_cooldown(candidate):
                self.duplicate_order_incident_count += 1
                continue
            if allowed_symbols and candidate.instrument_id not in allowed_symbols:
                rejected_symbol_count += 1
                continue
            try:
                market_div = _venue_code_to_market_div("KRX")
                current_price_payload = await self.gateway_client.query_price(
                    symbol=candidate.instrument_id,
                    market_div=market_div,
                )
                asking_price_payload = await self.gateway_client.query_asking_price(
                    symbol=candidate.instrument_id,
                    market_div=market_div,
                )
                quote_context = _build_quote_context(
                    candidate.side,
                    asking_price_payload,
                    current_price_payload,
                )
                selected_price_krw = quote_context["selected_price_krw"]
            except Exception:
                continue
            if selected_price_krw is None:
                continue
            spread_bps = quote_context["spread_bps"]
            if spread_limit_bps > 0 and spread_bps is not None and spread_bps > spread_limit_bps:
                rejected_spread_count += 1
                continue
            if effective_cap is not None and selected_price_krw > effective_cap:
                rejected_price_count += 1
                continue
            return candidate, None

        if rejected_spread_count > 0:
            return None, "No whitelisted portfolio candidate passed the micro test spread limit."
        if rejected_price_count > 0:
            return None, "No whitelisted portfolio candidate met the micro test price cap."
        if rejected_symbol_count > 0:
            return None, "No portfolio candidate matched KIS_LIVE_ALLOWED_SYMBOLS for micro test mode."
        return None, None

    def _current_trading_date(self) -> date:
        return _today_for_timezone(self.settings.app_timezone)

    @staticmethod
    def _session_code_from_market_guard(market_guard: dict[str, Any]) -> str:
        reason_codes = set(str(code) for code in (market_guard.get("reason_codes") or []))
        if not reason_codes:
            return "OPEN"
        if "TRADING_HALT_ACTIVE" in reason_codes:
            return "HALTED"
        if "SESSION_OUTSIDE_ENTRY_WINDOW" in reason_codes:
            return "ENTRY_CLOSED"
        if "WS_MARKET_DATA_STALE" in reason_codes:
            return "STALE"
        if "WS_FEED_NOT_RUNNING" in reason_codes:
            return "WS_DOWN"
        if any(code.startswith("WS_SYMBOL_NOT_SUBSCRIBED") for code in reason_codes):
            return "SYMBOL_PENDING"
        if "WS_QUOTE_UNAVAILABLE" in reason_codes:
            return "QUOTE_PENDING"
        return "BLOCKED"

    def _persist_session_state(
        self,
        *,
        venue: str,
        market_guard: dict[str, Any],
    ) -> None:
        quote_snapshot = market_guard.get("quote_snapshot") or {}
        trade_snapshot = market_guard.get("trade_snapshot") or {}
        reason_codes = _unique_reason_codes(
            [str(code) for code in (market_guard.get("reason_codes") or [])]
        )
        store.set_session_state(
            venue=venue.upper(),
            session_code=self._session_code_from_market_guard(market_guard),
            market_data_ok=bool(market_guard.get("ws_running")) and not bool(market_guard.get("ws_market_data_stale")),
            degraded=bool(reason_codes),
            entry_allowed=bool(market_guard.get("entry_allowed")),
            reason_codes=reason_codes,
            last_quote_at_utc=(
                _parse_snapshot_dt(market_guard.get("latest_quote_at_utc"))
                or _parse_snapshot_dt(quote_snapshot.get("exchange_ts_utc"))
            ),
            last_trade_at_utc=(
                _parse_snapshot_dt(market_guard.get("latest_trade_at_utc"))
                or _parse_snapshot_dt(trade_snapshot.get("received_ts_utc"))
                or _parse_snapshot_dt(trade_snapshot.get("exchange_ts_utc"))
            ),
        )

    def _requires_live_ws_market_data(self, *, execute_live: bool) -> bool:
        return bool(execute_live and self.settings.shadow_live_require_ws_live_market_data)

    async def _ensure_live_market_guard(
        self,
        *,
        symbol: str,
        venue_hint: str,
    ) -> dict[str, Any]:
        venue = (venue_hint or "KRX").upper()
        ws_status = await self.gateway_client.ws_status()
        active_symbols = [
            str(item).strip()
            for item in (ws_status.get("symbols") or [])
            if str(item).strip()
        ]
        current_venue = str(ws_status.get("venue") or "").upper()
        ws_running = bool(ws_status.get("running"))
        include_fill_notice = bool(ws_status.get("include_fill_notice", True))
        include_market_status = bool(ws_status.get("include_market_status", True))
        needs_restart = (
            not ws_running
            or symbol not in active_symbols
            or current_venue not in {"", venue, "TOTAL"}
            or not include_fill_notice
            or not include_market_status
        )
        if needs_restart:
            merged_symbols = list(dict.fromkeys(active_symbols + [symbol]))
            await self.gateway_client.start_ws(
                symbols=merged_symbols,
                venue=venue,
                include_fill_notice=True,
                include_market_status=True,
            )
            await asyncio.sleep(0)
        market_guard = await self.gateway_client.market_guard(symbol=symbol, venue=venue)
        self._persist_session_state(venue=venue, market_guard=market_guard)
        return market_guard

    async def _resolve_quote_context(
        self,
        *,
        candidate: TradeCandidate,
        execute_live: bool,
        venue_hint: str,
    ) -> tuple[dict[str, Any], bool, bool, dict[str, Any] | None]:
        if self._requires_live_ws_market_data(execute_live=execute_live):
            market_guard = await self._ensure_live_market_guard(
                symbol=candidate.instrument_id,
                venue_hint=venue_hint,
            )
            quote_context = _build_quote_context_from_ws_guard(candidate.side, market_guard)
            best_quote_available = (
                quote_context["best_ask_krw"] is not None
                if candidate.side == OrderSide.BUY
                else quote_context["best_bid_krw"] is not None
            )
            market_snapshot_ok = best_quote_available and bool(market_guard.get("quote_snapshot"))
            vendor_healthy = bool(market_guard.get("ws_running")) and not bool(
                market_guard.get("ws_market_data_stale")
            )
            return quote_context, market_snapshot_ok, vendor_healthy, market_guard

        market_div = _venue_code_to_market_div(venue_hint)
        current_price_payload = await self.gateway_client.query_price(
            symbol=candidate.instrument_id,
            market_div=market_div,
        )
        asking_price_payload = await self.gateway_client.query_asking_price(
            symbol=candidate.instrument_id,
            market_div=market_div,
        )
        quote_context = _build_quote_context(
            candidate.side,
            asking_price_payload,
            current_price_payload,
        )
        market_snapshot_ok = quote_context["selected_price_krw"] is not None and bool(
            (current_price_payload.get("output") or {})
        ) and bool((asking_price_payload.get("output1") or {}))
        return quote_context, market_snapshot_ok, True, None

    async def _build_micro_test_candidate_preview(
        self,
        *,
        candidate: TradeCandidate,
        risk_state: dict[str, Any] | None,
    ) -> MicroTestCandidatePreview:
        allowed_symbols = set(self.settings.kis_live_allowed_symbol_list)
        block_reason_codes: list[str] = []
        readiness_reason_codes: list[str] = []

        allowed_by_config = True
        if self.settings.trading_micro_test_require_allowed_symbols and not allowed_symbols:
            allowed_by_config = False
            block_reason_codes.append("MICRO_TEST_ALLOWED_SYMBOLS_REQUIRED")
        elif allowed_symbols and candidate.instrument_id not in allowed_symbols:
            allowed_by_config = False
            block_reason_codes.append(f"MICRO_TEST_SYMBOL_NOT_ALLOWED:{candidate.instrument_id}")

        on_cooldown = self._is_on_cooldown(candidate)
        if on_cooldown:
            block_reason_codes.append("COOLDOWN_ACTIVE")

        quote_context = {
            "best_bid_krw": None,
            "best_ask_krw": None,
            "last_price_krw": None,
            "selected_price_krw": None,
            "quote_basis": None,
            "spread_bps": None,
        }
        market_div = _venue_code_to_market_div("KRX")
        try:
            current_price_payload = await self.gateway_client.query_price(
                symbol=candidate.instrument_id,
                market_div=market_div,
            )
            asking_price_payload = await self.gateway_client.query_asking_price(
                symbol=candidate.instrument_id,
                market_div=market_div,
            )
            quote_context = _build_quote_context(
                candidate.side,
                asking_price_payload,
                current_price_payload,
            )
        except Exception as exc:
            block_reason_codes.append(f"MARKET_SNAPSHOT_ERROR:{type(exc).__name__}")

        market_snapshot_ok = quote_context["selected_price_krw"] is not None
        if not market_snapshot_ok:
            block_reason_codes.append("MARKET_SNAPSHOT_UNAVAILABLE")

        spread_bps = quote_context["spread_bps"]
        spread_limit_bps = max(float(self.settings.trading_micro_test_max_spread_bps or 0.0), 0.0)
        if spread_limit_bps > 0 and spread_bps is not None and spread_bps > spread_limit_bps:
            block_reason_codes.append(f"MICRO_TEST_SPREAD_TOO_WIDE:{spread_bps:.2f}")

        micro_test_block_reason = self._micro_test_block_reason(candidate=candidate, execute_live=True)
        if micro_test_block_reason is not None:
            block_reason_codes.append(micro_test_block_reason)

        proposed_qty: int | None = None
        proposed_order_value_krw: int | None = None

        if market_snapshot_ok:
            readiness = await self._build_execution_readiness(
                candidate=candidate,
                execute_live=True,
                risk_state=risk_state,
                market_snapshot_ok=True,
                vendor_healthy=True,
            )
            readiness_reason_codes = _unique_reason_codes(readiness.reason_codes)
            decision = trading_core_service.evaluate_candidate(
                candidate,
                execution_readiness=readiness,
            )
            intent = trading_core_service.build_trade_intent(candidate, decision)
            if decision.hard_block or intent is None:
                block_reason_codes.extend(decision.reason_codes)
            else:
                try:
                    command = trading_core_service.build_order_submit_command(
                        intent=intent,
                        strategy_id=candidate.strategy_id,
                        price_krw=quote_context["selected_price_krw"],
                        venue_hint="KRX",
                        order_type="LIMIT",
                        max_order_value_krw=self._effective_order_value_cap_krw(execute_live=True),
                        enforce_hard_value_cap=True,
                    )
                    proposed_qty = command.qty
                    proposed_order_value_krw = command.qty * int(command.price or 0)
                except ValueError as exc:
                    block_reason_codes.append(str(exc))

        block_reason_codes = _unique_reason_codes(block_reason_codes)
        allowlist_reasons = {
            "MICRO_TEST_ALLOWED_SYMBOLS_REQUIRED",
            f"MICRO_TEST_SYMBOL_NOT_ALLOWED:{candidate.instrument_id}",
        }
        non_allowlist_blocks = [code for code in block_reason_codes if code not in allowlist_reasons]
        eligible_if_allowlisted = (not allowed_by_config) and not non_allowlist_blocks and market_snapshot_ok
        eligible_now = not block_reason_codes

        return MicroTestCandidatePreview(
            candidate_id=candidate.candidate_id,
            instrument_id=candidate.instrument_id,
            side=candidate.side,
            eligible_now=eligible_now,
            eligible_if_allowlisted=eligible_if_allowlisted,
            allowed_by_config=allowed_by_config,
            on_cooldown=on_cooldown,
            selection_reason=candidate.selection_reason,
            matched_rule_id=candidate.matched_rule_id,
            quote_basis=quote_context["quote_basis"],
            selected_price_krw=quote_context["selected_price_krw"],
            best_bid_krw=quote_context["best_bid_krw"],
            best_ask_krw=quote_context["best_ask_krw"],
            last_price_krw=quote_context["last_price_krw"],
            spread_bps=quote_context["spread_bps"],
            proposed_qty=proposed_qty,
            proposed_order_value_krw=proposed_order_value_krw,
            readiness_reason_codes=readiness_reason_codes,
            block_reason_codes=block_reason_codes,
        )

    async def preview_micro_test_candidates(
        self,
        *,
        force_refresh: bool = False,
        limit: int = 10,
    ) -> MicroTestCandidatePreviewResponse:
        warnings: list[str] = []
        risk_state: dict[str, Any] | None = None
        try:
            risk_state = await self.gateway_client.refresh_live_risk_state()
        except Exception as exc:
            warnings.append(f"LIVE_RISK_CHECK_FAILED:{exc}")

        candidates = await self._fetch_portfolio_candidates(force_refresh=force_refresh)
        selected_count = await self._fetch_portfolio_selected_count()
        if not candidates and selected_count:
            warnings.append(f"PORTFOLIO_SELECTED_COUNT_WITHOUT_CANDIDATES:{selected_count}")

        preview_limit = max(limit, 1)
        previews: list[MicroTestCandidatePreview] = []
        for candidate in candidates[:preview_limit]:
            previews.append(
                await self._build_micro_test_candidate_preview(
                    candidate=candidate,
                    risk_state=risk_state,
                )
            )

        suggested_allowed_symbols = [
            preview.instrument_id
            for preview in previews
            if preview.eligible_if_allowlisted
        ]
        return MicroTestCandidatePreviewResponse(
            configured_allowed_symbols=self.settings.kis_live_allowed_symbol_list,
            suggested_allowed_symbols=_unique_reason_codes(suggested_allowed_symbols),
            effective_max_order_value_krw=self._effective_order_value_cap_krw(execute_live=True),
            live_risk_state=risk_state,
            warnings=warnings,
            candidates=previews,
        )

    @staticmethod
    def _match_daily_ccld_rows(
        payload: dict[str, Any],
        *,
        instrument_id: str | None,
        broker_order_no: str | None,
    ) -> list[dict[str, Any]]:
        rows = payload.get("output1") or []
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            return []
        normalized_order_no = _normalize_order_no(broker_order_no)
        matches: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_order_no = _normalize_order_no(_first_present(row, "odno", "ODNO", "ord_no", "ordno"))
            row_symbol = str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip()
            if normalized_order_no and row_order_no and row_order_no == normalized_order_no:
                matches.append(row)
                continue
            if not normalized_order_no and instrument_id and row_symbol == instrument_id:
                matches.append(row)
        return matches

    @staticmethod
    def _match_balance_rows(payload: dict[str, Any], *, instrument_id: str | None) -> list[dict[str, Any]]:
        if not instrument_id:
            return []
        rows = payload.get("output1") or []
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            return []
        return [
            row
            for row in rows
            if isinstance(row, dict)
            and str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip() == instrument_id
        ]

    @staticmethod
    def _aggregate_fill_details(rows: list[dict[str, Any]]) -> tuple[int, float | None]:
        total_qty = 0
        total_value = 0.0
        fallback_price: float | None = None
        for row in rows:
            qty = _safe_int(
                _first_present(
                    row,
                    "tot_ccld_qty",
                    "tot_ccld_qty1",
                    "ccld_qty",
                    "ft_ccld_qty",
                    "exec_qty",
                    "ord_qty",
                )
            ) or 0
            price = _safe_float(
                _first_present(
                    row,
                    "avg_prvs",
                    "avg_ccld_unpr",
                    "tot_ccld_unpr",
                    "ccld_avg_prc",
                    "ord_unpr",
                )
            )
            if price is not None and fallback_price is None:
                fallback_price = price
            if qty > 0 and price is not None:
                total_qty += qty
                total_value += price * qty
            elif qty > 0:
                total_qty += qty
        if total_qty > 0 and total_value > 0:
            return total_qty, round(total_value / total_qty, 2)
        return total_qty, fallback_price

    async def verify_latest_execution(self) -> PostTradeVerificationReport:
        plan = self.last_execution_plan
        if plan is None:
            return PostTradeVerificationReport(
                status="NOT_APPLICABLE",
                reason="No shadow-live execution plan is available yet.",
            )
        if not plan.execute_live:
            return PostTradeVerificationReport(
                status="NOT_APPLICABLE",
                reason="Latest shadow-live run did not request live execution.",
                candidate_id=plan.candidate_id or None,
            )
        if plan.status != "SUBMITTED":
            return PostTradeVerificationReport(
                status="NOT_APPLICABLE",
                reason=f"Latest live execution status is {plan.status}.",
                candidate_id=plan.candidate_id or None,
            )

        planned_order = plan.planned_order or {}
        candidate_id = plan.candidate_id or None
        account_id = str(planned_order.get("account_id") or self.settings.selector_default_account_scope)
        instrument_id = str(planned_order.get("instrument_id") or "").strip() or None
        internal_order_id = str(planned_order.get("internal_order_id") or self.last_internal_order_id or "").strip() or None
        raw_broker_order_no = (
            str(self.last_broker_order_no or "").strip()
            or str(_first_present(plan.normalized_ack or {}, "broker_order_no") or "").strip()
            or str(_first_present((plan.normalized_ack or {}).get("event") or {}, "broker_order_no") or "").strip()
            or str(_first_present((plan.broker_response or {}).get("output") or {}, "ODNO", "odno") or "").strip()
        )
        broker_order_no = _normalize_order_no(raw_broker_order_no)
        planned_qty = _safe_int(planned_order.get("qty"))
        planned_price_krw = _safe_int(planned_order.get("price"))
        venue_code = str(planned_order.get("venue_hint") or "KRX").upper()

        try:
            daily_ccld_payload = await self.gateway_client.query_daily_ccld(
                symbol=instrument_id,
                broker_order_no=raw_broker_order_no or None,
                venue_code=venue_code,
                trading_date=self._current_trading_date(),
            )
            balance_payload = await self.gateway_client.query_balance()
        except Exception as exc:
            return PostTradeVerificationReport(
                status="ERROR",
                reason=f"Broker verification query failed: {exc}",
                candidate_id=candidate_id,
                account_id=account_id,
                instrument_id=instrument_id,
                internal_order_id=internal_order_id,
                broker_order_no=broker_order_no,
                planned_qty=planned_qty,
                planned_price_krw=planned_price_krw,
            )

        matched_orders = self._match_daily_ccld_rows(
            daily_ccld_payload,
            instrument_id=instrument_id,
            broker_order_no=broker_order_no,
        )
        matched_positions = self._match_balance_rows(balance_payload, instrument_id=instrument_id)
        matched_fill_qty, matched_avg_fill_price_krw = self._aggregate_fill_details(matched_orders)

        balance_position_qty = 0
        for row in matched_positions:
            balance_position_qty += _safe_int(
                _first_present(row, "hldg_qty", "hold_qty", "cblc_qty", "ord_psbl_qty")
            ) or 0
        if not matched_positions:
            balance_position_qty = None

        summary_rows = balance_payload.get("output2") or [{}]
        summary_row = summary_rows[0] if isinstance(summary_rows, list) and summary_rows else {}
        if not isinstance(summary_row, dict):
            summary_row = {}
        balance_available_cash_krw = _safe_float(
            _first_present(summary_row, "dnca_tot_amt", "ord_psbl_cash", "ord_psbl_amt", "tot_evlu_amt")
        )

        if matched_orders and matched_fill_qty > 0:
            status = "VERIFIED"
            reason = None
        elif matched_orders:
            status = "ACKNOWLEDGED"
            reason = "Broker daily-ccld contains the order, but no filled quantity is visible yet."
        else:
            status = "PENDING"
            reason = "Broker daily-ccld did not return a matching order yet."

        return PostTradeVerificationReport(
            status=status,
            reason=reason,
            candidate_id=candidate_id,
            account_id=account_id,
            instrument_id=instrument_id,
            internal_order_id=internal_order_id,
            broker_order_no=broker_order_no,
            planned_qty=planned_qty,
            planned_price_krw=planned_price_krw,
            matched_order_count=len(matched_orders),
            matched_fill_qty=matched_fill_qty,
            matched_avg_fill_price_krw=matched_avg_fill_price_krw,
            balance_position_qty=balance_position_qty,
            balance_available_cash_krw=balance_available_cash_krw,
            matched_orders=matched_orders,
            matched_positions=matched_positions,
            daily_ccld_payload=daily_ccld_payload,
            balance_payload=balance_payload,
        )

    async def _fetch_promotion_readiness(self) -> dict[str, Any] | None:
        if not self.settings.selector_live_require_promotion_approval:
            return None
        payload = {
            "current_stage": self.settings.selector_runtime_stage,
            "target_stage": self.settings.selector_live_target_stage,
        }
        response = await self.selector_client.post("/selector/promotion-check", json=payload)
        response.raise_for_status()
        return response.json()

    def _audit_payload(
        self,
        *,
        candidate: TradeCandidate | None,
        plan: ExecutionPlan,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "intent_id": plan.intent_id,
            "selection_reason": plan.selection_reason,
            "matched_rule_id": plan.matched_rule_id,
            "source_report_name": plan.source_report_name,
            "source_receipt_no": plan.source_receipt_no,
            "quote_basis": plan.quote_basis,
            "selected_price_krw": plan.selected_price_krw,
            "risk_reason_summary": plan.risk_reason_summary,
            "planned_order": plan.planned_order,
            "promotion_readiness": plan.promotion_readiness,
        }
        if candidate is not None:
            payload.update(
                {
                    "account_scope": candidate.account_scope,
                    "strategy_id": candidate.strategy_id,
                    "event_cluster_id": candidate.event_cluster_id,
                    "selection_confidence": candidate.selection_confidence,
                }
            )
        return payload

    def _persist_run_audit(
        self,
        *,
        candidate: TradeCandidate | None,
        plan: ExecutionPlan,
        stale_before: int,
        duplicate_before: int,
        selector_mismatch_before: int,
    ) -> None:
        promotion_readiness = plan.promotion_readiness or {}
        audit = ShadowLiveRunAuditRecord(
            run_id=f"shadow-run-{uuid4().hex}",
            candidate_id=plan.candidate_id or None,
            instrument_id=(
                candidate.instrument_id
                if candidate is not None
                else None
            ),
            execute_live=plan.execute_live,
            persisted=plan.persisted,
            status=plan.status,
            reason=plan.reason or plan.risk_reason_summary,
            promotion_required=(
                plan.execute_live
                and self.settings.selector_live_require_promotion_approval
            ),
            promotion_approved=(
                None
                if not promotion_readiness
                else bool(promotion_readiness.get("approved"))
            ),
            stale_data_incident=self.stale_data_incident_count > stale_before,
            duplicate_order_incident=(
                self.duplicate_order_incident_count > duplicate_before
            ),
            selector_mismatch_incident=(
                self.selector_mismatch_incident_count > selector_mismatch_before
            ),
            payload=self._audit_payload(candidate=candidate, plan=plan),
        )
        data_ingest_service.persist_shadow_live_run(audit)

    def _finalize_plan(
        self,
        *,
        candidate: TradeCandidate | None,
        plan: ExecutionPlan,
        stale_before: int,
        duplicate_before: int,
        selector_mismatch_before: int,
    ) -> ExecutionPlan:
        try:
            self._persist_run_audit(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        except Exception as exc:
            plan.persistence_error = (
                f"{plan.persistence_error}; shadow_audit:{exc}"
                if plan.persistence_error
                else f"shadow_audit:{exc}"
            )
        self.last_execution_plan = plan
        return plan

    def _fallback_execution_readiness(
        self,
        *,
        candidate: TradeCandidate,
        execute_live: bool = False,
        risk_state: dict[str, Any] | None = None,
        market_snapshot_ok: bool = True,
        vendor_healthy: bool = True,
    ) -> ExecutionReadiness:
        readiness = store.resolve_execution_readiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            confidence_ok=(candidate.selection_confidence or 0.0) >= self.settings.selector_confidence_floor,
            market_data_ok=market_snapshot_ok,
            data_freshness_ok=market_snapshot_ok,
            vendor_healthy=vendor_healthy,
            session_entry_allowed=not bool((risk_state or {}).get("entry_paused")),
            max_allowed_notional_krw=self._effective_order_value_cap_krw(execute_live=execute_live),
        )
        if risk_state and risk_state.get("entry_paused"):
            readiness.reason_codes.append(str(risk_state.get("live_pause_reason") or "LIVE_ENTRY_PAUSED"))
        return readiness

    async def _build_execution_readiness(
        self,
        *,
        candidate: TradeCandidate,
        execute_live: bool = False,
        risk_state: dict[str, Any] | None = None,
        market_snapshot_ok: bool = True,
        vendor_healthy: bool = True,
    ) -> ExecutionReadiness:
        payload = {
            "account_id": candidate.account_scope,
            "strategy_id": candidate.strategy_id,
            "instrument_id": candidate.instrument_id,
            "confidence_ok": (candidate.selection_confidence or 0.0) >= self.settings.selector_confidence_floor,
            "market_data_ok": market_snapshot_ok,
            "data_freshness_ok": market_snapshot_ok,
            "vendor_healthy": vendor_healthy,
            "session_entry_allowed": not bool((risk_state or {}).get("entry_paused")),
            "max_allowed_notional_krw": self._effective_order_value_cap_krw(execute_live=execute_live),
        }
        try:
            response = await self.ops_client.post("/ops/execution-readiness", json=payload)
            response.raise_for_status()
            readiness = ExecutionReadiness.model_validate(response.json())
            if risk_state and risk_state.get("entry_paused"):
                readiness.reason_codes.append(str(risk_state.get("live_pause_reason") or "LIVE_ENTRY_PAUSED"))
            return readiness
        except Exception:
            return self._fallback_execution_readiness(
                candidate=candidate,
                execute_live=execute_live,
                risk_state=risk_state,
                market_snapshot_ok=market_snapshot_ok,
                vendor_healthy=vendor_healthy,
            )

    def _runtime_live_max_order_value_krw(self) -> int | None:
        try:
            store.reload_state()
            live_control = store.get_live_control()
        except Exception:
            return None
        value = int(live_control.max_order_value_krw or 0)
        if value <= 0:
            return None
        return value

    def _effective_order_value_cap_krw(self, *, execute_live: bool) -> int | None:
        caps: list[int] = []
        if self.settings.kis_live_max_order_value_krw > 0:
            caps.append(self.settings.kis_live_max_order_value_krw)
        if execute_live:
            runtime_live_cap = self._runtime_live_max_order_value_krw()
            if runtime_live_cap is not None:
                caps.append(runtime_live_cap)
        if execute_live and self.settings.trading_micro_test_mode_enabled:
            if self.settings.trading_micro_test_max_order_value_krw > 0:
                caps.append(self.settings.trading_micro_test_max_order_value_krw)
        if not caps:
            return None
        return min(caps)

    def _micro_test_block_reason(
        self,
        *,
        candidate: TradeCandidate,
        execute_live: bool,
    ) -> str | None:
        if not execute_live or not self.settings.trading_micro_test_mode_enabled:
            return None
        allowed_symbols = self.settings.kis_live_allowed_symbol_list
        if self.settings.trading_micro_test_require_allowed_symbols and not allowed_symbols:
            return "MICRO_TEST_ALLOWED_SYMBOLS_REQUIRED"
        if allowed_symbols and candidate.instrument_id not in allowed_symbols:
            return f"MICRO_TEST_SYMBOL_NOT_ALLOWED:{candidate.instrument_id}"
        if self.settings.trading_micro_test_max_order_value_krw <= 0:
            return "MICRO_TEST_MAX_ORDER_VALUE_REQUIRED"
        return None

    async def run_once(self, *, execute_live: bool, persist: bool) -> ExecutionPlan:
        stale_before = self.stale_data_incident_count
        duplicate_before = self.duplicate_order_incident_count
        selector_mismatch_before = self.selector_mismatch_incident_count
        no_trade_reason = "No eligible portfolio candidate is available."
        if execute_live and self.settings.trading_micro_test_mode_enabled:
            candidate, micro_reason = await self._select_micro_test_candidate()
            if micro_reason:
                no_trade_reason = micro_reason
        else:
            candidate = await self._select_candidate()
        if candidate is None:
            self.last_sync_utc = datetime.now(UTC)
            self.last_execution_status = "NO_TRADE"
            self.last_execution_reason = no_trade_reason
            self.no_trade_count += 1
            plan = ExecutionPlan(
                candidate_id="",
                intent_id=None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=False,
                risk_reason_summary=self.last_execution_reason,
                status="NO_TRADE",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=None,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )

        self.last_sync_utc = datetime.now(UTC)
        self.last_candidate_id = candidate.candidate_id
        persistence_error: str | None = None
        initial_readiness = await self._build_execution_readiness(candidate=candidate, execute_live=execute_live)
        decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=initial_readiness)
        intent = trading_core_service.build_trade_intent(candidate, decision)

        if persist:
            try:
                trading_core_service.persist_pipeline(candidate=candidate, decision=decision, intent=intent)
            except Exception as exc:
                persistence_error = str(exc)
                persist = False

        if decision.hard_block or intent is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = ",".join(decision.reason_codes) or "risk gate blocked"
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )

        venue_hint = "KRX"
        risk_state = await self.gateway_client.refresh_live_risk_state()
        micro_test_block_reason = self._micro_test_block_reason(candidate=candidate, execute_live=execute_live)
        if micro_test_block_reason is not None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = micro_test_block_reason
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id if intent is not None else None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                quantity_reason=intent.sizing_reason if intent is not None else None,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        market_guard: dict[str, Any] | None = None
        quote_context, market_snapshot_ok, vendor_healthy, market_guard = await self._resolve_quote_context(
            candidate=candidate,
            execute_live=execute_live,
            venue_hint=venue_hint,
        )
        if not market_snapshot_ok or (market_guard is not None and not market_guard.get("entry_allowed", True)):
            self.stale_data_incident_count += 1
        live_readiness = await self._build_execution_readiness(
            candidate=candidate,
            execute_live=execute_live,
            risk_state=risk_state,
            market_snapshot_ok=market_snapshot_ok,
            vendor_healthy=vendor_healthy,
        )
        if market_guard is not None and (
            not bool(market_guard.get("entry_allowed", True))
            or not market_snapshot_ok
            or not vendor_healthy
        ):
            guard_reason_codes = _unique_reason_codes(
                [str(code) for code in (market_guard.get("reason_codes") or [])]
                + list(live_readiness.reason_codes)
            )
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = ",".join(guard_reason_codes) or "LIVE_MARKET_GUARD_BLOCKED"
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id if intent is not None else None,
                planned_order=None,
                selected_price_krw=quote_context["selected_price_krw"],
                quote_basis=quote_context["quote_basis"],
                price_source_value=quote_context["selected_price_krw"],
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=live_readiness)
        intent = trading_core_service.build_trade_intent(candidate, decision)
        if decision.hard_block or intent is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = ",".join(decision.reason_codes) or "risk gate blocked"
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )

        spread_limit_bps = max(float(self.settings.trading_micro_test_max_spread_bps or 0.0), 0.0)
        spread_bps = quote_context["spread_bps"]
        if (
            execute_live
            and self.settings.trading_micro_test_mode_enabled
            and spread_limit_bps > 0
            and spread_bps is not None
            and spread_bps > spread_limit_bps
        ):
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = f"MICRO_TEST_SPREAD_TOO_WIDE:{spread_bps:.2f}"
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=None,
                selected_price_krw=quote_context["selected_price_krw"],
                quote_basis=quote_context["quote_basis"],
                price_source_value=quote_context["selected_price_krw"],
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                promotion_readiness=None,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )

        promotion_readiness: dict[str, Any] | None = None
        if execute_live and self.settings.selector_live_require_promotion_approval:
            try:
                promotion_readiness = await self._fetch_promotion_readiness()
            except Exception as exc:
                promotion_readiness = {
                    "approved": False,
                    "reason_codes": [f"PROMOTION_CHECK_FAILED:{exc}"],
                }
            if not promotion_readiness.get("approved", False):
                self.last_execution_status = "BLOCKED"
                reason_codes = promotion_readiness.get("reason_codes") or ["PROMOTION_NOT_APPROVED"]
                self.last_execution_reason = ",".join(str(code) for code in reason_codes)
                self.blocked_count += 1
                plan = ExecutionPlan(
                    candidate_id=candidate.candidate_id,
                    intent_id=intent.intent_id,
                    planned_order=None,
                    selected_price_krw=None,
                    quote_basis=None,
                    price_source_value=None,
                    execute_live=execute_live,
                    persisted=persist,
                    selection_reason=candidate.selection_reason,
                    matched_rule_id=candidate.matched_rule_id,
                    source_report_name=candidate.source_report_name,
                    source_receipt_no=candidate.source_receipt_no,
                    risk_reason_summary=self.last_execution_reason,
                    risk_state=risk_state,
                    promotion_readiness=promotion_readiness,
                    persistence_error=persistence_error,
                    status="BLOCKED",
                    reason=self.last_execution_reason,
                )
                return self._finalize_plan(
                    candidate=candidate,
                    plan=plan,
                    stale_before=stale_before,
                    duplicate_before=duplicate_before,
                    selector_mismatch_before=selector_mismatch_before,
                )

        selected_price_krw = quote_context["selected_price_krw"]
        quote_basis = quote_context["quote_basis"]
        if selected_price_krw is None or quote_basis is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = "MARKET_SNAPSHOT_UNAVAILABLE"
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                promotion_readiness=promotion_readiness,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        price_reason = _build_price_reason(quote_basis, selected_price_krw)
        try:
            command = trading_core_service.build_order_submit_command(
                intent=intent,
                strategy_id=candidate.strategy_id,
                price_krw=selected_price_krw,
                venue_hint=venue_hint,
                order_type="LIMIT",
                max_order_value_krw=self._effective_order_value_cap_krw(execute_live=execute_live),
                enforce_hard_value_cap=(
                    execute_live and self.settings.trading_micro_test_mode_enabled
                ),
            )
        except ValueError as exc:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = str(exc)
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=None,
                selected_price_krw=selected_price_krw,
                quote_basis=quote_basis,
                price_source_value=selected_price_krw,
                execute_live=execute_live,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                price_reason=price_reason,
                quantity_reason=intent.sizing_reason,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                promotion_readiness=promotion_readiness,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        self.last_intent_id = intent.intent_id
        self.last_internal_order_id = command.internal_order_id
        planned_order = command.model_dump(mode="json")
        quantity_reason = _build_quantity_reason(
            intent.target_notional_krw,
            selected_price_krw,
            planned_order["qty"],
            sizing_reason=intent.sizing_reason,
        )
        risk_reason_summary = _build_risk_reason_summary(
            decision_reason_codes=decision.reason_codes,
            live_risk_state=risk_state,
        )

        if not execute_live:
            self.last_execution_status = "PLANNED_ONLY"
            self.last_execution_reason = "live execution disabled for this run"
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=planned_order,
                selected_price_krw=selected_price_krw,
                quote_basis=quote_basis,
                price_source_value=selected_price_krw,
                execute_live=False,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                price_reason=price_reason,
                quantity_reason=quantity_reason,
                risk_reason_summary=risk_reason_summary,
                risk_state=risk_state,
                promotion_readiness=promotion_readiness,
                persistence_error=persistence_error,
                status="PLANNED_ONLY",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )

        broker_payload = _build_kis_order_payload(command)
        try:
            broker_response = await self.gateway_client.submit_cash_order(broker_payload)
        except BrokerGatewayRequestError as exc:
            self.last_execution_status = "BLOCKED"
            error_code = (
                "BROKER_GATEWAY_REJECTED"
                if 400 <= exc.status_code < 500
                else "BROKER_GATEWAY_ERROR"
            )
            self.last_execution_reason = f"{error_code}:{exc.detail}"
            self.blocked_count += 1
            rejected_payload = (
                exc.payload
                if isinstance(exc.payload, dict)
                else {"status_code": exc.status_code, "detail": exc.detail}
            )
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=planned_order,
                selected_price_krw=selected_price_krw,
                quote_basis=quote_basis,
                price_source_value=selected_price_krw,
                execute_live=True,
                persisted=persist,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                price_reason=price_reason,
                quantity_reason=quantity_reason,
                risk_reason_summary=self.last_execution_reason,
                risk_state=risk_state,
                broker_response=rejected_payload,
                promotion_readiness=promotion_readiness,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )
            return self._finalize_plan(
                candidate=candidate,
                plan=plan,
                stale_before=stale_before,
                duplicate_before=duplicate_before,
                selector_mismatch_before=selector_mismatch_before,
            )
        ack_payload = {
            **broker_response,
            "internal_order_id": command.internal_order_id,
            "client_order_id": command.client_order_id,
            "account_id": command.account_id,
            "instrument_id": command.instrument_id,
            "side_code": command.side.value.upper(),
            "order_type_code": command.order_type,
            "tif_code": command.tif,
            "instrument_pk": None,
        }
        normalized_ack = await self.gateway_client.normalize_order_ack(ack_payload)
        event = normalized_ack["event"]
        self.last_broker_order_no = event["broker_order_no"] if isinstance(event, dict) else event.broker_order_no
        self.last_execution_status = "SUBMITTED"
        self.last_execution_reason = None
        self.submitted_count += 1
        self._mark_submitted_cooldown(candidate)
        plan = ExecutionPlan(
            candidate_id=candidate.candidate_id,
            intent_id=intent.intent_id,
            planned_order=planned_order,
            selected_price_krw=selected_price_krw,
            quote_basis=quote_basis,
            price_source_value=selected_price_krw,
            execute_live=True,
            persisted=persist,
            selection_reason=candidate.selection_reason,
            matched_rule_id=candidate.matched_rule_id,
            source_report_name=candidate.source_report_name,
            source_receipt_no=candidate.source_receipt_no,
            price_reason=price_reason,
            quantity_reason=quantity_reason,
            risk_reason_summary=risk_reason_summary,
            risk_state=risk_state,
            broker_response=broker_response,
            normalized_ack=normalized_ack,
            promotion_readiness=promotion_readiness,
            persistence_error=persistence_error,
            status="SUBMITTED",
        )
        return self._finalize_plan(
            candidate=candidate,
            plan=plan,
            stale_before=stale_before,
            duplicate_before=duplicate_before,
            selector_mismatch_before=selector_mismatch_before,
        )


shadow_live_service = ShadowLiveService()
