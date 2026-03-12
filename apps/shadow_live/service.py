from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from apps.data_ingest.service import data_ingest_service
from apps.feature_pipeline.service import feature_pipeline_service
from apps.ops_api.store import store
from apps.trading_core.service import trading_core_service
from libs.adapters.openai_parser import OpenAIParserClient
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
from libs.db.base import SessionLocal
from libs.db.repositories import InstrumentProfileSnapshot, TradingRepository
from libs.domain.cross_asset_profile import score_instrument_profile_against_macro_points
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


@dataclass(slots=True)
class PositionInventorySnapshot:
    net_qty: int
    avg_cost_krw: float | None
    holding_days: int | None
    first_entry_ts_utc: datetime | None = None


@dataclass(slots=True)
class ExitCandidateSignal:
    candidate: TradeCandidate
    priority_score: float


@dataclass(slots=True)
class CandidateAttemptResult:
    plan: ExecutionPlan
    retryable_block: bool = False


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
    side: OrderSide,
    decision_reason_codes: list[str],
    live_risk_state: dict[str, Any] | None,
) -> str:
    if decision_reason_codes:
        return f"리스크 게이트 사유: {', '.join(decision_reason_codes)}"
    if side != OrderSide.SELL and live_risk_state and live_risk_state.get("entry_paused"):
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


def _ensure_utc_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _parse_date_value(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text[:10]
    for parser in (
        lambda raw: datetime.strptime(raw, "%Y%m%d").date(),
        lambda raw: date.fromisoformat(raw),
    ):
        try:
            return parser(normalized)
        except Exception:
            continue
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
        self.repository = TradingRepository(SessionLocal)
        self.ops_client = httpx.AsyncClient(base_url=self.settings.ops_api_url, timeout=15.0)
        self.selector_client = httpx.AsyncClient(base_url=self.settings.selector_engine_url, timeout=15.0)
        self.portfolio_client = httpx.AsyncClient(
            base_url=self.settings.portfolio_engine_url,
            timeout=20.0,
        )
        self.parser_client = OpenAIParserClient(settings=self.settings)
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
        self._recent_live_order_attempts_by_side: dict[OrderSide, list[datetime]] = {
            OrderSide.BUY: [],
            OrderSide.SELL: [],
        }
        self._instrument_profile_cache: dict[str, InstrumentProfileSnapshot | None] = {}
        self.no_trade_count = 0
        self.blocked_count = 0
        self.submitted_count = 0
        self.stale_data_incident_count = 0
        self.duplicate_order_incident_count = 0
        self.selector_mismatch_incident_count = 0
        self.latest_portfolio_selected_count = 0
        self._last_candidate_selection_reason: str | None = None

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
        await self.parser_client.close()

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
        self._recent_live_order_attempts_by_side = {
            OrderSide.BUY: [],
            OrderSide.SELL: [],
        }
        self._instrument_profile_cache = {}
        self.no_trade_count = 0
        self.blocked_count = 0
        self.submitted_count = 0
        self.stale_data_incident_count = 0
        self.duplicate_order_incident_count = 0
        self.selector_mismatch_incident_count = 0
        self.latest_portfolio_selected_count = 0
        self._last_candidate_selection_reason = None

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

    @staticmethod
    def _is_candidate_expired(candidate: TradeCandidate, *, as_of_utc: datetime | None = None) -> bool:
        return candidate.expire_ts_utc <= (as_of_utc or datetime.now(UTC))

    @staticmethod
    def _candidate_expiry_reason(candidate: TradeCandidate) -> str:
        return (
            f"TRADE_CANDIDATE_EXPIRED:side={candidate.side.value},"
            f"expired_at={candidate.expire_ts_utc.isoformat()}"
        )

    def _is_live_symbol_excluded(self, symbol: str | None) -> bool:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return False
        return normalized_symbol in set(self.settings.kis_live_excluded_symbol_list)

    def _prune_live_order_attempts(
        self,
        *,
        side: OrderSide,
        as_of_utc: datetime | None = None,
    ) -> list[datetime]:
        window_seconds = max(int(self.settings.shadow_live_side_rate_limit_window_seconds or 0), 0)
        attempts = self._recent_live_order_attempts_by_side.setdefault(side, [])
        if window_seconds <= 0:
            return attempts
        cutoff = (as_of_utc or datetime.now(UTC)) - timedelta(seconds=window_seconds)
        retained = [attempted_at for attempted_at in attempts if attempted_at > cutoff]
        self._recent_live_order_attempts_by_side[side] = retained
        return retained

    def _side_rate_limit_reason(
        self,
        *,
        side: OrderSide,
        as_of_utc: datetime | None = None,
    ) -> str | None:
        max_orders = max(int(self.settings.shadow_live_side_rate_limit_max_orders or 0), 0)
        window_seconds = max(int(self.settings.shadow_live_side_rate_limit_window_seconds or 0), 0)
        if max_orders <= 0 or window_seconds <= 0:
            return None
        attempts = self._prune_live_order_attempts(side=side, as_of_utc=as_of_utc)
        if len(attempts) < max_orders:
            return None
        return (
            f"ORDER_SIDE_RATE_LIMIT_EXCEEDED:side={side.value},window_seconds={window_seconds},"
            f"current_count={len(attempts)},max_count={max_orders}"
        )

    def _record_live_order_attempt(
        self,
        *,
        side: OrderSide,
        attempted_at_utc: datetime | None = None,
    ) -> None:
        attempted_at = attempted_at_utc or datetime.now(UTC)
        attempts = self._prune_live_order_attempts(side=side, as_of_utc=attempted_at)
        attempts.append(attempted_at)

    def _entry_pause_blocks_candidate(
        self,
        *,
        candidate: TradeCandidate,
        risk_state: dict[str, Any] | None,
    ) -> bool:
        return candidate.side != OrderSide.SELL and bool((risk_state or {}).get("entry_paused"))

    def _exit_strategy_id(self) -> str:
        try:
            store.reload_state()
            strategy = store.strategies.get("close-only-defense")
            if strategy is not None and strategy.enabled:
                return strategy.strategy_id
        except Exception:
            pass
        return "disclosure-alpha"

    @staticmethod
    def _balance_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows = payload.get("output1") or []
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    def _position_inventory_snapshots(self, *, account_id: str) -> dict[str, PositionInventorySnapshot]:
        try:
            fills = trading_core_service.repository.list_execution_fills(limit=2000)
        except Exception:
            return {}
        fills_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for fill in fills:
            if str(fill.get("account_uid") or account_id) != account_id:
                continue
            symbol = str(fill.get("instrument_id") or "").strip()
            if not symbol:
                continue
            fills_by_symbol.setdefault(symbol, []).append(fill)

        snapshots: dict[str, PositionInventorySnapshot] = {}
        today = _today_for_timezone(self.settings.app_timezone)
        for symbol, symbol_fills in fills_by_symbol.items():
            lots: list[list[Any]] = []
            ordered_fills = sorted(
                symbol_fills,
                key=lambda item: _ensure_utc_dt(_parse_snapshot_dt(item.get("fill_ts_utc"))) or datetime.now(UTC),
            )
            for fill in ordered_fills:
                side_code = str(fill.get("side_code") or "BUY").upper()
                qty = _safe_int(fill.get("fill_qty")) or 0
                if qty <= 0:
                    continue
                fill_ts = _ensure_utc_dt(_parse_snapshot_dt(fill.get("fill_ts_utc"))) or datetime.now(UTC)
                fill_price = _safe_float(fill.get("fill_price"))
                if side_code == "BUY":
                    lots.append([fill_ts, qty, fill_price])
                    continue
                remaining_qty = qty
                while remaining_qty > 0 and lots:
                    head_ts, head_qty, head_price = lots[0]
                    consumed_qty = min(head_qty, remaining_qty)
                    remaining_qty -= consumed_qty
                    head_qty -= consumed_qty
                    if head_qty <= 0:
                        lots.pop(0)
                    else:
                        lots[0] = [head_ts, head_qty, head_price]

            net_qty = sum(int(lot[1]) for lot in lots)
            if net_qty <= 0:
                continue
            total_cost = 0.0
            cost_qty = 0
            first_entry_ts_utc: datetime | None = None
            for lot_ts, lot_qty, lot_price in lots:
                if first_entry_ts_utc is None or lot_ts < first_entry_ts_utc:
                    first_entry_ts_utc = lot_ts
                if lot_price is None:
                    continue
                total_cost += float(lot_price) * int(lot_qty)
                cost_qty += int(lot_qty)
            avg_cost_krw = round(total_cost / cost_qty, 2) if cost_qty > 0 else None
            holding_days: int | None = None
            if first_entry_ts_utc is not None:
                try:
                    local_entry_date = first_entry_ts_utc.astimezone(ZoneInfo(self.settings.app_timezone)).date()
                except Exception:
                    local_entry_date = first_entry_ts_utc.date()
                holding_days = max((today - local_entry_date).days, 0)
            snapshots[symbol] = PositionInventorySnapshot(
                net_qty=net_qty,
                avg_cost_krw=avg_cost_krw,
                holding_days=holding_days,
                first_entry_ts_utc=first_entry_ts_utc,
            )
        return snapshots

    def _position_holding_days_from_row(self, row: dict[str, Any]) -> int | None:
        for key in ("buy_dt", "pchs_dt", "loan_dt", "stck_buy_dt", "trad_dt"):
            parsed = _parse_date_value(row.get(key))
            if parsed is None:
                continue
            return max((_today_for_timezone(self.settings.app_timezone) - parsed).days, 0)
        return None

    def _balance_position_metrics(
        self,
        *,
        row: dict[str, Any],
        inventory: PositionInventorySnapshot | None,
    ) -> dict[str, Any] | None:
        symbol = str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip()
        qty = _safe_int(_first_present(row, "hldg_qty", "hold_qty", "cblc_qty", "ord_psbl_qty"))
        if not symbol or qty is None or qty <= 0:
            return None
        avg_cost_krw = inventory.avg_cost_krw if inventory is not None else None
        if avg_cost_krw is None:
            avg_cost_krw = _safe_float(
                _first_present(row, "pchs_avg_pric", "avg_pchs_unpr", "pchs_avg_unpr", "pchs_unpr")
            )
        current_price_krw = _safe_float(_first_present(row, "prpr", "stck_prpr", "now_pric", "cur_prc"))
        market_value_krw = _safe_float(
            _first_present(row, "evlu_amt", "evlu_amt2", "evlu_amt_smtl", "stck_evlu_amt")
        )
        if current_price_krw is None and market_value_krw is not None and qty > 0:
            current_price_krw = round(market_value_krw / qty, 2)
        if market_value_krw is None and current_price_krw is not None:
            market_value_krw = round(current_price_krw * qty, 2)
        return_pct = _safe_float(
            _first_present(row, "evlu_pfls_rt", "evlu_erng_rt", "prft_rate", "sfts_pfls_rt")
        )
        if return_pct is None and avg_cost_krw and avg_cost_krw > 0 and current_price_krw is not None:
            return_pct = round(((current_price_krw - avg_cost_krw) / avg_cost_krw) * 100.0, 4)
        holding_days = inventory.holding_days if inventory is not None else self._position_holding_days_from_row(row)
        return {
            "symbol": symbol,
            "qty": qty,
            "avg_cost_krw": avg_cost_krw,
            "current_price_krw": current_price_krw,
            "market_value_krw": market_value_krw,
            "return_pct": return_pct,
            "holding_days": holding_days,
        }

    def _build_exit_signal(
        self,
        *,
        symbol: str,
        qty: int,
        target_qty: int,
        reference_price_krw: float | None,
        avg_cost_krw: float | None,
        return_pct: float | None,
        holding_days: int | None,
        exit_reason_code: str,
        matched_rule_id: str,
        selection_reason: str,
        priority_score: float,
        expected_edge_bps: float,
        selection_confidence: float,
        source_signal_refs: list[str] | None = None,
        sector_name: str | None = None,
        thematic_tags: list[str] | None = None,
        cross_asset_impact_score: float | None = None,
        thematic_alignment_score: float | None = None,
        macro_headwind_score: float | None = None,
    ) -> ExitCandidateSignal:
        effective_price_krw = reference_price_krw if reference_price_krw is not None else (avg_cost_krw or 1.0)
        target_notional_krw = max(int(round(effective_price_krw * target_qty)), 1)
        candidate = TradeCandidate(
            candidate_id=f"exit-{symbol}-{uuid4().hex[:10]}",
            strategy_id=self._exit_strategy_id(),
            account_scope=self.settings.selector_default_account_scope,
            instrument_id=symbol,
            side=OrderSide.SELL,
            expected_edge_bps=expected_edge_bps,
            target_notional_krw=target_notional_krw,
            entry_style="EXIT_LIMIT",
            expire_ts_utc=datetime.now(UTC) + timedelta(minutes=10),
            meta_model_version="exit-engine-v4",
            source_signal_refs=list(source_signal_refs or [exit_reason_code]),
            matched_rule_id=matched_rule_id,
            selection_reason=selection_reason,
            candidate_status="SELECTED_EXIT",
            ranking_score=priority_score,
            ranking_reason=selection_reason,
            selection_confidence=selection_confidence,
            expected_slippage_bps=10.0,
            tail_risk_penalty_bps=0.0,
            crowding_penalty_bps=0.0,
            cooldown_key=f"{symbol}:EXIT",
            candidate_family="EXIT",
            exit_reason_code=exit_reason_code,
            target_qty_override=target_qty,
            position_qty=qty,
            position_avg_cost_krw=avg_cost_krw,
            position_return_pct=return_pct,
            holding_days=holding_days,
            sector_name=sector_name,
            thematic_tags=list(thematic_tags or []),
            cross_asset_impact_score=cross_asset_impact_score,
            thematic_alignment_score=thematic_alignment_score,
            macro_headwind_score=macro_headwind_score,
        )
        return ExitCandidateSignal(candidate=candidate, priority_score=priority_score)

    def _load_persisted_instrument_profile(self, symbol: str) -> InstrumentProfileSnapshot | None:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return None
        if normalized_symbol in self._instrument_profile_cache:
            return self._instrument_profile_cache[normalized_symbol]
        try:
            profile = self.repository.get_instrument_profile(normalized_symbol)
        except Exception:
            profile = None
        self._instrument_profile_cache[normalized_symbol] = profile
        return profile

    @staticmethod
    def _merge_thematic_tags(primary: list[str] | None, secondary: list[str] | None) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for group in (primary or [], secondary or []):
            normalized = str(group or "").strip()
            key = normalized.lower()
            if not normalized or key in seen:
                continue
            seen.add(key)
            merged.append(normalized)
        return merged

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(value, upper))

    def _latest_exit_context_maps(self) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, float]], dict[str, float]]:
        try:
            price_context_by_symbol = feature_pipeline_service._price_context_by_symbol(data_ingest_service.latest_price_bars())
        except Exception:
            price_context_by_symbol = {}
        try:
            news_context_by_symbol = feature_pipeline_service._news_context_by_symbol(data_ingest_service.latest_news_items())
        except Exception:
            news_context_by_symbol = {}
        try:
            macro_context = feature_pipeline_service._macro_context(data_ingest_service.latest_macro_points())
        except Exception:
            macro_context = {}
        return price_context_by_symbol, news_context_by_symbol, macro_context

    def _resolve_macro_profile_overlay(
        self,
        *,
        symbol: str,
        live_candidate: TradeCandidate | None,
    ) -> dict[str, Any] | None:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return None

        sector_name = live_candidate.sector_name if live_candidate is not None else None
        thematic_tags = list(live_candidate.thematic_tags or []) if live_candidate is not None else []
        cross_asset = (
            float(live_candidate.cross_asset_impact_score)
            if live_candidate is not None and live_candidate.cross_asset_impact_score is not None
            else None
        )
        thematic_alignment = (
            float(live_candidate.thematic_alignment_score)
            if live_candidate is not None and live_candidate.thematic_alignment_score is not None
            else None
        )
        macro_headwind = (
            float(live_candidate.macro_headwind_score)
            if live_candidate is not None and live_candidate.macro_headwind_score is not None
            else None
        )
        source = "LIVE_CANDIDATE" if live_candidate is not None else None

        persisted_profile = self._load_persisted_instrument_profile(normalized_symbol)
        profile_confidence = None
        profile_used_fallback = None
        profile_updated_at_utc = None

        if persisted_profile is not None:
            if not sector_name:
                sector_name = persisted_profile.sector_name
            thematic_tags = self._merge_thematic_tags(thematic_tags, persisted_profile.thematic_tags)
            profile_confidence = persisted_profile.confidence_score
            profile_used_fallback = persisted_profile.used_fallback
            profile_updated_at_utc = persisted_profile.updated_at_utc

            needs_overlay_scores = any(
                value is None for value in (cross_asset, thematic_alignment, macro_headwind)
            )
            macro_points = data_ingest_service.latest_macro_points()
            if needs_overlay_scores and macro_points:
                try:
                    overlay = score_instrument_profile_against_macro_points(
                        profile_payload=persisted_profile.market_profile_payload(),
                        macro_points=macro_points,
                    )
                except Exception:
                    overlay = None
                if overlay is not None:
                    if cross_asset is None:
                        cross_asset = float(overlay["cross_asset_impact_score"])
                    if thematic_alignment is None:
                        thematic_alignment = float(overlay["thematic_alignment_score"])
                    if macro_headwind is None:
                        macro_headwind = float(overlay["macro_headwind_score"])
                    if not sector_name:
                        sector_name = overlay.get("sector_name")
                    thematic_tags = self._merge_thematic_tags(
                        thematic_tags,
                        overlay.get("thematic_tags") if isinstance(overlay.get("thematic_tags"), list) else [],
                    )
                    source = "HYBRID_PROFILE" if source == "LIVE_CANDIDATE" else "DB_PROFILE"

        if (
            sector_name is None
            and not thematic_tags
            and cross_asset is None
            and thematic_alignment is None
            and macro_headwind is None
        ):
            return None
        return {
            "sector_name": sector_name,
            "thematic_tags": thematic_tags,
            "cross_asset_impact_score": cross_asset,
            "thematic_alignment_score": thematic_alignment,
            "macro_headwind_score": macro_headwind,
            "profile_source": source,
            "profile_confidence": profile_confidence,
            "profile_used_fallback": profile_used_fallback,
            "profile_updated_at_utc": profile_updated_at_utc,
        }

    def _build_exit_candidate_from_balance_row(
        self,
        *,
        row: dict[str, Any],
        inventory: PositionInventorySnapshot | None,
    ) -> ExitCandidateSignal | None:
        metrics = self._balance_position_metrics(row=row, inventory=inventory)
        if metrics is None:
            return None
        symbol = str(metrics["symbol"])
        qty = int(metrics["qty"])
        avg_cost_krw = metrics["avg_cost_krw"]
        current_price_krw = metrics["current_price_krw"]
        market_value_krw = metrics["market_value_krw"]
        return_pct = metrics["return_pct"]
        holding_days = metrics["holding_days"]
        min_position_value_krw = max(int(self.settings.shadow_live_exit_min_position_value_krw or 0), 0)
        if market_value_krw is not None and market_value_krw < min_position_value_krw:
            return None

        stop_loss_pct = max(float(self.settings.shadow_live_exit_stop_loss_pct or 0.0), 0.1)
        take_profit_pct = max(float(self.settings.shadow_live_exit_take_profit_pct or 0.0), 0.1)
        take_profit_fraction = min(
            max(float(self.settings.shadow_live_exit_take_profit_fraction or 0.0), 0.1),
            1.0,
        )
        max_holding_days = max(
            int(self.settings.shadow_live_exit_max_holding_days or self.settings.selector_default_holding_days or 1),
            1,
        )
        time_stop_min_return_pct = float(self.settings.shadow_live_exit_time_stop_min_return_pct or 0.0)

        exit_reason_code: str | None = None
        matched_rule_id: str | None = None
        exit_fraction = 1.0
        priority_score = 0.0
        expected_edge_bps = 55.0
        selection_confidence = 0.75

        if return_pct is not None and return_pct <= -(stop_loss_pct * 1.75):
            exit_reason_code = "EXIT_STOP_LOSS_HARD"
            matched_rule_id = "exit.stop_loss.hard"
            priority_score = 140.0 + abs(return_pct)
            expected_edge_bps = 95.0
            selection_confidence = 0.97
        elif return_pct is not None and return_pct <= -stop_loss_pct:
            exit_reason_code = "EXIT_STOP_LOSS"
            matched_rule_id = "exit.stop_loss"
            priority_score = 120.0 + abs(return_pct)
            expected_edge_bps = 82.0
            selection_confidence = 0.93
        elif holding_days is not None and holding_days >= max_holding_days and (
            return_pct is None or return_pct <= time_stop_min_return_pct
        ):
            exit_reason_code = "EXIT_TIME_STOP"
            matched_rule_id = "exit.time_stop"
            priority_score = 92.0 + min(holding_days, 30)
            expected_edge_bps = 60.0
            selection_confidence = 0.8
        elif return_pct is not None and return_pct >= take_profit_pct:
            exit_reason_code = "EXIT_TAKE_PROFIT"
            matched_rule_id = "exit.take_profit"
            exit_fraction = take_profit_fraction
            priority_score = 86.0 + min(return_pct, 25.0)
            expected_edge_bps = 64.0
            selection_confidence = 0.84
        elif holding_days is not None and holding_days >= max_holding_days and return_pct is not None:
            exit_reason_code = "EXIT_PROFIT_LOCK"
            matched_rule_id = "exit.profit_lock"
            exit_fraction = max(take_profit_fraction, 0.5)
            priority_score = 74.0 + min(return_pct, 20.0)
            expected_edge_bps = 58.0
            selection_confidence = 0.78

        if exit_reason_code is None:
            return None

        target_qty = max(1, min(qty, int(round(qty * exit_fraction))))
        reference_price_krw = current_price_krw or avg_cost_krw or (market_value_krw / qty if market_value_krw else None)

        detail_parts = [f"symbol={symbol}", f"qty={qty}", f"exit_qty={target_qty}"]
        if return_pct is not None:
            detail_parts.append(f"return_pct={return_pct:.2f}")
        if avg_cost_krw is not None:
            detail_parts.append(f"avg_cost_krw={avg_cost_krw:.2f}")
        if holding_days is not None:
            detail_parts.append(f"holding_days={holding_days}")
        selection_reason = f"{exit_reason_code}: " + ", ".join(detail_parts)

        return self._build_exit_signal(
            symbol=symbol,
            qty=qty,
            target_qty=target_qty,
            reference_price_krw=reference_price_krw,
            avg_cost_krw=avg_cost_krw,
            return_pct=return_pct,
            holding_days=holding_days,
            exit_reason_code=exit_reason_code,
            matched_rule_id=matched_rule_id,
            selection_reason=selection_reason,
            priority_score=priority_score,
            expected_edge_bps=expected_edge_bps,
            selection_confidence=selection_confidence,
            source_signal_refs=[exit_reason_code],
        )

    def _build_macro_headwind_exit_signal(
        self,
        *,
        row: dict[str, Any],
        inventory: PositionInventorySnapshot | None,
        live_candidate: TradeCandidate | None,
    ) -> ExitCandidateSignal | None:
        metrics = self._balance_position_metrics(row=row, inventory=inventory)
        if metrics is None:
            return None
        symbol = str(metrics["symbol"])
        qty = int(metrics["qty"])
        avg_cost_krw = metrics["avg_cost_krw"]
        current_price_krw = metrics["current_price_krw"]
        market_value_krw = metrics["market_value_krw"]
        return_pct = metrics["return_pct"]
        holding_days = metrics["holding_days"]
        reference_price_krw = current_price_krw or avg_cost_krw or (market_value_krw / qty if market_value_krw else None)
        if reference_price_krw is None:
            reference_price_krw = 1.0

        overlay = self._resolve_macro_profile_overlay(symbol=symbol, live_candidate=live_candidate)
        if overlay is None:
            return None
        macro_headwind_raw = overlay.get("macro_headwind_score")
        cross_asset_raw = overlay.get("cross_asset_impact_score")
        thematic_alignment_raw = overlay.get("thematic_alignment_score")
        if macro_headwind_raw is None or cross_asset_raw is None or thematic_alignment_raw is None:
            return None
        macro_headwind = float(macro_headwind_raw)
        cross_asset = float(cross_asset_raw)
        thematic_alignment = float(thematic_alignment_raw)
        live_edge_bps = float(live_candidate.expected_edge_bps or 0.0) if live_candidate is not None else 0.0
        live_confidence = float(live_candidate.selection_confidence or 0.0) if live_candidate is not None else 0.0
        profile_source = str(overlay.get("profile_source") or ("LIVE_CANDIDATE" if live_candidate is not None else "DB_PROFILE"))
        profile_confidence = (
            float(overlay["profile_confidence"])
            if overlay.get("profile_confidence") is not None
            else 0.0
        )
        profile_used_fallback = bool(overlay.get("profile_used_fallback"))
        if macro_headwind < 0.45:
            return None

        exit_reason_code: str | None = None
        matched_rule_id: str | None = None
        exit_fraction = 0.0
        priority_score = 0.0
        expected_edge_bps = 58.0
        selection_confidence = max(live_confidence, 0.72)

        if live_candidate is not None:
            if macro_headwind >= 0.7 and cross_asset <= -0.35 and (live_edge_bps <= 20.0 or live_confidence < 0.62):
                exit_reason_code = "EXIT_MACRO_HEADWIND_FULL"
                matched_rule_id = "exit.macro_headwind.full"
                exit_fraction = 1.0
                priority_score = 100.0 + (macro_headwind * 20.0) + max(-cross_asset, 0.0) * 10.0
                expected_edge_bps = 72.0
                selection_confidence = max(selection_confidence, 0.86)
            elif macro_headwind >= 0.55 and (cross_asset <= -0.2 or thematic_alignment <= 0.15) and live_edge_bps <= 40.0:
                exit_reason_code = "EXIT_MACRO_HEADWIND_TRIM"
                matched_rule_id = "exit.macro_headwind.trim"
                exit_fraction = 0.5 if live_edge_bps <= 25.0 else 0.35
                priority_score = 76.0 + (macro_headwind * 16.0) + max(-cross_asset, 0.0) * 8.0
                expected_edge_bps = 61.0
                selection_confidence = max(selection_confidence, 0.78)
        else:
            if profile_used_fallback and profile_confidence < 0.25:
                return None
            holding_days_value = max(holding_days or 0, 0)
            return_pct_value = float(return_pct or 0.0)
            confidence_buffer = 0.05 if profile_used_fallback else 0.0
            if (
                macro_headwind >= (0.78 + confidence_buffer)
                and cross_asset <= -0.35
                and (holding_days_value >= 2 or return_pct_value <= 1.0)
            ):
                exit_reason_code = "EXIT_MACRO_PROFILE_FULL"
                matched_rule_id = "exit.macro_profile.full"
                exit_fraction = 1.0
                priority_score = 94.0 + (macro_headwind * 18.0) + max(-cross_asset, 0.0) * 12.0
                expected_edge_bps = 68.0
                selection_confidence = max(profile_confidence, 0.74 if not profile_used_fallback else 0.68)
            elif (
                macro_headwind >= (0.6 + confidence_buffer)
                and cross_asset <= -0.22
                and (holding_days_value >= 1 or return_pct_value <= 4.0)
            ):
                exit_reason_code = "EXIT_MACRO_PROFILE_TRIM"
                matched_rule_id = "exit.macro_profile.trim"
                exit_fraction = 0.5 if return_pct_value <= 1.5 else 0.35
                priority_score = 72.0 + (macro_headwind * 14.0) + max(-cross_asset, 0.0) * 10.0
                expected_edge_bps = 57.0
                selection_confidence = max(profile_confidence, 0.64 if not profile_used_fallback else 0.58)

        if exit_reason_code is None:
            return None

        target_qty = max(1, min(qty, int(round(qty * exit_fraction))))
        selection_reason = (
            f"{exit_reason_code}: symbol={symbol}, current_qty={qty}, exit_qty={target_qty}, "
            f"macro_headwind={macro_headwind:.2f}, cross_asset={cross_asset:+.2f}, "
            f"thematic_alignment={thematic_alignment:.2f}, live_edge_bps={live_edge_bps:.2f}, "
            f"live_confidence={live_confidence:.2f}, profile_source={profile_source}, "
            f"profile_confidence={profile_confidence:.2f}"
        )
        return self._build_exit_signal(
            symbol=symbol,
            qty=qty,
            target_qty=target_qty,
            reference_price_krw=reference_price_krw,
            avg_cost_krw=avg_cost_krw,
            return_pct=return_pct,
            holding_days=holding_days,
            exit_reason_code=exit_reason_code,
            matched_rule_id=matched_rule_id,
            selection_reason=selection_reason,
            priority_score=priority_score,
            expected_edge_bps=expected_edge_bps,
            selection_confidence=selection_confidence,
            source_signal_refs=(
                [exit_reason_code, live_candidate.candidate_id]
                if live_candidate is not None
                else [exit_reason_code, f"profile:{symbol}"]
            ),
            sector_name=overlay.get("sector_name"),
            thematic_tags=overlay.get("thematic_tags") if isinstance(overlay.get("thematic_tags"), list) else [],
            cross_asset_impact_score=cross_asset,
            thematic_alignment_score=thematic_alignment,
            macro_headwind_score=macro_headwind,
        )

    async def _build_gemini_exit_overlay_signal(
        self,
        *,
        row: dict[str, Any],
        inventory: PositionInventorySnapshot | None,
        live_candidate: TradeCandidate | None,
        price_context: dict[str, float] | None,
        news_context: dict[str, float] | None,
        macro_context: dict[str, float] | None,
    ) -> ExitCandidateSignal | None:
        metrics = self._balance_position_metrics(row=row, inventory=inventory)
        if metrics is None:
            return None
        symbol = str(metrics["symbol"])
        qty = int(metrics["qty"])
        avg_cost_krw = metrics["avg_cost_krw"]
        current_price_krw = metrics["current_price_krw"]
        market_value_krw = metrics["market_value_krw"]
        return_pct = metrics["return_pct"]
        holding_days = metrics["holding_days"]
        min_position_value_krw = max(int(self.settings.shadow_live_exit_min_position_value_krw or 0), 0)
        if market_value_krw is not None and market_value_krw < min_position_value_krw:
            return None

        overlay = self._resolve_macro_profile_overlay(symbol=symbol, live_candidate=live_candidate)
        macro_headwind = (
            float(overlay["macro_headwind_score"])
            if overlay is not None and overlay.get("macro_headwind_score") is not None
            else float(macro_context.get("macro_signal", 0.0) if macro_context else 0.0)
        )
        cross_asset = (
            float(overlay["cross_asset_impact_score"])
            if overlay is not None and overlay.get("cross_asset_impact_score") is not None
            else 0.0
        )
        thematic_alignment = (
            float(overlay["thematic_alignment_score"])
            if overlay is not None and overlay.get("thematic_alignment_score") is not None
            else 0.0
        )
        price_context = dict(price_context or {})
        news_context = dict(news_context or {})
        signal_context = {
            "surprise_score": self._clamp(float((live_candidate.expected_edge_bps or 0.0) / 90.0 if live_candidate is not None else 0.35), 0.0, 1.0),
            "follow_through_score": self._clamp(
                max(
                    0.0,
                    float(price_context.get("momentum_composite_score", 0.5))
                    - (float(price_context.get("signal_decay_score", 0.2)) * 0.35),
                ),
                0.0,
                1.0,
            ),
            "tail_risk_score": self._clamp(
                float((live_candidate.tail_risk_penalty_bps or 0.0) / 45.0 if live_candidate is not None else 0.0)
                + (macro_headwind * 0.55),
                0.0,
                1.0,
            ),
            "crowding_score": self._clamp(
                float((live_candidate.crowding_penalty_bps or 0.0) / 30.0 if live_candidate is not None else 0.0)
                + (float(price_context.get("breakout_score", 0.0)) * 0.3),
                0.0,
                1.0,
            ),
            "liquidity_score": self._clamp(
                1.0 - float(price_context.get("illiquidity_score", 0.2)),
                0.0,
                1.0,
            ),
            "signal_decay_score": self._clamp(float(price_context.get("signal_decay_score", 0.2)), 0.0, 1.0),
        }
        decision = await self.parser_client.infer_trade_decision_overlay(
            side="SELL",
            instrument_id=symbol,
            event_family=live_candidate.matched_rule_id if live_candidate is not None else "POSITION_EXIT",
            event_type=live_candidate.exit_reason_code if live_candidate is not None else "POSITION_EXIT",
            summary=live_candidate.selection_reason if live_candidate is not None else None,
            sector_name=(overlay.get("sector_name") if overlay is not None else (live_candidate.sector_name if live_candidate is not None else None)),
            thematic_tags=(
                overlay.get("thematic_tags")
                if overlay is not None and isinstance(overlay.get("thematic_tags"), list)
                else (list(live_candidate.thematic_tags or []) if live_candidate is not None else [])
            ),
            price_context=price_context,
            news_context=news_context,
            macro_context={
                "cross_asset_impact_score": cross_asset,
                "thematic_alignment_score": thematic_alignment,
                "macro_headwind_score": macro_headwind,
                **(macro_context or {}),
            },
            signal_context=signal_context,
            position_context={
                "return_pct": return_pct,
                "holding_days": holding_days,
                "live_edge_bps": float(live_candidate.expected_edge_bps or 0.0) if live_candidate is not None else 0.0,
                "live_confidence": float(live_candidate.selection_confidence or 0.0) if live_candidate is not None else 0.0,
                "position_qty": qty,
            },
        )
        if not (decision.hard_block or decision.action_bias in {"TRIM", "EXIT"} or decision.exit_urgency_score >= 0.62):
            return None

        exit_fraction = (
            1.0
            if decision.hard_block or decision.action_bias == "EXIT" or decision.exit_urgency_score >= 0.82
            else 0.5
            if decision.exit_urgency_score >= 0.68
            else 0.35
        )
        target_qty = max(1, min(qty, int(round(qty * exit_fraction))))
        reference_price_krw = current_price_krw or avg_cost_krw or (market_value_krw / qty if market_value_krw else 1.0)
        priority_score = (
            68.0
            + (decision.exit_urgency_score * 42.0)
            + (decision.signal_decay_score * 12.0)
            + max(-(return_pct or 0.0), 0.0) * 0.8
            + max((holding_days or 0) - self.settings.selector_default_holding_days, 0) * 1.2
        )
        expected_edge_bps = max(
            56.0,
            48.0 + (decision.exit_urgency_score * 36.0) + max(decision.alpha_adjust_bps, 0.0) * 0.4,
        )
        selection_confidence = self._clamp(
            0.58 + (decision.confidence * 0.4) + (decision.exit_urgency_score * 0.08),
            0.58,
            0.97,
        )
        exit_reason_code = (
            "EXIT_GEMINI_THESIS_BREAK"
            if exit_fraction >= 1.0
            else "EXIT_GEMINI_TRIM"
        )
        matched_rule_id = (
            "exit.gemini.thesis_break"
            if exit_fraction >= 1.0
            else "exit.gemini.trim"
        )
        selection_reason = (
            f"{exit_reason_code}: symbol={symbol}, current_qty={qty}, exit_qty={target_qty}, "
            f"return_pct={float(return_pct or 0.0):.2f}, holding_days={int(holding_days or 0)}, "
            f"urgency={decision.exit_urgency_score:.2f}, action={decision.action_bias}, "
            f"macro_headwind={macro_headwind:.2f}, rationale={decision.rationale}"
        )
        return self._build_exit_signal(
            symbol=symbol,
            qty=qty,
            target_qty=target_qty,
            reference_price_krw=reference_price_krw,
            avg_cost_krw=avg_cost_krw,
            return_pct=return_pct,
            holding_days=holding_days,
            exit_reason_code=exit_reason_code,
            matched_rule_id=matched_rule_id,
            selection_reason=selection_reason,
            priority_score=priority_score,
            expected_edge_bps=expected_edge_bps,
            selection_confidence=selection_confidence,
            source_signal_refs=(
                [exit_reason_code, live_candidate.candidate_id]
                if live_candidate is not None
                else [exit_reason_code, f"position:{symbol}"]
            ),
            sector_name=overlay.get("sector_name") if overlay is not None else (live_candidate.sector_name if live_candidate is not None else None),
            thematic_tags=(
                overlay.get("thematic_tags")
                if overlay is not None and isinstance(overlay.get("thematic_tags"), list)
                else (list(live_candidate.thematic_tags or []) if live_candidate is not None else [])
            ),
            cross_asset_impact_score=cross_asset,
            thematic_alignment_score=thematic_alignment,
            macro_headwind_score=macro_headwind,
        )

    @staticmethod
    def _portfolio_candidate_map(candidates: list[TradeCandidate]) -> dict[str, TradeCandidate]:
        by_symbol: dict[str, TradeCandidate] = {}
        for candidate in candidates:
            symbol = str(candidate.instrument_id or "").strip()
            if not symbol:
                continue
            current = by_symbol.get(symbol)
            if current is None or (candidate.ranking_score or 0.0) > (current.ranking_score or 0.0):
                by_symbol[symbol] = candidate
        return by_symbol

    def _desired_position_qty_from_live_candidate(
        self,
        *,
        candidate: TradeCandidate,
        current_price_krw: float | None,
    ) -> int:
        if candidate.target_notional_krw <= 0:
            return 0
        effective_price_krw = current_price_krw or float(max(self.settings.trading_proxy_price_krw, 1))
        normalized_price_krw = max(int(round(effective_price_krw)), 1)
        desired_qty = candidate.target_notional_krw // normalized_price_krw
        if desired_qty > 0:
            return int(desired_qty)
        overshoot_tolerance = max(self.settings.trading_single_share_overshoot_tolerance_pct, 0.0) / 100.0
        affordable_limit = candidate.target_notional_krw * (1.0 + overshoot_tolerance)
        if normalized_price_krw <= affordable_limit:
            return 1
        return 0

    def _build_portfolio_alignment_exit_signal(
        self,
        *,
        row: dict[str, Any],
        inventory: PositionInventorySnapshot | None,
        live_candidate: TradeCandidate | None,
    ) -> ExitCandidateSignal | None:
        metrics = self._balance_position_metrics(row=row, inventory=inventory)
        if metrics is None:
            return None
        symbol = str(metrics["symbol"])
        qty = int(metrics["qty"])
        avg_cost_krw = metrics["avg_cost_krw"]
        current_price_krw = metrics["current_price_krw"]
        market_value_krw = metrics["market_value_krw"]
        return_pct = metrics["return_pct"]
        holding_days = metrics["holding_days"]
        reference_price_krw = current_price_krw or avg_cost_krw or (market_value_krw / qty if market_value_krw else None)
        if reference_price_krw is None:
            reference_price_krw = 1.0
        overlay = self._resolve_macro_profile_overlay(symbol=symbol, live_candidate=live_candidate)
        sector_name = overlay.get("sector_name") if overlay is not None else (live_candidate.sector_name if live_candidate is not None else None)
        thematic_tags = (
            overlay.get("thematic_tags")
            if overlay is not None and isinstance(overlay.get("thematic_tags"), list)
            else (list(live_candidate.thematic_tags or []) if live_candidate is not None else [])
        )
        cross_asset = (
            float(overlay["cross_asset_impact_score"])
            if overlay is not None and overlay.get("cross_asset_impact_score") is not None
            else (live_candidate.cross_asset_impact_score if live_candidate is not None else None)
        )
        thematic_alignment = (
            float(overlay["thematic_alignment_score"])
            if overlay is not None and overlay.get("thematic_alignment_score") is not None
            else (live_candidate.thematic_alignment_score if live_candidate is not None else None)
        )
        macro_headwind = (
            float(overlay["macro_headwind_score"])
            if overlay is not None and overlay.get("macro_headwind_score") is not None
            else (live_candidate.macro_headwind_score if live_candidate is not None else None)
        )

        if live_candidate is None:
            priority_score = 106.0 + max(-min(return_pct or 0.0, 0.0), 0.0) + min(max(holding_days or 0, 0), 20) * 0.3
            detail_parts = [f"symbol={symbol}", f"qty={qty}"]
            if return_pct is not None:
                detail_parts.append(f"return_pct={return_pct:.2f}")
            if holding_days is not None:
                detail_parts.append(f"holding_days={holding_days}")
            selection_reason = (
                "EXIT_PORTFOLIO_REMOVED: "
                + ", ".join(detail_parts)
                + ", live_portfolio=absent"
            )
            return self._build_exit_signal(
                symbol=symbol,
                qty=qty,
                target_qty=qty,
                reference_price_krw=reference_price_krw,
                avg_cost_krw=avg_cost_krw,
                return_pct=return_pct,
                holding_days=holding_days,
                exit_reason_code="EXIT_PORTFOLIO_REMOVED",
                matched_rule_id="exit.portfolio_removed",
                selection_reason=selection_reason,
                priority_score=priority_score,
                expected_edge_bps=76.0,
                selection_confidence=0.9,
                source_signal_refs=["EXIT_PORTFOLIO_REMOVED"],
                sector_name=sector_name,
                thematic_tags=thematic_tags,
                cross_asset_impact_score=cross_asset,
                thematic_alignment_score=thematic_alignment,
                macro_headwind_score=macro_headwind,
            )

        desired_qty = self._desired_position_qty_from_live_candidate(
            candidate=live_candidate,
            current_price_krw=current_price_krw,
        )
        if desired_qty >= qty:
            return None

        exit_qty = max(qty - desired_qty, 0)
        if exit_qty <= 0:
            return None
        excess_ratio = exit_qty / max(qty, 1)
        priority_score = 82.0 + (excess_ratio * 34.0)
        if return_pct is not None and return_pct < 0:
            priority_score += min(abs(return_pct), 12.0) * 0.4
        matched_rule_id = "exit.rebalance.scale_down" if desired_qty > 0 else "exit.rebalance.full"
        exit_reason_code = "EXIT_REBALANCE_SCALE_DOWN" if desired_qty > 0 else "EXIT_REBALANCE_ZERO_TARGET"
        selection_reason = (
            f"{exit_reason_code}: symbol={symbol}, current_qty={qty}, desired_qty={desired_qty}, "
            f"exit_qty={exit_qty}, live_target_notional_krw={live_candidate.target_notional_krw:,}, "
            f"live_confidence={(live_candidate.selection_confidence or 0.0):.2f}"
        )
        return self._build_exit_signal(
            symbol=symbol,
            qty=qty,
            target_qty=exit_qty,
            reference_price_krw=reference_price_krw,
            avg_cost_krw=avg_cost_krw,
            return_pct=return_pct,
            holding_days=holding_days,
            exit_reason_code=exit_reason_code,
            matched_rule_id=matched_rule_id,
            selection_reason=selection_reason,
            priority_score=priority_score,
            expected_edge_bps=max(live_candidate.expected_edge_bps or 55.0, 55.0),
            selection_confidence=max(live_candidate.selection_confidence or 0.72, 0.72),
            source_signal_refs=[exit_reason_code, live_candidate.candidate_id],
            sector_name=sector_name,
            thematic_tags=thematic_tags,
            cross_asset_impact_score=cross_asset,
            thematic_alignment_score=thematic_alignment,
            macro_headwind_score=macro_headwind,
        )

    async def _select_exit_candidate(
        self,
        *,
        live_candidates: list[TradeCandidate] | None = None,
        portfolio_candidates_ok: bool = False,
    ) -> TradeCandidate | None:
        try:
            balance_payload = await self.gateway_client.query_balance()
        except Exception:
            return None
        balance_rows = self._balance_rows(balance_payload)
        if not balance_rows:
            return None
        inventory_snapshots = self._position_inventory_snapshots(
            account_id=self.settings.selector_default_account_scope,
        )
        live_candidate_by_symbol = (
            self._portfolio_candidate_map(live_candidates or [])
            if portfolio_candidates_ok
            else {}
        )
        price_context_by_symbol, news_context_by_symbol, macro_context = self._latest_exit_context_maps()
        exit_signals: list[ExitCandidateSignal] = []
        as_of_utc = datetime.now(UTC)
        for row in balance_rows:
            symbol = str(_first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd") or "").strip()
            if self._is_live_symbol_excluded(symbol):
                continue
            base_signal = self._build_exit_candidate_from_balance_row(
                row=row,
                inventory=inventory_snapshots.get(symbol),
            )
            rebalance_signal = None
            macro_signal = self._build_macro_headwind_exit_signal(
                row=row,
                inventory=inventory_snapshots.get(symbol),
                live_candidate=live_candidate_by_symbol.get(symbol),
            )
            if portfolio_candidates_ok:
                rebalance_signal = self._build_portfolio_alignment_exit_signal(
                    row=row,
                    inventory=inventory_snapshots.get(symbol),
                    live_candidate=live_candidate_by_symbol.get(symbol),
                )
            gemini_signal = await self._build_gemini_exit_overlay_signal(
                row=row,
                inventory=inventory_snapshots.get(symbol),
                live_candidate=live_candidate_by_symbol.get(symbol),
                price_context=price_context_by_symbol.get(symbol),
                news_context=news_context_by_symbol.get(symbol),
                macro_context=macro_context,
            )
            signal_candidates = [item for item in (base_signal, macro_signal, rebalance_signal, gemini_signal) if item is not None]
            if not signal_candidates:
                continue
            signal = max(signal_candidates, key=lambda item: item.priority_score)
            if self._is_candidate_expired(signal.candidate, as_of_utc=as_of_utc):
                continue
            if self._is_on_cooldown(signal.candidate):
                self.duplicate_order_incident_count += 1
                continue
            exit_signals.append(signal)
        if not exit_signals:
            return None
        exit_signals.sort(key=lambda item: item.priority_score, reverse=True)
        return exit_signals[0].candidate

    async def _select_run_candidate(self, *, execute_live: bool) -> tuple[TradeCandidate | None, str | None]:
        self._last_candidate_selection_reason = None
        live_candidates, portfolio_candidates_ok, portfolio_reason = await self._load_live_portfolio_candidates()
        exit_candidate = await self._select_exit_candidate(
            live_candidates=live_candidates,
            portfolio_candidates_ok=portfolio_candidates_ok,
        )
        if exit_candidate is not None:
            return exit_candidate, None
        no_trade_reason = portfolio_reason or "NO_ELIGIBLE_PORTFOLIO_CANDIDATE"
        if execute_live and self.settings.trading_micro_test_mode_enabled:
            candidate, micro_reason = await self._select_micro_test_candidate()
            return candidate, micro_reason or no_trade_reason
        candidate = await self._select_candidate()
        return candidate, self._last_candidate_selection_reason or no_trade_reason

    async def _fetch_portfolio_candidates_with_status(
        self,
        *,
        force_refresh: bool = False,
    ) -> tuple[list[TradeCandidate], bool]:
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
            return candidates, True
        except Exception:
            return [], False

    async def _fetch_portfolio_candidates(
        self,
        *,
        force_refresh: bool = False,
    ) -> list[TradeCandidate]:
        candidates, _ = await self._fetch_portfolio_candidates_with_status(force_refresh=force_refresh)
        return candidates

    async def _fetch_portfolio_selected_count(self) -> int:
        try:
            response = await self.portfolio_client.get("/snapshot")
            response.raise_for_status()
            payload = response.json()
            return int(payload.get("selected_count") or 0)
        except Exception:
            return self.latest_portfolio_selected_count

    async def _load_live_portfolio_candidates(self) -> tuple[list[TradeCandidate], bool, str | None]:
        live_candidates, portfolio_candidates_ok = await self._fetch_portfolio_candidates_with_status(force_refresh=False)
        if not portfolio_candidates_ok:
            return [], False, "PORTFOLIO_CANDIDATE_FETCH_FAILED"
        if live_candidates:
            return live_candidates, True, None
        selected_count = await self._fetch_portfolio_selected_count()
        if selected_count > 0:
            self.selector_mismatch_incident_count += 1
            refreshed_candidates, refreshed_ok = await self._fetch_portfolio_candidates_with_status(force_refresh=True)
            if refreshed_ok and refreshed_candidates:
                return refreshed_candidates, True, None
            return [], True, f"PORTFOLIO_SELECTED_WITHOUT_ACTIONABLE_CANDIDATES:selected_count={selected_count}"
        return [], True, "NO_PORTFOLIO_CANDIDATES"

    def _available_portfolio_candidates(
        self,
        live_candidates: list[TradeCandidate],
    ) -> tuple[list[TradeCandidate], str | None]:
        available_candidates: list[TradeCandidate] = []
        cooldown_count = 0
        expired_count = 0
        excluded_count = 0
        as_of_utc = datetime.now(UTC)
        for candidate in live_candidates:
            if self._is_live_symbol_excluded(candidate.instrument_id):
                excluded_count += 1
                continue
            if self._is_candidate_expired(candidate, as_of_utc=as_of_utc):
                expired_count += 1
                continue
            if self._is_on_cooldown(candidate):
                self.duplicate_order_incident_count += 1
                cooldown_count += 1
                continue
            available_candidates.append(candidate)
        if available_candidates:
            return available_candidates, None
        if live_candidates and expired_count == len(live_candidates):
            return [], f"ALL_PORTFOLIO_CANDIDATES_EXPIRED:count={expired_count}"
        if live_candidates and excluded_count == len(live_candidates):
            return [], f"ALL_PORTFOLIO_CANDIDATES_EXCLUDED:count={excluded_count}"
        if (
            live_candidates
            and excluded_count > 0
            and excluded_count + expired_count + cooldown_count == len(live_candidates)
        ):
            return (
                [],
                (
                    "PORTFOLIO_CANDIDATES_EXCLUDED_OR_UNAVAILABLE:"
                    f"excluded_count={excluded_count},expired_count={expired_count},cooldown_count={cooldown_count}"
                ),
            )
        if live_candidates and expired_count > 0 and expired_count + cooldown_count == len(live_candidates):
            return (
                [],
                (
                    "PORTFOLIO_CANDIDATES_EXPIRED_OR_COOLDOWN:"
                    f"expired_count={expired_count},cooldown_count={cooldown_count}"
                ),
            )
        if live_candidates and cooldown_count == len(live_candidates):
            return [], f"ALL_PORTFOLIO_CANDIDATES_ON_COOLDOWN:count={cooldown_count}"
        return [], None

    def _first_available_candidate(
        self,
        live_candidates: list[TradeCandidate],
    ) -> tuple[TradeCandidate | None, str | None]:
        candidates, reason = self._available_portfolio_candidates(live_candidates)
        if not candidates:
            return None, reason
        return candidates[0], None

    async def _select_candidate(self) -> TradeCandidate | None:
        self._last_candidate_selection_reason = None
        live_candidates, _, base_reason = await self._load_live_portfolio_candidates()
        candidate, candidate_reason = self._first_available_candidate(live_candidates)
        self._last_candidate_selection_reason = candidate_reason or base_reason
        return candidate

    async def _select_micro_test_candidate(self) -> tuple[TradeCandidate | None, str | None]:
        self._last_candidate_selection_reason = None
        live_candidates, _, base_reason = await self._load_live_portfolio_candidates()
        if not live_candidates:
            self._last_candidate_selection_reason = base_reason
            return None, base_reason

        allowed_symbols = set(self.settings.kis_live_allowed_symbol_list)
        effective_cap = self._effective_order_value_cap_krw(execute_live=True)
        rejected_symbol_count = 0
        rejected_price_count = 0
        rejected_spread_count = 0
        rejected_quote_count = 0
        cooldown_count = 0
        expired_count = 0
        excluded_count = 0
        spread_limit_bps = max(float(self.settings.trading_micro_test_max_spread_bps or 0.0), 0.0)
        as_of_utc = datetime.now(UTC)

        for candidate in live_candidates:
            if self._is_live_symbol_excluded(candidate.instrument_id):
                excluded_count += 1
                continue
            if self._is_candidate_expired(candidate, as_of_utc=as_of_utc):
                expired_count += 1
                continue
            if self._is_on_cooldown(candidate):
                self.duplicate_order_incident_count += 1
                cooldown_count += 1
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
                rejected_quote_count += 1
                continue
            if selected_price_krw is None:
                rejected_quote_count += 1
                continue
            spread_bps = quote_context["spread_bps"]
            if spread_limit_bps > 0 and spread_bps is not None and spread_bps > spread_limit_bps:
                rejected_spread_count += 1
                continue
            if effective_cap is not None and selected_price_krw > effective_cap:
                rejected_price_count += 1
                continue
            self._last_candidate_selection_reason = None
            return candidate, None

        if expired_count > 0 and expired_count == len(live_candidates):
            reason = f"ALL_PORTFOLIO_CANDIDATES_EXPIRED:count={expired_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if excluded_count > 0 and excluded_count == len(live_candidates):
            reason = f"ALL_PORTFOLIO_CANDIDATES_EXCLUDED:count={excluded_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if excluded_count > 0 and excluded_count + expired_count + cooldown_count == len(live_candidates):
            reason = (
                "PORTFOLIO_CANDIDATES_EXCLUDED_OR_UNAVAILABLE:"
                f"excluded_count={excluded_count},expired_count={expired_count},cooldown_count={cooldown_count}"
            )
            self._last_candidate_selection_reason = reason
            return None, reason
        if expired_count > 0 and expired_count + cooldown_count == len(live_candidates):
            reason = (
                "PORTFOLIO_CANDIDATES_EXPIRED_OR_COOLDOWN:"
                f"expired_count={expired_count},cooldown_count={cooldown_count}"
            )
            self._last_candidate_selection_reason = reason
            return None, reason
        if rejected_spread_count > 0:
            reason = f"MICRO_TEST_SPREAD_TOO_WIDE:count={rejected_spread_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if rejected_price_count > 0:
            reason = f"MICRO_TEST_PRICE_CAP_EXCEEDED:count={rejected_price_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if rejected_symbol_count > 0:
            reason = f"MICRO_TEST_SYMBOL_NOT_ALLOWED:count={rejected_symbol_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if cooldown_count > 0 and cooldown_count == len(live_candidates):
            reason = f"ALL_PORTFOLIO_CANDIDATES_ON_COOLDOWN:count={cooldown_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        if rejected_quote_count > 0:
            reason = f"MICRO_TEST_MARKET_SNAPSHOT_UNAVAILABLE:count={rejected_quote_count}"
            self._last_candidate_selection_reason = reason
            return None, reason
        self._last_candidate_selection_reason = base_reason
        return None, base_reason

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
        planned_side = str(planned_order.get("side") or "").strip().lower()
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
            balance_position_qty = 0 if planned_side == OrderSide.SELL.value else None

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
                    "candidate_side": candidate.side.value,
                    "candidate_family": candidate.candidate_family,
                    "event_cluster_id": candidate.event_cluster_id,
                    "exit_reason_code": candidate.exit_reason_code,
                    "position_qty": candidate.position_qty,
                    "position_return_pct": candidate.position_return_pct,
                    "holding_days": candidate.holding_days,
                    "cross_asset_impact_score": candidate.cross_asset_impact_score,
                    "thematic_alignment_score": candidate.thematic_alignment_score,
                    "macro_headwind_score": candidate.macro_headwind_score,
                    "thematic_tags": list(candidate.thematic_tags or []),
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
        entry_pause_blocks = self._entry_pause_blocks_candidate(candidate=candidate, risk_state=risk_state)
        readiness = store.resolve_execution_readiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            confidence_ok=(candidate.selection_confidence or 0.0) >= self.settings.selector_confidence_floor,
            market_data_ok=market_snapshot_ok,
            data_freshness_ok=market_snapshot_ok,
            vendor_healthy=vendor_healthy,
            session_entry_allowed=not entry_pause_blocks,
            session_exit_allowed=True,
            max_allowed_notional_krw=self._effective_order_value_cap_krw(execute_live=execute_live),
        )
        if entry_pause_blocks:
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
        entry_pause_blocks = self._entry_pause_blocks_candidate(candidate=candidate, risk_state=risk_state)
        payload = {
            "account_id": candidate.account_scope,
            "strategy_id": candidate.strategy_id,
            "instrument_id": candidate.instrument_id,
            "execution_side": candidate.side.value,
            "confidence_ok": (candidate.selection_confidence or 0.0) >= self.settings.selector_confidence_floor,
            "market_data_ok": market_snapshot_ok,
            "data_freshness_ok": market_snapshot_ok,
            "vendor_healthy": vendor_healthy,
            "session_entry_allowed": not entry_pause_blocks,
            "session_exit_allowed": True,
            "max_allowed_notional_krw": self._effective_order_value_cap_krw(execute_live=execute_live),
        }
        try:
            response = await self.ops_client.post("/ops/execution-readiness", json=payload)
            response.raise_for_status()
            readiness = ExecutionReadiness.model_validate(response.json())
            if entry_pause_blocks:
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
        if candidate.side == OrderSide.SELL:
            return None
        allowed_symbols = self.settings.kis_live_allowed_symbol_list
        if self.settings.trading_micro_test_require_allowed_symbols and not allowed_symbols:
            return "MICRO_TEST_ALLOWED_SYMBOLS_REQUIRED"
        if allowed_symbols and candidate.instrument_id not in allowed_symbols:
            return f"MICRO_TEST_SYMBOL_NOT_ALLOWED:{candidate.instrument_id}"
        if self.settings.trading_micro_test_max_order_value_krw <= 0:
            return "MICRO_TEST_MAX_ORDER_VALUE_REQUIRED"
        return None

    def _can_retry_entry_after_block(self, *, reason: str | None, execute_live: bool) -> bool:
        if not reason:
            return False
        if execute_live and self.settings.trading_micro_test_mode_enabled:
            return False
        tokens = [
            token.strip()
            for chunk in str(reason).split(";")
            for token in chunk.split(",")
            if token.strip()
        ]
        non_retry_prefixes = (
            "ACCOUNT_ENTRY_DISABLED",
            "KILL_SWITCH_ACTIVE",
            "RECONCILIATION_BREAK",
            "OPERATION_MODE_",
            "SESSION_ENTRY_BLOCKED",
            "SESSION_MARKET_DATA_UNAVAILABLE",
            "LIVE_ENTRY_PAUSED",
            "STRATEGY_DISABLED",
            "PROMOTION_",
            "ORDER_SIDE_RATE_LIMIT_EXCEEDED",
            "MICRO_TEST_ALLOWED_SYMBOLS_REQUIRED",
            "MICRO_TEST_MAX_ORDER_VALUE_REQUIRED",
            "BROKER_GATEWAY_ERROR",
            "BROKER_GATEWAY_REJECTED",
        )
        return not any(
            token.startswith(prefix)
            for token in tokens
            for prefix in non_retry_prefixes
        )

    async def _next_fallback_entry_candidate(
        self,
        *,
        current_candidate: TradeCandidate,
        attempted_candidate_ids: set[str],
        execute_live: bool,
    ) -> tuple[TradeCandidate | None, str | None]:
        if current_candidate.side != OrderSide.BUY:
            return None, None
        if execute_live and self.settings.trading_micro_test_mode_enabled:
            return None, None
        live_candidates, portfolio_candidates_ok, portfolio_reason = await self._load_live_portfolio_candidates()
        if not portfolio_candidates_ok:
            return None, portfolio_reason
        if current_candidate.candidate_id not in {candidate.candidate_id for candidate in live_candidates}:
            return None, None
        available_candidates, available_reason = self._available_portfolio_candidates(live_candidates)
        for candidate in available_candidates:
            if candidate.candidate_id in attempted_candidate_ids:
                continue
            return candidate, None
        return None, available_reason or portfolio_reason or "FALLBACK_PORTFOLIO_CANDIDATES_EXHAUSTED"

    @staticmethod
    def _annotate_plan_with_fallback_notes(
        *,
        plan: ExecutionPlan,
        blocked_attempt_notes: list[str],
    ) -> ExecutionPlan:
        if not blocked_attempt_notes:
            return plan
        fallback_note = f"fallback_attempts={'; '.join(blocked_attempt_notes)}"
        if plan.selection_reason:
            plan.selection_reason = f"{plan.selection_reason} [{fallback_note}]"
        else:
            plan.selection_reason = fallback_note
        if plan.status == "BLOCKED":
            if plan.reason:
                plan.reason = f"{plan.reason}; {fallback_note}"
            if plan.risk_reason_summary:
                plan.risk_reason_summary = f"{plan.risk_reason_summary}; {fallback_note}"
        return plan

    async def _execute_candidate_attempt(
        self,
        *,
        candidate: TradeCandidate,
        execute_live: bool,
        persist: bool,
    ) -> CandidateAttemptResult:
        self.last_sync_utc = datetime.now(UTC)
        self.last_candidate_id = candidate.candidate_id
        if self._is_candidate_expired(candidate):
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = self._candidate_expiry_reason(candidate)
            self.blocked_count += 1
            return CandidateAttemptResult(
                plan=ExecutionPlan(
                    candidate_id=candidate.candidate_id,
                    intent_id=None,
                    planned_order=None,
                    selected_price_krw=None,
                    quote_basis=None,
                    price_source_value=None,
                    execute_live=execute_live,
                    persisted=False,
                    selection_reason=candidate.selection_reason,
                    matched_rule_id=candidate.matched_rule_id,
                    source_report_name=candidate.source_report_name,
                    source_receipt_no=candidate.source_receipt_no,
                    risk_reason_summary=self.last_execution_reason,
                    status="BLOCKED",
                    reason=self.last_execution_reason,
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
            )

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
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
            )

        venue_hint = "KRX"
        risk_state = await self.gateway_client.refresh_live_risk_state()
        micro_test_block_reason = self._micro_test_block_reason(candidate=candidate, execute_live=execute_live)
        if micro_test_block_reason is not None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = micro_test_block_reason
            self.blocked_count += 1
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                    quantity_reason=intent.sizing_reason,
                    risk_reason_summary=self.last_execution_reason,
                    risk_state=risk_state,
                    persistence_error=persistence_error,
                    status="BLOCKED",
                    reason=self.last_execution_reason,
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                    persistence_error=persistence_error,
                    status="BLOCKED",
                    reason=self.last_execution_reason,
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
            )

        decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=live_readiness)
        intent = trading_core_service.build_trade_intent(candidate, decision)
        if decision.hard_block or intent is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = ",".join(decision.reason_codes) or "risk gate blocked"
            self.blocked_count += 1
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
                return CandidateAttemptResult(
                    plan=ExecutionPlan(
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
                    ),
                    retryable_block=(
                        candidate.side == OrderSide.BUY
                        and self._can_retry_entry_after_block(
                            reason=self.last_execution_reason,
                            execute_live=execute_live,
                        )
                    ),
                )

        selected_price_krw = quote_context["selected_price_krw"]
        quote_basis = quote_context["quote_basis"]
        if selected_price_krw is None or quote_basis is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = "MARKET_SNAPSHOT_UNAVAILABLE"
            self.blocked_count += 1
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
            side=candidate.side,
            decision_reason_codes=decision.reason_codes,
            live_risk_state=risk_state,
        )
        if not execute_live:
            self.last_execution_status = "PLANNED_ONLY"
            self.last_execution_reason = "live execution disabled for this run"
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=False,
            )

        rate_limit_reason = self._side_rate_limit_reason(side=candidate.side)
        if rate_limit_reason is not None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = rate_limit_reason
            self.blocked_count += 1
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                    promotion_readiness=promotion_readiness,
                    persistence_error=persistence_error,
                    status="BLOCKED",
                    reason=self.last_execution_reason,
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
            )

        broker_payload = _build_kis_order_payload(command)
        self._record_live_order_attempt(side=candidate.side)
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
            return CandidateAttemptResult(
                plan=ExecutionPlan(
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
                ),
                retryable_block=(
                    candidate.side == OrderSide.BUY
                    and self._can_retry_entry_after_block(
                        reason=self.last_execution_reason,
                        execute_live=execute_live,
                    )
                ),
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
        return CandidateAttemptResult(
            plan=ExecutionPlan(
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
            ),
            retryable_block=False,
        )

    async def run_once(self, *, execute_live: bool, persist: bool) -> ExecutionPlan:
        stale_before = self.stale_data_incident_count
        duplicate_before = self.duplicate_order_incident_count
        selector_mismatch_before = self.selector_mismatch_incident_count
        candidate, no_trade_reason = await self._select_run_candidate(execute_live=execute_live)
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

        blocked_attempt_notes: list[str] = []
        attempted_candidate_ids: set[str] = set()
        current_candidate = candidate
        while current_candidate is not None:
            attempted_candidate_ids.add(current_candidate.candidate_id)
            attempt = await self._execute_candidate_attempt(
                candidate=current_candidate,
                execute_live=execute_live,
                persist=persist,
            )
            plan = attempt.plan
            if not attempt.retryable_block:
                self._annotate_plan_with_fallback_notes(
                    plan=plan,
                    blocked_attempt_notes=blocked_attempt_notes,
                )
                return self._finalize_plan(
                    candidate=current_candidate,
                    plan=plan,
                    stale_before=stale_before,
                    duplicate_before=duplicate_before,
                    selector_mismatch_before=selector_mismatch_before,
                )
            blocked_attempt_notes.append(
                f"{current_candidate.instrument_id}:{plan.reason or plan.risk_reason_summary or 'BLOCKED'}"
            )
            fallback_candidate, fallback_reason = await self._next_fallback_entry_candidate(
                current_candidate=current_candidate,
                attempted_candidate_ids=attempted_candidate_ids,
                execute_live=execute_live,
            )
            if fallback_candidate is None:
                if fallback_reason:
                    blocked_attempt_notes.append(f"fallback:{fallback_reason}")
                self._annotate_plan_with_fallback_notes(
                    plan=plan,
                    blocked_attempt_notes=blocked_attempt_notes,
                )
                return self._finalize_plan(
                    candidate=current_candidate,
                    plan=plan,
                    stale_before=stale_before,
                    duplicate_before=duplicate_before,
                    selector_mismatch_before=selector_mismatch_before,
                )
            current_candidate = fallback_candidate

        self.last_sync_utc = datetime.now(UTC)
        self.last_execution_status = "NO_TRADE"
        self.last_execution_reason = "FALLBACK_CANDIDATE_SELECTION_FAILED"
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
        if self._is_candidate_expired(candidate):
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = self._candidate_expiry_reason(candidate)
            self.blocked_count += 1
            plan = ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                price_source_value=None,
                execute_live=execute_live,
                persisted=False,
                selection_reason=candidate.selection_reason,
                matched_rule_id=candidate.matched_rule_id,
                source_report_name=candidate.source_report_name,
                source_receipt_no=candidate.source_receipt_no,
                risk_reason_summary=self.last_execution_reason,
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
            side=candidate.side,
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

        rate_limit_reason = self._side_rate_limit_reason(side=candidate.side)
        if rate_limit_reason is not None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = rate_limit_reason
            self.blocked_count += 1
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

        broker_payload = _build_kis_order_payload(command)
        self._record_live_order_attempt(side=candidate.side)
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
