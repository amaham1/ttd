from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from apps.market_intel.service import market_intel_service
from apps.trading_core.service import trading_core_service
from libs.config.settings import get_settings
from libs.contracts.messages import OrderSubmitCommand, TradeCandidate
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


@dataclass(slots=True)
class ExecutionPlan:
    candidate_id: str
    intent_id: str | None
    planned_order: dict[str, Any] | None
    selected_price_krw: int | None
    quote_basis: str | None
    execute_live: bool
    persisted: bool
    risk_state: dict[str, Any] | None = None
    broker_response: dict[str, Any] | None = None
    normalized_ack: dict[str, Any] | None = None
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


class BrokerGatewayClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = httpx.AsyncClient(base_url=self.settings.broker_gateway_url, timeout=30.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def _post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self.client.post(path, json=payload or {})
        response.raise_for_status()
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

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/order/cash", {"payload": payload})

    async def normalize_order_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/normalize/order-ack", {"payload": payload})


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
        self.gateway_client = BrokerGatewayClient()
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
        self._loop_task: asyncio.Task[None] | None = None
        self._loop_stop_event = asyncio.Event()

    async def close(self) -> None:
        await self.stop_loop()
        await self.gateway_client.close()

    def snapshot(self) -> ShadowLiveSnapshot:
        return ShadowLiveSnapshot(
            mode="ACTIVE",
            candidate_count=1,
            fill_match_rate=0.78,
            last_sync_utc=self.last_sync_utc,
            last_candidate_id=self.last_candidate_id,
            last_intent_id=self.last_intent_id,
            last_internal_order_id=self.last_internal_order_id,
            last_broker_order_no=self.last_broker_order_no,
            last_execution_status=self.last_execution_status,
            last_execution_reason=self.last_execution_reason,
        )

    def loop_snapshot(self) -> ShadowLoopSnapshot:
        return ShadowLoopSnapshot(
            running=self._loop_task is not None and not self._loop_task.done(),
            execute_live=self.loop_execute_live,
            persist=self.loop_persist,
            interval_seconds=self.loop_interval_seconds,
            run_count=self.loop_run_count,
            last_started_at_utc=self.loop_last_started_at_utc,
            last_finished_at_utc=self.loop_last_finished_at_utc,
            last_result_status=self.loop_last_result_status,
            last_error=self.loop_last_error,
        )

    async def start_loop(self, *, interval_seconds: int, execute_live: bool, persist: bool) -> ShadowLoopSnapshot:
        if interval_seconds < 1:
            raise ValueError("interval_seconds must be at least 1")
        await self.stop_loop()
        self.loop_interval_seconds = interval_seconds
        self.loop_execute_live = execute_live
        self.loop_persist = persist
        self.loop_last_error = None
        self._loop_stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop_worker(), name="shadow-live-loop")
        return self.loop_snapshot()

    async def stop_loop(self) -> ShadowLoopSnapshot:
        self._loop_stop_event.set()
        task = self._loop_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._loop_task = None
        return self.loop_snapshot()

    async def _loop_worker(self) -> None:
        try:
            while not self._loop_stop_event.is_set():
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
                    await asyncio.wait_for(self._loop_stop_event.wait(), timeout=self.loop_interval_seconds)
                except TimeoutError:
                    continue
        finally:
            self._loop_task = None

    async def _select_candidate(self) -> TradeCandidate:
        live_candidates = await market_intel_service.live_candidates(limit=1)
        if live_candidates:
            return live_candidates[0]
        return market_intel_service.sample_candidates()[0]

    async def run_once(self, *, execute_live: bool, persist: bool) -> ExecutionPlan:
        candidate = await self._select_candidate()
        decision = trading_core_service.evaluate_candidate(candidate)
        intent = trading_core_service.build_trade_intent(candidate, decision)
        self.last_sync_utc = datetime.now(UTC)
        self.last_candidate_id = candidate.candidate_id
        persistence_error: str | None = None

        if persist:
            try:
                trading_core_service.persist_pipeline(candidate=candidate, decision=decision, intent=intent)
            except Exception as exc:
                persistence_error = str(exc)
                persist = False

        if decision.hard_block or intent is None:
            self.last_execution_status = "BLOCKED"
            self.last_execution_reason = ",".join(decision.reason_codes) or "risk gate blocked"
            return ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=None,
                planned_order=None,
                selected_price_krw=None,
                quote_basis=None,
                execute_live=execute_live,
                persisted=persist,
                persistence_error=persistence_error,
                status="BLOCKED",
                reason=self.last_execution_reason,
            )

        risk_state = await self.gateway_client.refresh_live_risk_state()
        venue_hint = "KRX"
        market_div = _venue_code_to_market_div(venue_hint)
        current_price_payload = await self.gateway_client.query_price(symbol=candidate.instrument_id, market_div=market_div)
        asking_price_payload = await self.gateway_client.query_asking_price(symbol=candidate.instrument_id, market_div=market_div)
        selected_price_krw, quote_basis = _pick_limit_price(candidate.side, asking_price_payload, current_price_payload)
        command = trading_core_service.build_order_submit_command(
            intent=intent,
            strategy_id=candidate.strategy_id,
            price_krw=selected_price_krw,
            venue_hint=venue_hint,
            order_type="LIMIT",
        )
        self.last_intent_id = intent.intent_id
        self.last_internal_order_id = command.internal_order_id
        planned_order = command.model_dump(mode="json")

        if not execute_live:
            self.last_execution_status = "PLANNED_ONLY"
            self.last_execution_reason = "live execution disabled for this run"
            return ExecutionPlan(
                candidate_id=candidate.candidate_id,
                intent_id=intent.intent_id,
                planned_order=planned_order,
                selected_price_krw=selected_price_krw,
                quote_basis=quote_basis,
                execute_live=False,
                persisted=persist,
                risk_state=risk_state,
                persistence_error=persistence_error,
                status="PLANNED_ONLY",
                reason=self.last_execution_reason,
            )

        broker_payload = _build_kis_order_payload(command)
        broker_response = await self.gateway_client.submit_cash_order(broker_payload)
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
        return ExecutionPlan(
            candidate_id=candidate.candidate_id,
            intent_id=intent.intent_id,
            planned_order=planned_order,
            selected_price_krw=selected_price_krw,
            quote_basis=quote_basis,
            execute_live=True,
            persisted=persist,
            risk_state=risk_state,
            broker_response=broker_response,
            normalized_ack=normalized_ack,
            persistence_error=persistence_error,
            status="SUBMITTED",
        )


shadow_live_service = ShadowLiveService()
