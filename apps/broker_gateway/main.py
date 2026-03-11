from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from apps.broker_gateway.service import LiveTradingGuardError
from apps.broker_gateway.service import runtime
from libs.config.settings import get_settings
from libs.domain.enums import Environment
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await runtime.close()


app = FastAPI(title="broker-gateway", version="0.1.0", lifespan=lifespan)


class EnvRequest(BaseModel):
    env: Environment = Environment.PROD


class LiveArmRequest(BaseModel):
    operator_id: str
    reason: str | None = None


class GenericPayloadRequest(BaseModel):
    payload: dict


class ReconciliationRunRequest(BaseModel):
    trading_date: date | None = None


class WebSocketStartRequest(BaseModel):
    env: Environment | None = None
    symbols: list[str] | None = None
    venue: str = "KRX"
    include_fill_notice: bool = True
    include_market_status: bool = True


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "broker-gateway"}


@app.get("/snapshot")
async def snapshot():
    return runtime.snapshot()


@app.get("/ws/status")
async def websocket_status():
    return runtime.ws_snapshot()


@app.post("/ws/start")
async def start_websocket_consumer(request: WebSocketStartRequest):
    try:
        return await runtime.start_ws_consumer(
            symbols=request.symbols,
            venue=request.venue,
            env=request.env,
            include_fill_notice=request.include_fill_notice,
            include_market_status=request.include_market_status,
        )
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/ws/stop")
async def stop_websocket_consumer():
    return await runtime.stop_ws_consumer()


@app.get("/ws/quotes")
async def list_ws_quotes(limit: int = 100, symbol: str | None = None):
    return runtime.list_latest_quotes(limit=limit, symbol=symbol)


@app.get("/ws/trades")
async def list_ws_trades(limit: int = 100, symbol: str | None = None):
    return runtime.list_latest_trades(limit=limit, symbol=symbol)


@app.get("/ws/market-status")
async def list_ws_market_status(limit: int = 20):
    return runtime.list_market_status_snapshots(limit=limit)


@app.get("/ws/order-notices")
async def list_ws_order_notices(limit: int = 20):
    return runtime.recent_order_notices[:limit]


@app.get("/market/guard/{symbol}")
async def market_guard(symbol: str, venue: str = "KRX"):
    return runtime.live_market_guard(symbol=symbol, venue=venue)


@app.get("/oms/orders")
async def list_oms_orders(limit: int = 100):
    return runtime.list_order_tickets(limit=limit)


@app.get("/oms/orders/{internal_order_id}")
async def get_oms_order(internal_order_id: str):
    order = runtime.get_order_ticket(internal_order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    return order


@app.get("/oms/fills")
async def list_oms_fills(
    limit: int = 100,
    internal_order_id: str | None = None,
    broker_order_no: str | None = None,
):
    return runtime.list_execution_fills(
        limit=limit,
        internal_order_id=internal_order_id,
        broker_order_no=broker_order_no,
    )


@app.post("/oms/recover")
async def recover_oms_state(limit: int = 200):
    return await runtime.recover_oms_state(limit=limit)


@app.get("/reconciliation/breaks")
async def list_reconciliation_breaks(limit: int = 100, open_only: bool = True):
    return runtime.list_reconciliation_breaks(limit=limit, open_only=open_only)


@app.post("/reconciliation/run")
async def run_reconciliation(request: ReconciliationRunRequest):
    return await runtime.run_intraday_reconciliation(trading_date=request.trading_date)


@app.post("/maintenance/purge-nonlive-orders")
async def purge_nonlive_order_artifacts():
    return runtime.purge_nonlive_order_artifacts()


@app.post("/auth/rest")
async def issue_rest_token(request: EnvRequest):
    return await runtime.issue_rest_token(request.env)


@app.post("/auth/ws")
async def issue_ws_approval(request: EnvRequest):
    return await runtime.issue_ws_approval(request.env)


@app.post("/live/arm")
async def arm_live_trading(request: LiveArmRequest):
    try:
        return await runtime.arm_live_trading(request.operator_id, request.reason)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/live/disarm")
async def disarm_live_trading(request: LiveArmRequest):
    return runtime.disarm_live_trading(request.operator_id, request.reason)


@app.post("/live/risk-check")
async def live_risk_check():
    try:
        return await runtime.refresh_live_risk_state()
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/order/cash")
async def submit_cash_order(request: GenericPayloadRequest):
    try:
        return await runtime.submit_cash_order(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/order/cancel-replace")
async def submit_cancel_replace(request: GenericPayloadRequest):
    try:
        return await runtime.submit_cancel_replace(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/query/psbl-order")
async def query_psbl_order(request: GenericPayloadRequest):
    try:
        return await runtime.query_psbl_order(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/query/price")
async def query_price(request: GenericPayloadRequest):
    try:
        return await runtime.query_price(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/query/asking-price")
async def query_asking_price(request: GenericPayloadRequest):
    try:
        return await runtime.query_asking_price(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/query/balance")
async def query_balance(request: GenericPayloadRequest):
    try:
        return await runtime.query_balance(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/query/daily-ccld")
async def query_daily_ccld(request: GenericPayloadRequest):
    try:
        return await runtime.query_daily_ccld(request.payload)
    except LiveTradingGuardError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/normalize/order-ack")
async def normalize_order_ack(request: GenericPayloadRequest):
    return await runtime.normalize_order_ack(request.payload)


@app.post("/normalize/fill-notice")
async def normalize_fill_notice(request: GenericPayloadRequest):
    return await runtime.normalize_fill_notice(request.payload)
