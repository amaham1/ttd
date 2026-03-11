from contextlib import asynccontextmanager

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


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "broker-gateway"}


@app.get("/snapshot")
async def snapshot():
    return runtime.snapshot()


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
