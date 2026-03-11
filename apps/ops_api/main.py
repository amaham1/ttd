from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from apps.ops_api.schemas import KillSwitchRequest, ReplayJobCreateRequest
from apps.ops_api.store import store
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)

app = FastAPI(title="ops-api", version="0.1.0")


class StrategyToggleRequest(BaseModel):
    enabled: bool


class AccountToggleRequest(BaseModel):
    entry_enabled: bool


class SymbolBlockRequest(BaseModel):
    blocked: bool
    reason_code: str | None = None


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ops-api"}


@app.get("/metrics")
async def metrics() -> Response:
    body = "\n".join(
        [
            "# HELP kis_operation_mode Current operation mode as state labels",
            "# TYPE kis_operation_mode gauge",
            f'kis_operation_mode{{mode="{store.mode.mode.value}"}} 1',
            f"kis_strategy_total {len(store.strategies)}",
            f"kis_reconciliation_break_total {len(store.breaks)}",
            f"kis_risk_flag_total {len(store.risk_flags)}",
        ]
    )
    return Response(content=body, media_type="text/plain; version=0.0.4")


@app.get("/ops/summary")
async def get_summary():
    return store.summary()


@app.get("/ops/mode")
async def get_mode():
    return store.mode


@app.post("/ops/kill-switch")
async def activate_kill_switch(request: KillSwitchRequest):
    return store.activate_kill_switch(request)


@app.get("/ops/strategies")
async def list_strategies():
    return list(store.strategies.values())


@app.post("/ops/strategies/{strategy_id}")
async def update_strategy(strategy_id: str, request: StrategyToggleRequest):
    return store.set_strategy_enabled(strategy_id, request.enabled)


@app.get("/ops/accounts")
async def list_accounts():
    return list(store.accounts.values())


@app.post("/ops/accounts/{account_id}")
async def update_account(account_id: str, request: AccountToggleRequest):
    return store.set_account_entry_enabled(account_id, request.entry_enabled)


@app.get("/ops/symbol-blocks")
async def list_symbol_blocks():
    return list(store.symbol_blocks.values())


@app.post("/ops/symbol-blocks/{symbol}")
async def update_symbol_block(symbol: str, request: SymbolBlockRequest):
    return store.set_symbol_block(symbol, request.blocked, request.reason_code)


@app.get("/ops/reconciliation-breaks")
async def list_reconciliation_breaks():
    return list(store.breaks.values())


@app.get("/ops/risk-flags")
async def list_risk_flags():
    return list(store.risk_flags.values())


@app.get("/ops/sessions")
async def list_sessions():
    return list(store.sessions.values())


@app.get("/ops/positions")
async def list_positions():
    return list(store.positions.values())


@app.get("/ops/orders/{order_id}")
async def get_order(order_id: str):
    order = store.orders.get(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    return order


@app.get("/ops/candidates/{candidate_id}")
async def get_candidate(candidate_id: str):
    candidate = store.candidates.get(candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return candidate


@app.get("/ops/replay-jobs")
async def list_replay_jobs():
    return store.list_replay_jobs()


@app.post("/ops/replay-jobs")
async def create_replay_job(request: ReplayJobCreateRequest):
    return store.create_replay_job(request)
