from fastapi import FastAPI, HTTPException, Query, Response
import httpx
from pydantic import BaseModel

from apps.market_intel.service import market_intel_service
from apps.ops_api.schemas import (
    ExecutionReadinessRequest,
    KillSwitchRequest,
    ReplayJobCreateRequest,
)
from apps.ops_api.store import store
from libs.adapters.dart import OpenDARTClient
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)

app = FastAPI(title="ops-api", version="0.1.0")


class StrategyToggleRequest(BaseModel):
    enabled: bool


class AccountToggleRequest(BaseModel):
    entry_enabled: bool | None = None
    exit_enabled: bool | None = None


class SymbolBlockRequest(BaseModel):
    blocked: bool
    reason_code: str | None = None


class LiveControlUpdateRequest(BaseModel):
    max_order_value_krw: int | None = None
    auto_loop_interval_seconds: int | None = None
    autonomous_loop_enabled: bool | None = None
    actor: str | None = "ops-console"
    reason_code: str | None = "MANUAL_UPDATE"


class LiveLoopCommandRequest(BaseModel):
    max_order_value_krw: int | None = None
    interval_seconds: int | None = None
    actor: str = "ops-console"
    reason_code: str = "DASHBOARD_START"


def _refresh_shared_state() -> None:
    store.reload_state()


def _balance_query_payload() -> dict:
    return {
        "env": "prod",
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


def _first_present(payload: dict, *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def _extract_balance_name_map(payload: dict) -> dict[str, str]:
    rows = payload.get("output1") or []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return {}
    name_by_symbol: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = _first_present(row, "pdno", "PDNO", "stck_shrn_iscd", "item_cd")
        name = _first_present(
            row,
            "prdt_name",
            "hts_kor_isnm",
            "stck_kor_isnm",
            "item_name",
            "prdt_abrv_name",
        )
        if not symbol or not name:
            continue
        name_by_symbol[symbol] = name
    return name_by_symbol


def _load_pykrx_name_map(symbols: list[str]) -> dict[str, str]:
    try:
        from pykrx import stock
    except Exception:
        return {}

    name_by_symbol: dict[str, str] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip()
        if not symbol:
            continue
        try:
            name = str(stock.get_market_ticker_name(symbol) or "").strip()
        except Exception:
            name = ""
        if not name or name == symbol:
            continue
        name_by_symbol[symbol] = name
    return name_by_symbol


async def _post_service_json(
    *,
    base_url: str,
    path: str,
    payload: dict,
) -> dict:
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=20.0) as client:
            response = await client.post(path, json=payload)
    except httpx.RequestError as exc:
        target = f"{base_url.rstrip('/')}{path}"
        raise HTTPException(
            status_code=502,
            detail=f"downstream service unavailable: {target}",
        ) from exc
    if response.is_error:
        detail = response.text.strip() or response.reason_phrase
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                detail = str(parsed.get("detail") or parsed)
        except Exception:
            parsed = None
        raise HTTPException(status_code=response.status_code, detail=detail)
    payload_json = response.json()
    if isinstance(payload_json, dict):
        return payload_json
    return {"payload": payload_json}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ops-api"}


@app.get("/metrics")
async def metrics() -> Response:
    reconciliation_breaks = store.list_reconciliation_breaks(limit=200)
    body = "\n".join(
        [
            "# HELP kis_operation_mode Current operation mode as state labels",
            "# TYPE kis_operation_mode gauge",
            f'kis_operation_mode{{mode="{store.mode.mode.value}"}} 1',
            f"kis_strategy_total {len(store.strategies)}",
            f"kis_reconciliation_break_total {len(reconciliation_breaks)}",
            f"kis_risk_flag_total {len(store.risk_flags)}",
        ]
    )
    return Response(content=body, media_type="text/plain; version=0.0.4")


@app.get("/ops/summary")
async def get_summary():
    _refresh_shared_state()
    return store.summary()


@app.get("/ops/audit-log")
async def get_audit_log(limit: int = 50):
    _refresh_shared_state()
    return store.list_audit_events(limit=limit)


@app.get("/ops/mode")
async def get_mode():
    _refresh_shared_state()
    return store.mode


@app.post("/ops/kill-switch")
async def activate_kill_switch(request: KillSwitchRequest):
    _refresh_shared_state()
    return store.activate_kill_switch(request)


@app.get("/ops/strategies")
async def list_strategies():
    _refresh_shared_state()
    return list(store.strategies.values())


@app.post("/ops/strategies/{strategy_id}")
async def update_strategy(strategy_id: str, request: StrategyToggleRequest):
    _refresh_shared_state()
    return store.set_strategy_enabled(strategy_id, request.enabled)


@app.get("/ops/accounts")
async def list_accounts():
    _refresh_shared_state()
    return list(store.accounts.values())


@app.post("/ops/accounts/{account_id}")
async def update_account(account_id: str, request: AccountToggleRequest):
    _refresh_shared_state()
    if request.entry_enabled is None and request.exit_enabled is None:
        raise HTTPException(status_code=400, detail="entry_enabled or exit_enabled must be provided")
    return store.set_account_permissions(
        account_id,
        entry_enabled=request.entry_enabled,
        exit_enabled=request.exit_enabled,
    )


@app.get("/ops/symbol-blocks")
async def list_symbol_blocks():
    _refresh_shared_state()
    return list(store.symbol_blocks.values())


@app.post("/ops/symbol-blocks/{symbol}")
async def update_symbol_block(symbol: str, request: SymbolBlockRequest):
    _refresh_shared_state()
    return store.set_symbol_block(symbol, request.blocked, request.reason_code)


@app.get("/ops/reconciliation-breaks")
async def list_reconciliation_breaks():
    _refresh_shared_state()
    return store.list_reconciliation_breaks()


@app.get("/ops/risk-flags")
async def list_risk_flags():
    _refresh_shared_state()
    return list(store.risk_flags.values())


@app.get("/ops/instrument-names")
async def list_instrument_names(symbol: list[str] = Query(default=[])):
    _refresh_shared_state()
    resolved_entries = store.resolve_instrument_names(symbols=symbol)
    unresolved_symbols = [
        str(entry.get("symbol") or "").strip()
        for entry in resolved_entries
        if not entry.get("name")
    ]
    if not unresolved_symbols:
        return resolved_entries

    balance_name_by_symbol = store.cached_broker_balance_names()
    if settings.kis_account_no and (
        not balance_name_by_symbol
        or store.broker_balance_name_cache_stale(ttl_seconds=120)
    ):
        try:
            balance_payload = await _post_service_json(
                base_url=settings.broker_gateway_url,
                path="/query/balance",
                payload=_balance_query_payload(),
            )
        except HTTPException:
            balance_payload = {}
        fetched_balance_names = _extract_balance_name_map(balance_payload)
        if fetched_balance_names:
            store.cache_broker_balance_names(fetched_balance_names)
            balance_name_by_symbol = store.cached_broker_balance_names()

    if not balance_name_by_symbol:
        resolved_entries = store.resolve_instrument_names(symbols=symbol)
    else:
        resolved_entries = store.resolve_instrument_names(
            symbols=symbol,
            balance_name_by_symbol=balance_name_by_symbol,
        )

    unresolved_symbols = [
        str(entry.get("symbol") or "").strip()
        for entry in resolved_entries
        if not entry.get("name")
    ]
    if unresolved_symbols and settings.opendart_api_key:
        dart_name_by_symbol = store.cached_dart_symbol_names()
        if not dart_name_by_symbol or store.dart_symbol_name_cache_stale(ttl_seconds=86400):
            dart_client = OpenDARTClient(settings=settings)
            try:
                corp_codes = await dart_client.download_corp_codes()
            except Exception:
                corp_codes = []
            finally:
                await dart_client.close()
            fetched_dart_names = {
                str(code.stock_code).strip(): str(code.corp_name).strip()
                for code in corp_codes
                if code.stock_code and code.corp_name
            }
            if fetched_dart_names:
                store.cache_dart_symbol_names(fetched_dart_names)
                dart_name_by_symbol = store.cached_dart_symbol_names()
        if dart_name_by_symbol:
            resolved_entries = store.resolve_instrument_names(
                symbols=symbol,
                dart_name_by_symbol=dart_name_by_symbol,
            )
            unresolved_symbols = [
                str(entry.get("symbol") or "").strip()
                for entry in resolved_entries
                if not entry.get("name")
            ]

    if unresolved_symbols:
        pykrx_name_by_symbol = store.cached_pykrx_symbol_names()
        missing_pykrx_symbols = [
            unresolved_symbol
            for unresolved_symbol in unresolved_symbols
            if unresolved_symbol not in pykrx_name_by_symbol
        ]
        if missing_pykrx_symbols or store.pykrx_symbol_name_cache_stale(ttl_seconds=86400):
            fetched_pykrx_names = _load_pykrx_name_map(unresolved_symbols)
            if fetched_pykrx_names:
                store.cache_pykrx_symbol_names(fetched_pykrx_names)
                pykrx_name_by_symbol = store.cached_pykrx_symbol_names()
        if pykrx_name_by_symbol:
            resolved_entries = store.resolve_instrument_names(
                symbols=symbol,
                pykrx_name_by_symbol=pykrx_name_by_symbol,
            )
    return resolved_entries


@app.get("/ops/sessions")
async def list_sessions():
    _refresh_shared_state()
    return list(store.sessions.values())


@app.get("/ops/loops")
async def list_loops():
    _refresh_shared_state()
    return store.list_loop_states()


@app.get("/ops/loops/{loop_id}")
async def get_loop(loop_id: str):
    _refresh_shared_state()
    loop_state = store.get_loop_state(loop_id)
    if loop_state is None:
        raise HTTPException(status_code=404, detail="loop not found")
    return loop_state


@app.get("/ops/live-controls")
async def get_live_controls():
    _refresh_shared_state()
    return store.get_live_control()


@app.post("/ops/live-controls")
async def update_live_controls(request: LiveControlUpdateRequest):
    _refresh_shared_state()
    if request.max_order_value_krw is not None and request.max_order_value_krw < 1:
        raise HTTPException(status_code=400, detail="max_order_value_krw must be at least 1")
    if (
        request.auto_loop_interval_seconds is not None
        and request.auto_loop_interval_seconds < 1
    ):
        raise HTTPException(
            status_code=400,
            detail="auto_loop_interval_seconds must be at least 1",
        )
    return store.set_live_control(
        max_order_value_krw=request.max_order_value_krw,
        auto_loop_interval_seconds=request.auto_loop_interval_seconds,
        autonomous_loop_enabled=request.autonomous_loop_enabled,
        actor=request.actor,
        reason_code=request.reason_code,
    )


@app.post("/ops/live-loop/start")
async def start_live_loop(request: LiveLoopCommandRequest):
    _refresh_shared_state()
    if request.max_order_value_krw is not None and request.max_order_value_krw < 1:
        raise HTTPException(status_code=400, detail="max_order_value_krw must be at least 1")
    if request.interval_seconds is not None and request.interval_seconds < 1:
        raise HTTPException(status_code=400, detail="interval_seconds must be at least 1")

    previous_live_control = store.get_live_control().model_copy(deep=True)
    live_control = store.set_live_control(
        max_order_value_krw=request.max_order_value_krw,
        auto_loop_interval_seconds=request.interval_seconds,
        autonomous_loop_enabled=True,
        actor=request.actor,
        reason_code=request.reason_code,
    )

    broker_payload: dict | None = None
    try:
        broker_payload = await _post_service_json(
            base_url=settings.broker_gateway_url,
            path="/live/arm",
            payload={"operator_id": request.actor, "reason": request.reason_code},
        )
        loop_payload = await _post_service_json(
            base_url=settings.shadow_live_url,
            path="/loop/start",
            payload={
                "interval_seconds": live_control.auto_loop_interval_seconds,
                "execute_live": True,
                "persist": True,
            },
        )
    except HTTPException as exc:
        store.set_live_control(
            max_order_value_krw=previous_live_control.max_order_value_krw,
            auto_loop_interval_seconds=previous_live_control.auto_loop_interval_seconds,
            autonomous_loop_enabled=previous_live_control.autonomous_loop_enabled,
            actor=request.actor,
            reason_code="START_LIVE_LOOP_ROLLBACK",
        )
        if broker_payload is not None:
            try:
                await _post_service_json(
                    base_url=settings.broker_gateway_url,
                    path="/live/disarm",
                    payload={"operator_id": request.actor, "reason": "START_LOOP_FAILED"},
                )
            except HTTPException:
                pass
        raise exc

    return {
        "live_control": store.get_live_control(),
        "loop": loop_payload,
        "broker": broker_payload,
    }


@app.post("/ops/live-loop/stop")
async def stop_live_loop(request: LiveLoopCommandRequest):
    _refresh_shared_state()
    stop_reason = request.reason_code or "DASHBOARD_STOP"
    loop_payload = await _post_service_json(
        base_url=settings.shadow_live_url,
        path="/loop/stop",
        payload={},
    )
    broker_payload = await _post_service_json(
        base_url=settings.broker_gateway_url,
        path="/live/disarm",
        payload={"operator_id": request.actor, "reason": stop_reason},
    )
    live_control = store.set_live_control(
        max_order_value_krw=request.max_order_value_krw,
        auto_loop_interval_seconds=request.interval_seconds,
        autonomous_loop_enabled=False,
        actor=request.actor,
        reason_code=stop_reason,
    )
    return {
        "live_control": live_control,
        "loop": loop_payload,
        "broker": broker_payload,
    }


@app.get("/ops/positions")
async def list_positions():
    _refresh_shared_state()
    return list(store.positions.values())


@app.post("/ops/execution-readiness")
async def execution_readiness(request: ExecutionReadinessRequest):
    _refresh_shared_state()
    return store.resolve_execution_readiness(
        account_id=request.account_id,
        strategy_id=request.strategy_id,
        instrument_id=request.instrument_id,
        execution_side=request.execution_side,
        confidence_ok=request.confidence_ok,
        market_data_ok=request.market_data_ok,
        data_freshness_ok=request.data_freshness_ok,
        vendor_healthy=request.vendor_healthy,
        session_entry_allowed=request.session_entry_allowed,
        session_exit_allowed=request.session_exit_allowed,
        max_allowed_notional_krw=request.max_allowed_notional_krw,
    )


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


@app.get("/ops/candidate-decisions")
async def list_candidate_decisions():
    return market_intel_service.list_candidate_decisions()


@app.get("/ops/candidate-decisions/{candidate_id}")
async def get_candidate_decision(candidate_id: str):
    decision = market_intel_service.get_candidate_decision(candidate_id)
    if decision is None:
        raise HTTPException(status_code=404, detail="candidate decision not found")
    return decision


@app.get("/ops/rules/disclosure")
async def list_disclosure_rules():
    return market_intel_service.list_disclosure_rules()


@app.get("/ops/replay-jobs")
async def list_replay_jobs():
    return store.list_replay_jobs()


@app.post("/ops/replay-jobs")
async def create_replay_job(request: ReplayJobCreateRequest):
    _refresh_shared_state()
    return store.create_replay_job(request)
