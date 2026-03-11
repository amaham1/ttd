import asyncio

from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from apps.data_ingest import service as data_ingest_service_module
from apps.market_intel.service import market_intel_service
from apps.ops_api.store import store
from apps.shadow_live import service as shadow_service_module
from apps.shadow_live.service import BrokerGatewayRequestError, ExecutionPlan
from apps.shadow_live.main import app


client = TestClient(app)


async def fake_select_candidate():
    return market_intel_service.sample_candidates()[0]


def test_shadow_live_plans_order_without_live_execution() -> None:
    monkeypatch = MonkeyPatch()
    persisted_audits = []

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    def fake_persist_shadow_live_run(audit):
        persisted_audits.append(audit)

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "persist_shadow_live_run",
        fake_persist_shadow_live_run,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["quote_basis"] == "BEST_ASK"
        assert body["planned_order"]["price"] == 70200
        assert body["planned_order"]["qty"] >= 1
        assert body["matched_rule_id"] == "disclosure.positive.buyback"
        assert body["selection_reason"] is not None
        assert body["price_reason"] is not None
        assert body["quantity_reason"] is not None
        assert len(persisted_audits) == 1
        assert persisted_audits[0].status == "PLANNED_ONLY"

        latest_plan_response = client.get("/plan/latest")
        assert latest_plan_response.status_code == 200
        assert latest_plan_response.json()["candidate_id"] == body["candidate_id"]
    finally:
        monkeypatch.undo()


def test_shadow_live_submits_order_when_live_execution_enabled() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300019999",
                "ORD_TMD": "103000",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "submit_cash_order", fake_submit_cash_order)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "normalize_order_ack", fake_normalize_order_ack)
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["broker_response"]["output"]["ODNO"] == "8300019999"
        assert body["normalized_ack"]["event"]["broker_order_no"] == "8300019999"
        assert body["source_receipt_no"] == "20260311000123"
    finally:
        monkeypatch.undo()


def test_shadow_live_returns_no_trade_without_fallback_candidate() -> None:
    monkeypatch = MonkeyPatch()

    async def no_candidate():
        return None

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", no_candidate)
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["candidate_id"] == ""
    finally:
        monkeypatch.undo()


def test_reset_runtime_cancels_existing_loop_task() -> None:
    async def scenario():
        shadow_service_module.shadow_live_service._loop_stop_event = asyncio.Event()
        task = asyncio.create_task(asyncio.sleep(30))
        shadow_service_module.shadow_live_service._loop_task = task
        shadow_service_module.shadow_live_service.reset_runtime()
        await asyncio.sleep(0)
        return task.cancelled(), shadow_service_module.shadow_live_service._loop_task is None

    cancelled, cleared = asyncio.run(scenario())

    assert cancelled is True
    assert cleared is True


def test_shadow_live_blocks_live_execution_when_promotion_is_not_ready() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_promotion_readiness():
        return {
            "approved": False,
            "reason_codes": ["INSUFFICIENT_SHADOW_RUNS"],
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_promotion_readiness",
        fake_promotion_readiness,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        True,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "INSUFFICIENT_SHADOW_RUNS" in body["reason"]
        assert body["promotion_readiness"]["approved"] is False
    finally:
        monkeypatch.undo()


def test_shadow_live_returns_blocked_when_broker_gateway_rejects_order() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_submit_cash_order(payload):
        raise BrokerGatewayRequestError(
            status_code=409,
            detail="live trading is not armed",
            payload={"detail": "live trading is not armed"},
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "BROKER_GATEWAY_REJECTED:live trading is not armed" == body["reason"]
        assert body["broker_response"]["detail"] == "live trading is not armed"
    finally:
        monkeypatch.undo()


def test_shadow_live_blocks_micro_test_when_share_price_exceeds_cap() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-expensive-cap-test",
            "cooldown_key": "005930:BUYBACK:20990101",
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_fetch_candidates(*, force_refresh=False):
        return [candidate]

    async def fake_selected_count():
        return 1

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert "micro test price cap" in body["reason"]
    finally:
        monkeypatch.undo()


def test_shadow_live_submits_micro_test_single_share_within_cap() -> None:
    monkeypatch = MonkeyPatch()
    expensive_candidate = market_intel_service.sample_candidates()[0]
    cheap_candidate = expensive_candidate.model_copy(
        update={
            "candidate_id": "candidate-cheap",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311000999",
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output": {"stck_prpr": "4890"}}
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output1": {"askp1": "4900", "bidp1": "4890"}}
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_fetch_candidates(*, force_refresh=False):
        return [expensive_candidate, cheap_candidate]

    async def fake_selected_count():
        return 2

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300019998",
                "ORD_TMD": "103001",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930,123456",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["candidate_id"] == "candidate-cheap"
        assert body["planned_order"]["qty"] == 1
        assert body["planned_order"]["price"] == 4900
        assert body["planned_order"]["qty"] * body["planned_order"]["price"] <= 5_000
    finally:
        monkeypatch.undo()


def test_micro_test_candidate_preview_endpoint_suggests_allowlist_update() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-preview",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311001001",
        }
    )

    async def fake_fetch_candidates(*, force_refresh=False):
        return [candidate]

    async def fake_selected_count():
        return 1

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "4890"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "4900", "bidp1": "4890"}}

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930",
    )
    try:
        response = client.get("/preview/micro-test-candidates")
        assert response.status_code == 200
        body = response.json()
        assert body["configured_allowed_symbols"] == ["005930"]
        assert body["suggested_allowed_symbols"] == ["123456"]
        assert body["live_risk_state"]["entry_paused"] is False
        assert body["candidates"][0]["instrument_id"] == "123456"
        assert body["candidates"][0]["eligible_now"] is False
        assert body["candidates"][0]["eligible_if_allowlisted"] is True
        assert body["candidates"][0]["selected_price_krw"] == 4900
        assert body["candidates"][0]["proposed_qty"] == 1
        assert body["candidates"][0]["proposed_order_value_krw"] == 4900
    finally:
        monkeypatch.undo()


def test_shadow_live_returns_no_trade_when_no_whitelisted_candidate_meets_micro_cap() -> None:
    monkeypatch = MonkeyPatch()
    expensive_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-expensive-1",
            "cooldown_key": "005930:BUYBACK:20990102",
        }
    )
    second_expensive = expensive_candidate.model_copy(
        update={
            "candidate_id": "candidate-expensive-2",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
        }
    )

    async def fake_fetch_candidates(*, force_refresh=False):
        return [expensive_candidate, second_expensive]

    async def fake_selected_count():
        return 2

    async def fake_query_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output": {"stck_prpr": "5100"}}
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output1": {"askp1": "5200", "bidp1": "5100"}}
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930,123456",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert "micro test price cap" in body["reason"]
    finally:
        monkeypatch.undo()


def test_post_trade_verification_endpoint_reports_verified_fill() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-verify",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311001002",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "4890"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "4900", "bidp1": "4890"}}

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300011000",
                "ORD_TMD": "103101",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    async def fake_query_daily_ccld(*, symbol, broker_order_no, venue_code="KRX", trading_date=None):
        return {
            "output1": [
                {
                    "ODNO": broker_order_no,
                    "PDNO": symbol,
                    "tot_ccld_qty": "1",
                    "avg_prvs": "4900",
                }
            ]
        }

    async def fake_query_balance():
        return {
            "output1": [{"pdno": "123456", "hldg_qty": "1"}],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6495100"}],
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_daily_ccld",
        fake_query_daily_ccld,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        submit_response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert submit_response.status_code == 200
        assert submit_response.json()["status"] == "SUBMITTED"

        verify_response = client.get("/verify/post-trade/latest")
        assert verify_response.status_code == 200
        body = verify_response.json()
        assert body["status"] == "VERIFIED"
        assert body["broker_order_no"] == "8300011000"
        assert body["matched_order_count"] == 1
        assert body["matched_fill_qty"] == 1
        assert body["matched_avg_fill_price_krw"] == 4900.0
        assert body["balance_position_qty"] == 1
    finally:
        monkeypatch.undo()


def test_shadow_live_rejects_live_loop_start_in_micro_test_mode() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_run_once_only",
        True,
    )
    try:
        response = client.post(
            "/loop/start",
            json={"interval_seconds": 15, "execute_live": True, "persist": False},
        )
        assert response.status_code == 400
        assert "only via /run/sample" in response.json()["detail"]
    finally:
        monkeypatch.undo()


def test_shadow_live_allows_live_loop_start_when_autonomous_control_is_enabled() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_run_once_only",
        True,
    )
    store.set_live_control(
        max_order_value_krw=500000,
        auto_loop_interval_seconds=15,
        autonomous_loop_enabled=True,
        actor="ops-console",
        reason_code="ENABLE_AUTONOMOUS",
    )

    async def fake_run_once(*, execute_live, persist):
        shadow_service_module.shadow_live_service._loop_stop_event.set()
        return ExecutionPlan(
            candidate_id="",
            intent_id=None,
            planned_order=None,
            selected_price_krw=None,
            quote_basis=None,
            price_source_value=None,
            execute_live=execute_live,
            persisted=persist,
            status="NO_TRADE",
            reason="AUTONOMOUS_LOOP_TEST",
            risk_reason_summary="AUTONOMOUS_LOOP_TEST",
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "run_once", fake_run_once)
    try:
        async def orchestrate():
            snapshot = await shadow_service_module.shadow_live_service.start_loop(
                interval_seconds=1,
                execute_live=True,
                persist=True,
            )
            await asyncio.sleep(0)
            running_snapshot = shadow_service_module.shadow_live_service.loop_snapshot()
            await shadow_service_module.shadow_live_service.stop_loop()
            return snapshot, running_snapshot

        snapshot, running_snapshot = asyncio.run(orchestrate())
        assert snapshot.desired_running is True
        assert running_snapshot.execute_live is True
    finally:
        monkeypatch.undo()


def test_shadow_live_loop_control_endpoints() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_start_loop(*, interval_seconds, execute_live, persist):
        return {
            "running": True,
            "execute_live": execute_live,
            "persist": persist,
            "interval_seconds": interval_seconds,
            "run_count": 0,
            "last_started_at_utc": None,
            "last_finished_at_utc": None,
            "last_result_status": None,
            "last_error": None,
        }

    async def fake_stop_loop():
        return {
            "running": False,
            "execute_live": False,
            "persist": True,
            "interval_seconds": 60,
            "run_count": 0,
            "last_started_at_utc": None,
            "last_finished_at_utc": None,
            "last_result_status": None,
            "last_error": None,
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "start_loop", fake_start_loop)
    monkeypatch.setattr(shadow_service_module.shadow_live_service, "stop_loop", fake_stop_loop)
    try:
        start_response = client.post(
            "/loop/start",
            json={"interval_seconds": 15, "execute_live": False, "persist": True},
        )
        assert start_response.status_code == 200
        assert start_response.json()["running"] is True
        assert start_response.json()["interval_seconds"] == 15

        stop_response = client.post("/loop/stop")
        assert stop_response.status_code == 200
        assert stop_response.json()["running"] is False
    finally:
        monkeypatch.undo()


def test_shadow_live_live_run_uses_ws_market_guard_and_persists_session_state() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-ws-live",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20990101",
            "source_receipt_no": "20260311002001",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_ws_status():
        return {
            "running": True,
            "symbols": ["123456"],
            "venue": "KRX",
            "include_fill_notice": True,
            "include_market_status": True,
        }

    async def fake_market_guard(*, symbol, venue="KRX"):
        return {
            "instrument_id": symbol,
            "venue": venue,
            "entry_allowed": True,
            "reason_codes": [],
            "ws_running": True,
            "ws_market_data_stale": False,
            "session_open": True,
            "halt_detected": False,
            "latest_quote_at_utc": "2026-03-11T00:31:00+00:00",
            "latest_trade_at_utc": "2026-03-11T00:31:00+00:00",
            "quote_snapshot": {
                "instrument_id": symbol,
                "best_ask_px": 4900,
                "best_bid_px": 4890,
                "exchange_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "trade_snapshot": {
                "instrument_id": symbol,
                "last_price": 4890,
                "received_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "market_status_snapshot": {
                "venue": venue,
                "status_code": "OPEN",
            },
        }

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300012000",
                "ORD_TMD": "103000",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "ws_status",
        fake_ws_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "market_guard",
        fake_market_guard,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_require_ws_live_market_data",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["quote_basis"] == "WS_BEST_ASK"
        assert body["planned_order"]["price"] == 4900
        assert store.sessions["KRX"].session_code == "OPEN"
        assert store.sessions["KRX"].entry_allowed is True
    finally:
        monkeypatch.undo()


def test_shadow_live_blocks_live_execution_when_ws_market_guard_blocks_entry() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-ws-blocked",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20990101",
            "source_receipt_no": "20260311002002",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_ws_status():
        return {
            "running": True,
            "symbols": ["123456"],
            "venue": "KRX",
            "include_fill_notice": True,
            "include_market_status": True,
        }

    async def fake_market_guard(*, symbol, venue="KRX"):
        return {
            "instrument_id": symbol,
            "venue": venue,
            "entry_allowed": False,
            "reason_codes": ["TRADING_HALT_ACTIVE"],
            "ws_running": True,
            "ws_market_data_stale": False,
            "session_open": True,
            "halt_detected": True,
            "latest_quote_at_utc": "2026-03-11T00:31:00+00:00",
            "latest_trade_at_utc": "2026-03-11T00:31:00+00:00",
            "quote_snapshot": {
                "instrument_id": symbol,
                "best_ask_px": 4900,
                "best_bid_px": 4890,
                "exchange_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "trade_snapshot": {
                "instrument_id": symbol,
                "last_price": 4890,
                "received_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "market_status_snapshot": {
                "venue": venue,
                "status_code": "HALT",
            },
        }

    async def fail_submit_cash_order(_payload):
        raise AssertionError("submit_cash_order should not be called when market guard blocks entry")

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "ws_status",
        fake_ws_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "market_guard",
        fake_market_guard,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fail_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_require_ws_live_market_data",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "TRADING_HALT_ACTIVE" in body["reason"]
        assert store.sessions["KRX"].session_code == "HALTED"
        assert store.sessions["KRX"].entry_allowed is False
    finally:
        monkeypatch.undo()


def test_shadow_live_restore_loop_if_needed_resumes_durable_loop_state() -> None:
    monkeypatch = MonkeyPatch()
    owner_id = "shadow-live-previous"
    store.acquire_loop_lease(
        loop_id="shadow-live-main",
        service_name="shadow-live",
        owner_id=owner_id,
        interval_seconds=5,
        execute_live=False,
        persist=True,
        ttl_seconds=60.0,
        actor=owner_id,
        reason_code="TEST_START",
    )
    store.release_loop_lease(
        loop_id="shadow-live-main",
        owner_id=owner_id,
        desired_running=True,
        reason_code="TEST_SUSPEND",
    )

    async def fake_run_once(*, execute_live, persist):
        shadow_service_module.shadow_live_service._loop_stop_event.set()
        return ExecutionPlan(
            candidate_id="",
            intent_id=None,
            planned_order=None,
            selected_price_krw=None,
            quote_basis=None,
            price_source_value=None,
            execute_live=execute_live,
            persisted=persist,
            status="NO_TRADE",
            reason="AUTO_RESUME_TEST",
            risk_reason_summary="AUTO_RESUME_TEST",
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "run_once", fake_run_once)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_loop_auto_resume",
        True,
    )

    async def orchestrate():
        snapshot = await shadow_service_module.shadow_live_service.restore_loop_if_needed()
        await asyncio.sleep(0)
        running_snapshot = shadow_service_module.shadow_live_service.loop_snapshot()
        await shadow_service_module.shadow_live_service.stop_loop()
        return snapshot, running_snapshot

    try:
        restored_snapshot, running_snapshot = asyncio.run(orchestrate())
        assert restored_snapshot.desired_running is True
        assert running_snapshot.restored_from_durable is True
        assert running_snapshot.run_count >= 1
    finally:
        monkeypatch.undo()


def test_shadow_live_uses_runtime_live_control_cap_for_live_orders() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    store.set_live_control(
        max_order_value_krw=50000,
        auto_loop_interval_seconds=60,
        actor="test",
        reason_code="LOW_CAP",
    )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        False,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "exceeds sizing limit 50,000KRW" in body["reason"]
    finally:
        monkeypatch.undo()
