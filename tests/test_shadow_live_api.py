from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from apps.shadow_live.main import app
from apps.shadow_live import service as shadow_service_module


client = TestClient(app)


async def fake_select_candidate():
    return shadow_service_module.market_intel_service.sample_candidates()[0]


def test_shadow_live_plans_order_without_live_execution() -> None:
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

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_asking_price", fake_query_asking_price)
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["quote_basis"] == "BEST_ASK"
        assert body["planned_order"]["price"] == 70200
        assert body["planned_order"]["qty"] >= 1
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
