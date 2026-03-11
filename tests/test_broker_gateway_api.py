from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from apps.broker_gateway.main import app
from apps.broker_gateway.main import runtime
from apps.broker_gateway import service as broker_service_module
from libs.config.settings import Settings


client = TestClient(app)


def setup_function() -> None:
    runtime.live_trading_armed = False
    runtime.live_trading_armed_by = None
    runtime.live_trading_armed_at_utc = None
    runtime.last_total_equity_krw = None
    runtime.baseline_total_equity_krw = None
    runtime.daily_loss_pct = None
    runtime.entry_paused = False
    runtime.live_pause_reason = None


def test_normalize_order_ack_endpoint() -> None:
    response = client.post(
        "/normalize/order-ack",
        json={
            "payload": {
                "internal_order_id": "order-1",
                "client_order_id": "client-1",
                "output": {"ODNO": "8300012345", "ORD_TMD": "101530", "EXCG_ID_DVSN_CD": "KRX"},
            }
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["event"]["broker_order_no"] == "8300012345"
    assert body["envelope"]["message_name"] == "OrderAcked"


def test_normalize_fill_notice_endpoint() -> None:
    response = client.post(
        "/normalize/fill-notice",
        json={
            "payload": {
                "internal_order_id": "order-1",
                "account_id": "default",
                "ODER_NO": "8300012345",
                "SELN_BYOV_CLS": "02",
                "STCK_SHRN_ISCD": "005930",
                "STCK_CNTG_HOUR": "102000",
                "CNTG_UNPR": "70100",
                "CNTG_QTY": "1",
            }
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["event"]["instrument_id"] == "005930"
    assert body["envelope"]["message_name"] == "FillReceived"


def test_live_order_is_blocked_when_not_armed() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        broker_service_module,
        "get_settings",
        lambda: Settings(
            KIS_ENABLE_PAPER="false",
            KIS_LIVE_TRADING_ENABLED="true",
            KIS_LIVE_REQUIRE_ARM="true",
            KIS_ACCOUNT_NO="73908547",
            KIS_ACCOUNT_PRODUCT_CODE="01",
            KIS_LIVE_COMMON_STOCK_ONLY="false",
        ),
    )
    async def fake_query_balance(_payload):
        return {"output2": [{"tot_evlu_amt": "6500000", "bfdy_tot_asst_evlu_amt": "6500000"}]}
    monkeypatch.setattr(runtime.adapter, "query_balance", fake_query_balance)
    try:
        response = client.post(
            "/order/cash",
            json={
                "payload": {
                    "env": "prod",
                    "ord_dv": "buy",
                    "cano": "73908547",
                    "acnt_prdt_cd": "01",
                    "pdno": "005930",
                    "ord_dvsn": "00",
                    "ord_qty": "1",
                    "ord_unpr": "70000",
                }
            },
        )

        assert response.status_code == 409
        assert "not armed" in response.json()["detail"]
    finally:
        monkeypatch.undo()


def test_live_arm_endpoint_sets_runtime_state() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        broker_service_module,
        "get_settings",
        lambda: Settings(
            KIS_ENABLE_PAPER="false",
            KIS_LIVE_TRADING_ENABLED="false",
            KIS_LIVE_REQUIRE_ARM="true",
            KIS_ACCOUNT_NO="73908547",
            KIS_ACCOUNT_PRODUCT_CODE="01",
        ),
    )
    try:
        response = client.post(
            "/live/arm",
            json={"operator_id": "tester", "reason": "manual-check"},
        )

        assert response.status_code == 409
        assert "KIS_LIVE_TRADING_ENABLED=true" in response.json()["detail"]
    finally:
        monkeypatch.undo()


def test_live_arm_and_disarm_work_when_live_is_enabled() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        broker_service_module,
        "get_settings",
        lambda: Settings(
            KIS_ENABLE_PAPER="false",
            KIS_LIVE_TRADING_ENABLED="true",
            KIS_LIVE_REQUIRE_ARM="true",
            KIS_ACCOUNT_NO="73908547",
            KIS_ACCOUNT_PRODUCT_CODE="01",
            KIS_LIVE_COMMON_STOCK_ONLY="false",
        ),
    )
    async def fake_query_balance(_payload):
        return {"output2": [{"tot_evlu_amt": "6500000", "bfdy_tot_asst_evlu_amt": "6500000"}]}
    monkeypatch.setattr(runtime.adapter, "query_balance", fake_query_balance)
    try:
        arm_response = client.post(
            "/live/arm",
            json={"operator_id": "tester", "reason": "manual-check"},
        )
        assert arm_response.status_code == 200
        assert arm_response.json()["live_trading_armed"] is True

        disarm_response = client.post(
            "/live/disarm",
            json={"operator_id": "tester", "reason": "done"},
        )
        assert disarm_response.status_code == 200
        assert disarm_response.json()["live_trading_armed"] is False
    finally:
        monkeypatch.undo()


def test_live_risk_check_pauses_when_total_equity_breaches_limit() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        broker_service_module,
        "get_settings",
        lambda: Settings(
            KIS_ENABLE_PAPER="false",
            KIS_LIVE_TRADING_ENABLED="true",
            KIS_LIVE_REQUIRE_ARM="true",
            KIS_ACCOUNT_NO="73908547",
            KIS_ACCOUNT_PRODUCT_CODE="01",
            KIS_LIVE_MIN_TOTAL_EQUITY_KRW="5000000",
            KIS_LIVE_DAILY_LOSS_LIMIT_PCT="5.0",
        ),
    )
    async def fake_query_balance(_payload):
        return {"output2": [{"tot_evlu_amt": "4900000", "bfdy_tot_asst_evlu_amt": "6000000"}]}
    monkeypatch.setattr(runtime.adapter, "query_balance", fake_query_balance)
    try:
        response = client.post("/live/risk-check")

        assert response.status_code == 200
        body = response.json()
        assert body["entry_paused"] is True
        assert "TOTAL_EQUITY_AT_OR_BELOW_5000000" in body["live_pause_reason"]
    finally:
        monkeypatch.undo()
