import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from apps.broker_gateway.main import app
from apps.broker_gateway.main import runtime
from apps.broker_gateway import service as broker_service_module
from libs.adapters.kis import KISApproval
from libs.config.settings import Settings


client = TestClient(app)


def setup_function() -> None:
    client.post("/ws/stop")
    runtime.reset_runtime()


def teardown_function() -> None:
    client.post("/ws/stop")
    runtime.reset_runtime()


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
    assert body["order_ticket"]["internal_order_id"] == "order-1"
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
    assert body["fill_record"]["internal_order_id"] == "order-1"
    assert body["envelope"]["message_name"] == "FillReceived"


def test_oms_order_and_fill_endpoints_expose_runtime_state() -> None:
    ack_response = client.post(
        "/normalize/order-ack",
        json={
            "payload": {
                "internal_order_id": "order-oms-1",
                "client_order_id": "client-oms-1",
                "account_id": "default",
                "instrument_id": "005930",
                "qty": 2,
                "output": {"ODNO": "8300099999", "ORD_TMD": "101530", "EXCG_ID_DVSN_CD": "KRX"},
            }
        },
    )
    assert ack_response.status_code == 200

    fill_response = client.post(
        "/normalize/fill-notice",
        json={
            "payload": {
                "internal_order_id": "order-oms-1",
                "client_order_id": "client-oms-1",
                "account_id": "default",
                "ODER_NO": "8300099999",
                "SELN_BYOV_CLS": "02",
                "STCK_SHRN_ISCD": "005930",
                "STCK_CNTG_HOUR": "102000",
                "CNTG_UNPR": "70100",
                "CNTG_QTY": "1",
            }
        },
    )
    assert fill_response.status_code == 200

    order_list_response = client.get("/oms/orders")
    assert order_list_response.status_code == 200
    assert order_list_response.json()[0]["internal_order_id"] == "order-oms-1"

    order_detail_response = client.get("/oms/orders/order-oms-1")
    assert order_detail_response.status_code == 200
    assert order_detail_response.json()["broker_order_no"] == "8300099999"

    fills_response = client.get("/oms/fills", params={"internal_order_id": "order-oms-1"})
    assert fills_response.status_code == 200
    assert fills_response.json()[0]["fill_qty"] == 1

    recover_response = client.post("/oms/recover")
    assert recover_response.status_code == 200
    assert recover_response.json()["order_ticket_count"] >= 1


def test_reconciliation_run_opens_break_when_broker_order_is_missing() -> None:
    monkeypatch = MonkeyPatch()
    runtime.oms_order_tickets["order-recon-1"] = {
        "internal_order_id": "order-recon-1",
        "client_order_id": "client-recon-1",
        "broker_order_no": "8300077777",
        "account_uid": "default",
        "instrument_id": "005930",
        "side_code": "BUY",
        "order_state_code": "ACKED",
        "order_type_code": "LIMIT",
        "tif_code": "DAY",
        "working_qty": 1,
        "filled_qty": 0,
        "avg_fill_price": None,
        "last_event_at_utc": "2026-03-11T10:15:30",
        "payload_json": {"instrument_id": "005930"},
    }

    async def fake_query_daily_ccld(_payload):
        return {"output1": []}

    async def fake_query_balance(_payload):
        return {"output1": [], "output2": [{"tot_evlu_amt": "6500000"}]}

    monkeypatch.setattr(runtime.adapter, "query_daily_ccld", fake_query_daily_ccld)
    monkeypatch.setattr(runtime.adapter, "query_balance", fake_query_balance)
    monkeypatch.setattr(runtime.repository, "list_order_tickets", lambda limit=100: [])
    monkeypatch.setattr(
        runtime.repository,
        "list_execution_fills",
        lambda limit=100, internal_order_id=None, broker_order_no=None: [],
    )
    monkeypatch.setattr(
        runtime.repository,
        "list_reconciliation_breaks",
        lambda limit=100, status_code=None: [],
    )
    monkeypatch.setattr(
        runtime.repository,
        "upsert_reconciliation_break",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        runtime.repository,
        "resolve_reconciliation_breaks",
        lambda scope_type, scope_id: 0,
    )
    try:
        response = client.post("/reconciliation/run", json={"trading_date": "2026-03-11"})
        assert response.status_code == 200
        body = response.json()
        assert "ORDER_MISSING:order-recon-1" in body["issues"]
        assert body["open_break_count"] >= 1

        breaks_response = client.get("/reconciliation/breaks")
        assert breaks_response.status_code == 200
        assert breaks_response.json()[0]["scope_id"] == "order-recon-1"
    finally:
        monkeypatch.undo()


def test_purge_nonlive_orders_endpoint_removes_test_artifacts_only() -> None:
    nonlive_ack_response = client.post(
        "/normalize/order-ack",
        json={
            "payload": {
                "internal_order_id": "order-oms-1",
                "client_order_id": "client-oms-1",
                "account_id": "default",
                "instrument_id": "005930",
                "qty": 2,
                "output": {"ODNO": "8300099999", "ORD_TMD": "101530", "EXCG_ID_DVSN_CD": "KRX"},
            }
        },
    )
    assert nonlive_ack_response.status_code == 200

    runtime._record_order_ticket(
        {
            "internal_order_id": "order-abcdef123456",
            "client_order_id": "client-abcdef123456",
            "broker_order_no": "8300011111",
            "account_uid": "default",
            "instrument_id": "005930",
            "side_code": "BUY",
            "order_state_code": "ACKED",
            "order_type_code": "LIMIT",
            "tif_code": "DAY",
            "working_qty": 1,
            "filled_qty": 0,
            "avg_fill_price": None,
            "last_event_at_utc": "2026-03-11T10:15:31",
            "payload_json": {"instrument_id": "005930"},
        }
    )

    runtime._upsert_reconciliation_break(
        scope_type="ORDER",
        scope_id="order-oms-1",
        severity_code="HIGH",
        expected_payload={"ticket": {"internal_order_id": "order-oms-1"}},
        actual_payload={"broker_rows": []},
        notes="synthetic break",
    )

    purge_response = client.post("/maintenance/purge-nonlive-orders")
    assert purge_response.status_code == 200
    purge_payload = purge_response.json()
    assert "order-oms-1" in purge_payload["purged_order_ids"]
    assert "order-abcdef123456" not in purge_payload["purged_order_ids"]

    remaining_orders_response = client.get("/oms/orders")
    assert remaining_orders_response.status_code == 200
    remaining_order_ids = {item["internal_order_id"] for item in remaining_orders_response.json()}
    assert "order-oms-1" not in remaining_order_ids
    assert "order-abcdef123456" in remaining_order_ids


def test_ws_start_and_stop_runtime_manage_consumer_state() -> None:
    monkeypatch = MonkeyPatch()
    subscriptions: list[tuple[str, str, str, str]] = []
    pong_payloads: list[str] = []

    async def fake_issue_ws_approval(env):
        return KISApproval(approval_key="approval-key", issued_at_utc=datetime.now(UTC))

    async def fake_subscribe_quote(symbol, venue="KRX", env=None):
        subscriptions.append(("quote", symbol, venue, getattr(env, "value", str(env))))

    async def fake_subscribe_trade(symbol, venue="KRX", env=None):
        subscriptions.append(("trade", symbol, venue, getattr(env, "value", str(env))))

    async def fake_subscribe_fill_notice(symbol="", env=None):
        subscriptions.append(("fill", symbol, "KRX", getattr(env, "value", str(env))))

    async def fake_subscribe_market_status(symbol="", venue="KRX", env=None):
        subscriptions.append(("market", symbol or venue, venue, getattr(env, "value", str(env))))

    async def fake_recv_ws_message():
        await asyncio.sleep(0.05)
        return {"type": "control", "payload": {"tr_id": "PINGPONG", "is_pingpong": True}}

    async def fake_send_ws_pong(payload=None):
        pong_payloads.append("" if payload is None else str(payload))

    async def fake_close_ws():
        return None

    monkeypatch.setattr(runtime.adapter, "issue_ws_approval", fake_issue_ws_approval)
    monkeypatch.setattr(runtime.adapter, "subscribe_quote", fake_subscribe_quote)
    monkeypatch.setattr(runtime.adapter, "subscribe_trade", fake_subscribe_trade)
    monkeypatch.setattr(runtime.adapter, "subscribe_fill_notice", fake_subscribe_fill_notice)
    monkeypatch.setattr(runtime.adapter, "subscribe_market_status", fake_subscribe_market_status)
    monkeypatch.setattr(runtime.adapter, "recv_ws_message", fake_recv_ws_message)
    monkeypatch.setattr(runtime.adapter, "send_ws_pong", fake_send_ws_pong)
    monkeypatch.setattr(runtime.adapter, "close_ws", fake_close_ws)

    async def scenario():
        started = await runtime.start_ws_consumer(
            env=broker_service_module.Environment.PROD,
            symbols=["005930"],
            venue="KRX",
            include_fill_notice=True,
            include_market_status=True,
        )
        await asyncio.sleep(0.12)
        live_snapshot = runtime.ws_snapshot()
        stopped = await runtime.stop_ws_consumer()
        return started, live_snapshot, stopped

    try:
        started_snapshot, live_snapshot, stopped_snapshot = asyncio.run(scenario())
    finally:
        monkeypatch.undo()

    assert started_snapshot.running is True
    assert live_snapshot.running is True
    assert live_snapshot.connect_count >= 1
    assert live_snapshot.control_count >= 1
    assert live_snapshot.symbol_count == 1
    assert ("quote", "005930", "KRX", "prod") in subscriptions
    assert ("trade", "005930", "KRX", "prod") in subscriptions
    assert any(item[0] == "fill" for item in subscriptions)
    assert any(item[0] == "market" for item in subscriptions)
    assert "PINGPONG" in pong_payloads
    assert stopped_snapshot.running is False


def test_reset_runtime_cancels_existing_ws_task() -> None:
    async def scenario():
        runtime._ws_stop_event = asyncio.Event()
        task = asyncio.create_task(asyncio.sleep(30))
        runtime._ws_loop_task = task
        runtime.reset_runtime()
        await asyncio.sleep(0)
        return task.cancelled(), runtime._ws_loop_task is None

    cancelled, cleared = asyncio.run(scenario())

    assert cancelled is True
    assert cleared is True


def test_ws_message_consumer_updates_snapshots_and_oms_state() -> None:
    monkeypatch = MonkeyPatch()
    runtime.oms_order_tickets["order-ws-1"] = {
        "internal_order_id": "order-ws-1",
        "client_order_id": "client-ws-1",
        "broker_order_no": "8300011111",
        "account_uid": "default",
        "instrument_id": "005930",
        "side_code": "BUY",
        "order_state_code": "ACKED",
        "order_type_code": "LIMIT",
        "tif_code": "DAY",
        "working_qty": 1,
        "filled_qty": 0,
        "avg_fill_price": None,
        "last_event_at_utc": "2026-03-11T10:15:30",
        "payload_json": {},
    }

    async def fake_publish_quote_l1(_event):
        return SimpleNamespace(subject="evt.market.quote_l1", envelope={"message_name": "QuoteL1Received"})

    async def fake_publish_market_tick(_event):
        return SimpleNamespace(subject="evt.market.tick", envelope={"message_name": "MarketTickReceived"})

    async def fake_publish_fill(_event):
        return SimpleNamespace(subject="evt.execution.fill", envelope={"message_name": "FillReceived"})

    async def fake_send_ws_pong(_payload=None):
        return None

    def fake_store(**_kwargs):
        return SimpleNamespace(checksum="0123456789abcdef", stored_at_utc=datetime.now(UTC))

    monkeypatch.setattr(runtime.event_pipeline, "publish_quote_l1", fake_publish_quote_l1)
    monkeypatch.setattr(runtime.event_pipeline, "publish_market_tick", fake_publish_market_tick)
    monkeypatch.setattr(runtime.event_pipeline, "publish_fill", fake_publish_fill)
    monkeypatch.setattr(runtime.raw_event_service, "store", fake_store)
    monkeypatch.setattr(runtime.repository, "store_fill", lambda event, payload: SimpleNamespace(primary_key=1))
    monkeypatch.setattr(runtime.adapter, "send_ws_pong", fake_send_ws_pong)

    quote_fields = [""] * 59
    quote_fields[0] = "005930"
    quote_fields[1] = "101530"
    quote_fields[3] = "70100"
    quote_fields[13] = "70000"
    quote_fields[23] = "120"
    quote_fields[33] = "130"
    quote_fields[43] = "900"
    quote_fields[44] = "800"

    trade_fields = [""] * 46
    trade_fields[0] = "005930"
    trade_fields[1] = "101531"
    trade_fields[2] = "70100"
    trade_fields[12] = "3"
    trade_fields[13] = "1550"
    trade_fields[18] = "112.4"

    order_notice_fields = [""] * 26
    order_notice_fields[1] = "default"
    order_notice_fields[2] = "8300011111"
    order_notice_fields[4] = "02"
    order_notice_fields[8] = "005930"
    order_notice_fields[11] = "101532"
    order_notice_fields[13] = "0"
    order_notice_fields[14] = "1"
    order_notice_fields[16] = "1"
    order_notice_fields[19] = "KRX"
    order_notice_fields[25] = "70100"

    fill_notice_fields = [""] * 26
    fill_notice_fields[1] = "default"
    fill_notice_fields[2] = "8300011111"
    fill_notice_fields[4] = "02"
    fill_notice_fields[8] = "005930"
    fill_notice_fields[9] = "1"
    fill_notice_fields[10] = "70100"
    fill_notice_fields[11] = "101533"
    fill_notice_fields[13] = "2"
    fill_notice_fields[14] = "1"
    fill_notice_fields[16] = "1"
    fill_notice_fields[19] = "KRX"
    fill_notice_fields[25] = "70100"

    try:
        asyncio.run(runtime.consume_ws_message({"type": "control", "payload": {"tr_id": "PINGPONG", "is_pingpong": True}}))
        asyncio.run(runtime.consume_ws_message({"type": "stream", "tr_id": "H0STASP0", "count": 1, "payload": "^".join(quote_fields)}))
        asyncio.run(runtime.consume_ws_message({"type": "stream", "tr_id": "H0STCNT0", "count": 1, "payload": "^".join(trade_fields)}))
        asyncio.run(
            runtime.consume_ws_message(
                {"type": "stream", "tr_id": "H0STCNI0", "count": 1, "payload": "^".join(order_notice_fields)}
            )
        )
        asyncio.run(
            runtime.consume_ws_message(
                {"type": "stream", "tr_id": "H0STCNI0", "count": 1, "payload": "^".join(fill_notice_fields)}
            )
        )
    finally:
        monkeypatch.undo()

    ws_status = runtime.ws_snapshot()
    assert ws_status.control_count == 1
    assert ws_status.quote_count == 1
    assert ws_status.trade_count == 1
    assert ws_status.order_notice_count == 1
    assert ws_status.fill_notice_count == 1

    quotes_response = client.get("/ws/quotes", params={"symbol": "005930"})
    assert quotes_response.status_code == 200
    assert quotes_response.json()[0]["best_ask_px"] == 70100

    trades_response = client.get("/ws/trades", params={"symbol": "005930"})
    assert trades_response.status_code == 200
    assert trades_response.json()[0]["last_qty"] == 3

    notices_response = client.get("/ws/order-notices")
    assert notices_response.status_code == 200
    assert notices_response.json()[0]["ODER_NO"] == "8300011111"
    assert notices_response.json()[0]["raw_ref"].startswith("raw:")

    fills_response = client.get("/oms/fills", params={"internal_order_id": "order-ws-1"})
    assert fills_response.status_code == 200
    assert fills_response.json()[0]["fill_qty"] == 1

    order_ticket = runtime.get_order_ticket("order-ws-1")
    assert order_ticket is not None
    assert order_ticket["order_state_code"] == "FILLED"


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


def test_live_order_is_blocked_when_micro_test_cap_is_breached() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        broker_service_module,
        "get_settings",
        lambda: Settings(
            KIS_ENABLE_PAPER="false",
            KIS_LIVE_TRADING_ENABLED="true",
            KIS_LIVE_REQUIRE_ARM="false",
            KIS_ACCOUNT_NO="73908547",
            KIS_ACCOUNT_PRODUCT_CODE="01",
            KIS_LIVE_COMMON_STOCK_ONLY="false",
            KIS_LIVE_ALLOWED_SYMBOLS="005930",
            TRADING_MICRO_TEST_MODE_ENABLED="true",
            TRADING_MICRO_TEST_MAX_ORDER_VALUE_KRW="5000",
            TRADING_MICRO_TEST_REQUIRE_ALLOWED_SYMBOLS="true",
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
                    "ord_qty": "2",
                    "ord_unpr": "4000",
                }
            },
        )

        assert response.status_code == 409
        assert "TRADING_MICRO_TEST_MAX_ORDER_VALUE_KRW=5000" in response.json()["detail"]
    finally:
        monkeypatch.undo()
