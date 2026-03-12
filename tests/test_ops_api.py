import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
import httpx
from pytest import MonkeyPatch

from apps.ops_api import main as ops_main_module
from apps.ops_api.main import app
from apps.ops_api.schemas import LiveControlState
from apps.ops_api.schemas import BreakState
from apps.ops_api.store import store
from apps.market_intel.service import market_intel_service
from libs.contracts.messages import CandidateDecisionRecord, DisclosureRuleDefinition


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "ops-api"


def test_kill_switch_endpoint() -> None:
    response = client.post(
        "/ops/kill-switch",
        json={"reason_code": "MANUAL_TEST", "activated_by": "tester"},
    )
    assert response.status_code == 200
    assert response.json()["mode"] == "KILL_SWITCH"


def test_summary_endpoint() -> None:
    response = client.get("/ops/summary")
    assert response.status_code == 200
    body = response.json()
    assert "strategy_enabled_count" in body
    assert "active_position_count" in body


def test_create_replay_job_endpoint() -> None:
    response = client.post(
        "/ops/replay-jobs",
        json={"trading_date": "2026-03-11", "scenario": "ws-gap"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "QUEUED"


def test_candidate_decision_and_rule_endpoints() -> None:
    monkeypatch = MonkeyPatch()
    decision = CandidateDecisionRecord(
        decision_id="decision-candidate-demo",
        candidate_id="candidate-demo",
        source_receipt_no="20260311000123",
        source_report_name="자기주식취득결정",
        source_symbol="005930",
        matched_positive_rule_id="disclosure.positive.buyback",
        candidate_status="SELECTED",
        selection_reason="자기주식 취득결정 공시가 매칭되어 후보로 선택되었습니다.",
    )
    rule = DisclosureRuleDefinition(
        rule_id="disclosure.positive.buyback",
        rule_type="positive",
        rule_name="자기주식 취득결정",
        match_field="report_nm",
        match_pattern="자기주식취득결정",
        decision_effect="candidate_allow",
        reason_template="자기주식 취득결정 공시는 주주환원 신호로 해석해 매수 후보로 분류합니다.",
    )
    monkeypatch.setattr(market_intel_service, "list_candidate_decisions", lambda: [decision])
    monkeypatch.setattr(market_intel_service, "get_candidate_decision", lambda candidate_id: decision if candidate_id == "candidate-demo" else None)
    monkeypatch.setattr(market_intel_service, "list_disclosure_rules", lambda: [rule])
    try:
        decisions_response = client.get("/ops/candidate-decisions")
        assert decisions_response.status_code == 200
        assert decisions_response.json()[0]["candidate_id"] == "candidate-demo"

        decision_response = client.get("/ops/candidate-decisions/candidate-demo")
        assert decision_response.status_code == 200
        assert decision_response.json()["matched_positive_rule_id"] == "disclosure.positive.buyback"

        rules_response = client.get("/ops/rules/disclosure")
        assert rules_response.status_code == 200
        assert rules_response.json()[0]["rule_id"] == "disclosure.positive.buyback"
    finally:
        monkeypatch.undo()


def test_execution_readiness_endpoint() -> None:
    response = client.post(
        "/ops/execution-readiness",
        json={
            "account_id": "default",
            "strategy_id": "disclosure-alpha",
            "instrument_id": "005930",
            "confidence_ok": True,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == "default"
    assert body["instrument_id"] == "005930"


def test_execution_readiness_endpoint_uses_exit_permission_for_sell() -> None:
    previous_account = store.accounts["default"].model_copy(deep=True)
    try:
        update_response = client.post(
            "/ops/accounts/default",
            json={"exit_enabled": False},
        )
        assert update_response.status_code == 200
        assert update_response.json()["exit_enabled"] is False

        readiness_response = client.post(
            "/ops/execution-readiness",
            json={
                "account_id": "default",
                "strategy_id": "disclosure-alpha",
                "instrument_id": "005930",
                "execution_side": "sell",
            },
        )
        assert readiness_response.status_code == 200
        body = readiness_response.json()
        assert body["execution_side"] == "sell"
        assert body["account_exit_enabled"] is False
        assert "ACCOUNT_EXIT_DISABLED" in body["reason_codes"]
    finally:
        store.accounts["default"] = previous_account
        store._persist_without_audit()


def test_audit_log_endpoint_records_control_plane_changes() -> None:
    response = client.post(
        "/ops/symbol-blocks/005930",
        json={"blocked": True, "reason_code": "TEST_BLOCK"},
    )
    assert response.status_code == 200

    audit_response = client.get("/ops/audit-log", params={"limit": 10})
    assert audit_response.status_code == 200
    body = audit_response.json()
    assert body[0]["resource_type"] == "symbol_block"
    assert body[0]["resource_id"] == "005930"
    assert body[0]["reason_code"] == "TEST_BLOCK"


def test_instrument_names_endpoint_resolves_profile_and_refreshes_stale_balance_cache() -> None:
    monkeypatch = MonkeyPatch()
    previous_cache = dict(store.instrument_name_cache)
    previous_balance_cache = dict(store._broker_balance_name_cache)
    previous_balance_updated_at = store._broker_balance_name_cache_updated_at_utc
    previous_dart_cache = dict(store._dart_symbol_name_cache)
    previous_dart_updated_at = store._dart_symbol_name_cache_updated_at_utc
    store.instrument_name_cache = {}
    store._broker_balance_name_cache = {"005935": "이전이름"}
    store._broker_balance_name_cache_updated_at_utc = datetime.now(UTC) - timedelta(minutes=10)
    store._dart_symbol_name_cache = {}
    store._dart_symbol_name_cache_updated_at_utc = None

    async def fake_post_service_json(*, base_url: str, path: str, payload: dict) -> dict:
        assert path == "/query/balance"
        return {
            "output1": [
                {"pdno": "005935", "prdt_name": "삼성전자우"},
            ]
        }

    def fake_get_instrument_profile(symbol: str):
        if symbol == "005930":
            return SimpleNamespace(
                issuer_name="삼성전자",
                updated_at_utc=datetime(2026, 3, 12, 0, 0, tzinfo=UTC),
            )
        return None

    monkeypatch.setattr(store.repository, "get_instrument_profile", fake_get_instrument_profile)
    monkeypatch.setattr(ops_main_module, "_post_service_json", fake_post_service_json)
    try:
        response = client.get(
            "/ops/instrument-names",
            params=[("symbol", "005930"), ("symbol", "005935")],
        )
        assert response.status_code == 200
        body = response.json()
        assert body[0]["symbol"] == "005930"
        assert body[0]["name"] == "삼성전자"
        assert body[0]["source"] == "INSTRUMENT_PROFILE"
        assert body[1]["symbol"] == "005935"
        assert body[1]["name"] == "삼성전자우"
        assert body[1]["source"] == "BROKER_BALANCE"
    finally:
        store.instrument_name_cache = previous_cache
        store._broker_balance_name_cache = previous_balance_cache
        store._broker_balance_name_cache_updated_at_utc = previous_balance_updated_at
        store._dart_symbol_name_cache = previous_dart_cache
        store._dart_symbol_name_cache_updated_at_utc = previous_dart_updated_at
        monkeypatch.undo()


def test_instrument_names_endpoint_resolves_via_dart_corp_code_cache() -> None:
    monkeypatch = MonkeyPatch()
    previous_cache = dict(store.instrument_name_cache)
    previous_balance_cache = dict(store._broker_balance_name_cache)
    previous_balance_updated_at = store._broker_balance_name_cache_updated_at_utc
    previous_dart_cache = dict(store._dart_symbol_name_cache)
    previous_dart_updated_at = store._dart_symbol_name_cache_updated_at_utc
    previous_pykrx_cache = dict(store._pykrx_symbol_name_cache)
    previous_pykrx_updated_at = store._pykrx_symbol_name_cache_updated_at_utc
    previous_opendart_api_key = ops_main_module.settings.opendart_api_key
    store.instrument_name_cache = {}
    store._broker_balance_name_cache = {}
    store._broker_balance_name_cache_updated_at_utc = None
    store._dart_symbol_name_cache = {}
    store._dart_symbol_name_cache_updated_at_utc = None
    store._pykrx_symbol_name_cache = {}
    store._pykrx_symbol_name_cache_updated_at_utc = None
    ops_main_module.settings.opendart_api_key = "test-dart-key"

    class FakeOpenDARTClient:
        def __init__(self, settings):
            self.settings = settings

        async def download_corp_codes(self):
            return [
                SimpleNamespace(stock_code="005935", corp_name="삼성전자우"),
            ]

        async def close(self):
            return None

    monkeypatch.setattr(store.repository, "get_instrument_profile", lambda symbol: None)
    monkeypatch.setattr(ops_main_module, "OpenDARTClient", FakeOpenDARTClient)
    try:
        response = client.get(
            "/ops/instrument-names",
            params=[("symbol", "005935")],
        )
        assert response.status_code == 200
        body = response.json()
        assert body == [
            {
                "symbol": "005935",
                "name": "삼성전자우",
                "source": "DART_CORP_CODE",
                "updated_at_utc": body[0]["updated_at_utc"],
            }
        ]
    finally:
        store.instrument_name_cache = previous_cache
        store._broker_balance_name_cache = previous_balance_cache
        store._broker_balance_name_cache_updated_at_utc = previous_balance_updated_at
        store._dart_symbol_name_cache = previous_dart_cache
        store._dart_symbol_name_cache_updated_at_utc = previous_dart_updated_at
        store._pykrx_symbol_name_cache = previous_pykrx_cache
        store._pykrx_symbol_name_cache_updated_at_utc = previous_pykrx_updated_at
        ops_main_module.settings.opendart_api_key = previous_opendart_api_key
        monkeypatch.undo()


def test_instrument_names_endpoint_resolves_via_pykrx_fallback() -> None:
    monkeypatch = MonkeyPatch()
    previous_cache = dict(store.instrument_name_cache)
    previous_balance_cache = dict(store._broker_balance_name_cache)
    previous_balance_updated_at = store._broker_balance_name_cache_updated_at_utc
    previous_dart_cache = dict(store._dart_symbol_name_cache)
    previous_dart_updated_at = store._dart_symbol_name_cache_updated_at_utc
    previous_pykrx_cache = dict(store._pykrx_symbol_name_cache)
    previous_pykrx_updated_at = store._pykrx_symbol_name_cache_updated_at_utc
    previous_opendart_api_key = ops_main_module.settings.opendart_api_key
    store.instrument_name_cache = {}
    store._broker_balance_name_cache = {}
    store._broker_balance_name_cache_updated_at_utc = None
    store._dart_symbol_name_cache = {}
    store._dart_symbol_name_cache_updated_at_utc = None
    store._pykrx_symbol_name_cache = {}
    store._pykrx_symbol_name_cache_updated_at_utc = None
    ops_main_module.settings.opendart_api_key = ""

    monkeypatch.setattr(store.repository, "get_instrument_profile", lambda symbol: None)
    monkeypatch.setattr(ops_main_module, "_load_pykrx_name_map", lambda symbols: {"005935": "삼성전자우"})
    try:
        response = client.get(
            "/ops/instrument-names",
            params=[("symbol", "005935")],
        )
        assert response.status_code == 200
        body = response.json()
        assert body == [
            {
                "symbol": "005935",
                "name": "삼성전자우",
                "source": "PYKRX",
                "updated_at_utc": body[0]["updated_at_utc"],
            }
        ]
    finally:
        store.instrument_name_cache = previous_cache
        store._broker_balance_name_cache = previous_balance_cache
        store._broker_balance_name_cache_updated_at_utc = previous_balance_updated_at
        store._dart_symbol_name_cache = previous_dart_cache
        store._dart_symbol_name_cache_updated_at_utc = previous_dart_updated_at
        store._pykrx_symbol_name_cache = previous_pykrx_cache
        store._pykrx_symbol_name_cache_updated_at_utc = previous_pykrx_updated_at
        ops_main_module.settings.opendart_api_key = previous_opendart_api_key
        monkeypatch.undo()


def test_reconciliation_break_endpoint_reads_repository_backed_breaks() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        store.repository,
        "list_reconciliation_breaks",
        lambda limit=100, status_code=None: [
            {
                "break_id": "recon-order-demo",
                "scope_type": "ORDER",
                "scope_id": "order-demo",
                "severity_code": "HIGH",
                "status_code": "OPEN",
                "detected_at_utc": "2026-03-11T10:00:00+00:00",
            }
        ],
    )
    try:
        response = client.get("/ops/reconciliation-breaks")
        assert response.status_code == 200
        assert response.json()[0]["break_id"] == "recon-order-demo"
        readiness_response = client.post(
            "/ops/execution-readiness",
            json={
                "account_id": "default",
                "strategy_id": "disclosure-alpha",
                "instrument_id": "005930",
            },
        )
        assert readiness_response.status_code == 200
        assert readiness_response.json()["reconciliation_break_active"] is True
    finally:
        monkeypatch.undo()


def test_reconciliation_break_endpoint_clears_cached_breaks_when_repository_is_empty() -> None:
    monkeypatch = MonkeyPatch()
    store.breaks = {
        "recon-order-demo": BreakState(
            break_id="recon-order-demo",
            scope="ORDER",
            severity="HIGH",
            status="OPEN",
            detected_at_utc="2026-03-11T10:00:00+00:00",
        ),
    }
    monkeypatch.setattr(store.repository, "list_reconciliation_breaks", lambda limit=100, status_code=None: [])
    try:
        response = client.get("/ops/reconciliation-breaks")
        assert response.status_code == 200
        assert response.json() == []
    finally:
        monkeypatch.undo()


def test_ops_store_reloads_persisted_state_from_disk(tmp_path: Path) -> None:
    monkeypatch = MonkeyPatch()
    state_path = tmp_path / "ops-state.json"
    monkeypatch.setattr(store.settings, "ops_state_path", str(state_path))
    store.reset_state(delete_persisted=True)
    try:
        response = client.post(
            "/ops/kill-switch",
            json={"reason_code": "MANUAL_TEST", "activated_by": "tester"},
        )
        assert response.status_code == 200
        assert state_path.exists()

        store._reset_defaults()
        store.audit_events = []
        store.reload_state()

        assert store.mode.mode.value == "KILL_SWITCH"
        assert store.audit_events[0].action == "ACTIVATE_KILL_SWITCH"
    finally:
        monkeypatch.undo()


def test_loop_endpoints_reload_durable_scheduler_state() -> None:
    store.acquire_loop_lease(
        loop_id="shadow-live-main",
        service_name="shadow-live",
        owner_id="shadow-live-test",
        interval_seconds=30,
        execute_live=False,
        persist=True,
        ttl_seconds=180.0,
        actor="shadow-live-test",
        reason_code="TEST_START",
    )

    loops_response = client.get("/ops/loops")
    assert loops_response.status_code == 200
    assert loops_response.json()[0]["loop_id"] == "shadow-live-main"
    assert loops_response.json()[0]["desired_running"] is True

    loop_response = client.get("/ops/loops/shadow-live-main")
    assert loop_response.status_code == 200
    assert loop_response.json()["owner_id"] == "shadow-live-test"


def test_execution_readiness_uses_persisted_session_state() -> None:
    store.set_session_state(
        venue="KRX",
        session_code="HALTED",
        market_data_ok=False,
        degraded=True,
        entry_allowed=False,
        reason_codes=["TRADING_HALT_ACTIVE"],
    )

    readiness_response = client.post(
        "/ops/execution-readiness",
        json={
            "account_id": "default",
            "strategy_id": "disclosure-alpha",
            "instrument_id": "005930",
        },
    )

    assert readiness_response.status_code == 200
    body = readiness_response.json()
    assert body["market_data_ok"] is False
    assert body["session_entry_allowed"] is False
    assert "TRADING_HALT_ACTIVE" in body["reason_codes"]


def test_live_controls_endpoint_uses_default_limit_and_updates_state() -> None:
    get_response = client.get("/ops/live-controls")
    assert get_response.status_code == 200
    assert get_response.json()["max_order_value_krw"] == 500000
    assert get_response.json()["auto_loop_interval_seconds"] == 60
    assert get_response.json()["autonomous_loop_enabled"] is False

    update_response = client.post(
        "/ops/live-controls",
        json={
            "max_order_value_krw": 750000,
            "auto_loop_interval_seconds": 45,
            "actor": "ops-console",
            "reason_code": "TEST_UPDATE",
        },
    )
    assert update_response.status_code == 200
    body = update_response.json()
    assert body["max_order_value_krw"] == 750000
    assert body["auto_loop_interval_seconds"] == 45

    persisted_response = client.get("/ops/live-controls")
    assert persisted_response.status_code == 200
    assert persisted_response.json()["max_order_value_krw"] == 750000


def test_live_controls_endpoint_reloads_durable_state_before_partial_update(tmp_path: Path) -> None:
    monkeypatch = MonkeyPatch()
    state_path = tmp_path / "ops-state.json"
    monkeypatch.setattr(store.settings, "ops_state_path", str(state_path))
    store.reset_state(delete_persisted=True)
    store.set_live_control(
        max_order_value_krw=640000,
        auto_loop_interval_seconds=75,
        autonomous_loop_enabled=True,
        actor="test",
        reason_code="PREPARE_RELOAD",
    )
    store.live_control = LiveControlState()
    try:
        response = client.post(
            "/ops/live-controls",
            json={
                "max_order_value_krw": 700000,
                "actor": "ops-console",
                "reason_code": "PARTIAL_UPDATE",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["max_order_value_krw"] == 700000
        assert body["auto_loop_interval_seconds"] == 75
        assert body["autonomous_loop_enabled"] is True
    finally:
        monkeypatch.undo()


def test_live_loop_start_endpoint_orchestrates_shadow_and_broker() -> None:
    monkeypatch = MonkeyPatch()
    service_calls: list[tuple[str, str, dict]] = []

    async def fake_post_service_json(*, base_url: str, path: str, payload: dict) -> dict:
        service_calls.append((base_url, path, payload))
        if path == "/live/arm":
            return {"live_trading_armed": True}
        if path == "/loop/start":
            return {
                "running": True,
                "execute_live": True,
                "persist": True,
                "interval_seconds": payload["interval_seconds"],
                "desired_running": True,
                "run_count": 0,
            }
        raise AssertionError(f"unexpected service path {path}")

    monkeypatch.setattr(ops_main_module, "_post_service_json", fake_post_service_json)
    try:
        response = client.post(
            "/ops/live-loop/start",
            json={
                "max_order_value_krw": 600000,
                "interval_seconds": 45,
                "actor": "ops-console",
                "reason_code": "DASHBOARD_START",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["live_control"]["autonomous_loop_enabled"] is True
        assert body["live_control"]["max_order_value_krw"] == 600000
        assert body["loop"]["running"] is True
        assert service_calls[0][1] == "/live/arm"
        assert service_calls[1][1] == "/loop/start"
    finally:
        monkeypatch.undo()


def test_post_service_json_maps_connect_errors_to_bad_gateway() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_post(self, path, json):
        request = httpx.Request("POST", f"http://broker-gateway:8001{path}")
        raise httpx.ConnectError("connection failed", request=request)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    try:
        try:
            asyncio.run(
                ops_main_module._post_service_json(
                    base_url="http://broker-gateway:8001",
                    path="/live/arm",
                    payload={"operator_id": "ops-console"},
                )
            )
            raise AssertionError("expected _post_service_json to raise HTTPException")
        except ops_main_module.HTTPException as exc:
            assert exc.status_code == 502
            assert "broker-gateway:8001/live/arm" in str(exc.detail)
    finally:
        monkeypatch.undo()


def test_live_loop_stop_endpoint_orchestrates_shadow_and_broker() -> None:
    monkeypatch = MonkeyPatch()
    service_calls: list[tuple[str, str, dict]] = []
    store.set_live_control(
        max_order_value_krw=550000,
        auto_loop_interval_seconds=30,
        autonomous_loop_enabled=True,
        actor="test",
        reason_code="PREPARE_STOP",
    )

    async def fake_post_service_json(*, base_url: str, path: str, payload: dict) -> dict:
        service_calls.append((base_url, path, payload))
        if path == "/loop/stop":
            return {
                "running": False,
                "execute_live": False,
                "persist": True,
                "interval_seconds": 30,
                "desired_running": False,
                "run_count": 4,
            }
        if path == "/live/disarm":
            return {"live_trading_armed": False}
        raise AssertionError(f"unexpected service path {path}")

    monkeypatch.setattr(ops_main_module, "_post_service_json", fake_post_service_json)
    try:
        response = client.post(
            "/ops/live-loop/stop",
            json={"actor": "ops-console", "reason_code": "DASHBOARD_STOP"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["live_control"]["autonomous_loop_enabled"] is False
        assert body["loop"]["running"] is False
        assert service_calls[0][1] == "/loop/stop"
        assert service_calls[1][1] == "/live/disarm"
    finally:
        monkeypatch.undo()


def test_live_loop_stop_endpoint_preserves_enabled_state_when_remote_stop_fails() -> None:
    monkeypatch = MonkeyPatch()
    store.set_live_control(
        max_order_value_krw=500000,
        auto_loop_interval_seconds=60,
        autonomous_loop_enabled=True,
        actor="test",
        reason_code="PREPARE_STOP_FAILURE",
    )

    async def fake_post_service_json(*, base_url: str, path: str, payload: dict) -> dict:
        if path == "/loop/stop":
            raise ops_main_module.HTTPException(status_code=502, detail="shadow stop failed")
        raise AssertionError(f"unexpected service path {path}")

    monkeypatch.setattr(ops_main_module, "_post_service_json", fake_post_service_json)
    try:
        response = client.post(
            "/ops/live-loop/stop",
            json={"actor": "ops-console", "reason_code": "DASHBOARD_STOP"},
        )
        assert response.status_code == 502
        assert store.get_live_control().autonomous_loop_enabled is True
    finally:
        monkeypatch.undo()
