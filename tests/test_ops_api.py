from fastapi.testclient import TestClient

from apps.ops_api.main import app


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
