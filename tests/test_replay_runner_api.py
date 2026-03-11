from fastapi.testclient import TestClient

from apps.replay_runner.main import app


client = TestClient(app)


def test_build_sample_package_endpoint() -> None:
    response = client.post("/package/sample", json={"trading_date": "2026-03-11"})

    assert response.status_code == 200
    body = response.json()
    assert body["package_name"] == "replay-2026-03-11.tar.gz"
    assert body["manifest"]["raw_event_count"] == 2
