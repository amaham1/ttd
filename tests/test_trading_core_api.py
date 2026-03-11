from fastapi.testclient import TestClient

from apps.trading_core.main import app


client = TestClient(app)


def test_sample_pipeline_without_persistence() -> None:
    response = client.post("/pipeline/sample", json={"persist": False})

    assert response.status_code == 200
    body = response.json()
    assert body["persisted"] is False
    assert body["candidate"]["candidate_id"] == "candidate-demo"
