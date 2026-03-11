from datetime import date

from libs.replay.service import ReplayJobService


def test_replay_job_service_creates_queued_job() -> None:
    service = ReplayJobService()
    job = service.create_job(date(2026, 3, 11), "delay-injection")

    assert job.status == "QUEUED"
    assert job.scenario == "delay-injection"
    assert len(service.list_jobs()) >= 2
