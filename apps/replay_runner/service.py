from dataclasses import dataclass
from datetime import UTC, datetime

from libs.replay.service import ReplayJobService, replay_job_service
from libs.services.replay_package_service import ReplayPackageService


@dataclass(slots=True)
class ReplayRunnerSnapshot:
    active_jobs: int
    last_run_utc: datetime | None
    worker_mode: str


class ReplayRunnerService:
    def __init__(self, jobs: ReplayJobService) -> None:
        self.jobs = jobs
        self.package_service = ReplayPackageService()

    def snapshot(self) -> ReplayRunnerSnapshot:
        return ReplayRunnerSnapshot(
            active_jobs=len([job for job in self.jobs.list_jobs() if job.status in {"QUEUED", "RUNNING"}]),
            last_run_utc=datetime.now(UTC),
            worker_mode="IDLE",
        )

    def build_sample_package(self, trading_date: str) -> dict:
        package = self.package_service.build_package(
            trading_date=trading_date,
            raw_events=[
                {"source": "KIS", "channel": "REST", "endpoint": "order-cash", "odno": "8300012345"},
                {"source": "KIS", "channel": "WS", "endpoint": "fill-notice", "oder_no": "8300012345"},
            ],
            canonical_events=[
                {"message_name": "OrderAcked", "internal_order_id": "order-demo"},
                {"message_name": "FillReceived", "internal_order_id": "order-demo"},
            ],
            config_snapshot={"policy_version": "1.0.0", "mode": "NORMAL"},
            model_snapshot={"parser_version": "fallback-v1", "signal_model": "disclosure-alpha-v1"},
        )
        return {
            "package_name": package.package_name,
            "manifest": package.manifest,
            "size_bytes": len(package.content),
        }


replay_runner_service = ReplayRunnerService(replay_job_service)
