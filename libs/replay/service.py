from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from uuid import uuid4


@dataclass(slots=True)
class ReplayJob:
    replay_job_id: str
    trading_date: date
    status: str
    source_bundle: str
    scenario: str
    created_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))
    notes: str | None = None


class ReplayJobService:
    def __init__(self) -> None:
        self._jobs: dict[str, ReplayJob] = self._default_jobs()

    @staticmethod
    def _default_jobs() -> dict[str, ReplayJob]:
        return {
            "replay-demo": ReplayJob(
                replay_job_id="replay-demo",
                trading_date=date(2026, 3, 10),
                status="READY",
                source_bundle="minio://replay-packages/demo-2026-03-10.tar.zst",
                scenario="baseline",
            )
        }

    def list_jobs(self) -> list[ReplayJob]:
        return sorted(self._jobs.values(), key=lambda job: job.created_at_utc, reverse=True)

    def create_job(self, trading_date: date, scenario: str, notes: str | None = None) -> ReplayJob:
        replay_job = ReplayJob(
            replay_job_id=f"replay-{uuid4().hex[:12]}",
            trading_date=trading_date,
            status="QUEUED",
            source_bundle=f"minio://replay-packages/{trading_date.isoformat()}.tar.zst",
            scenario=scenario,
            notes=notes,
        )
        self._jobs[replay_job.replay_job_id] = replay_job
        return replay_job

    def reset(self) -> None:
        self._jobs = self._default_jobs()


replay_job_service = ReplayJobService()
