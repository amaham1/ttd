from fastapi import FastAPI
from pydantic import BaseModel

from apps.replay_runner.service import replay_runner_service
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)

app = FastAPI(title="replay-runner", version="0.1.0")


class ReplayPackageRequest(BaseModel):
    trading_date: str


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "replay-runner"}


@app.get("/snapshot")
async def snapshot():
    return replay_runner_service.snapshot()


@app.post("/package/sample")
async def build_sample_package(request: ReplayPackageRequest):
    return replay_runner_service.build_sample_package(request.trading_date)
