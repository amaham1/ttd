from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from apps.shadow_live.service import shadow_live_service
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        await shadow_live_service.restore_loop_if_needed()
    except Exception as exc:
        shadow_live_service.loop_last_error = str(exc)
    yield
    await shadow_live_service.close()


app = FastAPI(title="shadow-live", version="0.1.0", lifespan=lifespan)


class ShadowRunRequest(BaseModel):
    execute_live: bool = False
    persist: bool = True


class ShadowLoopStartRequest(BaseModel):
    interval_seconds: int = 60
    execute_live: bool = False
    persist: bool = True


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "shadow-live"}


@app.get("/snapshot")
async def snapshot():
    return shadow_live_service.snapshot()


@app.get("/loop/status")
async def loop_status():
    return shadow_live_service.loop_snapshot()


@app.get("/plan/latest")
async def latest_plan():
    return shadow_live_service.last_execution_plan


@app.get("/preview/micro-test-candidates")
async def preview_micro_test_candidates(force_refresh: bool = False, limit: int = 10):
    return await shadow_live_service.preview_micro_test_candidates(
        force_refresh=force_refresh,
        limit=limit,
    )


@app.get("/verify/post-trade/latest")
async def verify_post_trade_latest():
    return await shadow_live_service.verify_latest_execution()


@app.post("/loop/start")
async def start_loop(request: ShadowLoopStartRequest):
    try:
        return await shadow_live_service.start_loop(
            interval_seconds=request.interval_seconds,
            execute_live=request.execute_live,
            persist=request.persist,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/loop/stop")
async def stop_loop():
    return await shadow_live_service.stop_loop()


@app.post("/run/sample")
async def run_sample_shadow_live(request: ShadowRunRequest):
    return await shadow_live_service.run_once(execute_live=request.execute_live, persist=request.persist)
