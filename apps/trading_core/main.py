from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from apps.market_intel.service import market_intel_service
from apps.trading_core.service import trading_core_service
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)

app = FastAPI(title="trading-core", version="0.1.0")


class PersistRequest(BaseModel):
    persist: bool = False


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "trading-core"}


@app.get("/snapshot")
async def snapshot():
    return trading_core_service.snapshot()


@app.post("/evaluate/sample-candidate")
async def evaluate_sample_candidate():
    candidate = market_intel_service.sample_candidates()[0]
    decision = trading_core_service.evaluate_candidate(candidate)
    intent = trading_core_service.build_trade_intent(candidate, decision)
    return {
        "candidate": candidate,
        "decision": decision,
        "intent": intent,
    }


@app.post("/pipeline/sample")
async def run_sample_pipeline(request: PersistRequest):
    candidate = market_intel_service.sample_candidates()[0]
    decision = trading_core_service.evaluate_candidate(candidate)
    intent = trading_core_service.build_trade_intent(candidate, decision)
    if not request.persist:
        return {
            "candidate": candidate,
            "decision": decision,
            "intent": intent,
            "persisted": False,
        }
    try:
        result = trading_core_service.persist_pipeline(candidate=candidate, decision=decision, intent=intent)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"persistence failed: {exc}") from exc
    result["persisted"] = True
    return result
