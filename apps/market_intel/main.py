from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from apps.market_intel.service import market_intel_service
from libs.config.settings import get_settings
from libs.observability.logging import configure_logging

settings = get_settings()
configure_logging(settings.app_log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await market_intel_service.dart_client.close()
    await market_intel_service.parser_client.close()


app = FastAPI(title="market-intel", version="0.1.0", lifespan=lifespan)


class DisclosureParseRequest(BaseModel):
    instrument_id: str
    raw_text: str


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "market-intel"}


@app.get("/snapshot")
async def snapshot():
    return market_intel_service.snapshot()


@app.get("/sample/disclosures")
async def sample_disclosures():
    return market_intel_service.sample_disclosures()


@app.get("/sample/candidates")
async def sample_candidates():
    return market_intel_service.sample_candidates()


@app.get("/candidates/live")
async def live_candidates(limit: int = 5, force_refresh: bool = False):
    return await market_intel_service.live_candidates(limit=limit, force_refresh=force_refresh)


@app.post("/parse/disclosure")
async def parse_disclosure(request: DisclosureParseRequest):
    return await market_intel_service.parse_raw_disclosure(
        instrument_id=request.instrument_id,
        raw_text=request.raw_text,
    )


@app.post("/dart/corp-codes")
async def sync_corp_codes():
    return await market_intel_service.sync_corp_codes()
