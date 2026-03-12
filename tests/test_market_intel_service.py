from datetime import UTC, datetime, timedelta

import pytest

from apps.market_intel.service import MarketIntelService
from libs.adapters.openai_parser import InstrumentProfileResult
from libs.db.repositories import InstrumentProfileSnapshot


BUYBACK_REPORT = "\uc790\uae30\uc8fc\uc2dd\ucde8\ub4dd\uacb0\uc815"
RIGHTS_OFFERING_REPORT = "\uc720\uc0c1\uc99d\uc790\uacb0\uc815"


@pytest.mark.asyncio
async def test_live_structured_events_build_clusters_and_candidates() -> None:
    repository_calls: list[dict] = []

    class FakeRepository:
        def upsert_instrument_profile(self, **kwargs):
            repository_calls.append(kwargs)
            return None

    service = MarketIntelService(repository=FakeRepository())  # type: ignore[arg-type]

    async def fake_list_disclosures(begin, end):
        return {
            "list": [
                {
                    "corp_cls": "Y",
                    "stock_code": "005930",
                    "corp_name": "Samsung Electronics",
                    "report_nm": BUYBACK_REPORT,
                    "rcept_no": "20260311000001",
                },
                {
                    "corp_cls": "Y",
                    "stock_code": "012345",
                    "corp_name": "Dilution Risk Co",
                    "report_nm": RIGHTS_OFFERING_REPORT,
                    "rcept_no": "20260311000002",
                },
                {
                    "corp_cls": "E",
                    "stock_code": "",
                    "report_nm": BUYBACK_REPORT,
                    "rcept_no": "20260311000003",
                },
            ]
        }

    service.dart_client.list_disclosures = fake_list_disclosures  # type: ignore[method-assign]

    async def fake_infer_instrument_profile(**kwargs):
        return InstrumentProfileResult(
            sector_name="Technology",
            oil_up_beta=-0.2,
            usdkrw_up_beta=0.5,
            rates_up_beta=-0.1,
            china_growth_beta=0.3,
            domestic_demand_beta=0.1,
            export_beta=0.8,
            thematic_tags=["exporter", "technology"],
            rationale="Test market profile",
            confidence=0.9,
            used_fallback=False,
        )

    service.parser_client.infer_instrument_profile = fake_infer_instrument_profile  # type: ignore[method-assign]
    try:
        structured_events = await service.live_structured_events(force_refresh=True)
    finally:
        await service.dart_client.close()
        await service.parser_client.close()

    assert len(structured_events) == 1
    assert structured_events[0].instrument_id == "005930"
    assert structured_events[0].event_type == "BUYBACK"

    clusters = service.list_event_clusters()
    assert len(clusters) == 1
    assert clusters[0].instrument_id == "005930"

    triggers = service.list_watchlist_triggers()
    assert len(triggers) == 1
    assert triggers[0].instrument_id == "005930"

    candidates = await service.live_candidates(force_refresh=False)
    assert len(candidates) == 1
    assert candidates[0].matched_rule_id == "disclosure.positive.buyback"
    assert candidates[0].selection_confidence is not None
    assert candidates[0].sector_name == "Technology"
    assert structured_events[0].extraction_payload["market_profile"]["usdkrw_up_beta"] == 0.5
    assert repository_calls
    assert repository_calls[0]["instrument_id"] == "005930"
    assert repository_calls[0]["sector_name"] == "Technology"
    assert repository_calls[0]["thematic_tags"] == ["exporter", "technology"]
    assert repository_calls[0]["source_event_family"] == "CAPITAL_RETURN"

    decisions = service.list_candidate_decisions()
    assert len(decisions) == 3
    assert any(decision.candidate_status == "REJECTED_BLOCK_RULE" for decision in decisions)
    assert any(decision.candidate_status == "REJECTED_INVALID_SYMBOL" for decision in decisions)
    assert any(decision.candidate_status == "SELECTED" for decision in decisions)


@pytest.mark.asyncio
async def test_market_intel_pipeline_diagnostics_exposes_stage_counts() -> None:
    service = MarketIntelService()
    service.settings.google_api_key = ""

    async def fake_list_disclosures(begin, end):
        return {
            "list": [
                {
                    "corp_cls": "Y",
                    "stock_code": "005930",
                    "corp_name": "Samsung Electronics",
                    "report_nm": BUYBACK_REPORT,
                    "rcept_no": "20260311000001",
                },
                {
                    "corp_cls": "Y",
                    "stock_code": "012345",
                    "corp_name": "Dilution Risk Co",
                    "report_nm": RIGHTS_OFFERING_REPORT,
                    "rcept_no": "20260311000002",
                },
            ]
        }

    service.dart_client.list_disclosures = fake_list_disclosures  # type: ignore[method-assign]
    try:
        diagnostics = await service.pipeline_diagnostics(force_refresh=True, limit=5)
    finally:
        await service.dart_client.close()
        await service.parser_client.close()

    assert diagnostics["pipeline_counts"]["disclosure_input_count"] == 2
    assert diagnostics["pipeline_counts"]["structured_event_count"] == 1
    assert diagnostics["pipeline_counts"]["watchlist_trigger_count"] == 1
    assert diagnostics["status_counts"]["SELECTED"] == 1
    assert diagnostics["block_rule_counts"]["disclosure.risk.rights_offering"] == 1


@pytest.mark.asyncio
async def test_market_intel_reprofiles_stale_profiles() -> None:
    persisted_updates: list[dict] = []
    stale_row = InstrumentProfileSnapshot(
        instrument_id="005930",
        issuer_name="Samsung Electronics",
        sector_name="Technology",
        oil_up_beta=0.1,
        usdkrw_up_beta=0.2,
        rates_up_beta=-0.1,
        china_growth_beta=0.2,
        domestic_demand_beta=0.1,
        export_beta=0.7,
        thematic_tags=["technology"],
        rationale="old profile",
        confidence_score=0.6,
        used_fallback=False,
        source_event_family="CAPITAL_RETURN",
        source_event_type="BUYBACK",
        source_report_name=BUYBACK_REPORT,
        source_receipt_no="20260311000001",
        source_summary_text="Buyback decision with clean shareholder-return signal.",
        created_at_utc=datetime.now(UTC).replace(tzinfo=None) - timedelta(days=10),
        updated_at_utc=datetime.now(UTC).replace(tzinfo=None) - timedelta(days=10),
    )

    class FakeRepository:
        def list_instrument_profiles(self, *, limit=100, stale_before_utc=None):
            assert limit == 1
            assert stale_before_utc is not None
            return [stale_row]

        def count_instrument_profiles(self, *, stale_before_utc=None):
            return 1

        def upsert_instrument_profile(self, **kwargs):
            persisted_updates.append(kwargs)
            return None

    service = MarketIntelService(repository=FakeRepository())  # type: ignore[arg-type]
    service.settings.instrument_profile_reprofile_batch_size = 1
    service.settings.instrument_profile_stale_after_hours = 24

    async def fake_infer_instrument_profile(**kwargs):
        return InstrumentProfileResult(
            sector_name="Technology",
            oil_up_beta=-0.1,
            usdkrw_up_beta=0.55,
            rates_up_beta=-0.15,
            china_growth_beta=0.25,
            domestic_demand_beta=0.1,
            export_beta=0.8,
            thematic_tags=["exporter", "technology"],
            rationale="refreshed profile",
            confidence=0.88,
            used_fallback=False,
        )

    service.parser_client.infer_instrument_profile = fake_infer_instrument_profile  # type: ignore[method-assign]
    try:
        result = await service.reprofile_stale_instrument_profiles(limit=1)
    finally:
        await service.dart_client.close()
        await service.parser_client.close()

    assert result["scanned_count"] == 1
    assert result["updated_count"] == 1
    assert result["failed_count"] == 0
    assert persisted_updates
    assert persisted_updates[0]["instrument_id"] == "005930"
    assert persisted_updates[0]["source_event_family"] == "CAPITAL_RETURN"
    assert persisted_updates[0]["thematic_tags"] == ["exporter", "technology"]
