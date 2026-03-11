import pytest

from apps.market_intel.service import MarketIntelService


BUYBACK_REPORT = "\uc790\uae30\uc8fc\uc2dd\ucde8\ub4dd\uacb0\uc815"
RIGHTS_OFFERING_REPORT = "\uc720\uc0c1\uc99d\uc790\uacb0\uc815"


@pytest.mark.asyncio
async def test_live_structured_events_build_clusters_and_candidates() -> None:
    service = MarketIntelService()

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

    decisions = service.list_candidate_decisions()
    assert len(decisions) == 3
    assert any(decision.candidate_status == "REJECTED_BLOCK_RULE" for decision in decisions)
    assert any(decision.candidate_status == "REJECTED_INVALID_SYMBOL" for decision in decisions)
    assert any(decision.candidate_status == "SELECTED" for decision in decisions)
