import pytest

from apps.market_intel.service import MarketIntelService


@pytest.mark.asyncio
async def test_live_candidates_filters_to_positive_listed_common_stock() -> None:
    service = MarketIntelService()

    async def fake_list_disclosures(begin, end):
        return {
            "list": [
                {
                    "corp_cls": "Y",
                    "stock_code": "005930",
                    "report_nm": "자기주식취득결정",
                    "rcept_no": "20260311000001",
                },
                {
                    "corp_cls": "Y",
                    "stock_code": "012345",
                    "report_nm": "유상증자결정",
                    "rcept_no": "20260311000002",
                },
                {
                    "corp_cls": "E",
                    "stock_code": "",
                    "report_nm": "자기주식취득결정",
                    "rcept_no": "20260311000003",
                },
            ]
        }

    service.dart_client.list_disclosures = fake_list_disclosures  # type: ignore[method-assign]
    try:
        candidates = await service.live_candidates(limit=5, force_refresh=True)
    finally:
        await service.dart_client.close()
        await service.parser_client.close()

    assert len(candidates) == 1
    assert candidates[0].instrument_id == "005930"
    assert candidates[0].strategy_id == "disclosure-alpha-live"
