import pytest

from libs.adapters.dart import DartCorpCode
from libs.services.common_stock_universe import CommonStockUniverseService


class FakeDartClient:
    async def download_corp_codes(self):
        return [
            DartCorpCode(corp_code="001", corp_name="Samsung", stock_code="005930", modify_date="20260311"),
            DartCorpCode(corp_code="002", corp_name="ETF", stock_code=None, modify_date="20260311"),
        ]

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_common_stock_universe_accepts_only_listed_common_stock() -> None:
    service = CommonStockUniverseService(dart_client=FakeDartClient())

    assert await service.is_common_stock("005930") is True
    assert await service.is_common_stock("069500") is False
