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


class FakePyKRXClient:
    def __init__(self, name_by_symbol: dict[str, str] | None = None) -> None:
        self.name_by_symbol = name_by_symbol or {}

    def get_instrument_name(self, instrument_id: str) -> str | None:
        return self.name_by_symbol.get(str(instrument_id or "").strip())


@pytest.mark.asyncio
async def test_common_stock_universe_accepts_only_listed_common_stock() -> None:
    service = CommonStockUniverseService(dart_client=FakeDartClient())

    assert await service.is_common_stock("005930") is True
    assert await service.is_common_stock("069500") is False


@pytest.mark.asyncio
async def test_common_stock_universe_accepts_preferred_stock_via_pykrx_name() -> None:
    service = CommonStockUniverseService(
        dart_client=FakeDartClient(),
        pykrx_client=FakePyKRXClient(
            {
                "005935": "삼성전자우",
                "004989": "동원시스템즈우",
                "069500": "KODEX 200",
            }
        ),
    )

    assert await service.is_common_or_preferred_stock("005930") is True
    assert await service.is_common_or_preferred_stock("005935") is True
    assert await service.is_common_or_preferred_stock("004989") is True
    assert await service.is_common_or_preferred_stock("069500") is False
