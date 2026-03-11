from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from libs.adapters.dart import OpenDARTClient


class CommonStockUniverseError(RuntimeError):
    pass


@dataclass(slots=True)
class CommonStockUniverseSnapshot:
    symbol_count: int
    loaded_at_utc: datetime | None


class CommonStockUniverseService:
    def __init__(self, dart_client: OpenDARTClient | None = None) -> None:
        self.dart_client = dart_client or OpenDARTClient()
        self._symbols: set[str] = set()
        self._loaded_at_utc: datetime | None = None

    async def close(self) -> None:
        await self.dart_client.close()

    async def ensure_loaded(self) -> None:
        if self._loaded_at_utc and self._loaded_at_utc > datetime.now(UTC) - timedelta(hours=12):
            return
        corp_codes = await self.dart_client.download_corp_codes()
        symbols = {row.stock_code for row in corp_codes if row.stock_code and len(row.stock_code) == 6}
        if not symbols:
            raise CommonStockUniverseError("failed to load common stock universe from OpenDART")
        self._symbols = symbols
        self._loaded_at_utc = datetime.now(UTC)

    async def is_common_stock(self, symbol: str) -> bool:
        await self.ensure_loaded()
        return symbol in self._symbols

    def snapshot(self) -> CommonStockUniverseSnapshot:
        return CommonStockUniverseSnapshot(
            symbol_count=len(self._symbols),
            loaded_at_utc=self._loaded_at_utc,
        )
