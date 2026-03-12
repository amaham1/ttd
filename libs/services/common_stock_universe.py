from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import re

from libs.adapters.pykrx_backfill import PyKRXBackfillClient
from libs.adapters.pykrx_backfill import PyKRXBackfillError
from libs.adapters.dart import OpenDARTClient


class CommonStockUniverseError(RuntimeError):
    pass


@dataclass(slots=True)
class CommonStockUniverseSnapshot:
    symbol_count: int
    loaded_at_utc: datetime | None


_PREFERRED_STOCK_NAME_PATTERN = re.compile(r"(?:\d+우(?:B|C)?|우(?:B|C)?)(?:\([^)]*\))?$")


class CommonStockUniverseService:
    def __init__(
        self,
        dart_client: OpenDARTClient | None = None,
        pykrx_client: PyKRXBackfillClient | None = None,
    ) -> None:
        self.dart_client = dart_client or OpenDARTClient()
        self.pykrx_client = pykrx_client or PyKRXBackfillClient()
        self._symbols: set[str] = set()
        self._preferred_symbols: set[str] = set()
        self._non_preferred_symbols: set[str] = set()
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

    @staticmethod
    def _is_preferred_stock_name(name: str | None) -> bool:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            return False
        return _PREFERRED_STOCK_NAME_PATTERN.search(normalized_name) is not None

    def _preferred_stock_name(self, symbol: str) -> str | None:
        try:
            return self.pykrx_client.get_instrument_name(symbol)
        except PyKRXBackfillError:
            return None

    async def is_preferred_stock(self, symbol: str) -> bool:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return False
        if normalized_symbol in self._preferred_symbols:
            return True
        if normalized_symbol in self._non_preferred_symbols:
            return False
        preferred_name = self._preferred_stock_name(normalized_symbol)
        if not self._is_preferred_stock_name(preferred_name):
            self._non_preferred_symbols.add(normalized_symbol)
            return False
        self._preferred_symbols.add(normalized_symbol)
        return True

    async def is_common_or_preferred_stock(self, symbol: str) -> bool:
        if await self.is_common_stock(symbol):
            return True
        return await self.is_preferred_stock(symbol)

    def snapshot(self) -> CommonStockUniverseSnapshot:
        return CommonStockUniverseSnapshot(
            symbol_count=len(self._symbols),
            loaded_at_utc=self._loaded_at_utc,
        )
