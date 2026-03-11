from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from uuid import uuid4

from libs.adapters.dart import OpenDARTClient
from libs.adapters.openai_parser import OpenAIParserClient
from libs.contracts.messages import DisclosureEvent, TradeCandidate
from libs.config.settings import get_settings
from libs.domain.enums import OrderSide


@dataclass(slots=True)
class MarketIntelSnapshot:
    disclosure_backlog: int
    parser_mode: str
    low_confidence_count: int
    last_disclosure_utc: datetime | None


class MarketIntelService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.dart_client = OpenDARTClient()
        self.parser_client = OpenAIParserClient()
        self._live_candidate_cache: list[TradeCandidate] = []
        self._live_candidate_cache_utc: datetime | None = None

    def snapshot(self) -> MarketIntelSnapshot:
        return MarketIntelSnapshot(
            disclosure_backlog=3,
            parser_mode="LLM_WITH_FALLBACK",
            low_confidence_count=1,
            last_disclosure_utc=datetime.now(UTC) - timedelta(minutes=7),
        )

    def sample_disclosures(self) -> list[DisclosureEvent]:
        return [
            DisclosureEvent(
                disclosure_id="20260311000123",
                instrument_id="005930",
                event_type="EARNINGS_PRELIM",
                direction="POSITIVE",
                magnitude=0.81,
                confidence=0.92,
                tradeability="ALLOW",
                hard_block_candidate=False,
                parser_version="llm-structured-v1",
            )
        ]

    def sample_candidates(self) -> list[TradeCandidate]:
        return [
            TradeCandidate(
                candidate_id="candidate-demo",
                strategy_id="disclosure-alpha",
                account_scope="default",
                instrument_id="005930",
                side=OrderSide.BUY,
                expected_edge_bps=28.5,
                target_notional_krw=1_500_000,
                entry_style="PASSIVE_FIRST",
                expire_ts_utc=datetime.now(UTC) + timedelta(minutes=15),
                meta_model_version="v1",
                source_signal_refs=["disclosure:20260311000123"],
            )
        ]

    def _normalized_report_name(self, report_name: str) -> str:
        return " ".join((report_name or "").split())

    def _classify_disclosure(self, item: dict) -> tuple[str, float, int] | None:
        report_name = self._normalized_report_name(str(item.get("report_nm") or ""))
        block_keywords = [
            "불성실공시",
            "관리종목",
            "상장폐지",
            "감사의견",
            "회생절차",
            "감자결정",
            "유상증자결정",
            "전환사채권발행결정",
            "신주인수권부사채권발행결정",
            "교환사채권발행결정",
        ]
        if any(keyword in report_name for keyword in block_keywords):
            return None

        positive_rules = [
            ("자기주식취득결정", "BUYBACK", 45.0, 1_500_000),
            ("단일판매ㆍ공급계약체결", "SUPPLY_CONTRACT", 32.0, 1_500_000),
            ("현금ㆍ현물배당결정", "DIVIDEND", 14.0, 900_000),
            ("영업(잠정)실적", "EARNINGS_PRELIM", 20.0, 1_200_000),
            ("매출액또는손익구조30", "EARNINGS_SHIFT", 24.0, 1_300_000),
        ]
        for keyword, event_type, expected_edge_bps, target_notional in positive_rules:
            if keyword in report_name:
                return event_type, expected_edge_bps, target_notional
        return None

    async def live_candidates(self, *, limit: int = 5, force_refresh: bool = False) -> list[TradeCandidate]:
        now = datetime.now(UTC)
        if (
            not force_refresh
            and self._live_candidate_cache_utc is not None
            and (now - self._live_candidate_cache_utc) < timedelta(minutes=5)
        ):
            return self._live_candidate_cache[:limit]

        try:
            trading_date = datetime.now(ZoneInfo(self.settings.app_timezone)).date()
        except ZoneInfoNotFoundError:
            trading_date = datetime.now().date()
        payload = await self.dart_client.list_disclosures(trading_date, trading_date)
        candidates: list[TradeCandidate] = []
        for item in payload.get("list", []):
            stock_code = str(item.get("stock_code") or "").strip()
            corp_cls = str(item.get("corp_cls") or "").strip().upper()
            receipt_no = str(item.get("rcept_no") or "").strip()
            if corp_cls not in {"Y", "K"} or len(stock_code) != 6 or not receipt_no:
                continue
            classified = self._classify_disclosure(item)
            if classified is None:
                continue
            event_type, expected_edge_bps, target_notional = classified
            candidates.append(
                TradeCandidate(
                    candidate_id=f"candidate-{stock_code}-{receipt_no}",
                    strategy_id="disclosure-alpha-live",
                    account_scope="default",
                    instrument_id=stock_code,
                    side=OrderSide.BUY,
                    expected_edge_bps=expected_edge_bps,
                    target_notional_krw=target_notional,
                    entry_style="PASSIVE_FIRST",
                    expire_ts_utc=datetime.now(UTC) + timedelta(minutes=20),
                    meta_model_version="disclosure-heuristic-v1",
                    source_signal_refs=[
                        f"disclosure:{receipt_no}",
                        f"event:{event_type}",
                    ],
                )
            )

        self._live_candidate_cache = candidates
        self._live_candidate_cache_utc = now
        return candidates[:limit]

    async def parse_raw_disclosure(self, *, instrument_id: str, raw_text: str) -> dict:
        parsed = await self.parser_client.parse_disclosure(raw_text)
        disclosure = DisclosureEvent(
            disclosure_id=f"manual-{uuid4().hex[:12]}",
            instrument_id=instrument_id,
            event_type=str(parsed.structured["event_type"]),
            direction=str(parsed.structured["direction"]),
            confidence=float(parsed.structured["confidence"]),
            tradeability=str(parsed.structured["tradeability"]),
            hard_block_candidate=bool(parsed.structured["hard_block_candidate"]),
            parser_version="openai-v1" if not parsed.used_fallback else "fallback-v1",
        )
        candidate = None
        if disclosure.tradeability == "ALLOW" and disclosure.direction in {"POSITIVE", "UP"}:
            candidate = TradeCandidate(
                candidate_id=f"candidate-{uuid4().hex[:12]}",
                strategy_id="disclosure-alpha",
                account_scope="default",
                instrument_id=instrument_id,
                side=OrderSide.BUY,
                expected_edge_bps=20.0 if parsed.used_fallback else 35.0,
                target_notional_krw=1_000_000,
                entry_style="PASSIVE_FIRST",
                expire_ts_utc=datetime.now(UTC) + timedelta(minutes=20),
                meta_model_version="llm-disclosure-v1",
                source_signal_refs=[disclosure.disclosure_id],
            )
        return {"disclosure": disclosure, "candidate": candidate, "parser": parsed}

    async def sync_corp_codes(self) -> dict:
        codes = await self.dart_client.download_corp_codes()
        return {"count": len(codes), "sample": codes[:5]}

    async def list_latest_disclosures(self, begin: date, end: date) -> dict:
        return await self.dart_client.list_disclosures(begin, end)


market_intel_service = MarketIntelService()
