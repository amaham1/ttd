from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from uuid import uuid4

from libs.adapters.dart import OpenDARTClient
from libs.adapters.openai_parser import OpenAIParserClient
from libs.config.settings import get_settings
from libs.contracts.messages import (
    CandidateDecisionRecord,
    DisclosureEvent,
    DisclosureRuleDefinition,
    EventCluster,
    StructuredEvent,
    TradeCandidate,
    WatchlistTrigger,
)
from libs.domain.enums import OrderSide


@dataclass(slots=True)
class MarketIntelSnapshot:
    disclosure_backlog: int
    parser_mode: str
    low_confidence_count: int
    last_disclosure_utc: datetime | None
    structured_event_count: int = 0
    event_cluster_count: int = 0
    watchlist_trigger_count: int = 0


@dataclass(frozen=True, slots=True)
class DisclosureRuleSpec:
    rule: DisclosureRuleDefinition
    event_family: str
    event_type: str
    direction: str
    severity: float
    expected_edge_bps: float
    target_notional_krw: int
    time_sensitivity_minutes: int
    cash_impact_score: float = 0.0
    earnings_impact_score: float = 0.0
    dilution_risk_score: float = 0.0
    peer_spillover_score: float = 0.0
    hard_block_candidate: bool = False


@dataclass(slots=True)
class DisclosureRuleMatch:
    matched: bool
    rule_id: str | None = None
    rule_name: str | None = None
    rule_type: str | None = None
    reason: str | None = None
    event_family: str | None = None
    event_type: str | None = None
    direction: str | None = None
    severity: float | None = None
    expected_edge_bps: float | None = None
    target_notional_krw: int | None = None
    time_sensitivity_minutes: int | None = None
    cash_impact_score: float | None = None
    earnings_impact_score: float | None = None
    dilution_risk_score: float | None = None
    peer_spillover_score: float | None = None
    hard_block_candidate: bool = False


def _disclosure_rule(
    *,
    rule_id: str,
    rule_type: str,
    rule_name: str,
    match_pattern: str,
    reason_template: str,
    priority: int,
    event_family: str,
    event_type: str,
    direction: str,
    severity: float,
    expected_edge_bps: float,
    target_notional_krw: int,
    time_sensitivity_minutes: int,
    cash_impact_score: float = 0.0,
    earnings_impact_score: float = 0.0,
    dilution_risk_score: float = 0.0,
    peer_spillover_score: float = 0.0,
    hard_block_candidate: bool = False,
) -> DisclosureRuleSpec:
    return DisclosureRuleSpec(
        rule=DisclosureRuleDefinition(
            rule_id=rule_id,
            rule_type=rule_type,
            rule_name=rule_name,
            match_field="report_nm",
            match_pattern=match_pattern,
            decision_effect="candidate_reject" if rule_type == "block" else "candidate_allow",
            reason_template=reason_template,
            priority=priority,
        ),
        event_family=event_family,
        event_type=event_type,
        direction=direction,
        severity=severity,
        expected_edge_bps=expected_edge_bps,
        target_notional_krw=target_notional_krw,
        time_sensitivity_minutes=time_sensitivity_minutes,
        cash_impact_score=cash_impact_score,
        earnings_impact_score=earnings_impact_score,
        dilution_risk_score=dilution_risk_score,
        peer_spillover_score=peer_spillover_score,
        hard_block_candidate=hard_block_candidate,
    )


def _build_block_rule_specs() -> list[DisclosureRuleSpec]:
    return [
        _disclosure_rule(
            rule_id="disclosure.risk.unfaithful_disclosure",
            rule_type="block",
            rule_name="불성실 공시",
            match_pattern="\ubd88\uc131\uc2e4\uacf5\uc2dc",
            reason_template="불성실 공시는 해당 종목을 즉시 차단합니다.",
            priority=10,
            event_family="GOVERNANCE",
            event_type="UNFAITHFUL_DISCLOSURE",
            direction="NEGATIVE",
            severity=0.98,
            expected_edge_bps=-120.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.8,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.management_issue",
            rule_type="block",
            rule_name="관리종목",
            match_pattern="\uad00\ub9ac\uc885\ubaa9",
            reason_template="관리종목 지정 상태에서는 신규 진입을 차단합니다.",
            priority=20,
            event_family="REGULATORY",
            event_type="MANAGEMENT_ISSUE",
            direction="NEGATIVE",
            severity=0.96,
            expected_edge_bps=-110.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.6,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.delisting",
            rule_type="block",
            rule_name="상장폐지",
            match_pattern="\uc0c1\uc7a5\ud3d0\uc9c0",
            reason_template="상장폐지 관련 공시는 신규 진입을 차단합니다.",
            priority=30,
            event_family="REGULATORY",
            event_type="DELISTING",
            direction="NEGATIVE",
            severity=1.0,
            expected_edge_bps=-150.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.9,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.audit_opinion",
            rule_type="block",
            rule_name="감사의견",
            match_pattern="\uac10\uc0ac\uc758\uacac",
            reason_template="감사의견 리스크로 해당 종목을 차단합니다.",
            priority=40,
            event_family="GOVERNANCE",
            event_type="AUDIT_OPINION",
            direction="NEGATIVE",
            severity=0.92,
            expected_edge_bps=-95.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            earnings_impact_score=-0.5,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.rehabilitation",
            rule_type="block",
            rule_name="회생",
            match_pattern="\ud68c\uc0dd",
            reason_template="회생 관련 리스크로 해당 종목을 차단합니다.",
            priority=50,
            event_family="GOVERNANCE",
            event_type="REHABILITATION",
            direction="NEGATIVE",
            severity=0.94,
            expected_edge_bps=-100.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.capital_reduction",
            rule_type="block",
            rule_name="감자",
            match_pattern="\uac10\uc790\uacb0\uc815",
            reason_template="감자는 기존 주주 가치 훼손 가능성이 있어 해당 종목을 차단합니다.",
            priority=60,
            event_family="CAPITAL_STRUCTURE",
            event_type="CAPITAL_REDUCTION",
            direction="NEGATIVE",
            severity=0.9,
            expected_edge_bps=-90.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.7,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.rights_offering",
            rule_type="block",
            rule_name="유상증자",
            match_pattern="\uc720\uc0c1\uc99d\uc790\uacb0\uc815",
            reason_template="유상증자는 희석 리스크가 크므로 해당 종목을 차단합니다.",
            priority=70,
            event_family="CAPITAL_STRUCTURE",
            event_type="RIGHTS_OFFERING",
            direction="NEGATIVE",
            severity=0.88,
            expected_edge_bps=-85.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=1.0,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.convertible_bond",
            rule_type="block",
            rule_name="전환사채",
            match_pattern="\uc804\ud658\uc0ac\ucc44\ubc1c\ud589\uacb0\uc815",
            reason_template="전환사채 발행은 희석 리스크가 커서 해당 종목을 차단합니다.",
            priority=80,
            event_family="CAPITAL_STRUCTURE",
            event_type="CONVERTIBLE_BOND",
            direction="NEGATIVE",
            severity=0.84,
            expected_edge_bps=-70.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.85,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.bond_with_warrant",
            rule_type="block",
            rule_name="신주인수권부사채",
            match_pattern="\uc2e0\uc8fc\uc778\uc218\uad8c\ubd80\uc0ac\ucc44\ubc1c\ud589\uacb0\uc815",
            reason_template="신주인수권부사채 발행은 희석 리스크가 커서 해당 종목을 차단합니다.",
            priority=90,
            event_family="CAPITAL_STRUCTURE",
            event_type="BOND_WITH_WARRANT",
            direction="NEGATIVE",
            severity=0.83,
            expected_edge_bps=-68.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.82,
            hard_block_candidate=True,
        ),
        _disclosure_rule(
            rule_id="disclosure.risk.exchangeable_bond",
            rule_type="block",
            rule_name="교환사채",
            match_pattern="\uad50\ud658\uc0ac\ucc44\ubc1c\ud589\uacb0\uc815",
            reason_template="교환사채 발행은 잠재 매물 부담이 커서 해당 종목을 차단합니다.",
            priority=100,
            event_family="CAPITAL_STRUCTURE",
            event_type="EXCHANGEABLE_BOND",
            direction="NEGATIVE",
            severity=0.82,
            expected_edge_bps=-65.0,
            target_notional_krw=0,
            time_sensitivity_minutes=1440,
            dilution_risk_score=0.8,
            hard_block_candidate=True,
        ),
    ]


def _build_positive_rule_specs() -> list[DisclosureRuleSpec]:
    return [
        _disclosure_rule(
            rule_id="disclosure.positive.buyback",
            rule_type="positive",
            rule_name="자기주식 취득",
            match_pattern="\uc790\uae30\uc8fc\uc2dd\ucde8\ub4dd\uacb0\uc815",
            reason_template="자기주식 취득 공시는 주주환원 촉매로 해석합니다.",
            priority=10,
            event_family="CAPITAL_RETURN",
            event_type="BUYBACK",
            direction="POSITIVE",
            severity=0.92,
            expected_edge_bps=52.0,
            target_notional_krw=1_800_000,
            time_sensitivity_minutes=480,
            cash_impact_score=0.8,
            peer_spillover_score=0.25,
        ),
        _disclosure_rule(
            rule_id="disclosure.positive.supply_contract",
            rule_type="positive",
            rule_name="단일판매·공급계약",
            match_pattern="\ub2e8\uc77c\ud310\ub9e4\u00b7\uacf5\uae09\uacc4\uc57d\uccb4\uacb0",
            reason_template="단일판매·공급계약 공시는 실적 가시성 확대 촉매로 해석합니다.",
            priority=20,
            event_family="CONTRACT",
            event_type="SUPPLY_CONTRACT",
            direction="POSITIVE",
            severity=0.84,
            expected_edge_bps=38.0,
            target_notional_krw=1_500_000,
            time_sensitivity_minutes=720,
            cash_impact_score=0.7,
            earnings_impact_score=0.65,
            peer_spillover_score=0.2,
        ),
        _disclosure_rule(
            rule_id="disclosure.positive.dividend",
            rule_type="positive",
            rule_name="배당",
            match_pattern="\ud604\uae08\u00b7\ud604\ubb3c\ubc30\ub2f9\uacb0\uc815",
            reason_template="배당 공시는 주주환원 신호로 해석합니다.",
            priority=30,
            event_family="CAPITAL_RETURN",
            event_type="DIVIDEND",
            direction="POSITIVE",
            severity=0.68,
            expected_edge_bps=18.0,
            target_notional_krw=900_000,
            time_sensitivity_minutes=1440,
            cash_impact_score=0.5,
            peer_spillover_score=0.1,
        ),
        _disclosure_rule(
            rule_id="disclosure.positive.earnings_prelim",
            rule_type="positive",
            rule_name="잠정 실적",
            match_pattern="\uc601\uc5c5\(\uc7a0\uc815\)\uc2e4\uc801",
            reason_template="잠정 실적 공시는 단기 실적 촉매로 해석합니다.",
            priority=40,
            event_family="EARNINGS",
            event_type="EARNINGS_PRELIM",
            direction="POSITIVE",
            severity=0.78,
            expected_edge_bps=28.0,
            target_notional_krw=1_250_000,
            time_sensitivity_minutes=240,
            earnings_impact_score=0.9,
            peer_spillover_score=0.3,
        ),
        _disclosure_rule(
            rule_id="disclosure.positive.earnings_shift",
            rule_type="positive",
            rule_name="손익 구조 변화",
            match_pattern="\ub9e4\ucd9c\uc561\ub610\ub294\uc190\uc775\uad6c\uc870",
            reason_template="손익 구조 변화 공시는 실적 질 개선 촉매로 해석합니다.",
            priority=50,
            event_family="EARNINGS",
            event_type="EARNINGS_SHIFT",
            direction="POSITIVE",
            severity=0.72,
            expected_edge_bps=24.0,
            target_notional_krw=1_100_000,
            time_sensitivity_minutes=720,
            earnings_impact_score=0.72,
            peer_spillover_score=0.15,
        ),
    ]


class MarketIntelService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.dart_client = OpenDARTClient()
        self.parser_client = OpenAIParserClient()
        self._block_rule_specs = sorted(_build_block_rule_specs(), key=lambda spec: spec.rule.priority)
        self._positive_rule_specs = sorted(_build_positive_rule_specs(), key=lambda spec: spec.rule.priority)
        self._live_candidate_cache: list[TradeCandidate] = []
        self._latest_candidate_decisions: list[CandidateDecisionRecord] = []
        self._live_candidate_cache_utc: datetime | None = None
        self._latest_structured_events: list[StructuredEvent] = []
        self._latest_event_clusters: list[EventCluster] = []
        self._latest_watchlist_triggers: list[WatchlistTrigger] = []
        self._event_history_by_key: dict[str, list[datetime]] = defaultdict(list)

    def snapshot(self) -> MarketIntelSnapshot:
        low_confidence_count = len(
            [
                decision
                for decision in self._latest_candidate_decisions
                if decision.candidate_status in {"REJECTED_LOW_CONFIDENCE", "REJECTED_PARSER_REVIEW"}
            ]
        )
        last_event_utc = (
            max((event.event_ts_utc for event in self._latest_structured_events), default=None)
            if self._latest_structured_events
            else None
        )
        return MarketIntelSnapshot(
            disclosure_backlog=max(len(self._latest_candidate_decisions) - len(self._live_candidate_cache), 0),
            parser_mode="RULES_PLUS_STRUCTURED_EVENTS",
            low_confidence_count=low_confidence_count,
            last_disclosure_utc=last_event_utc,
            structured_event_count=len(self._latest_structured_events),
            event_cluster_count=len(self._latest_event_clusters),
            watchlist_trigger_count=len(self._latest_watchlist_triggers),
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
                parser_version="structured-event-v2",
            )
        ]

    def sample_structured_events(self) -> list[StructuredEvent]:
        return [
            StructuredEvent(
                event_id="event-005930-20260311000123",
                source_receipt_no="20260311000123",
                instrument_id="005930",
                issuer_name="Samsung Electronics",
                event_family="CAPITAL_RETURN",
                event_type="BUYBACK",
                direction="POSITIVE",
                severity=0.92,
                novelty=0.88,
                magnitude=0.81,
                parser_confidence=0.92,
                model_confidence=0.9,
                tradeability="ALLOW",
                hard_block_candidate=False,
                time_sensitivity_minutes=480,
                cash_impact_score=0.8,
                peer_spillover_score=0.25,
                extracted_summary="Buyback decision with high confidence and clean governance context.",
                extraction_payload={"matched_rule_id": "disclosure.positive.buyback"},
            )
        ]

    def sample_candidates(self) -> list[TradeCandidate]:
        sample_event = self.sample_structured_events()[0]
        return [
            TradeCandidate(
                candidate_id="candidate-demo",
                strategy_id=self.settings.selector_default_strategy_id,
                account_scope=self.settings.selector_default_account_scope,
                instrument_id=sample_event.instrument_id,
                side=OrderSide.BUY,
                expected_edge_bps=28.5,
                target_notional_krw=1_500_000,
                entry_style="PASSIVE_FIRST",
                expire_ts_utc=datetime.now(UTC) + timedelta(minutes=15),
                meta_model_version="selector-bridge-v2",
                source_signal_refs=[f"disclosure:{sample_event.source_receipt_no}"],
                source_event_ids=[sample_event.event_id],
                event_cluster_id="cluster-005930-buyback",
                source_report_name="\uc790\uae30\uc8fc\uc2dd\ucde8\ub4dd\uacb0\uc815",
                source_receipt_no=sample_event.source_receipt_no,
                matched_rule_id="disclosure.positive.buyback",
                selection_reason="예시 이벤트를 주주환원 후보로 승격했습니다.",
                candidate_status="SELECTED",
                ranking_score=45.0,
                ranking_reason="예시 후보가 이벤트 기반 점수가 가장 높습니다.",
                selected_rank=1,
                selection_confidence=0.9,
                expected_slippage_bps=6.0,
                tail_risk_penalty_bps=4.0,
                crowding_penalty_bps=3.0,
                cooldown_key="005930:BUYBACK:20260311",
                decision_id="selection-sample-005930",
            )
        ]

    def _normalized_report_name(self, report_name: str) -> str:
        return " ".join((report_name or "").split())

    def list_disclosure_rules(self) -> list[DisclosureRuleDefinition]:
        return [spec.rule for spec in [*self._positive_rule_specs, *self._block_rule_specs]]

    def list_candidate_decisions(self) -> list[CandidateDecisionRecord]:
        return list(self._latest_candidate_decisions)

    def get_candidate_decision(self, candidate_id: str) -> CandidateDecisionRecord | None:
        for decision in self._latest_candidate_decisions:
            if decision.candidate_id == candidate_id or decision.decision_id == candidate_id:
                return decision
        return None

    def list_structured_events(self) -> list[StructuredEvent]:
        return list(self._latest_structured_events)

    def list_event_clusters(self) -> list[EventCluster]:
        return list(self._latest_event_clusters)

    def list_watchlist_triggers(self) -> list[WatchlistTrigger]:
        return list(self._latest_watchlist_triggers)

    def _match_rule(self, report_name: str, specs: list[DisclosureRuleSpec]) -> DisclosureRuleMatch:
        for spec in specs:
            if not spec.rule.enabled:
                continue
            if spec.rule.match_pattern in report_name:
                return DisclosureRuleMatch(
                    matched=True,
                    rule_id=spec.rule.rule_id,
                    rule_name=spec.rule.rule_name,
                    rule_type=spec.rule.rule_type,
                    reason=spec.rule.reason_template,
                    event_family=spec.event_family,
                    event_type=spec.event_type,
                    direction=spec.direction,
                    severity=spec.severity,
                    expected_edge_bps=spec.expected_edge_bps,
                    target_notional_krw=spec.target_notional_krw,
                    time_sensitivity_minutes=spec.time_sensitivity_minutes,
                    cash_impact_score=spec.cash_impact_score,
                    earnings_impact_score=spec.earnings_impact_score,
                    dilution_risk_score=spec.dilution_risk_score,
                    peer_spillover_score=spec.peer_spillover_score,
                    hard_block_candidate=spec.hard_block_candidate,
                )
        return DisclosureRuleMatch(matched=False)

    def _classify_disclosure(self, item: dict) -> tuple[DisclosureRuleMatch, DisclosureRuleMatch]:
        report_name = self._normalized_report_name(str(item.get("report_nm") or ""))
        return (
            self._match_rule(report_name, self._block_rule_specs),
            self._match_rule(report_name, self._positive_rule_specs),
        )

    def _build_decision_id(self, stock_code: str, receipt_no: str) -> str:
        suffix = receipt_no or uuid4().hex[:12]
        symbol = stock_code or "unknown"
        return f"decision-{symbol}-{suffix}"

    def _build_invalid_symbol_decision(
        self,
        *,
        stock_code: str,
        receipt_no: str,
        report_name: str,
        corp_cls: str,
    ) -> CandidateDecisionRecord:
        return CandidateDecisionRecord(
            decision_id=self._build_decision_id(stock_code, receipt_no),
            source_receipt_no=receipt_no or None,
            source_report_name=report_name or None,
            source_symbol=stock_code or None,
            candidate_status="REJECTED_INVALID_SYMBOL",
            rejection_reason="국내 상장 보통주 종목만 거래 후보로 허용됩니다.",
            confidence_summary=f"corp_cls={corp_cls or 'EMPTY'}",
            decision_payload_json={
                "corp_cls": corp_cls,
                "stock_code": stock_code,
                "receipt_no": receipt_no,
                "report_name": report_name,
            },
        )

    def _build_blocked_decision(
        self,
        *,
        stock_code: str,
        receipt_no: str,
        report_name: str,
        block_match: DisclosureRuleMatch,
    ) -> CandidateDecisionRecord:
        return CandidateDecisionRecord(
            decision_id=self._build_decision_id(stock_code, receipt_no),
            source_receipt_no=receipt_no,
            source_report_name=report_name,
            source_symbol=stock_code,
            matched_block_rule_id=block_match.rule_id,
            candidate_status="REJECTED_BLOCK_RULE",
            rejection_reason=block_match.reason,
            confidence_summary=f"{block_match.rule_name} 규칙으로 차단됨",
            decision_payload_json={
                "report_name": report_name,
                "matched_block_rule": block_match.rule_id,
            },
        )

    def _build_low_confidence_decision(
        self,
        *,
        stock_code: str,
        receipt_no: str,
        report_name: str,
    ) -> CandidateDecisionRecord:
        return CandidateDecisionRecord(
            decision_id=self._build_decision_id(stock_code, receipt_no),
            source_receipt_no=receipt_no,
            source_report_name=report_name,
            source_symbol=stock_code,
            candidate_status="REJECTED_LOW_CONFIDENCE",
            rejection_reason="충분한 신뢰도로 매핑된 활성 긍정 규칙이 없어 제외했습니다.",
            confidence_summary="긍정 규칙 매칭 없음",
            decision_payload_json={"report_name": report_name},
        )

    def _event_dedupe_key(self, *, instrument_id: str, event_type: str, event_date: date) -> str:
        return f"{instrument_id}:{event_type}:{event_date.isoformat()}"

    def _compute_novelty(self, *, dedupe_key: str, event_ts_utc: datetime) -> float:
        recent_events = [
            timestamp
            for timestamp in self._event_history_by_key.get(dedupe_key, [])
            if (event_ts_utc - timestamp) <= timedelta(days=10)
        ]
        if not recent_events:
            return 0.92
        most_recent = max(recent_events)
        age_hours = max((event_ts_utc - most_recent).total_seconds() / 3600.0, 0.0)
        return max(0.18, min(0.9, age_hours / 48.0))

    def _build_structured_event(
        self,
        *,
        item: dict,
        positive_match: DisclosureRuleMatch,
        receipt_no: str,
        report_name: str,
        event_ts_utc: datetime,
    ) -> StructuredEvent:
        stock_code = str(item.get("stock_code") or "").strip()
        corp_name = str(item.get("corp_name") or "").strip() or None
        dedupe_key = self._event_dedupe_key(
            instrument_id=stock_code,
            event_type=positive_match.event_type or "GENERAL",
            event_date=event_ts_utc.date(),
        )
        novelty = self._compute_novelty(dedupe_key=dedupe_key, event_ts_utc=event_ts_utc)
        model_confidence = round(
            min(
                0.98,
                0.42
                + 0.33 * float(positive_match.severity or 0.0)
                + 0.25 * novelty
                - 0.18 * float(positive_match.dilution_risk_score or 0.0),
            ),
            4,
        )
        event_id = f"event-{stock_code}-{receipt_no}"
        return StructuredEvent(
            event_id=event_id,
            source_receipt_no=receipt_no,
            instrument_id=stock_code,
            issuer_id=stock_code,
            issuer_name=corp_name,
            event_family=positive_match.event_family or "GENERAL",
            event_type=positive_match.event_type or "GENERAL",
            direction=positive_match.direction or "NEUTRAL",
            severity=float(positive_match.severity or 0.0),
            novelty=novelty,
            magnitude=round(
                min(
                    1.0,
                    0.5 * float(positive_match.cash_impact_score or 0.0)
                    + 0.5 * float(positive_match.earnings_impact_score or 0.0)
                    + 0.2 * novelty,
                ),
                4,
            ),
            parser_confidence=0.86,
            model_confidence=model_confidence,
            tradeability="ALLOW",
            hard_block_candidate=bool(positive_match.hard_block_candidate),
            time_sensitivity_minutes=positive_match.time_sensitivity_minutes,
            cash_impact_score=positive_match.cash_impact_score,
            earnings_impact_score=positive_match.earnings_impact_score,
            dilution_risk_score=positive_match.dilution_risk_score,
            peer_spillover_score=positive_match.peer_spillover_score,
            extracted_summary=positive_match.reason,
            extraction_payload={
                "report_name": report_name,
                "matched_positive_rule": positive_match.rule_id,
                "dedupe_key": dedupe_key,
            },
            event_ts_utc=event_ts_utc,
        )

    def _build_event_clusters(self, structured_events: list[StructuredEvent]) -> list[EventCluster]:
        clusters: dict[str, list[StructuredEvent]] = defaultdict(list)
        for event in structured_events:
            key = f"{event.instrument_id}:{event.event_type}:{event.event_ts_utc.date().isoformat()}"
            clusters[key].append(event)

        results: list[EventCluster] = []
        for key, events in clusters.items():
            latest_event = max(events, key=lambda event: event.event_ts_utc)
            results.append(
                EventCluster(
                    cluster_id=f"cluster-{latest_event.instrument_id}-{latest_event.event_type.lower()}",
                    cluster_key=key,
                    instrument_id=latest_event.instrument_id,
                    issuer_id=latest_event.issuer_id,
                    event_family=latest_event.event_family,
                    event_type=latest_event.event_type,
                    event_direction=latest_event.direction,
                    source_receipt_nos=[
                        event.source_receipt_no for event in events if event.source_receipt_no is not None
                    ],
                    source_event_ids=[event.event_id for event in events],
                    severity=max(event.severity for event in events),
                    novelty=max(event.novelty for event in events),
                    representative_summary=latest_event.extracted_summary,
                    event_count=len(events),
                    latest_event_ts_utc=latest_event.event_ts_utc,
                )
            )
        results.sort(key=lambda cluster: (-(cluster.severity + cluster.novelty), cluster.instrument_id))
        return results

    def _build_watchlist_triggers(self, clusters: list[EventCluster]) -> list[WatchlistTrigger]:
        limit = min(self.settings.selector_watchlist_size, max(len(clusters), 0))
        triggers: list[WatchlistTrigger] = []
        for rank, cluster in enumerate(clusters[:limit], start=1):
            abnormal_event = cluster.severity >= 0.88 or cluster.novelty >= 0.88
            trigger_type = "REALTIME_INTENSIVE" if abnormal_event else "EVENT_SWING"
            reason_code = "SEVERE_EVENT" if abnormal_event else "TOP_RANKED_EVENT"
            triggers.append(
                WatchlistTrigger(
                    trigger_id=f"watch-{cluster.cluster_id}-{rank}",
                    instrument_id=cluster.instrument_id,
                    event_cluster_id=cluster.cluster_id,
                    trigger_type=trigger_type,
                    reason_code=reason_code,
                    priority=rank,
                    expires_at_utc=datetime.now(UTC) + timedelta(minutes=180 if abnormal_event else 480),
                    metadata={
                        "event_type": cluster.event_type,
                        "event_family": cluster.event_family,
                        "severity": cluster.severity,
                        "novelty": cluster.novelty,
                    },
                )
            )
        return triggers

    def _candidate_from_structured_event(
        self,
        *,
        structured_event: StructuredEvent,
        cluster: EventCluster,
        selected_rank: int,
    ) -> TradeCandidate:
        matched_rule_id = str(structured_event.extraction_payload.get("matched_positive_rule") or "")
        ranking_score = round(
            (structured_event.severity * 45.0)
            + (structured_event.novelty * 25.0)
            + (float(structured_event.cash_impact_score or 0.0) * 15.0)
            + (float(structured_event.earnings_impact_score or 0.0) * 15.0)
            - (float(structured_event.dilution_risk_score or 0.0) * 30.0),
            4,
        )
        selection_confidence = round(
            max(0.0, min(0.99, 0.6 * structured_event.model_confidence + 0.4 * structured_event.parser_confidence)),
            4,
        )
        return TradeCandidate(
            candidate_id=f"candidate-{structured_event.instrument_id}-{structured_event.source_receipt_no}",
            strategy_id=self.settings.selector_default_strategy_id,
            account_scope=self.settings.selector_default_account_scope,
            instrument_id=structured_event.instrument_id,
            side=OrderSide.BUY,
            expected_edge_bps=max(ranking_score, 0.0),
            target_notional_krw=max(
                500_000,
                int(
                    700_000
                    + (structured_event.severity * 600_000)
                    + ((structured_event.cash_impact_score or 0.0) * 400_000)
                ),
            ),
            entry_style="PASSIVE_FIRST",
            expire_ts_utc=datetime.now(UTC) + timedelta(minutes=cluster.event_count * 5 + 30),
            meta_model_version="structured-event-bridge-v2",
            source_signal_refs=[
                f"disclosure:{structured_event.source_receipt_no}",
                f"event:{structured_event.event_type}",
                f"cluster:{cluster.cluster_id}",
            ],
            source_event_ids=[structured_event.event_id],
            event_cluster_id=cluster.cluster_id,
            issuer_id=structured_event.issuer_id,
            source_report_name=str(structured_event.extraction_payload.get("report_name") or ""),
            source_receipt_no=structured_event.source_receipt_no,
            matched_rule_id=matched_rule_id or None,
            selection_reason=(
                f"{structured_event.event_type} 이벤트를 심각도 {structured_event.severity:.2f}, "
                f"신규성 {structured_event.novelty:.2f}, 신뢰도 {selection_confidence:.2f} 기준으로 상위에 배치했습니다."
            ),
            candidate_status="SELECTED" if selected_rank == 1 else "ELIGIBLE_NOT_SELECTED",
            ranking_score=ranking_score,
            ranking_reason=(
                "구조화 이벤트 브리지 점수는 심각도, 신규성, 현금 영향, "
                f"실적 영향, 희석 페널티를 반영합니다. 현재 순위는 {selected_rank}위입니다."
            ),
            selected_rank=selected_rank,
            selection_confidence=selection_confidence,
            expected_slippage_bps=8.0,
            tail_risk_penalty_bps=float(structured_event.dilution_risk_score or 0.0) * 15.0,
            crowding_penalty_bps=max(0.0, structured_event.severity - structured_event.novelty) * 10.0,
            cooldown_key=self._event_dedupe_key(
                instrument_id=structured_event.instrument_id,
                event_type=structured_event.event_type,
                event_date=structured_event.event_ts_utc.date(),
            ),
            decision_id=f"selection-{structured_event.instrument_id}-{structured_event.source_receipt_no}",
        )

    async def live_structured_events(
        self,
        *,
        limit: int | None = None,
        force_refresh: bool = False,
    ) -> list[StructuredEvent]:
        now = datetime.now(UTC)
        if (
            not force_refresh
            and self._live_candidate_cache_utc is not None
            and (now - self._live_candidate_cache_utc) < timedelta(minutes=5)
            and self._latest_structured_events
        ):
            return self._latest_structured_events[: limit or len(self._latest_structured_events)]

        try:
            trading_date = datetime.now(ZoneInfo(self.settings.app_timezone)).date()
        except ZoneInfoNotFoundError:
            trading_date = datetime.now().date()

        payload = await self.dart_client.list_disclosures(trading_date, trading_date)
        structured_events: list[StructuredEvent] = []
        decisions: list[CandidateDecisionRecord] = []

        for item in payload.get("list", []):
            stock_code = str(item.get("stock_code") or "").strip()
            corp_cls = str(item.get("corp_cls") or "").strip().upper()
            receipt_no = str(item.get("rcept_no") or "").strip()
            report_name = self._normalized_report_name(str(item.get("report_nm") or ""))
            if corp_cls not in {"Y", "K"} or len(stock_code) != 6 or not receipt_no:
                decisions.append(
                    self._build_invalid_symbol_decision(
                        stock_code=stock_code,
                        receipt_no=receipt_no,
                        report_name=report_name,
                        corp_cls=corp_cls,
                    )
                )
                continue

            block_match, positive_match = self._classify_disclosure(item)
            if block_match.matched:
                decisions.append(
                    self._build_blocked_decision(
                        stock_code=stock_code,
                        receipt_no=receipt_no,
                        report_name=report_name,
                        block_match=block_match,
                    )
                )
                continue

            if not positive_match.matched:
                decisions.append(
                    self._build_low_confidence_decision(
                        stock_code=stock_code,
                        receipt_no=receipt_no,
                        report_name=report_name,
                    )
                )
                continue

            event_ts_utc = datetime.now(UTC)
            structured_event = self._build_structured_event(
                item=item,
                positive_match=positive_match,
                receipt_no=receipt_no,
                report_name=report_name,
                event_ts_utc=event_ts_utc,
            )
            structured_events.append(structured_event)
            decisions.append(
                CandidateDecisionRecord(
                    decision_id=self._build_decision_id(stock_code, receipt_no),
                    candidate_id=f"candidate-{stock_code}-{receipt_no}",
                    source_receipt_no=receipt_no,
                    source_report_name=report_name,
                    source_symbol=stock_code,
                    matched_positive_rule_id=positive_match.rule_id,
                    candidate_status="ELIGIBLE_NOT_SELECTED",
                    selection_reason=positive_match.reason,
                    confidence_summary=(
                        f"이벤트군={structured_event.event_family}, 심각도={structured_event.severity:.2f}, "
                        f"신규성={structured_event.novelty:.2f}, 신뢰도={structured_event.model_confidence:.2f}"
                    ),
                    ranking_score=round(structured_event.model_confidence * 100.0, 4),
                    ranking_reason="구조화 이벤트 1차 통과가 완료되었습니다.",
                    decision_payload_json={
                        "event_id": structured_event.event_id,
                        "event_family": structured_event.event_family,
                        "event_type": structured_event.event_type,
                        "severity": structured_event.severity,
                        "novelty": structured_event.novelty,
                        "model_confidence": structured_event.model_confidence,
                    },
                )
            )

        clusters = self._build_event_clusters(structured_events)
        watchlist_triggers = self._build_watchlist_triggers(clusters)
        cluster_by_id = {cluster.cluster_id: cluster for cluster in clusters}
        structured_event_by_cluster: dict[str, StructuredEvent] = {}
        for event in structured_events:
            cluster_id = f"cluster-{event.instrument_id}-{event.event_type.lower()}"
            if cluster_id not in structured_event_by_cluster:
                structured_event_by_cluster[cluster_id] = event

        candidates: list[TradeCandidate] = []
        ranked_clusters = sorted(
            clusters,
            key=lambda cluster: (-(cluster.severity * 0.65 + cluster.novelty * 0.35), cluster.instrument_id),
        )
        for rank, cluster in enumerate(ranked_clusters, start=1):
            event = structured_event_by_cluster.get(cluster.cluster_id)
            if event is None:
                continue
            candidate = self._candidate_from_structured_event(
                structured_event=event,
                cluster=cluster_by_id[cluster.cluster_id],
                selected_rank=rank,
            )
            candidates.append(candidate)
            for decision in decisions:
                if decision.candidate_id == candidate.candidate_id:
                    decision.candidate_status = candidate.candidate_status or decision.candidate_status
                    decision.selected_rank = rank
                    decision.ranking_score = candidate.ranking_score
                    decision.ranking_reason = candidate.ranking_reason
                    if candidate.candidate_status == "SELECTED":
                        decision.selection_reason = candidate.selection_reason
                    else:
                        decision.rejection_reason = "더 높은 순위의 구조화 이벤트 후보가 우선 선정되었습니다."

        for event in structured_events:
            dedupe_key = str(event.extraction_payload.get("dedupe_key") or "")
            if dedupe_key:
                self._event_history_by_key[dedupe_key].append(event.event_ts_utc)
                self._event_history_by_key[dedupe_key] = [
                    timestamp
                    for timestamp in self._event_history_by_key[dedupe_key]
                    if (event.event_ts_utc - timestamp) <= timedelta(days=10)
                ]

        self._latest_structured_events = structured_events
        self._latest_event_clusters = clusters
        self._latest_watchlist_triggers = watchlist_triggers
        self._live_candidate_cache = candidates
        self._latest_candidate_decisions = decisions
        self._live_candidate_cache_utc = now

        if limit is None:
            return structured_events
        return structured_events[:limit]

    async def live_candidates(self, *, limit: int = 5, force_refresh: bool = False) -> list[TradeCandidate]:
        await self.live_structured_events(force_refresh=force_refresh)
        return self._live_candidate_cache[:limit]

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
        structured_event = StructuredEvent(
            event_id=f"manual-event-{uuid4().hex[:12]}",
            source_type="MANUAL_PARSE",
            instrument_id=instrument_id,
            issuer_id=instrument_id,
            event_family="MANUAL",
            event_type=str(parsed.structured["event_type"]),
            direction=str(parsed.structured["direction"]),
            severity=0.6 if not parsed.used_fallback else 0.35,
            novelty=0.75,
            magnitude=float(parsed.structured.get("confidence", 0.35)),
            parser_confidence=float(parsed.structured["confidence"]),
            model_confidence=float(parsed.structured["confidence"]),
            tradeability=str(parsed.structured["tradeability"]),
            hard_block_candidate=bool(parsed.structured["hard_block_candidate"]),
            time_sensitivity_minutes=240,
            extracted_summary=str(parsed.structured.get("summary") or raw_text[:280]),
            extraction_payload=parsed.structured,
        )
        candidate = None
        if disclosure.tradeability == "ALLOW" and disclosure.direction in {"POSITIVE", "UP"}:
            candidate = TradeCandidate(
                candidate_id=f"candidate-{uuid4().hex[:12]}",
                strategy_id=self.settings.selector_default_strategy_id,
                account_scope=self.settings.selector_default_account_scope,
                instrument_id=instrument_id,
                side=OrderSide.BUY,
                expected_edge_bps=20.0 if parsed.used_fallback else 35.0,
                target_notional_krw=1_000_000,
                entry_style="PASSIVE_FIRST",
                expire_ts_utc=datetime.now(UTC) + timedelta(minutes=20),
                meta_model_version="llm-disclosure-v1",
                source_signal_refs=[disclosure.disclosure_id],
                source_event_ids=[structured_event.event_id],
                selection_reason="수동 파싱 결과, 해당 텍스트를 허용 가능한 긍정 촉매로 분류했습니다.",
                candidate_status="SELECTED",
                selection_confidence=structured_event.model_confidence,
            )
        return {
            "disclosure": disclosure,
            "structured_event": structured_event,
            "candidate": candidate,
            "parser": parsed,
        }

    async def sync_corp_codes(self) -> dict:
        codes = await self.dart_client.download_corp_codes()
        return {"count": len(codes), "sample": codes[:5]}

    async def list_latest_disclosures(self, begin: date, end: date) -> dict:
        return await self.dart_client.list_disclosures(begin, end)


market_intel_service = MarketIntelService()
