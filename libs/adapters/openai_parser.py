from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import httpx

from libs.config.settings import Settings, get_settings


@dataclass(slots=True)
class ParserResult:
    structured: dict[str, Any]
    confidence: float
    used_fallback: bool


@dataclass(slots=True)
class InstrumentProfileResult:
    sector_name: str | None
    oil_up_beta: float
    usdkrw_up_beta: float
    rates_up_beta: float
    china_growth_beta: float
    domestic_demand_beta: float
    export_beta: float
    thematic_tags: list[str]
    rationale: str
    confidence: float
    used_fallback: bool


@dataclass(slots=True)
class TradeDecisionOverlayResult:
    action_bias: str
    alpha_adjust_bps: float
    confidence_adjust: float
    position_scale: float
    holding_days_adjust: int
    exit_urgency_score: float
    thesis_quality_score: float
    crowding_risk_score: float
    signal_decay_score: float
    hard_block: bool
    rationale: str
    confidence: float
    used_fallback: bool


class GeminiAPIError(RuntimeError):
    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        error_status: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.error_status = error_status


class GeminiParserClient:
    def __init__(self, settings: Settings | None = None, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client or httpx.AsyncClient(timeout=20.0)
        self._instrument_profile_cache: dict[str, InstrumentProfileResult] = {}
        self._trade_decision_cache: dict[str, TradeDecisionOverlayResult] = {}

    async def close(self) -> None:
        await self.client.aclose()

    def _candidate_models(self) -> list[str]:
        models: list[str] = []
        for value in [self.settings.google_model, *self.settings.google_model_candidate_list]:
            normalized = str(value or "").strip()
            if normalized and normalized not in models:
                models.append(normalized)
        return models

    async def _gemini_json_schema(
        self,
        *,
        schema_name: str,
        schema: dict[str, Any],
        system_prompt: str,
        user_payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.google_api_key:
            raise RuntimeError("GOOGLE_API_KEY is not configured")

        request_body = {
            "systemInstruction": {
                "parts": [
                    {
                        "text": system_prompt,
                    }
                ]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": json.dumps(
                                {
                                    "schema_name": schema_name,
                                    "payload": user_payload,
                                },
                                ensure_ascii=False,
                            ),
                        }
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
                "responseJsonSchema": schema,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        last_error: GeminiAPIError | None = None
        for model_name in self._candidate_models():
            try:
                return await self._call_gemini_model(model_name=model_name, request_body=request_body)
            except GeminiAPIError as exc:
                last_error = exc
                if self._should_try_next_model(exc):
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("No Gemini model candidates are configured")

    async def _call_gemini_model(self, *, model_name: str, request_body: dict[str, Any]) -> dict[str, Any]:
        response = await self.client.post(
            f"{self.settings.google_genai_base_url}/models/{model_name}:generateContent",
            headers={
                "x-goog-api-key": self.settings.google_api_key,
                "Content-Type": "application/json",
            },
            json=request_body,
        )
        if response.is_error:
            parsed_payload: Any | None = None
            message = response.text.strip() or response.reason_phrase
            error_status = None
            try:
                parsed_payload = response.json()
                if isinstance(parsed_payload, dict):
                    error_payload = parsed_payload.get("error") or {}
                    message = str(error_payload.get("message") or parsed_payload)
                    error_status = str(error_payload.get("status") or "") or None
            except Exception:
                parsed_payload = None
            raise GeminiAPIError(
                status_code=response.status_code,
                message=f"{model_name}: {message}",
                error_status=error_status,
            )

        payload = response.json()
        candidates = payload.get("candidates") or []
        if not candidates:
            prompt_feedback = payload.get("promptFeedback") or {}
            raise GeminiAPIError(
                status_code=response.status_code,
                message=f"{model_name}: no candidate returned ({prompt_feedback})",
                error_status=str(prompt_feedback.get("blockReason") or "") or None,
            )
        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []
        text_segments = [
            str(part.get("text") or "").strip()
            for part in parts
            if isinstance(part, dict) and str(part.get("text") or "").strip()
        ]
        if not text_segments:
            finish_reason = str(candidates[0].get("finishReason") or "UNKNOWN")
            raise GeminiAPIError(
                status_code=response.status_code,
                message=f"{model_name}: no text part returned (finish_reason={finish_reason})",
                error_status=finish_reason,
            )
        return json.loads("".join(text_segments))

    @staticmethod
    def _should_try_next_model(exc: GeminiAPIError) -> bool:
        if exc.status_code == 404 or exc.error_status == "NOT_FOUND":
            return True
        lowered = exc.message.lower()
        if exc.status_code == 400 and exc.error_status == "INVALID_ARGUMENT":
            return "model" in lowered and ("not found" in lowered or "unsupported" in lowered or "unknown" in lowered)
        return False

    async def parse_disclosure(self, raw_text: str) -> ParserResult:
        if not self.settings.google_api_key:
            return self._fallback(raw_text)
        try:
            structured = await self._gemini_json_schema(
                schema_name="disclosure_event",
                schema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string"},
                        "direction": {"type": "string"},
                        "confidence": {"type": "number"},
                        "tradeability": {"type": "string"},
                        "hard_block_candidate": {"type": "boolean"},
                        "summary": {"type": "string"},
                    },
                    "required": [
                        "event_type",
                        "direction",
                        "confidence",
                        "tradeability",
                        "hard_block_candidate",
                        "summary",
                    ],
                    "additionalProperties": True,
                },
                system_prompt="Extract structured Korean disclosure event data. Return only valid JSON.",
                user_payload={"raw_text": raw_text},
            )
            confidence = float(structured.get("confidence", 0.0))
            if confidence < 0.5:
                fallback = self._fallback(raw_text)
                fallback.structured["model_output"] = structured
                return fallback
            return ParserResult(structured=structured, confidence=confidence, used_fallback=False)
        except Exception:
            return self._fallback(raw_text)

    async def infer_instrument_profile(
        self,
        *,
        issuer_name: str | None,
        report_name: str | None,
        event_family: str,
        event_type: str,
        summary: str | None = None,
    ) -> InstrumentProfileResult:
        cache_key = "|".join(
            [
                str(issuer_name or "").strip().lower(),
                str(report_name or "").strip().lower(),
                str(event_family or "").strip().upper(),
                str(event_type or "").strip().upper(),
            ]
        )
        cached = self._instrument_profile_cache.get(cache_key)
        if cached is not None:
            return cached
        if not self.settings.google_api_key:
            profile = self._fallback_instrument_profile(
                issuer_name=issuer_name,
                report_name=report_name,
                event_family=event_family,
                event_type=event_type,
            )
            self._instrument_profile_cache[cache_key] = profile
            return profile
        try:
            structured = await self._gemini_json_schema(
                schema_name="instrument_market_profile",
                schema={
                    "type": "object",
                    "properties": {
                        "sector_name": {"type": ["string", "null"]},
                        "oil_up_beta": {"type": "number"},
                        "usdkrw_up_beta": {"type": "number"},
                        "rates_up_beta": {"type": "number"},
                        "china_growth_beta": {"type": "number"},
                        "domestic_demand_beta": {"type": "number"},
                        "export_beta": {"type": "number"},
                        "confidence": {"type": "number"},
                        "thematic_tags": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "rationale": {"type": "string"},
                    },
                    "required": [
                        "sector_name",
                        "oil_up_beta",
                        "usdkrw_up_beta",
                        "rates_up_beta",
                        "china_growth_beta",
                        "domestic_demand_beta",
                        "export_beta",
                        "confidence",
                        "thematic_tags",
                        "rationale",
                    ],
                    "additionalProperties": False,
                },
                system_prompt=(
                    "You are a Korean equity cross-asset strategist. "
                    "Estimate short-horizon sensitivities for 1-10 trading days. "
                    "Positive oil_up_beta means the stock benefits when oil rises. "
                    "Positive usdkrw_up_beta means the stock benefits when USDKRW rises. "
                    "Positive rates_up_beta means the stock benefits when rates rise. "
                    "Positive china_growth_beta means the stock benefits from stronger China or global industrial demand. "
                    "Positive domestic_demand_beta means the stock benefits from stronger Korean domestic demand. "
                    "Positive export_beta means export orientation is high. "
                    "Keep all betas within [-1, 1] and return only valid JSON."
                ),
                user_payload={
                    "issuer_name": issuer_name,
                    "report_name": report_name,
                    "event_family": event_family,
                    "event_type": event_type,
                    "summary": summary,
                },
            )
            profile = InstrumentProfileResult(
                sector_name=(str(structured.get("sector_name") or "").strip() or None),
                oil_up_beta=self._clamp_beta(structured.get("oil_up_beta")),
                usdkrw_up_beta=self._clamp_beta(structured.get("usdkrw_up_beta")),
                rates_up_beta=self._clamp_beta(structured.get("rates_up_beta")),
                china_growth_beta=self._clamp_beta(structured.get("china_growth_beta")),
                domestic_demand_beta=self._clamp_beta(structured.get("domestic_demand_beta")),
                export_beta=self._clamp_beta(structured.get("export_beta")),
                thematic_tags=self._normalize_tags(structured.get("thematic_tags")),
                rationale=str(structured.get("rationale") or "").strip(),
                confidence=max(0.0, min(float(structured.get("confidence", 0.0)), 1.0)),
                used_fallback=False,
            )
            if profile.confidence < 0.45:
                fallback = self._fallback_instrument_profile(
                    issuer_name=issuer_name,
                    report_name=report_name,
                    event_family=event_family,
                    event_type=event_type,
                )
                fallback.rationale = profile.rationale or fallback.rationale
                self._instrument_profile_cache[cache_key] = fallback
                return fallback
            self._instrument_profile_cache[cache_key] = profile
            return profile
        except Exception:
            profile = self._fallback_instrument_profile(
                issuer_name=issuer_name,
                report_name=report_name,
                event_family=event_family,
                event_type=event_type,
            )
            self._instrument_profile_cache[cache_key] = profile
            return profile

    async def infer_trade_decision_overlay(
        self,
        *,
        side: str,
        instrument_id: str | None = None,
        issuer_name: str | None = None,
        event_family: str | None = None,
        event_type: str | None = None,
        summary: str | None = None,
        sector_name: str | None = None,
        thematic_tags: list[str] | None = None,
        price_context: dict[str, Any] | None = None,
        news_context: dict[str, Any] | None = None,
        macro_context: dict[str, Any] | None = None,
        signal_context: dict[str, Any] | None = None,
        position_context: dict[str, Any] | None = None,
    ) -> TradeDecisionOverlayResult:
        side_normalized = str(side or "").strip().upper() or "BUY"
        thematic_tags = self._normalize_tags(thematic_tags)
        price_context = dict(price_context or {})
        news_context = dict(news_context or {})
        macro_context = dict(macro_context or {})
        signal_context = dict(signal_context or {})
        position_context = dict(position_context or {})
        cache_key = hashlib.sha1(
            json.dumps(
                {
                    "side": side_normalized,
                    "instrument_id": instrument_id,
                    "issuer_name": issuer_name,
                    "event_family": event_family,
                    "event_type": event_type,
                    "summary": summary,
                    "sector_name": sector_name,
                    "thematic_tags": thematic_tags,
                    "price_context": price_context,
                    "news_context": news_context,
                    "macro_context": macro_context,
                    "signal_context": signal_context,
                    "position_context": position_context,
                },
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        cached = self._trade_decision_cache.get(cache_key)
        if cached is not None:
            return cached

        if not self.settings.google_api_key:
            overlay = self._fallback_trade_decision_overlay(
                side=side_normalized,
                instrument_id=instrument_id,
                issuer_name=issuer_name,
                event_family=event_family,
                event_type=event_type,
                summary=summary,
                sector_name=sector_name,
                thematic_tags=thematic_tags,
                price_context=price_context,
                news_context=news_context,
                macro_context=macro_context,
                signal_context=signal_context,
                position_context=position_context,
            )
            self._trade_decision_cache[cache_key] = overlay
            return overlay

        try:
            structured = await self._gemini_json_schema(
                schema_name="trade_decision_overlay",
                schema={
                    "type": "object",
                    "properties": {
                        "action_bias": {"type": "string"},
                        "alpha_adjust_bps": {"type": "number"},
                        "confidence_adjust": {"type": "number"},
                        "position_scale": {"type": "number"},
                        "holding_days_adjust": {"type": "integer"},
                        "exit_urgency_score": {"type": "number"},
                        "thesis_quality_score": {"type": "number"},
                        "crowding_risk_score": {"type": "number"},
                        "signal_decay_score": {"type": "number"},
                        "hard_block": {"type": "boolean"},
                        "confidence": {"type": "number"},
                        "rationale": {"type": "string"},
                    },
                    "required": [
                        "action_bias",
                        "alpha_adjust_bps",
                        "confidence_adjust",
                        "position_scale",
                        "holding_days_adjust",
                        "exit_urgency_score",
                        "thesis_quality_score",
                        "crowding_risk_score",
                        "signal_decay_score",
                        "hard_block",
                        "confidence",
                        "rationale",
                    ],
                    "additionalProperties": False,
                },
                system_prompt=(
                    "You are a Korean equity event-driven portfolio overlay model. "
                    "Synthesize the supplied catalyst, price, liquidity, volatility, news, macro, and position context "
                    "for the next 1 to 10 trading days. "
                    "Use only the supplied payload. "
                    "For BUY, action_bias must be BUY or HOLD. "
                    "For SELL, action_bias must be HOLD, TRIM, or EXIT. "
                    "alpha_adjust_bps must stay within [-80, 80], confidence_adjust within [-0.25, 0.25], "
                    "position_scale within [0.2, 1.5], holding_days_adjust within [-4, 4], "
                    "and all scores must stay within [0, 1]. "
                    "Set hard_block=true only for clearly unfavorable cases. "
                    "Return only valid JSON."
                ),
                user_payload={
                    "side": side_normalized,
                    "instrument_id": instrument_id,
                    "issuer_name": issuer_name,
                    "event_family": event_family,
                    "event_type": event_type,
                    "summary": summary,
                    "sector_name": sector_name,
                    "thematic_tags": thematic_tags,
                    "price_context": price_context,
                    "news_context": news_context,
                    "macro_context": macro_context,
                    "signal_context": signal_context,
                    "position_context": position_context,
                },
            )
            overlay = TradeDecisionOverlayResult(
                action_bias=self._normalize_action_bias(
                    side=side_normalized,
                    value=structured.get("action_bias"),
                ),
                alpha_adjust_bps=round(self._clamp_range(structured.get("alpha_adjust_bps"), -80.0, 80.0), 4),
                confidence_adjust=round(self._clamp_range(structured.get("confidence_adjust"), -0.25, 0.25), 4),
                position_scale=round(self._clamp_range(structured.get("position_scale"), 0.2, 1.5), 4),
                holding_days_adjust=int(round(self._clamp_range(structured.get("holding_days_adjust"), -4.0, 4.0))),
                exit_urgency_score=round(self._clamp_unit(structured.get("exit_urgency_score")), 4),
                thesis_quality_score=round(self._clamp_unit(structured.get("thesis_quality_score")), 4),
                crowding_risk_score=round(self._clamp_unit(structured.get("crowding_risk_score")), 4),
                signal_decay_score=round(self._clamp_unit(structured.get("signal_decay_score")), 4),
                hard_block=bool(structured.get("hard_block")),
                rationale=str(structured.get("rationale") or "").strip(),
                confidence=round(self._clamp_unit(structured.get("confidence")), 4),
                used_fallback=False,
            )
            if overlay.confidence < 0.45:
                fallback = self._fallback_trade_decision_overlay(
                    side=side_normalized,
                    instrument_id=instrument_id,
                    issuer_name=issuer_name,
                    event_family=event_family,
                    event_type=event_type,
                    summary=summary,
                    sector_name=sector_name,
                    thematic_tags=thematic_tags,
                    price_context=price_context,
                    news_context=news_context,
                    macro_context=macro_context,
                    signal_context=signal_context,
                    position_context=position_context,
                )
                fallback.rationale = overlay.rationale or fallback.rationale
                self._trade_decision_cache[cache_key] = fallback
                return fallback
            self._trade_decision_cache[cache_key] = overlay
            return overlay
        except Exception:
            overlay = self._fallback_trade_decision_overlay(
                side=side_normalized,
                instrument_id=instrument_id,
                issuer_name=issuer_name,
                event_family=event_family,
                event_type=event_type,
                summary=summary,
                sector_name=sector_name,
                thematic_tags=thematic_tags,
                price_context=price_context,
                news_context=news_context,
                macro_context=macro_context,
                signal_context=signal_context,
                position_context=position_context,
            )
            self._trade_decision_cache[cache_key] = overlay
            return overlay

    @staticmethod
    def _clamp_beta(value: Any) -> float:
        try:
            return max(-1.0, min(float(value), 1.0))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _clamp_unit(value: Any) -> float:
        try:
            return max(0.0, min(float(value), 1.0))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _clamp_range(value: Any, lower: float, upper: float) -> float:
        try:
            return max(lower, min(float(value), upper))
        except (TypeError, ValueError):
            return lower

    @staticmethod
    def _normalize_tags(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        tags: list[str] = []
        seen: set[str] = set()
        for item in value:
            normalized = str(item or "").strip()
            key = normalized.lower()
            if not normalized or key in seen:
                continue
            seen.add(key)
            tags.append(normalized)
        return tags[:8]

    @staticmethod
    def _normalize_action_bias(*, side: str, value: Any) -> str:
        normalized = str(value or "").strip().upper()
        if side == "SELL":
            if normalized in {"EXIT", "TRIM", "HOLD"}:
                return normalized
            return "HOLD"
        if normalized in {"BUY", "HOLD"}:
            return normalized
        return "HOLD"

    def _fallback_instrument_profile(
        self,
        *,
        issuer_name: str | None,
        report_name: str | None,
        event_family: str,
        event_type: str,
    ) -> InstrumentProfileResult:
        text = " ".join(
            [
                str(issuer_name or "").strip().lower(),
                str(report_name or "").strip().lower(),
                str(event_family or "").strip().lower(),
                str(event_type or "").strip().lower(),
            ]
        )
        sector_name: str | None = None
        oil_up_beta = 0.0
        usdkrw_up_beta = 0.0
        rates_up_beta = 0.0
        china_growth_beta = 0.0
        domestic_demand_beta = 0.0
        export_beta = 0.0
        thematic_tags: list[str] = []

        if any(token in text for token in ("semiconductor", "electronics", "samsung", "chip")):
            sector_name = "Technology"
            usdkrw_up_beta = 0.45
            rates_up_beta = -0.2
            export_beta = 0.7
            china_growth_beta = 0.25
            thematic_tags.extend(["exporter", "technology"])
        elif any(token in text for token in ("auto", "motor", "mobility")):
            sector_name = "Automobiles"
            oil_up_beta = -0.15
            usdkrw_up_beta = 0.4
            rates_up_beta = -0.1
            export_beta = 0.65
            domestic_demand_beta = 0.15
            thematic_tags.extend(["exporter", "cyclical"])
        elif any(token in text for token in ("air", "airline", "travel", "tour", "transport")):
            sector_name = "Transportation"
            oil_up_beta = -0.75
            usdkrw_up_beta = -0.35
            domestic_demand_beta = 0.35
            thematic_tags.extend(["oil-sensitive", "consumer"])
        elif any(token in text for token in ("energy", "refining", "oil", "gas")):
            sector_name = "Energy"
            oil_up_beta = 0.8
            usdkrw_up_beta = 0.15
            china_growth_beta = 0.2
            thematic_tags.extend(["energy", "commodity"])
        elif any(token in text for token in ("chemical", "petro", "battery")):
            sector_name = "Materials"
            oil_up_beta = -0.35
            china_growth_beta = 0.3
            usdkrw_up_beta = 0.15
            thematic_tags.extend(["materials", "industrial"])
        elif any(token in text for token in ("bank", "financial", "insurance", "capital")):
            sector_name = "Financials"
            rates_up_beta = 0.45
            domestic_demand_beta = 0.2
            thematic_tags.extend(["financials", "rates"])

        if event_family == "CAPITAL_RETURN":
            rates_up_beta = min(rates_up_beta, -0.1)
            thematic_tags.append("shareholder-return")
        elif event_family == "CONTRACT":
            china_growth_beta = max(china_growth_beta, 0.25)
            export_beta = max(export_beta, 0.3)
            thematic_tags.append("order-book")
        elif event_family == "EARNINGS":
            domestic_demand_beta = max(domestic_demand_beta, 0.15)
            thematic_tags.append("earnings")

        return InstrumentProfileResult(
            sector_name=sector_name,
            oil_up_beta=oil_up_beta,
            usdkrw_up_beta=usdkrw_up_beta,
            rates_up_beta=rates_up_beta,
            china_growth_beta=china_growth_beta,
            domestic_demand_beta=domestic_demand_beta,
            export_beta=export_beta,
            thematic_tags=self._normalize_tags(thematic_tags),
            rationale="Fallback market profile inferred from issuer and event family.",
            confidence=0.3,
            used_fallback=True,
        )

    def _fallback_trade_decision_overlay(
        self,
        *,
        side: str,
        instrument_id: str | None,
        issuer_name: str | None,
        event_family: str | None,
        event_type: str | None,
        summary: str | None,
        sector_name: str | None,
        thematic_tags: list[str] | None,
        price_context: dict[str, Any],
        news_context: dict[str, Any],
        macro_context: dict[str, Any],
        signal_context: dict[str, Any],
        position_context: dict[str, Any],
    ) -> TradeDecisionOverlayResult:
        momentum = self._clamp_unit(price_context.get("momentum_composite_score", 0.5))
        trend_persistence = self._clamp_unit(price_context.get("trend_persistence_score", 0.5))
        breakout = self._clamp_unit(price_context.get("breakout_score", 0.0))
        volatility = self._clamp_unit(price_context.get("volatility_score", 0.25))
        illiquidity = self._clamp_unit(price_context.get("illiquidity_score", 0.2))
        reversal_pressure = self._clamp_unit(price_context.get("reversal_pressure_score", 0.15))
        close_strength = self._clamp_unit(price_context.get("close_strength_score", 0.5))
        signal_decay = self._clamp_unit(
            signal_context.get(
                "signal_decay_score",
                price_context.get("signal_decay_score", 0.25),
            )
        )
        surprise = self._clamp_unit(signal_context.get("surprise_score", 0.5))
        follow_through = self._clamp_unit(signal_context.get("follow_through_score", 0.45))
        tail_risk = self._clamp_unit(signal_context.get("tail_risk_score", 0.25))
        crowding = self._clamp_unit(signal_context.get("crowding_score", 0.2))
        liquidity = self._clamp_unit(signal_context.get("liquidity_score", max(0.0, 1.0 - illiquidity)))
        sentiment = self._clamp_beta(news_context.get("news_sentiment_score", 0.0))
        sentiment_consistency = self._clamp_unit(news_context.get("sentiment_consistency_score", 0.5))
        macro_headwind = self._clamp_unit(macro_context.get("macro_headwind_score", 0.0))
        cross_asset = self._clamp_beta(macro_context.get("cross_asset_impact_score", 0.0))
        thematic_alignment = self._clamp_unit(macro_context.get("thematic_alignment_score", 0.5))

        thesis_quality = self._clamp_unit(
            0.38
            + (surprise * 0.18)
            + (follow_through * 0.18)
            + (momentum * 0.14)
            + (trend_persistence * 0.1)
            + (max(sentiment, 0.0) * 0.08)
            + (sentiment_consistency * 0.06)
            + (thematic_alignment * 0.08)
            + (close_strength * 0.04)
            - (tail_risk * 0.16)
            - (macro_headwind * 0.14)
            - (signal_decay * 0.12)
            - (reversal_pressure * 0.06)
        )
        crowding_risk = self._clamp_unit(
            (crowding * 0.5)
            + (breakout * 0.15)
            + (max(momentum - 0.5, 0.0) * 0.24)
            + ((1.0 - liquidity) * 0.12)
            + (illiquidity * 0.15)
        )
        exit_urgency = self._clamp_unit(
            (macro_headwind * 0.24)
            + (tail_risk * 0.18)
            + (signal_decay * 0.18)
            + (reversal_pressure * 0.14)
            + (max(-sentiment, 0.0) * 0.08)
            + (illiquidity * 0.08)
            + (max(0.0, 0.55 - thesis_quality) * 0.1)
        )
        confidence = self._clamp_unit(
            0.52
            + (sentiment_consistency * 0.12)
            + (liquidity * 0.08)
            + ((1.0 - signal_decay) * 0.08)
            + ((1.0 - illiquidity) * 0.08)
        )

        text_hint = " ".join(
            [
                str(instrument_id or "").strip(),
                str(issuer_name or "").strip(),
                str(event_family or "").strip(),
                str(event_type or "").strip(),
                str(summary or "").strip(),
                str(sector_name or "").strip(),
                " ".join(thematic_tags or []),
            ]
        ).strip()

        if side == "BUY":
            alpha_adjust = (
                ((thesis_quality - 0.5) * 44.0)
                + ((momentum - 0.5) * 28.0)
                + ((trend_persistence - 0.5) * 16.0)
                + (max(sentiment, 0.0) * 10.0)
                + (cross_asset * 10.0)
                - (macro_headwind * 22.0)
                - (crowding_risk * 14.0)
                - (signal_decay * 16.0)
                - (reversal_pressure * 10.0)
            )
            confidence_adjust = (
                ((thesis_quality - 0.5) * 0.16)
                + ((sentiment_consistency - 0.5) * 0.06)
                + ((liquidity - 0.5) * 0.06)
                - (crowding_risk * 0.05)
                - (signal_decay * 0.08)
            )
            holding_days_adjust = int(
                round((thesis_quality - signal_decay - (macro_headwind * 0.8)) * 3.0)
            )
            position_scale = self._clamp_range(
                0.7
                + (thesis_quality * 0.45)
                + (liquidity * 0.15)
                - (crowding_risk * 0.18)
                - (signal_decay * 0.15)
                - (macro_headwind * 0.15),
                0.45,
                1.35,
            )
            hard_block = macro_headwind >= 0.88 and tail_risk >= 0.72 and illiquidity >= 0.55
            action_bias = "BUY" if alpha_adjust >= 4.0 and not hard_block else "HOLD"
            rationale = (
                f"Fallback BUY overlay for {text_hint or 'unknown instrument'}: "
                f"thesis={thesis_quality:.2f}, momentum={momentum:.2f}, macro={macro_headwind:.2f}, "
                f"crowding={crowding_risk:.2f}, decay={signal_decay:.2f}."
            )
        else:
            return_pct = self._clamp_range(position_context.get("return_pct", 0.0), -50.0, 50.0)
            holding_days = int(round(self._clamp_range(position_context.get("holding_days", 0.0), 0.0, 60.0)))
            live_edge_bps = self._clamp_range(position_context.get("live_edge_bps", 0.0), -200.0, 200.0)
            live_confidence = self._clamp_unit(position_context.get("live_confidence", 0.0))
            loss_pressure = self._clamp_unit(max(-return_pct, 0.0) / 8.0)
            stale_pressure = self._clamp_unit(max(holding_days - 3, 0) / 6.0)
            exit_urgency = self._clamp_unit(
                exit_urgency
                + (loss_pressure * 0.18)
                + (stale_pressure * 0.12)
                + (max(0.0, 0.45 - live_confidence) * 0.12)
                + (max(0.0, 25.0 - live_edge_bps) / 60.0)
            )
            alpha_adjust = (
                (exit_urgency * 34.0)
                + (tail_risk * 12.0)
                + (macro_headwind * 12.0)
                + (loss_pressure * 10.0)
                + (stale_pressure * 7.0)
            )
            confidence_adjust = (
                ((exit_urgency - 0.5) * 0.18)
                + (((1.0 - signal_decay) - 0.5) * 0.04)
            )
            holding_days_adjust = int(
                round(-((signal_decay * 1.4) + macro_headwind + stale_pressure) * 3.0)
            )
            position_scale = self._clamp_range(1.05 - (exit_urgency * 0.6), 0.2, 1.0)
            hard_block = exit_urgency >= 0.92 or (macro_headwind >= 0.9 and cross_asset <= -0.4)
            action_bias = "EXIT" if hard_block or exit_urgency >= 0.72 else "TRIM" if exit_urgency >= 0.58 else "HOLD"
            rationale = (
                f"Fallback SELL overlay for {text_hint or 'unknown instrument'}: "
                f"urgency={exit_urgency:.2f}, macro={macro_headwind:.2f}, loss={loss_pressure:.2f}, "
                f"stale={stale_pressure:.2f}, decay={signal_decay:.2f}."
            )

        return TradeDecisionOverlayResult(
            action_bias=action_bias,
            alpha_adjust_bps=round(self._clamp_range(alpha_adjust, -80.0, 80.0), 4),
            confidence_adjust=round(self._clamp_range(confidence_adjust, -0.25, 0.25), 4),
            position_scale=round(position_scale, 4),
            holding_days_adjust=holding_days_adjust,
            exit_urgency_score=round(exit_urgency, 4),
            thesis_quality_score=round(thesis_quality, 4),
            crowding_risk_score=round(crowding_risk, 4),
            signal_decay_score=round(signal_decay, 4),
            hard_block=hard_block,
            rationale=rationale,
            confidence=round(confidence, 4),
            used_fallback=True,
        )

    def _fallback(self, raw_text: str) -> ParserResult:
        lowered = raw_text.lower()
        event_type = "GENERAL"
        direction = "NEUTRAL"
        tradeability = "REVIEW"
        if "유상증자" in raw_text or "third-party allotment" in lowered:
            event_type = "EQUITY_DILUTION"
            direction = "NEGATIVE"
            tradeability = "BLOCK"
        elif "실적" in raw_text or "earnings" in lowered:
            event_type = "EARNINGS"
            direction = "POSITIVE" if "증가" in raw_text or "흑자" in raw_text else "NEUTRAL"
            tradeability = "ALLOW"
        return ParserResult(
            structured={
                "event_type": event_type,
                "direction": direction,
                "confidence": 0.35,
                "tradeability": tradeability,
                "hard_block_candidate": tradeability == "BLOCK",
                "summary": raw_text[:280],
            },
            confidence=0.35,
            used_fallback=True,
        )


OpenAIParserClient = GeminiParserClient
