from __future__ import annotations

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


class OpenAIParserClient:
    def __init__(self, settings: Settings | None = None, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client or httpx.AsyncClient(timeout=20.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def parse_disclosure(self, raw_text: str) -> ParserResult:
        if not self.settings.openai_api_key:
            return self._fallback(raw_text)
        response = await self.client.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {self.settings.openai_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.settings.openai_model,
                "input": [
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": "Extract structured Korean disclosure event data. Return JSON only.",
                            }
                        ],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": raw_text}],
                    },
                ],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "disclosure_event",
                        "schema": {
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
                    }
                },
            },
        )
        payload = response.json()
        try:
            text = payload["output"][0]["content"][0]["text"]
            structured = json.loads(text)
            confidence = float(structured.get("confidence", 0.0))
            if confidence < 0.5:
                fallback = self._fallback(raw_text)
                fallback.structured["model_output"] = structured
                return fallback
            return ParserResult(structured=structured, confidence=confidence, used_fallback=False)
        except Exception:
            return self._fallback(raw_text)

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
            direction = "POSITIVE" if "증가" in raw_text or "상승" in raw_text else "NEUTRAL"
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
