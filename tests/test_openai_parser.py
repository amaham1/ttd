import pytest

from libs.adapters.openai_parser import OpenAIParserClient


@pytest.mark.asyncio
async def test_infer_instrument_profile_falls_back_without_api_key() -> None:
    client = OpenAIParserClient()
    client.settings.google_api_key = ""
    try:
        profile = await client.infer_instrument_profile(
            issuer_name="Samsung Electronics",
            report_name="Share repurchase decision",
            event_family="CAPITAL_RETURN",
            event_type="BUYBACK",
            summary="Company announced a buyback.",
        )
    finally:
        await client.close()

    assert profile.used_fallback is True
    assert profile.sector_name == "Technology"
    assert profile.usdkrw_up_beta > 0
    assert "technology" in profile.thematic_tags


@pytest.mark.asyncio
async def test_trade_decision_overlay_falls_back_without_api_key() -> None:
    client = OpenAIParserClient()
    client.settings.google_api_key = ""
    try:
        overlay = await client.infer_trade_decision_overlay(
            side="BUY",
            instrument_id="005930",
            issuer_name="Samsung Electronics",
            event_family="EARNINGS",
            event_type="EARNINGS_PRELIM",
            summary="Strong earnings beat with positive guidance.",
            sector_name="Technology",
            thematic_tags=["technology", "exporter"],
            price_context={
                "momentum_composite_score": 0.76,
                "trend_persistence_score": 0.72,
                "breakout_score": 0.68,
                "volatility_score": 0.24,
                "illiquidity_score": 0.12,
                "signal_decay_score": 0.18,
            },
            news_context={
                "news_sentiment_score": 0.44,
                "sentiment_consistency_score": 0.83,
            },
            macro_context={
                "cross_asset_impact_score": 0.32,
                "thematic_alignment_score": 0.74,
                "macro_headwind_score": 0.18,
            },
            signal_context={
                "surprise_score": 0.81,
                "follow_through_score": 0.77,
                "tail_risk_score": 0.24,
                "crowding_score": 0.22,
                "liquidity_score": 0.84,
                "signal_decay_score": 0.18,
            },
        )
    finally:
        await client.close()

    assert overlay.used_fallback is True
    assert overlay.action_bias == "BUY"
    assert overlay.alpha_adjust_bps > 0
    assert overlay.position_scale > 0.8
