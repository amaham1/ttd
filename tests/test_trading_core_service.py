import pytest

from apps.market_intel.service import market_intel_service
from apps.trading_core.service import trading_core_service
from libs.contracts.messages import ExecutionReadiness, TradeIntent
from libs.domain.enums import OrderSide


def test_trading_core_blocks_candidate_when_execution_readiness_fails() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    readiness = ExecutionReadiness(
        account_id="default",
        strategy_id=candidate.strategy_id,
        instrument_id=candidate.instrument_id,
        market_data_ok=True,
        account_entry_enabled=False,
        confidence_ok=False,
        reason_codes=["ACCOUNT_ENTRY_DISABLED", "CONFIDENCE_FLOOR"],
    )

    decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=readiness)

    assert decision.hard_block is True
    assert "ACCOUNT_ENTRY_DISABLED" in decision.reason_codes
    assert "CONFIDENCE_FLOOR" in decision.reason_codes


def test_trading_core_scales_notional_down_with_quality_and_cap() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    readiness = ExecutionReadiness(
        account_id="default",
        strategy_id=candidate.strategy_id,
        instrument_id=candidate.instrument_id,
        max_allowed_notional_krw=500_000,
    )

    decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=readiness)
    intent = trading_core_service.build_trade_intent(candidate, decision)

    assert intent is not None
    assert intent.base_notional_krw == 500_000
    assert 0 < intent.target_notional_krw < intent.base_notional_krw
    assert intent.sizing_reason is not None
    assert "edge_score" in intent.sizing_reason


def test_trading_core_rejects_order_when_single_share_is_far_above_budget() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    readiness = ExecutionReadiness(
        account_id="default",
        strategy_id=candidate.strategy_id,
        instrument_id=candidate.instrument_id,
        max_allowed_notional_krw=5_000,
    )

    decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=readiness)
    intent = trading_core_service.build_trade_intent(candidate, decision)

    assert intent is not None
    with pytest.raises(ValueError, match="exceeds sizing limit"):
        trading_core_service.build_order_submit_command(
            intent=intent,
            strategy_id=candidate.strategy_id,
            price_krw=70_000,
            venue_hint="KRX",
        )


def test_trading_core_allows_one_share_when_price_is_within_tolerance() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    intent = TradeIntent(
        intent_id="intent-demo",
        candidate_id="candidate-demo",
        account_id="default",
        instrument_id="123456",
        side=OrderSide.BUY,
        target_qty=0,
        target_notional_krw=5_000,
        base_notional_krw=5_000,
        max_slippage_bps=35.0,
        urgency="NORMAL",
        route_policy="VENUE_HINT_THEN_FALLBACK",
        tif="DAY",
        sizing_reason="micro budget test",
        expire_ts_utc=candidate.expire_ts_utc,
    )

    command = trading_core_service.build_order_submit_command(
        intent=intent,
        strategy_id="test-strategy",
        price_krw=5_100,
        venue_hint="KRX",
    )

    assert command.qty == 1


def test_trading_core_blocks_sell_candidate_when_exit_permission_is_disabled() -> None:
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-exit-blocked",
            "side": OrderSide.SELL,
            "target_qty_override": 3,
            "target_notional_krw": 150000,
            "candidate_family": "EXIT",
            "exit_reason_code": "EXIT_STOP_LOSS",
        }
    )
    readiness = ExecutionReadiness(
        account_id="default",
        strategy_id=candidate.strategy_id,
        instrument_id=candidate.instrument_id,
        execution_side=OrderSide.SELL,
        account_entry_enabled=True,
        account_exit_enabled=False,
    )

    decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=readiness)

    assert decision.hard_block is True
    assert "ACCOUNT_EXIT_DISABLED" in decision.reason_codes


def test_trading_core_uses_sell_target_qty_override_for_exit_orders() -> None:
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-exit-qty",
            "side": OrderSide.SELL,
            "target_qty_override": 7,
            "target_notional_krw": 490000,
            "candidate_family": "EXIT",
            "exit_reason_code": "EXIT_TIME_STOP",
        }
    )
    readiness = ExecutionReadiness(
        account_id="default",
        strategy_id=candidate.strategy_id,
        instrument_id=candidate.instrument_id,
        execution_side=OrderSide.SELL,
        account_exit_enabled=True,
    )

    decision = trading_core_service.evaluate_candidate(candidate, execution_readiness=readiness)
    intent = trading_core_service.build_trade_intent(candidate, decision)

    assert intent is not None
    command = trading_core_service.build_order_submit_command(
        intent=intent,
        strategy_id="close-only-defense",
        price_krw=70000,
        venue_hint="KRX",
        max_order_value_krw=5000,
        enforce_hard_value_cap=True,
    )

    assert command.side == OrderSide.SELL
    assert command.qty == 7
