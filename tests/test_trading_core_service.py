from apps.market_intel.service import market_intel_service
from apps.trading_core.service import trading_core_service


def test_candidate_evaluation_passes_when_no_blocks() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    decision = trading_core_service.evaluate_candidate(candidate)
    intent = trading_core_service.build_trade_intent(candidate, decision)

    assert decision.hard_block is False
    assert intent is not None
    assert intent.candidate_id == candidate.candidate_id


def test_candidate_evaluation_blocks_when_kill_switch_active() -> None:
    candidate = market_intel_service.sample_candidates()[0]
    decision = trading_core_service.evaluate_candidate(candidate, has_kill_switch=True)
    intent = trading_core_service.build_trade_intent(candidate, decision)

    assert decision.hard_block is True
    assert "KILL_SWITCH_ACTIVE" in decision.failed_gate_codes
    assert intent is None
