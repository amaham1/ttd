from libs.risk.gate import evaluate_hard_block


def test_risk_gate_blocks_when_kill_switch_active() -> None:
    result = evaluate_hard_block(
        market_data_ok=True,
        account_entry_enabled=True,
        has_kill_switch=True,
        has_reconciliation_break=False,
        has_risk_flag=False,
    )

    assert result.passed is False
    assert result.hard_block is True
    assert "KILL_SWITCH_ACTIVE" in result.failed_gate_codes

