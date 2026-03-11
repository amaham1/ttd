from dataclasses import dataclass, field


@dataclass(slots=True)
class RiskGateResult:
    passed: bool
    hard_block: bool
    failed_gate_codes: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    penalty_bps_total: float = 0.0


def evaluate_hard_block(
    *,
    market_data_ok: bool,
    account_entry_enabled: bool,
    has_kill_switch: bool,
    has_reconciliation_break: bool,
    has_risk_flag: bool,
) -> RiskGateResult:
    failed: list[str] = []
    if not market_data_ok:
        failed.append("STALE_MARKET_DATA")
    if not account_entry_enabled:
        failed.append("ACCOUNT_ENTRY_DISABLED")
    if has_kill_switch:
        failed.append("KILL_SWITCH_ACTIVE")
    if has_reconciliation_break:
        failed.append("RECONCILIATION_BREAK")
    if has_risk_flag:
        failed.append("INSTRUMENT_RISK_FLAG")
    return RiskGateResult(
        passed=not failed,
        hard_block=bool(failed),
        failed_gate_codes=failed,
        reason_codes=failed.copy(),
    )

