from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from libs.domain.enums import RiskPolicyType


@dataclass(slots=True)
class PolicyRecord:
    policy_id: str
    policy_type: RiskPolicyType
    policy_version: str
    scope: str
    effective_from_utc: datetime
    payload: dict[str, Any]
    owner: str | None = None


class PolicyRegistryService:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self._policies: dict[str, PolicyRecord] = {
            "global-safety-v1": PolicyRecord(
                policy_id="global-safety-v1",
                policy_type=RiskPolicyType.GLOBAL_SAFETY,
                policy_version="1.0.0",
                scope="global",
                effective_from_utc=now,
                payload={
                    "kill_switch_blocks_entry": True,
                    "reconciliation_break_blocks_entry": True,
                    "stale_market_data_blocks_entry": True,
                },
                owner="ops",
            ),
            "execution-v1": PolicyRecord(
                policy_id="execution-v1",
                policy_type=RiskPolicyType.EXECUTION,
                policy_version="1.0.0",
                scope="default",
                effective_from_utc=now,
                payload={
                    "default_tif": "DAY",
                    "max_slippage_bps": 35.0,
                    "venue_fallback_enabled": True,
                },
                owner="trading-core",
            ),
        }

    def list_policies(self) -> list[PolicyRecord]:
        return sorted(self._policies.values(), key=lambda record: record.policy_id)

    def get_policy(self, policy_id: str) -> PolicyRecord | None:
        return self._policies.get(policy_id)

    def upsert_policy(self, policy: PolicyRecord) -> PolicyRecord:
        self._policies[policy.policy_id] = policy
        return policy
