from pydantic import BaseModel, Field

from libs.domain.enums import RiskPolicyType


class PolicyObject(BaseModel):
    policy_id: str
    policy_name: str
    policy_scope: str
    policy_type: RiskPolicyType
    policy_version: str
    payload: dict = Field(default_factory=dict)

