from datetime import UTC, date, datetime

from libs.contracts.messages import MessageEnvelope
from libs.domain.enums import MessageType


def test_message_envelope_defaults() -> None:
    envelope = MessageEnvelope(
        message_type=MessageType.EVENT,
        message_name="OrderSubmitRequested",
        producer="ops-api",
        trading_date=date(2026, 3, 11),
        correlation_id="corr-1",
        idempotency_key="idemp-1",
        payload={"value": 1},
        occurred_at_utc=datetime.now(UTC),
        observed_at_utc=datetime.now(UTC),
    )

    assert envelope.message_id
    assert envelope.message_version == "1.0"
    assert envelope.schema_version == "1.0"

