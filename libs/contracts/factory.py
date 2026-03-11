from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from libs.contracts.messages import MessageEnvelope
from libs.domain.enums import MessageType


def build_envelope(
    *,
    message_type: MessageType,
    message_name: str,
    producer: str,
    payload: dict[str, Any],
    idempotency_key: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    trading_date: date | None = None,
    account_scope: str | None = None,
    instrument_scope: str | None = None,
    trace_scope: dict[str, Any] | None = None,
) -> MessageEnvelope:
    now = datetime.now(UTC)
    return MessageEnvelope(
        message_type=message_type,
        message_name=message_name,
        producer=producer,
        trading_date=trading_date or now.date(),
        correlation_id=correlation_id or str(uuid4()),
        causation_id=causation_id,
        idempotency_key=idempotency_key,
        account_scope=account_scope,
        instrument_scope=instrument_scope,
        trace_scope=trace_scope or {},
        payload=payload,
    )
