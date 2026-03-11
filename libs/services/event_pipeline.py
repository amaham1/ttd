from __future__ import annotations

from dataclasses import dataclass

from libs.contracts.factory import build_envelope
from libs.contracts.messages import FillEvent, OrderAckEvent
from libs.domain.enums import MessageType


@dataclass(slots=True)
class PublishedEnvelope:
    subject: str
    envelope: dict


class EventPipelineService:
    def __init__(self, event_bus=None) -> None:
        self.event_bus = event_bus

    async def publish_order_ack(self, event: OrderAckEvent) -> PublishedEnvelope:
        envelope = build_envelope(
            message_type=MessageType.EVENT,
            message_name="OrderAcked",
            producer="broker-gateway",
            payload=event.model_dump(mode="json"),
            idempotency_key=f"{event.client_order_id}:{event.broker_order_no}",
            correlation_id=event.client_order_id,
            account_scope=None,
            instrument_scope=None,
        )
        subject = "evt.order.acked"
        if self.event_bus is not None:
            await self.event_bus.publish_json(subject, envelope.model_dump(mode="json"))
        return PublishedEnvelope(subject=subject, envelope=envelope.model_dump(mode="json"))

    async def publish_fill(self, event: FillEvent) -> PublishedEnvelope:
        envelope = build_envelope(
            message_type=MessageType.EVENT,
            message_name="FillReceived",
            producer="broker-gateway",
            payload=event.model_dump(mode="json"),
            idempotency_key=event.broker_trade_id or f"{event.broker_order_no}:{event.fill_ts_utc.isoformat()}:{event.qty}",
            correlation_id=event.internal_order_id,
            account_scope=event.account_id,
            instrument_scope=event.instrument_id,
        )
        subject = "evt.execution.fill"
        if self.event_bus is not None:
            await self.event_bus.publish_json(subject, envelope.model_dump(mode="json"))
        return PublishedEnvelope(subject=subject, envelope=envelope.model_dump(mode="json"))
