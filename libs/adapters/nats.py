from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from nats.aio.client import Client as NATS
from nats.js.api import RetentionPolicy
from nats.js.api import StorageType
from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError

from libs.config.settings import Settings, get_settings


@dataclass(slots=True)
class PublishedMessage:
    subject: str
    size: int
    stream: str | None = None
    sequence: int | None = None


class NatsEventBus:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = NATS()
        self.jetstream = None
        self._connected = False

    async def connect(self) -> None:
        if not self._connected:
            await self.client.connect(servers=[self.settings.nats_url])
            self.jetstream = self.client.jetstream()
            await self.ensure_stream()
            self._connected = True

    async def ensure_stream(self) -> None:
        if self.jetstream is None:
            return
        stream_name = self.settings.nats_stream_name
        subjects = self.settings.nats_stream_subject_list
        try:
            info = await self.jetstream.stream_info(stream_name)
            existing_subjects = set(info.config.subjects or [])
            if set(subjects) != existing_subjects:
                await self.jetstream.update_stream(
                    StreamConfig(
                        name=stream_name,
                        subjects=subjects,
                        storage=StorageType.FILE,
                        retention=RetentionPolicy.LIMITS,
                    )
                )
        except NotFoundError:
            await self.jetstream.add_stream(
                StreamConfig(
                    name=stream_name,
                    subjects=subjects,
                    storage=StorageType.FILE,
                    retention=RetentionPolicy.LIMITS,
                )
            )

    async def close(self) -> None:
        if self._connected:
            await self.client.close()
            self._connected = False
            self.jetstream = None

    async def publish_json(self, subject: str, payload: dict[str, Any]) -> PublishedMessage:
        await self.connect()
        encoded = json.dumps(payload, default=str).encode("utf-8")
        if self.jetstream is not None:
            ack = await self.jetstream.publish(subject, encoded)
            return PublishedMessage(
                subject=subject,
                size=len(encoded),
                stream=getattr(ack, "stream", None),
                sequence=getattr(ack, "seq", None),
            )
        await self.client.publish(subject, encoded)
        return PublishedMessage(subject=subject, size=len(encoded))
