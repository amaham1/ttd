import pytest

from libs.adapters.nats import NatsEventBus
from libs.config.settings import Settings


class FakeAck:
    def __init__(self, stream: str = "KIS_EVENTS", seq: int = 12) -> None:
        self.stream = stream
        self.seq = seq


class FakeStreamInfo:
    def __init__(self, subjects: list[str]) -> None:
        self.config = type("Config", (), {"subjects": subjects})()


class FakeJetStream:
    def __init__(self, found: bool = False, subjects: list[str] | None = None) -> None:
        self.found = found
        self.subjects = subjects or ["evt.>"]
        self.added = []
        self.updated = []
        self.published = []

    async def stream_info(self, name: str):
        if not self.found:
            from nats.js.errors import NotFoundError

            raise NotFoundError()
        return FakeStreamInfo(self.subjects)

    async def add_stream(self, config) -> None:
        self.added.append(config)

    async def update_stream(self, config) -> None:
        self.updated.append(config)

    async def publish(self, subject: str, payload: bytes) -> FakeAck:
        self.published.append((subject, payload))
        return FakeAck()


class FakeClient:
    def __init__(self, jetstream: FakeJetStream) -> None:
        self._jetstream = jetstream
        self.connect_calls = []
        self.closed = False

    async def connect(self, servers: list[str]) -> None:
        self.connect_calls.append(servers)

    def jetstream(self) -> FakeJetStream:
        return self._jetstream

    async def publish(self, subject: str, payload: bytes) -> None:
        self._jetstream.published.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_event_bus_creates_stream_and_publishes_with_jetstream() -> None:
    settings = Settings(NATS_URL="nats://example:4222", NATS_STREAM_NAME="KIS_EVENTS", NATS_STREAM_SUBJECTS="evt.>")
    bus = NatsEventBus(settings=settings)
    fake_js = FakeJetStream(found=False)
    bus.client = FakeClient(fake_js)

    published = await bus.publish_json("evt.order.acked", {"hello": "world"})

    assert fake_js.added
    assert fake_js.published
    assert published.stream == "KIS_EVENTS"
    assert published.sequence == 12


@pytest.mark.asyncio
async def test_event_bus_updates_subjects_when_stream_differs() -> None:
    settings = Settings(NATS_URL="nats://example:4222", NATS_STREAM_NAME="KIS_EVENTS", NATS_STREAM_SUBJECTS="evt.>,cmd.>")
    bus = NatsEventBus(settings=settings)
    fake_js = FakeJetStream(found=True, subjects=["evt.>"])
    bus.client = FakeClient(fake_js)

    await bus.connect()

    assert fake_js.updated
