from dataclasses import dataclass

from libs.services.raw_event_service import RawEventService


@dataclass
class FakeObjectRef:
    bucket: str
    object_name: str
    size: int


class FakeObjectStore:
    def put_json_bytes(self, bucket: str, object_name: str, content: bytes) -> FakeObjectRef:
        return FakeObjectRef(bucket=bucket, object_name=object_name, size=len(content))


class FakeRepository:
    def __init__(self) -> None:
        self.calls = []

    def store_raw_event(self, **kwargs):
        self.calls.append(kwargs)
        return type("Result", (), {"primary_key": 7})()


def test_raw_event_service_returns_checksum_and_refs() -> None:
    repository = FakeRepository()
    service = RawEventService(repository=repository, object_store=FakeObjectStore())

    receipt = service.store(
        source_system_code="KIS",
        channel_code="REST",
        endpoint_code="order-cash",
        payload_json={"odno": "8300012345"},
        source_object_id="8300012345",
    )

    assert receipt.db_ref.primary_key == 7
    assert receipt.object_ref is not None
    assert receipt.object_ref.bucket == "raw-events"
    assert len(receipt.checksum) == 64
    assert repository.calls[0]["endpoint_code"] == "order-cash"
