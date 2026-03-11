from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

from libs.adapters.minio_store import MinioObjectStore, ObjectRef
from libs.db.repositories import PersistenceResult, TradingRepository


@dataclass(slots=True)
class RawEventReceipt:
    db_ref: PersistenceResult
    object_ref: ObjectRef | None
    checksum: str
    stored_at_utc: datetime


class RawEventService:
    def __init__(
        self,
        *,
        repository: TradingRepository | None = None,
        object_store: MinioObjectStore | None = None,
        raw_bucket: str = "raw-events",
    ) -> None:
        self.repository = repository
        self.object_store = object_store
        self.raw_bucket = raw_bucket

    def store(
        self,
        *,
        source_system_code: str,
        channel_code: str,
        endpoint_code: str,
        payload_json: dict,
        source_object_id: str | None = None,
        venue_code: str | None = None,
    ) -> RawEventReceipt:
        encoded = json.dumps(payload_json, ensure_ascii=False, default=str).encode("utf-8")
        checksum = sha256(encoded).hexdigest()
        object_ref = None
        if self.object_store is not None:
            object_name = f"{source_system_code}/{channel_code}/{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{checksum[:12]}.json"
            object_ref = self.object_store.put_json_bytes(self.raw_bucket, object_name, encoded)

        db_ref = PersistenceResult(primary_key=0)
        if self.repository is not None:
            db_ref = self.repository.store_raw_event(
                source_system_code=source_system_code,
                channel_code=channel_code,
                endpoint_code=endpoint_code,
                payload_json=payload_json,
                source_object_id=source_object_id or checksum,
                venue_code=venue_code,
            )
        return RawEventReceipt(
            db_ref=db_ref,
            object_ref=object_ref,
            checksum=checksum,
            stored_at_utc=datetime.now(UTC),
        )
