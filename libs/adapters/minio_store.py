from __future__ import annotations

import io
from dataclasses import dataclass

from minio import Minio

from libs.config.settings import Settings, get_settings


@dataclass(slots=True)
class ObjectRef:
    bucket: str
    object_name: str
    size: int


class MinioObjectStore:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = Minio(
            self.settings.minio_endpoint,
            access_key=self.settings.minio_access_key,
            secret_key=self.settings.minio_secret_key,
            secure=self.settings.minio_secure,
        )

    def ensure_bucket(self, bucket: str) -> None:
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def put_json_bytes(self, bucket: str, object_name: str, content: bytes) -> ObjectRef:
        self.ensure_bucket(bucket)
        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(content),
            length=len(content),
            content_type="application/json",
        )
        return ObjectRef(bucket=bucket, object_name=object_name, size=len(content))

    def put_bytes(self, bucket: str, object_name: str, content: bytes, content_type: str) -> ObjectRef:
        self.ensure_bucket(bucket)
        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(content),
            length=len(content),
            content_type=content_type,
        )
        return ObjectRef(bucket=bucket, object_name=object_name, size=len(content))
