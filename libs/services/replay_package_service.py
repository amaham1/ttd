from __future__ import annotations

import io
import json
import tarfile
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(slots=True)
class ReplayPackage:
    package_name: str
    content: bytes
    manifest: dict[str, Any]


class ReplayPackageService:
    def build_package(
        self,
        *,
        trading_date: str,
        raw_events: list[dict[str, Any]],
        canonical_events: list[dict[str, Any]],
        config_snapshot: dict[str, Any],
        model_snapshot: dict[str, Any],
    ) -> ReplayPackage:
        manifest = {
            "trading_date": trading_date,
            "raw_event_count": len(raw_events),
            "canonical_event_count": len(canonical_events),
            "generated_at_utc": datetime.now(UTC).isoformat(),
        }
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            for name, payload in {
                "manifest.json": manifest,
                "raw-events.json": raw_events,
                "canonical-events.json": canonical_events,
                "config-snapshot.json": config_snapshot,
                "model-snapshot.json": model_snapshot,
            }.items():
                encoded = json.dumps(payload, ensure_ascii=False, default=str, indent=2).encode("utf-8")
                info = tarfile.TarInfo(name=name)
                info.size = len(encoded)
                archive.addfile(info, io.BytesIO(encoded))
        return ReplayPackage(
            package_name=f"replay-{trading_date}.tar.gz",
            content=buffer.getvalue(),
            manifest=manifest,
        )
