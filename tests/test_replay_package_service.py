import io
import tarfile

from libs.services.replay_package_service import ReplayPackageService


def test_replay_package_contains_manifest_and_payloads() -> None:
    service = ReplayPackageService()
    package = service.build_package(
        trading_date="2026-03-11",
        raw_events=[{"source": "KIS"}],
        canonical_events=[{"message_name": "OrderAcked"}],
        config_snapshot={"policy_version": "1.0.0"},
        model_snapshot={"parser_version": "fallback-v1"},
    )

    assert package.package_name == "replay-2026-03-11.tar.gz"
    with tarfile.open(fileobj=io.BytesIO(package.content), mode="r:gz") as archive:
        names = archive.getnames()

    assert "manifest.json" in names
    assert "raw-events.json" in names
    assert package.manifest["raw_event_count"] == 1
