from __future__ import annotations

import zipfile
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pytest

import forest_pipelines.profiling as profiling_module
from forest_pipelines.profiling import (
    FreshnessSignal,
    profile_cache_from_manifest,
    profile_downloaded_file,
    profile_source_url,
    profiled_item,
    use_profile_cache,
)


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.status_code = status_code
        self.headers = headers or {
            "Content-Type": "text/csv",
            "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        }
        self.iterated = False

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        self.iterated = True
        yield self.body


def test_profile_csv_counts_rows_and_columns(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    profile = profile_downloaded_file(path, source_url="https://example.test/sample.csv")

    assert profile["profile_status"] == "ok"
    assert profile["row_count"] == 2
    assert profile["column_count"] == 2
    assert profile["columns"] == ["a", "b"]
    assert profile["size_bytes"] == path.stat().st_size
    assert len(profile["sha256"]) == 64


def test_profile_zip_with_tabular_member(tmp_path: Path) -> None:
    path = tmp_path / "sample.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("data.csv", "x;y\n1;2\n")

    profile = profile_downloaded_file(path, source_url="https://example.test/sample.zip")

    assert profile["profile_status"] == "ok"
    assert profile["row_count"] == 1
    assert profile["archive_profile"]["member_count"] == 1
    member = profile["archive_profile"]["tabular_members"][0]
    assert member["filename"] == "data.csv"
    assert member["columns"] == ["x", "y"]


def test_profile_xlsx_counts_rows_and_columns(tmp_path: Path) -> None:
    path = tmp_path / "sample.xlsx"
    pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_excel(path, index=False)

    profile = profile_downloaded_file(path, source_url="https://example.test/sample.xlsx")

    assert profile["profile_status"] == "ok"
    assert profile["row_count"] == 2
    assert profile["column_count"] == 2
    assert profile["columns"] == ["a", "b"]


def test_profile_source_url_failure_is_public_safe() -> None:
    profile = profile_source_url(
        "http://127.0.0.1:9/not-available.csv",
        filename="not-available.csv",
    )

    assert profile["profile_status"] == "failed"
    assert profile["profile_warnings"][0]["code"] == "download_timeout"
    assert "Traceback" not in profile["profile_warnings"][0]["message"]


def test_profile_source_url_deletes_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[Path] = []

    def fake_named_temporary_file(*, prefix: str, suffix: str, delete: bool):
        tmp = NamedTemporaryFile(prefix=prefix, suffix=suffix, delete=delete, dir=tmp_path)
        created.append(Path(tmp.name))
        return tmp

    monkeypatch.setattr(
        profiling_module.tempfile,
        "NamedTemporaryFile",
        fake_named_temporary_file,
    )
    monkeypatch.setattr(
        profiling_module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(b"a,b\n1,2\n"),
    )

    profile = profile_source_url("https://example.test/sample.csv", filename="sample.csv")

    assert profile["profile_status"] == "ok"
    assert created
    assert not created[0].exists()


def test_profile_source_url_uses_fresh_source_signal_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_get(*args, **kwargs):
        raise AssertionError("network should not be used")

    monkeypatch.setattr(profiling_module.requests, "get", fail_get)
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/cached.csv",
                "filename": "cached.csv",
                "size_bytes": 12,
                "sha256": "a" * 64,
                "row_count": 2,
                "profiled_at": "2026-05-29T00:00:00Z",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }
    signal = FreshnessSignal(
        source_modified_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
        precision="date",
        method="test",
        raw_label="28/05/2026",
    )

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        profile = profile_source_url(
            "https://example.test/cached.csv",
            filename="cached.csv",
            freshness_signal=signal,
        )

    assert profile["size_bytes"] == 12
    assert profile["row_count"] == 2
    assert profile["profile_status"] == "ok"


def test_profile_source_url_reprofiles_stale_source_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/cached.csv",
                "filename": "cached.csv",
                "size_bytes": 12,
                "sha256": "a" * 64,
                "row_count": 2,
                "profiled_at": "2026-05-27T00:00:00Z",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }
    signal = FreshnessSignal(
        source_modified_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        precision="datetime",
        method="test",
        raw_label="28/05/2026 12h00",
    )
    monkeypatch.setattr(
        profiling_module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(b"a,b\n1,2\n3,4\n5,6\n"),
    )

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        profile = profile_source_url(
            "https://example.test/cached.csv",
            filename="cached.csv",
            freshness_signal=signal,
        )

    assert profile["row_count"] == 3
    assert profile["sha256"] != "a" * 64


def test_profile_source_url_uses_http_304_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(*args, **kwargs):
        assert kwargs["headers"] == {"If-Modified-Since": "Wed, 01 Jan 2025 00:00:00 GMT"}
        return FakeResponse(b"", status_code=304)

    monkeypatch.setattr(profiling_module.requests, "get", fake_get)
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/cached.csv",
                "filename": "cached.csv",
                "size_bytes": 12,
                "sha256": "a" * 64,
                "row_count": 2,
                "last_modified": "Wed, 01 Jan 2025 00:00:00 GMT",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        profile = profile_source_url("https://example.test/cached.csv", filename="cached.csv")

    assert profile["sha256"] == "a" * 64
    assert profile["row_count"] == 2


def test_profile_source_url_uses_unchanged_http_headers_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = FakeResponse(
        b"would-not-be-read",
        headers={
            "Content-Type": "text/csv",
            "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
            "Content-Length": "12",
        },
    )
    monkeypatch.setattr(profiling_module.requests, "get", lambda *args, **kwargs: response)
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/cached.csv",
                "filename": "cached.csv",
                "size_bytes": 12,
                "sha256": "a" * 64,
                "row_count": 2,
                "last_modified": "Wed, 01 Jan 2025 00:00:00 GMT",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        profile = profile_source_url("https://example.test/cached.csv", filename="cached.csv")

    assert profile["sha256"] == "a" * 64
    assert not response.iterated


def test_profile_source_url_without_validator_reprofiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiling_module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(
            b"a,b\n1,2\n3,4\n",
            headers={"Content-Type": "text/csv"},
        ),
    )
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/cached.csv",
                "filename": "cached.csv",
                "size_bytes": 12,
                "sha256": "a" * 64,
                "row_count": 9,
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        profile = profile_source_url("https://example.test/cached.csv", filename="cached.csv")

    assert profile["row_count"] == 2
    assert profile["sha256"] != "a" * 64


def test_profiled_item_keeps_current_identity_with_cached_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_get(*args, **kwargs):
        raise AssertionError("network should not be used")

    monkeypatch.setattr(profiling_module.requests, "get", fail_get)
    manifest = {
        "items": [
            {
                "kind": "data",
                "period": "2024",
                "filename": "old.csv",
                "source_url": "https://example.test/cached.csv",
                "row_count": 9,
                "profiled_at": "2026-05-29T00:00:00Z",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }
    signal = FreshnessSignal(
        source_modified_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
        precision="date",
        method="test",
        raw_label="28/05/2026",
    )

    with use_profile_cache(profile_cache_from_manifest(manifest)):
        item = profiled_item(
            source_url="https://example.test/cached.csv",
            filename="new.csv",
            period="2025",
            freshness_signal=signal,
        )

    assert item["filename"] == "new.csv"
    assert item["period"] == "2025"
    assert item["row_count"] == 9


def test_profile_cache_skips_url_only_sentinels() -> None:
    # Legacy URL-only manifests carry sha256="external" and size_bytes=0.
    # Reusing them across syncs would freeze the cache to placeholder data
    # and suppress every subsequent reprofile.
    manifest = {
        "items": [
            {
                "source_url": "https://example.test/legacy.pdf",
                "filename": "legacy.pdf",
                "size_bytes": 0,
                "sha256": "external",
            },
            {
                "source_url": "https://example.test/skipped.csv",
                "filename": "skipped.csv",
                "size_bytes": 0,
                "profile_status": "skipped",
            },
            {
                "source_url": "https://example.test/real.csv",
                "filename": "real.csv",
                "size_bytes": 42,
                "sha256": "f" * 64,
                "profile_status": "ok",
            },
        ]
    }

    cache = profile_cache_from_manifest(manifest)
    assert "https://example.test/legacy.pdf" not in cache
    assert "https://example.test/skipped.csv" not in cache
    assert cache["https://example.test/real.csv"]["sha256"] == "f" * 64
