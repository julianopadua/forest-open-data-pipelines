from __future__ import annotations

import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pytest

import forest_pipelines.profiling as profiling_module
from forest_pipelines.profiling import (
    profile_downloaded_file,
    profile_source_url,
)


class FakeResponse:
    headers = {"Content-Type": "text/csv", "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT"}

    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
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
