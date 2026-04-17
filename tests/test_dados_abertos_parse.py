# tests/test_dados_abertos_parse.py
from __future__ import annotations

from forest_pipelines.dados_abertos.parse import (
    csv_url_extension_mismatch,
    extract_csv_rows_from_record,
    is_csv_format,
)


def test_is_csv_format_case_insensitive() -> None:
    assert is_csv_format("CSV")
    assert is_csv_format("csv")
    assert not is_csv_format("JSON")


def test_csv_url_extension_mismatch() -> None:
    assert csv_url_extension_mismatch("https://x.com/a.xlsx")
    assert csv_url_extension_mismatch("https://x.com/a.XLS?query=1")
    assert not csv_url_extension_mismatch("https://x.com/a.csv")


def test_extract_dedupes_urls_in_resources() -> None:
    record = {
        "title": "Dataset A",
        "resources": [
            {"format": "CSV", "name": "f1.csv", "url": "https://u1/file.csv"},
            {"format": "CSV", "name": "f1.csv", "url": "https://u1/file.csv"},
            {"format": "CSV", "name": "f2.csv", "url": "https://u2/file.csv"},
        ],
    }
    rows = extract_csv_rows_from_record(record)
    assert len(rows) == 2
    urls = {r["download_url"] for r in rows}
    assert urls == {"https://u1/file.csv", "https://u2/file.csv"}


def test_extract_skip_non_csv() -> None:
    record = {
        "title": "X",
        "resources": [
            {"format": "JSON", "name": "j", "url": "https://j"},
        ],
    }
    assert extract_csv_rows_from_record(record) == []


def test_extract_null_or_invalid_resources() -> None:
    assert extract_csv_rows_from_record({"title": "Z", "resources": None}) == []
    assert extract_csv_rows_from_record({"title": "Z"}) == []


def test_warn_scenario_xlsx_url() -> None:
    record = {
        "title": "Y",
        "resources": [
            {"format": "CSV", "name": "bad", "url": "https://cdn/file.XLSX"},
        ],
    }
    rows = extract_csv_rows_from_record(record)
    assert len(rows) == 1
    assert csv_url_extension_mismatch(rows[0]["download_url"])
