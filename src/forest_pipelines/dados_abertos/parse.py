# src/forest_pipelines/dados_abertos/parse.py
from __future__ import annotations

from typing import Any


def _normalize_format(fmt: Any) -> str:
    if fmt is None:
        return ""
    return str(fmt).strip().lower()


def is_csv_format(fmt: Any) -> bool:
    return _normalize_format(fmt) == "csv"


def csv_url_extension_mismatch(url: str) -> bool:
    """True if URL path suggests xlsx/xls while format is CSV (portal inconsistency)."""
    base = url.split("?", 1)[0].strip().lower()
    return base.endswith(".xlsx") or base.endswith(".xls")


def extract_csv_rows_from_record(record: dict[str, Any]) -> list[dict[str, str]]:
    """
    From one CKAN package record, collect CSV resources from ``resources``.
    Returns rows: dataset_title, file_name, download_url (deduped within this record by URL).
    """
    title = str(record.get("title") or "").strip() or "(sem título)"

    seen_urls: set[str] = set()
    out: list[dict[str, str]] = []

    raw = record.get("resources")
    if not isinstance(raw, list):
        return out

    for item in raw:
        if not isinstance(item, dict):
            continue
        if not is_csv_format(item.get("format")):
            continue
        name = str(item.get("name") or "").strip() or "(sem nome)"
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        out.append(
            {
                "dataset_title": title,
                "file_name": name,
                "download_url": url,
            }
        )

    return out
