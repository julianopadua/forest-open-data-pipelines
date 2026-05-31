from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


DOWNLOAD_SUFFIXES = {
    ".csv",
    ".zip",
    ".pdf",
    ".txt",
    ".kml",
    ".kmz",
    ".json",
    ".geojson",
}

SORT_QUERY_KEYS = {"C", "O"}


@dataclass(frozen=True)
class CoidsEntry:
    name: str
    url: str
    is_dir: bool
    size_label: str | None = None
    last_modified_label: str | None = None

    @property
    def filename(self) -> str:
        return Path(unquote(urlparse(self.url).path.rstrip("/"))).name

    @property
    def suffix(self) -> str:
        return Path(self.filename).suffix.lower()


def fetch_directory_entries(url: str, *, timeout_s: int = 60) -> list[CoidsEntry]:
    response = requests.get(url, timeout=timeout_s)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    if content_type and "html" not in content_type.lower():
        raise ValueError(f"COIDS URL did not return HTML: {url}")
    return parse_directory_entries(response.text, url)


def parse_directory_entries(html: str, base_url: str) -> list[CoidsEntry]:
    soup = BeautifulSoup(html, "html.parser")
    text_lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    out: list[CoidsEntry] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        label = anchor.get_text(" ", strip=True).strip()
        if not _keep_href(href, label):
            continue
        full_url = _normalized_url(urljoin(base_url, href))
        if full_url in seen:
            continue
        seen.add(full_url)
        is_dir = _is_dir_href(href, full_url)
        name = _entry_name(label, full_url, is_dir)
        size_label, last_modified_label = _metadata_near_label(text_lines, label)
        out.append(
            CoidsEntry(
                name=name,
                url=full_url.rstrip("/") + "/" if is_dir else full_url,
                is_dir=is_dir,
                size_label=size_label,
                last_modified_label=last_modified_label,
            )
        )
    return out


def discover_files(
    source_url: str,
    *,
    recursive: bool,
    max_depth: int = 4,
    allowed_suffixes: Iterable[str] = DOWNLOAD_SUFFIXES,
    timeout_s: int = 60,
) -> list[CoidsEntry]:
    allowed = {suffix.lower() for suffix in allowed_suffixes}
    seen_dirs: set[str] = set()
    found: dict[str, CoidsEntry] = {}

    def walk(page_url: str, depth: int) -> None:
        normalized_page = page_url.rstrip("/") + "/"
        if normalized_page in seen_dirs or depth > max_depth:
            return
        seen_dirs.add(normalized_page)
        for entry in fetch_directory_entries(normalized_page, timeout_s=timeout_s):
            if entry.is_dir:
                if recursive:
                    walk(entry.url, depth + 1)
                continue
            if entry.suffix in allowed:
                found[entry.url] = entry

    walk(source_url, 0)
    return sorted(found.values(), key=lambda entry: entry.url)


def parse_last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    formats = (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _keep_href(href: str, label: str) -> bool:
    if not href or href in {"../", "..", "/"}:
        return False
    low = href.lower()
    if low.startswith(("#", "mailto:", "javascript:")):
        return False
    if "parent directory" in label.lower():
        return False
    parsed = urlparse(href)
    query_keys = set(parse_qs(parsed.query))
    if query_keys and query_keys.issubset(SORT_QUERY_KEYS):
        return False
    return True


def _is_dir_href(href: str, full_url: str) -> bool:
    if href.rstrip().endswith("/") or full_url.rstrip().endswith("/"):
        return True
    return Path(unquote(urlparse(full_url).path)).suffix == ""


def _entry_name(label: str, url: str, is_dir: bool) -> str:
    clean = label.strip().strip("/")
    if clean and clean not in {"/", "Name"}:
        return clean
    path_name = Path(unquote(urlparse(url).path.rstrip("/"))).name
    return path_name + "/" if is_dir else path_name


def _normalized_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="", query="").geturl()


def _metadata_near_label(
    text_lines: list[str],
    label: str,
) -> tuple[str | None, str | None]:
    if not label:
        return None, None
    try:
        idx = text_lines.index(label.strip())
    except ValueError:
        return None, None
    window = text_lines[idx + 1 : idx + 5]
    date_label = next((item for item in window if _looks_like_datetime(item)), None)
    size_label = next((item for item in window if _looks_like_size(item)), None)
    return size_label, date_label


def _looks_like_datetime(value: str) -> bool:
    return parse_last_modified(value) is not None


def _looks_like_size(value: str) -> bool:
    return bool(re.fullmatch(r"(\d+(\.\d+)?\s*)?[KMGTP]?B|-", value.strip(), re.IGNORECASE))
