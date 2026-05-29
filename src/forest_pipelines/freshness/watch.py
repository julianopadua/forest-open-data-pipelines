from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from forest_pipelines.datasets.anp.govbr import (
    extract_page_freshness_labels,
    extract_resource_links,
    parse_govbr_freshness_label,
)
from forest_pipelines.freshness.config import WatchConfig, WatchEntry
from forest_pipelines.freshness.models import FreshnessSignalRecord
from forest_pipelines.profiling import FreshnessSignal


def collect_watch_signals(
    config: WatchConfig,
    *,
    observed_at: datetime,
    timeout_s: int | None = None,
) -> list[FreshnessSignalRecord]:
    records: list[FreshnessSignalRecord] = []
    timeout = timeout_s or config.default_timeout_s
    for watch in config.watches:
        records.extend(collect_one_watch(watch, observed_at=observed_at, timeout_s=timeout))
    return records


def collect_one_watch(
    watch: WatchEntry,
    *,
    observed_at: datetime,
    timeout_s: int,
) -> list[FreshnessSignalRecord]:
    try:
        if watch.signal_strategy == "anp_govbr_resource_label":
            return _collect_anp_govbr(watch, timeout_s=timeout_s)
        if watch.signal_strategy == "http_listing_last_modified":
            return _collect_http_listing(watch, timeout_s=timeout_s)
        if watch.signal_strategy == "manifest_profiled_at":
            return _collect_manifest_profiled_at(watch, timeout_s=timeout_s)
        if watch.signal_strategy == "api_window_clock":
            return [_clock_record(watch, observed_at=observed_at)]
    except Exception as exc:
        return [_missing_signal_record(watch, warning=f"{type(exc).__name__}: {exc}")]
    return [_missing_signal_record(watch, warning=f"Unsupported strategy: {watch.signal_strategy}")]


def _record(
    watch: WatchEntry,
    *,
    resource_key: str,
    source_url: str,
    signal: FreshnessSignal | None,
    warning: str = "",
) -> FreshnessSignalRecord:
    status = "ok" if signal is not None else "no_signal"
    return FreshnessSignalRecord(
        watch_id=watch.watch_id,
        dataset_id=watch.dataset_id,
        resource_key=resource_key or _resource_key(source_url),
        source_url=source_url,
        social_presets=watch.social_presets,
        suggested_cadence_hint=watch.suggested_cadence,
        signal=signal,
        status=status,
        warning=warning,
    )


def _missing_signal_record(watch: WatchEntry, *, warning: str) -> FreshnessSignalRecord:
    source_url = watch.source_url or watch.source_dataset_url or watch.manifest_url
    return _record(
        watch,
        resource_key=watch.resource_key or "source",
        source_url=source_url,
        signal=None,
        warning=warning,
    )


def _collect_anp_govbr(watch: WatchEntry, *, timeout_s: int) -> list[FreshnessSignalRecord]:
    page_url = watch.source_dataset_url or watch.source_url
    if not page_url:
        return [_missing_signal_record(watch, warning="ANP gov.br watch requires source_dataset_url")]
    response = requests.get(page_url, timeout=timeout_s)
    response.raise_for_status()
    html = response.text
    page_labels = extract_page_freshness_labels(html)
    page_signal = parse_govbr_freshness_label(
        page_labels.get("modified_label"),
        method="anp_page_modified_label",
    )
    resources = [
        resource
        for resource in extract_resource_links(html, page_url)
        if resource.kind == "data" and resource.direct_download
    ]
    if watch.resource_pattern:
        pattern = re.compile(watch.resource_pattern, re.IGNORECASE)
        resources = [
            resource
            for resource in resources
            if pattern.search(resource.filename) or pattern.search(resource.source_url)
        ]
    if not resources:
        return [_missing_signal_record(watch, warning="No direct ANP data resource discovered")]
    records = []
    for resource in resources:
        signal = parse_govbr_freshness_label(
            resource.updated_label,
            method="anp_resource_updated_label",
        ) or page_signal
        records.append(
            _record(
                watch,
                resource_key=watch.resource_key or resource.filename,
                source_url=resource.source_url,
                signal=signal,
                warning="" if signal is not None else "ANP resource has no freshness label",
            )
        )
    return records


def _collect_http_listing(watch: WatchEntry, *, timeout_s: int) -> list[FreshnessSignalRecord]:
    base_url = watch.source_dataset_url or watch.source_url
    if not base_url:
        return [_missing_signal_record(watch, warning="HTTP listing watch requires source_dataset_url")]
    response = requests.get(base_url, timeout=timeout_s)
    response.raise_for_status()
    links = _links_from_listing(response.text, base_url)
    if watch.resource_pattern:
        pattern = re.compile(watch.resource_pattern, re.IGNORECASE)
        links = [link for link in links if pattern.search(Path(link).name) or pattern.search(link)]
    links = sorted(set(links))
    if watch.latest_resources:
        links = links[-watch.latest_resources :]
    if not links:
        return [_missing_signal_record(watch, warning="No resource link found in HTTP listing")]
    return [_record_from_http_headers(watch, url, timeout_s=timeout_s) for url in links]


def _links_from_listing(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        links.append(urljoin(base_url, href))
    return links


def _record_from_http_headers(
    watch: WatchEntry,
    source_url: str,
    *,
    timeout_s: int,
) -> FreshnessSignalRecord:
    headers = _resource_headers(source_url, timeout_s=timeout_s)
    last_modified = headers.get("Last-Modified") or headers.get("last-modified")
    signal = _signal_from_http_last_modified(last_modified)
    warning = "" if signal is not None else "HTTP resource has no Last-Modified header"
    return _record(
        watch,
        resource_key=watch.resource_key or Path(source_url).name or _resource_key(source_url),
        source_url=source_url,
        signal=signal,
        warning=warning,
    )


def _resource_headers(source_url: str, *, timeout_s: int) -> dict[str, str]:
    try:
        response = requests.head(source_url, allow_redirects=True, timeout=timeout_s)
        if response.status_code < 400 and response.headers:
            return dict(response.headers)
    except requests.RequestException:
        pass
    response = requests.get(source_url, stream=True, timeout=timeout_s)
    response.raise_for_status()
    return dict(response.headers)


def _signal_from_http_last_modified(value: str | None) -> FreshnessSignal | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value.strip())
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return FreshnessSignal(
        source_modified_at=parsed.astimezone(timezone.utc),
        precision="datetime",
        method="http_last_modified",
        raw_label=value.strip(),
    )


def _collect_manifest_profiled_at(watch: WatchEntry, *, timeout_s: int) -> list[FreshnessSignalRecord]:
    if not watch.manifest_url:
        return [_missing_signal_record(watch, warning="Manifest watch requires manifest_url")]
    response = requests.get(watch.manifest_url, timeout=timeout_s)
    response.raise_for_status()
    manifest = response.json()
    items = manifest.get("items") if isinstance(manifest, dict) else None
    if not isinstance(items, list):
        return [_missing_signal_record(watch, warning="Manifest has no items list")]
    records = []
    for item in items:
        if not isinstance(item, dict):
            continue
        profiled_at = str(item.get("profiled_at") or "").strip()
        source_url = str(item.get("source_url") or watch.manifest_url)
        filename = str(item.get("filename") or "")
        signal = _signal_from_iso(profiled_at, method="manifest_profiled_at")
        records.append(
            _record(
                watch,
                resource_key=watch.resource_key or filename or _resource_key(source_url),
                source_url=source_url,
                signal=signal,
                warning="" if signal is not None else "Manifest item has no profiled_at",
            )
        )
    return records or [_missing_signal_record(watch, warning="Manifest has no observable items")]


def _signal_from_iso(value: str, *, method: str) -> FreshnessSignal | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return FreshnessSignal(
        source_modified_at=parsed.astimezone(timezone.utc),
        precision="datetime",
        method=method,
        raw_label=value,
    )


def _clock_record(watch: WatchEntry, *, observed_at: datetime) -> FreshnessSignalRecord:
    current = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    day_index = (current.date() - epoch.date()).days
    bucket_start_day = day_index - (day_index % watch.clock_interval_days)
    source_modified_at = epoch + timedelta(days=bucket_start_day)
    signal = FreshnessSignal(
        source_modified_at=source_modified_at,
        precision="date",
        method="api_window_clock",
        raw_label=f"{watch.clock_interval_days}d:{source_modified_at.date().isoformat()}",
    )
    return _record(
        watch,
        resource_key=watch.resource_key or "api_window",
        source_url=watch.source_url or watch.source_dataset_url or watch.watch_id,
        signal=signal,
    )


def _resource_key(source_url: str) -> str:
    name = Path(source_url).name
    if name:
        return name
    digest = hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:16]
    return f"url_{digest}"


def observation_log(event: str, **fields: object) -> str:
    return json.dumps({"event": event, **fields}, ensure_ascii=False, sort_keys=True)
