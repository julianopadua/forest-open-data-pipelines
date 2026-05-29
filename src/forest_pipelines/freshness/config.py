from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

SignalStrategy = Literal[
    "anp_govbr_resource_label",
    "http_listing_last_modified",
    "manifest_profiled_at",
    "api_window_clock",
]


@dataclass(frozen=True)
class WatchEntry:
    watch_id: str
    dataset_id: str
    social_presets: tuple[str, ...]
    signal_strategy: SignalStrategy
    suggested_cadence: str
    source_dataset_url: str = ""
    source_url: str = ""
    resource_key: str = ""
    resource_pattern: str = ""
    latest_resources: int = 1
    manifest_url: str = ""
    clock_interval_days: int = 7


@dataclass(frozen=True)
class WatchConfig:
    schema_version: str
    default_timeout_s: int
    watches: tuple[WatchEntry, ...]


def _required_text(raw: dict[str, Any], key: str) -> str:
    value = str(raw.get(key) or "").strip()
    if not value:
        raise ValueError(f"Missing required freshness watch field: {key}")
    return value


def _text(raw: dict[str, Any], key: str) -> str:
    return str(raw.get(key) or "").strip()


def _positive_int(raw: dict[str, Any], key: str, default: int) -> int:
    value = raw.get(key, default)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer for freshness watch field: {key}") from exc
    return max(1, parsed)


def _parse_watch(raw: dict[str, Any]) -> WatchEntry:
    presets = raw.get("social_presets") or []
    if not isinstance(presets, list) or not presets:
        raise ValueError("Freshness watch social_presets must be a non-empty list")
    strategy = _required_text(raw, "signal_strategy")
    allowed = {
        "anp_govbr_resource_label",
        "http_listing_last_modified",
        "manifest_profiled_at",
        "api_window_clock",
    }
    if strategy not in allowed:
        raise ValueError(f"Unsupported freshness signal_strategy: {strategy}")
    return WatchEntry(
        watch_id=_required_text(raw, "watch_id"),
        dataset_id=_required_text(raw, "dataset_id"),
        social_presets=tuple(str(item).strip() for item in presets if str(item).strip()),
        signal_strategy=strategy,  # type: ignore[arg-type]
        suggested_cadence=_required_text(raw, "suggested_cadence"),
        source_dataset_url=_text(raw, "source_dataset_url"),
        source_url=_text(raw, "source_url"),
        resource_key=_text(raw, "resource_key"),
        resource_pattern=_text(raw, "resource_pattern"),
        latest_resources=_positive_int(raw, "latest_resources", 1),
        manifest_url=_text(raw, "manifest_url"),
        clock_interval_days=_positive_int(raw, "clock_interval_days", 7),
    )


def load_watch_config(path: str | Path) -> WatchConfig:
    config_path = Path(path)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Freshness watch config must be a YAML mapping")
    watches_raw = raw.get("watches") or []
    if not isinstance(watches_raw, list) or not watches_raw:
        raise ValueError("Freshness watch config requires a non-empty watches list")
    return WatchConfig(
        schema_version=str(raw.get("schema_version") or "1.0"),
        default_timeout_s=_positive_int(raw, "default_timeout_s", 60),
        watches=tuple(_parse_watch(item) for item in watches_raw if isinstance(item, dict)),
    )
