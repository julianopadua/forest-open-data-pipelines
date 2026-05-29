from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from forest_pipelines.profiling import FreshnessSignal, FreshnessPrecision

ObservationStatus = Literal["ok", "no_signal", "failed"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    current = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def signal_datetime_utc(signal: FreshnessSignal) -> datetime:
    value = signal.source_modified_at
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class FreshnessSignalRecord:
    watch_id: str
    dataset_id: str
    resource_key: str
    source_url: str
    social_presets: tuple[str, ...]
    suggested_cadence_hint: str
    signal: FreshnessSignal | None
    status: ObservationStatus = "ok"
    warning: str = ""

    @property
    def source_modified_at_iso(self) -> str:
        if self.signal is None:
            return ""
        return isoformat_utc(signal_datetime_utc(self.signal))

    @property
    def precision(self) -> FreshnessPrecision | str:
        return self.signal.precision if self.signal is not None else ""

    @property
    def signal_method(self) -> str:
        return self.signal.method if self.signal is not None else ""

    @property
    def raw_label(self) -> str:
        return self.signal.raw_label if self.signal is not None else ""


@dataclass(frozen=True)
class FreshnessObservation:
    observed_at: str
    watch_id: str
    dataset_id: str
    resource_key: str
    source_url: str
    source_modified_at: str
    precision: str
    signal_method: str
    raw_label: str
    changed: bool
    previous_source_modified_at: str
    interval_hours: str
    interval_days: str
    social_presets: str
    suggested_cadence_hint: str
    status: str
    warning: str

    def key(self) -> tuple[str, str]:
        return (self.watch_id, self.resource_key)

    def as_row(self) -> dict[str, str]:
        return {
            "observed_at": self.observed_at,
            "watch_id": self.watch_id,
            "dataset_id": self.dataset_id,
            "resource_key": self.resource_key,
            "source_url": self.source_url,
            "source_modified_at": self.source_modified_at,
            "precision": self.precision,
            "signal_method": self.signal_method,
            "raw_label": self.raw_label,
            "changed": "true" if self.changed else "false",
            "previous_source_modified_at": self.previous_source_modified_at,
            "interval_hours": self.interval_hours,
            "interval_days": self.interval_days,
            "social_presets": self.social_presets,
            "suggested_cadence_hint": self.suggested_cadence_hint,
            "status": self.status,
            "warning": self.warning,
        }


OBSERVATION_FIELDS = [
    "observed_at",
    "watch_id",
    "dataset_id",
    "resource_key",
    "source_url",
    "source_modified_at",
    "precision",
    "signal_method",
    "raw_label",
    "changed",
    "previous_source_modified_at",
    "interval_hours",
    "interval_days",
    "social_presets",
    "suggested_cadence_hint",
    "status",
    "warning",
]
