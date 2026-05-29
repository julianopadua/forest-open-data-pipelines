from __future__ import annotations

import csv
import statistics
from dataclasses import dataclass
from pathlib import Path

from forest_pipelines.freshness.models import FreshnessObservation
from forest_pipelines.freshness.storage import load_observations


@dataclass(frozen=True)
class CadenceClassification:
    preset: str
    watch_ids: tuple[str, ...]
    suggested_cadence: str
    confidence: str
    last_source_modified_at: str
    last_observed_at: str
    median_interval_days: str
    changes_observed: int
    signal_methods: tuple[str, ...]
    warnings: tuple[str, ...]

    def as_row(self) -> dict[str, str]:
        return {
            "preset": self.preset,
            "watch_ids": ",".join(self.watch_ids),
            "suggested_cadence": self.suggested_cadence,
            "confidence": self.confidence,
            "last_source_modified_at": self.last_source_modified_at,
            "last_observed_at": self.last_observed_at,
            "median_interval_days": self.median_interval_days,
            "changes_observed": str(self.changes_observed),
            "signal_methods": ",".join(self.signal_methods),
            "warnings": "; ".join(self.warnings),
        }


def classify_presets(history_path: str | Path) -> list[CadenceClassification]:
    return classify_observations(load_observations(history_path))


def classify_observations(rows: list[FreshnessObservation]) -> list[CadenceClassification]:
    by_preset: dict[str, list[FreshnessObservation]] = {}
    for row in rows:
        for preset in _split_presets(row.social_presets):
            by_preset.setdefault(preset, []).append(row)
    return [
        _classify_preset(preset, sorted(preset_rows, key=lambda item: item.observed_at))
        for preset, preset_rows in sorted(by_preset.items())
    ]


def write_classifications_csv(
    path: str | Path,
    classifications: list[CadenceClassification],
) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "preset",
        "watch_ids",
        "suggested_cadence",
        "confidence",
        "last_source_modified_at",
        "last_observed_at",
        "median_interval_days",
        "changes_observed",
        "signal_methods",
        "warnings",
    ]
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for classification in classifications:
            writer.writerow(classification.as_row())


def _classify_preset(preset: str, rows: list[FreshnessObservation]) -> CadenceClassification:
    ok_rows = [row for row in rows if row.status == "ok"]
    changed_rows = [row for row in ok_rows if row.changed]
    intervals = [_float(row.interval_days) for row in changed_rows]
    intervals = [value for value in intervals if value is not None and value > 0]
    median = statistics.median(intervals) if intervals else None
    cv = _coefficient_of_variation(intervals)
    cadence = _cadence_for(
        observations=len(ok_rows),
        changes=len(changed_rows),
        median=median,
        cv=cv,
    )
    warnings = tuple(sorted({row.warning for row in rows if row.warning}))
    confidence = _confidence_for(
        observations=len(ok_rows),
        changes=len(changed_rows),
        cv=cv,
        warnings=warnings,
        methods=tuple(sorted({row.signal_method for row in ok_rows if row.signal_method})),
    )
    latest = ok_rows[-1] if ok_rows else rows[-1]
    return CadenceClassification(
        preset=preset,
        watch_ids=tuple(sorted({row.watch_id for row in rows})),
        suggested_cadence=cadence,
        confidence=confidence,
        last_source_modified_at=latest.source_modified_at,
        last_observed_at=latest.observed_at,
        median_interval_days=f"{median:.2f}" if median is not None else "",
        changes_observed=len(changed_rows),
        signal_methods=tuple(sorted({row.signal_method for row in ok_rows if row.signal_method})),
        warnings=warnings,
    )


def _cadence_for(
    *,
    observations: int,
    changes: int,
    median: float | None,
    cv: float | None,
) -> str:
    if observations >= 6 and changes < 2:
        return "ad_hoc"
    if changes < 3:
        return "insufficient_data"
    if cv is not None and cv > 0.75:
        return "irregular"
    if observations >= 30 and median is not None and median <= 1.5:
        return "daily"
    if observations >= 8 and median is not None and 5 <= median <= 10:
        return "weekly"
    if median is not None and 25 <= median <= 35:
        return "monthly"
    return "irregular"


def _confidence_for(
    *,
    observations: int,
    changes: int,
    cv: float | None,
    warnings: tuple[str, ...],
    methods: tuple[str, ...],
) -> str:
    if changes < 3 or observations < 6 or warnings:
        return "low"
    if len(methods) > 1:
        return "low"
    if observations >= 30 and (cv is None or cv <= 0.25):
        return "high"
    if cv is None or cv <= 0.75:
        return "medium"
    return "low"


def _coefficient_of_variation(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = statistics.mean(values)
    if mean == 0:
        return None
    return statistics.pstdev(values) / mean


def _float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _split_presets(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())
