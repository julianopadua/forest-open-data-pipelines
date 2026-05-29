from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from forest_pipelines.freshness.models import (
    OBSERVATION_FIELDS,
    FreshnessObservation,
    FreshnessSignalRecord,
    isoformat_utc,
)


def load_observations(path: str | Path) -> list[FreshnessObservation]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            rows.append(
                FreshnessObservation(
                    observed_at=str(row.get("observed_at") or ""),
                    watch_id=str(row.get("watch_id") or ""),
                    dataset_id=str(row.get("dataset_id") or ""),
                    resource_key=str(row.get("resource_key") or ""),
                    source_url=str(row.get("source_url") or ""),
                    source_modified_at=str(row.get("source_modified_at") or ""),
                    precision=str(row.get("precision") or ""),
                    signal_method=str(row.get("signal_method") or ""),
                    raw_label=str(row.get("raw_label") or ""),
                    changed=str(row.get("changed") or "").lower() == "true",
                    previous_source_modified_at=str(row.get("previous_source_modified_at") or ""),
                    interval_hours=str(row.get("interval_hours") or ""),
                    interval_days=str(row.get("interval_days") or ""),
                    social_presets=str(row.get("social_presets") or ""),
                    suggested_cadence_hint=str(row.get("suggested_cadence_hint") or ""),
                    status=str(row.get("status") or ""),
                    warning=str(row.get("warning") or ""),
                )
            )
        return rows


def _parse_iso(value: str) -> datetime | None:
    if not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _same_source_timestamp(current: str, previous: str, precision: str) -> bool:
    if not current or not previous:
        return False
    if precision == "date":
        current_dt = _parse_iso(current)
        previous_dt = _parse_iso(previous)
        if current_dt is not None and previous_dt is not None:
            return current_dt.date() == previous_dt.date()
    return current == previous


def _interval_values(current: str, previous: str) -> tuple[str, str]:
    current_dt = _parse_iso(current)
    previous_dt = _parse_iso(previous)
    if current_dt is None or previous_dt is None:
        return "", ""
    hours = (current_dt - previous_dt).total_seconds() / 3600
    return f"{hours:.2f}", f"{hours / 24:.2f}"


def _latest_by_key(rows: list[FreshnessObservation]) -> dict[tuple[str, str], FreshnessObservation]:
    latest: dict[tuple[str, str], FreshnessObservation] = {}
    for row in rows:
        latest[row.key()] = row
    return latest


def append_observations(
    path: str | Path,
    records: list[FreshnessSignalRecord],
    *,
    observed_at: datetime,
) -> list[FreshnessObservation]:
    csv_path = Path(path)
    previous_rows = load_observations(csv_path)
    latest = _latest_by_key(previous_rows)
    observed_at_iso = isoformat_utc(observed_at)
    observations: list[FreshnessObservation] = []
    for record in records:
        current_modified = record.source_modified_at_iso
        previous = latest.get((record.watch_id, record.resource_key))
        previous_modified = previous.source_modified_at if previous else ""
        changed = False
        interval_hours = ""
        interval_days = ""
        if record.status == "ok" and current_modified:
            if previous is not None and previous_modified:
                changed = not _same_source_timestamp(
                    current_modified,
                    previous_modified,
                    str(record.precision),
                )
                if changed:
                    interval_hours, interval_days = _interval_values(current_modified, previous_modified)
            else:
                changed = False
        observation = FreshnessObservation(
            observed_at=observed_at_iso,
            watch_id=record.watch_id,
            dataset_id=record.dataset_id,
            resource_key=record.resource_key,
            source_url=record.source_url,
            source_modified_at=current_modified,
            precision=str(record.precision),
            signal_method=record.signal_method,
            raw_label=record.raw_label,
            changed=changed,
            previous_source_modified_at=previous_modified,
            interval_hours=interval_hours,
            interval_days=interval_days,
            social_presets=",".join(record.social_presets),
            suggested_cadence_hint=record.suggested_cadence_hint,
            status=record.status,
            warning=record.warning,
        )
        observations.append(observation)
        latest[observation.key()] = observation

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OBSERVATION_FIELDS)
        if write_header:
            writer.writeheader()
        for observation in observations:
            writer.writerow(observation.as_row())
    return observations


def write_latest_snapshot(path: str | Path, observations: list[FreshnessObservation]) -> None:
    snapshot_path = Path(path)
    latest = _latest_by_key(observations)
    by_watch: dict[str, dict[str, dict[str, str]]] = {}
    for observation in latest.values():
        by_watch.setdefault(observation.watch_id, {})[observation.resource_key] = observation.as_row()
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "generated_at": isoformat_utc(datetime.now(timezone.utc)),
                "watches": by_watch,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
