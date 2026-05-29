from __future__ import annotations

from forest_pipelines.freshness.classifier import classify_observations
from forest_pipelines.freshness.models import FreshnessObservation


def _row(
    preset: str,
    index: int,
    *,
    changed: bool = True,
    interval_days: str = "7.00",
    status: str = "ok",
    warning: str = "",
) -> FreshnessObservation:
    return FreshnessObservation(
        observed_at=f"2026-05-{index + 1:02d}T00:00:00Z",
        watch_id=f"watch_{preset}",
        dataset_id="dataset",
        resource_key="resource",
        source_url="https://example.test",
        source_modified_at=f"2026-05-{index + 1:02d}T00:00:00Z",
        precision="datetime",
        signal_method="test_signal",
        raw_label="raw",
        changed=changed,
        previous_source_modified_at="",
        interval_hours="168.00" if interval_days else "",
        interval_days=interval_days,
        social_presets=preset,
        suggested_cadence_hint="weekly",
        status=status,
        warning=warning,
    )


def test_classifier_detects_daily() -> None:
    rows = [_row("daily-preset", index, interval_days="1.00") for index in range(30)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "daily"
    assert result.confidence == "high"


def test_classifier_detects_weekly() -> None:
    rows = [_row("weekly-preset", index, interval_days="7.00") for index in range(8)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "weekly"


def test_classifier_detects_monthly() -> None:
    rows = [_row("monthly-preset", index, interval_days="30.00") for index in range(3)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "monthly"


def test_classifier_detects_ad_hoc() -> None:
    rows = [_row("stable-preset", index, changed=False, interval_days="") for index in range(6)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "ad_hoc"
    assert result.confidence == "low"


def test_classifier_detects_irregular() -> None:
    intervals = ["1.00", "20.00", "2.00", "30.00", "1.00", "25.00"]
    rows = [_row("irregular-preset", index, interval_days=value) for index, value in enumerate(intervals)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "irregular"


def test_classifier_detects_insufficient_data() -> None:
    rows = [_row("new-preset", index, interval_days="7.00") for index in range(2)]

    result = classify_observations(rows)[0]

    assert result.suggested_cadence == "insufficient_data"
