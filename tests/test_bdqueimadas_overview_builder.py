from __future__ import annotations

import pandas as pd

from forest_pipelines.reports.builders.bdqueimadas_overview import (
    _build_annual_totals_from_monthly_series,
    _build_effective_national_monthly_series,
    _compute_rolling_12m_metrics,
    _truncate_mensal_counts,
)


def test_effective_national_series_replaces_current_year_with_mensal() -> None:
    monthly_all_df = pd.DataFrame(
        [
            {"period": "2025-11", "year": 2025, "value": 110},
            {"period": "2025-12", "year": 2025, "value": 120},
            {"period": "2026-01", "year": 2026, "value": 9999},
        ]
    )
    mensal_counts = {
        "national": {1: 210, 2: 220},
        "by_biome": {},
        "by_state": {},
        "by_state_biome": {},
        "last_closed_month": 2,
    }

    series = _build_effective_national_monthly_series(
        monthly_all_df=monthly_all_df,
        mensal_counts=mensal_counts,
        mensal_is_current=True,
        calendar_year=2026,
    )
    assert series[-3:] == [("2025-12", 120), ("2026-01", 210), ("2026-02", 220)]


def test_rolling_12m_uses_latest_period_and_prior_window() -> None:
    series = [(f"2025-{str(m).zfill(2)}", m) for m in range(1, 13)] + [
        (f"2026-{str(m).zfill(2)}", 100 + m) for m in range(1, 5)
    ]

    rolling = _compute_rolling_12m_metrics(series, latest_period="2026-04")
    assert rolling["recent_window_start_period"] == "2025-05"
    assert rolling["recent_total"] == sum(range(5, 13)) + sum(100 + m for m in range(1, 5))
    assert rolling["prior_window_start_period"] is None
    assert rolling["has_full_prior_window"] is False


def test_truncate_mensal_counts_cuts_values_after_selected_month() -> None:
    mensal_counts = {
        "last_closed_month": 4,
        "national": {1: 10, 2: 20, 3: 30, 4: 40},
        "by_biome": {"CERRADO": {1: 1, 2: 2, 3: 3}},
        "by_state": {"GOIAS": {2: 5, 3: 6}},
        "by_state_biome": {("GOIAS", "CERRADO"): {2: 8, 4: 9}},
    }

    truncated = _truncate_mensal_counts(mensal_counts, max_month=2)
    assert truncated["last_closed_month"] == 2
    assert truncated["national"] == {1: 10, 2: 20}
    assert truncated["by_biome"]["CERRADO"] == {1: 1, 2: 2}
    assert truncated["by_state"]["GOIAS"] == {2: 5}
    assert truncated["by_state_biome"][("GOIAS", "CERRADO")] == {2: 8}


def test_annual_totals_from_monthly_series_includes_partial_current_year() -> None:
    monthly_series = [
        {"period": "2025-12", "year": 2025, "value": 12, "biome": "__all__", "state": "__all__"},
        {"period": "2026-01", "year": 2026, "value": 20, "biome": "__all__", "state": "__all__"},
        {"period": "2026-02", "year": 2026, "value": 30, "biome": "__all__", "state": "__all__"},
    ]

    annual = _build_annual_totals_from_monthly_series(monthly_series)
    assert annual == [
        {"year": 2025, "biome": "__all__", "state": "__all__", "value": 12},
        {"year": 2026, "biome": "__all__", "state": "__all__", "value": 50},
    ]
