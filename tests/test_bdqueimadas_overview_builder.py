from __future__ import annotations

import json

import pandas as pd

from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    CACHE_SCHEMA_VERSION,
    build_incremental_year_caches,
    _build_signature,
)
from forest_pipelines.reports.builders.bdqueimadas_overview import (
    _build_annual_totals_from_monthly_series,
    _build_effective_national_monthly_series,
    _build_monthly_year_comparison_records,
    _compute_rolling_12m_metrics,
    _resolve_historical_average_years,
    _truncate_mensal_counts,
)


class FakeStorage:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def download_bytes(self, object_path: str) -> bytes | None:
        return self.objects.get(object_path)

    def upload_bytes(
        self,
        object_path: str,
        data: bytes,
        content_type: str,
        upsert: bool,
    ) -> None:
        self.objects[object_path] = data


class NullLogger:
    def info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def warning(self, *_args: object, **_kwargs: object) -> None:
        return None


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


def test_historical_average_years_uses_last_five_closed_years() -> None:
    years = _resolve_historical_average_years(
        available_years=[2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026],
        current_year=2026,
        window=5,
    )

    assert years == [2021, 2022, 2023, 2024, 2025]


def test_monthly_year_comparison_averages_five_year_window() -> None:
    monthly_all_df = pd.DataFrame(
        [
            {"period": f"{year}-01", "year": year, "value": value}
            for year, value in [
                (2021, 10),
                (2022, 20),
                (2023, 30),
                (2024, 40),
                (2025, 50),
            ]
        ]
    )
    mensal_counts = {
        "national": {1: 100},
        "by_biome": {},
        "by_state": {},
        "by_state_biome": {},
        "last_closed_month": 1,
    }

    rows = _build_monthly_year_comparison_records(
        monthly_all_df=monthly_all_df,
        monthly_by_biome_df=pd.DataFrame(),
        state_month_all_df=pd.DataFrame(),
        latest_year=2026,
        previous_year=2025,
        five_avg_candidate_years=[2021, 2022, 2023, 2024, 2025],
        last_closed_month=1,
        mensal_counts=mensal_counts,
    )

    national_january = next(
        row for row in rows if row["biome"] == "__all__" and row["state"] == "__all__" and row["month"] == 1
    )
    assert national_january["current_year_val"] == 100
    assert national_january["previous_year_val"] == 50
    assert national_january["avg_5yr_val"] == 30.0


def test_incremental_current_scope_can_reuse_historical_cache() -> None:
    build_signature = _build_signature(
        datetime_candidates=["data"],
        state_candidates=["estado"],
        biome_candidates=["bioma"],
    )
    fingerprint = {"zip_name": "focos_br_ref_2025.zip"}
    payload = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "build_signature": build_signature,
        "fingerprint": fingerprint,
        "file_name": "focos_br_ref_2025.zip",
        "file_size_bytes": 1,
        "inferred_year": 2025,
        "row_count": 1,
        "monthly_all": [{"period": "2025-01", "year": 2025, "value": 10}],
        "monthly_by_biome": [],
        "annual_all": [{"year": 2025, "value": 10}],
        "annual_by_biome": [],
        "state_year_all": [],
        "state_year_by_biome": [],
        "state_month_all": [],
        "state_month_by_biome": [],
        "available_biomes": [],
    }
    manifest = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "build_signature": build_signature,
        "cache_prefix": "reports/test/_cache",
        "files": {
            "focos_br_ref_2025.zip": {
                "year": 2025,
                "cache_object_path": "reports/test/_cache/yearly/2025.json",
                "build_signature": build_signature,
                "fingerprint": fingerprint,
                "row_count": 1,
                "processed_at": "2026-01-01T00:00:00Z",
            }
        },
    }
    storage = FakeStorage(
        {
            "reports/test/_cache/incremental_manifest.json": json.dumps(manifest).encode("utf-8"),
            "reports/test/_cache/yearly/2025.json": json.dumps(payload).encode("utf-8"),
        }
    )

    result = build_incremental_year_caches(
        storage=storage,
        cache_prefix="reports/test/_cache",
        zip_files=[],
        datetime_candidates=["data"],
        state_candidates=["estado"],
        biome_candidates=["bioma"],
        logger=NullLogger(),
        include_cached_payloads=True,
    )

    assert [item["inferred_year"] for item in result["year_payloads"]] == [2025]
    assert result["cache_stats"]["reused_count"] == 1
