"""Testes unitários do carrossel BDQueimadas (sem rede)."""

from __future__ import annotations

from forest_pipelines.social.bdqueimadas_monthly_chart import (
    _monthly_by_biome_payloads_to_df_dedupe,
)


def test_monthly_by_biome_payloads_filters_and_dedupes() -> None:
    payloads = [
        {
            "monthly_by_biome": [
                {"period": "2024-01", "year": 2024, "biome": "AMAZÔNIA", "value": 10},
                {"period": "2024-01", "year": 2024, "biome": "CERRADO", "value": 99},
                {"period": "2024-02", "year": 2024, "biome": "AMAZÔNIA", "value": 20},
            ]
        },
        {
            "monthly_by_biome": [
                # duplicate period,year for AMAZÔNIA — first wins (same as nacional)
                {"period": "2024-01", "year": 2024, "biome": "AMAZÔNIA", "value": 777},
            ]
        },
    ]
    df = _monthly_by_biome_payloads_to_df_dedupe(payloads, "AMAZÔNIA")
    assert len(df) == 2
    assert df["period"].tolist() == ["2024-01", "2024-02"]
    assert df["value"].tolist() == [10, 20]


def test_monthly_by_biome_payloads_empty_when_no_match() -> None:
    df = _monthly_by_biome_payloads_to_df_dedupe(
        [{"monthly_by_biome": [{"period": "2024-01", "year": 2024, "biome": "CERRADO", "value": 1}]}],
        "AMAZÔNIA",
    )
    assert df.empty
    assert list(df.columns) == ["period", "year", "value"]


def test_monthly_by_biome_normalizes_numeric_columns() -> None:
    df = _monthly_by_biome_payloads_to_df_dedupe(
        [
            {
                "monthly_by_biome": [
                    {"period": "2024-03", "year": 2024, "biome": "PANTANAL", "value": "5"},
                ]
            }
        ],
        "PANTANAL",
    )
    assert int(df["value"].iloc[0]) == 5
