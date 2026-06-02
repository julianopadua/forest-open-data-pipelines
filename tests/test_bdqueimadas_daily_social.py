from __future__ import annotations

from datetime import date

import pandas as pd

from forest_pipelines.social.bdqueimadas_daily import pipeline


def test_select_daily_window_uses_latest_available_when_as_of_missing() -> None:
    resources = [
        pipeline.DailyResource(date(2026, 5, day), f"focos_diario_br_202605{day:02d}.csv", f"https://x/{day}")
        for day in range(1, 10)
    ]

    selected = pipeline.select_daily_window(resources, as_of=None, days=7)

    assert [item.period.day for item in selected] == [3, 4, 5, 6, 7, 8, 9]


def test_select_daily_window_skips_current_day_by_default() -> None:
    resources = [
        pipeline.DailyResource(date(2026, 5, 31), "focos_diario_br_20260531.csv", "https://x/31"),
        pipeline.DailyResource(date(2026, 6, 1), "focos_diario_br_20260601.csv", "https://x/1"),
        pipeline.DailyResource(date(2026, 6, 2), "focos_diario_br_20260602.csv", "https://x/2"),
    ]

    selected = pipeline.select_daily_window(resources, as_of=None, days=2, today=date(2026, 6, 2))

    assert [item.period.isoformat() for item in selected] == ["2026-05-31", "2026-06-01"]


def test_filter_reference_satellite_keeps_only_aqua_mt() -> None:
    frame = pd.DataFrame(
        [
            {"satelite": "AQUA_M-T", "lat": "-10.5", "lon": "-55.1"},
            {"satelite": "NOAA-20", "lat": "-11", "lon": "-56"},
        ]
    )

    out = pipeline.filter_reference_satellite(frame)

    assert len(out) == 1
    assert out["satelite"].iloc[0] == "AQUA_M-T"
    assert float(out["lat"].iloc[0]) == -10.5


def test_top_n_with_other_groups_remainder() -> None:
    frame = pd.DataFrame(
        {
            "estado": ["MT", "MT", "PA", "AM", "RO", "AC", "AC", "MA"],
        }
    )

    rows = pipeline.top_n_with_other(frame, "estado", top_n=4)

    assert rows == [
        {"label": "MT", "value": 2},
        {"label": "AC", "value": 2},
        {"label": "PA", "value": 1},
        {"label": "AM", "value": 1},
        {"label": "Outros", "value": 2},
    ]


def test_build_region_rank_maps_states_to_regions() -> None:
    frame = pd.DataFrame(
        {
            "estado": ["MATO GROSSO", "GOIAS", "SAO PAULO", "PARA", "XX"],
        }
    )

    rows = pipeline.build_region_rank(frame)

    assert rows == [
        {"label": "Centro-Oeste", "value": 2},
        {"label": "Sudeste", "value": 1},
        {"label": "Norte", "value": 1},
        {"label": "Não identificada", "value": 1},
    ]


def test_build_llm_payload_contains_sources_and_rankings() -> None:
    resources = [
        pipeline.DailyResource(date(2026, 5, 30), "a.csv", "https://example.test/a.csv"),
        pipeline.DailyResource(date(2026, 5, 31), "b.csv", "https://example.test/b.csv"),
    ]

    payload = pipeline.build_llm_payload(
        resources=resources,
        daily_counts=[{"date": "2026-05-30", "value": 2}, {"date": "2026-05-31", "value": 3}],
        state_rank=[{"label": "MT", "value": 3}, {"label": "Outros", "value": 2}],
        biome_rank=[{"label": "CERRADO", "value": 4}, {"label": "Outros", "value": 1}],
        region_rank=[{"label": "Centro-Oeste", "value": 4}, {"label": "Norte", "value": 1}],
        total_focos=5,
        total_raw_rows=8,
        map_status={"geojson_ok": False},
        warnings=[],
    )

    assert payload["reference_satellite"] == "AQUA_M-T"
    assert payload["window"]["start_date"] == "2026-05-30"
    assert payload["source_urls"][1]["url"] == "https://example.test/b.csv"
    assert payload["metrics"]["total_focos_reference_satellite"] == 5
    assert payload["slide_context"]["daily"]["max_day"] == {"date": "2026-05-31", "value": 3}
    assert payload["slide_context"]["daily"]["min_day"] == {"date": "2026-05-30", "value": 2}
    assert payload["slide_context"]["states"]["top_state_share_pct"] == 60.0
    assert payload["slide_context"]["biomes"]["top_biome_share_pct"] == 80.0
    assert payload["slide_context"]["map"]["top_region_by_focus_count"] == {
        "label": "Centro-Oeste",
        "value": 4,
    }


def test_deterministic_texts_include_cover_key() -> None:
    resources = [
        pipeline.DailyResource(date(2026, 5, 30), "a.csv", "https://example.test/a.csv"),
    ]
    payload = pipeline.build_llm_payload(
        resources=resources,
        daily_counts=[{"date": "2026-05-30", "value": 2}],
        state_rank=[{"label": "MT", "value": 2}],
        biome_rank=[{"label": "CERRADO", "value": 2}],
        region_rank=[{"label": "Centro-Oeste", "value": 2}],
        total_focos=2,
        total_raw_rows=4,
        map_status={"geojson_ok": True},
        warnings=[],
    )

    texts = pipeline.deterministic_texts(payload)

    assert set(texts["slides"]) == {"cover", "daily", "states", "biomes", "map"}


def test_iter_geojson_polygons_reads_multipolygon() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [[[-1, -1], [1, -1], [1, 1], [-1, -1]]],
                    ],
                },
            }
        ],
    }

    polygons = pipeline.iter_geojson_polygons(geojson)

    assert polygons == [[(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, -1.0)]]
