"""Testes do payload analítico para LLM (sem rede)."""

from __future__ import annotations

from datetime import date

from forest_pipelines.social.bdqueimadas_monthly_chart import last_closed_month_for_calendar_year
from forest_pipelines.social.llm.payloads.focos_incendio import (
    build_focos_incendio_llm_payload,
    payload_to_prompt_block,
)


def _minimal_spec() -> dict:
    return {
        "schema_version": 3,
        "month_labels": [
            "Jan",
            "Fev",
            "Mar",
            "Abr",
            "Mai",
            "Jun",
            "Jul",
            "Ago",
            "Set",
            "Out",
            "Nov",
            "Dez",
        ],
        "series": {
            "current": {"key": "current", "label": "2026", "values": [100, 110, 120, None, None, None, None, None, None, None, None, None]},
            "previous": {"key": "previous", "label": "2025", "values": [90] * 12},
            "avg_5y": {
                "key": "avg_5y",
                "label": "Média 2021–2025 (por mês)",
                "values": [95.0, 100.0, 105.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
        },
        "metadata": {
            "latest_year": 2026,
            "previous_year": 2025,
            "avg_window_years_from": 2021,
            "avg_window_years_to": 2025,
            "last_closed_month": 3,
            "published_at_label": "Mar 2026",
            "source": "INPE (teste)",
            "biome_scope": "nacional",
            "biome_label_pt": "Brasil (Nacional)",
        },
    }


def test_last_closed_month_for_calendar_year() -> None:
    assert last_closed_month_for_calendar_year(date(2026, 4, 18), 2026) == 3
    assert last_closed_month_for_calendar_year(date(2026, 5, 1), 2026) == 4
    assert last_closed_month_for_calendar_year(date(2026, 1, 10), 2026) == 0
    assert last_closed_month_for_calendar_year(date(2027, 1, 5), 2026) == 12


def test_acumulado_e_mom_vs_mes() -> None:
    spec = _minimal_spec()
    ref = date(2026, 3, 15)
    p = build_focos_incendio_llm_payload(spec, ref)
    acc = p["metrics"]["acumulado_desde_jan_ate_ultimo_mes_fechado"]
    assert acc["soma_focos_ano_atual"] == 100 + 110 + 120
    assert acc["soma_focos_ano_anterior_mesmo_periodo"] == 90 * 3
    mom = p["metrics"]["ultimo_mes_fechado_vs_mesmo_mes_ano_anterior"]
    assert mom["mes_rotulo"] == "Mar"
    assert mom["focos_ano_atual"] == 120
    assert mom["focos_ano_anterior"] == 90


def test_payload_v3_bioma_metadata() -> None:
    spec = _minimal_spec()
    p = build_focos_incendio_llm_payload(spec, date(2026, 3, 1))
    assert p["schema"] == "focos_incendio_br_v3"
    assert "flags" not in p
    assert "escopo" in p["metadata"]
    assert p["metadata"]["bioma"] == "Brasil (Nacional)"
    p2 = build_focos_incendio_llm_payload(
        spec, date(2026, 3, 1), biome="Amazônia"
    )
    assert p2["metadata"]["bioma"] == "Amazônia"


def test_payload_to_prompt_block_roundtrip() -> None:
    spec = _minimal_spec()
    p = build_focos_incendio_llm_payload(spec, date(2026, 3, 1))
    s = payload_to_prompt_block(p)
    assert "focos_incendio_br_v3" in s
    assert "2026-03-01" in s
