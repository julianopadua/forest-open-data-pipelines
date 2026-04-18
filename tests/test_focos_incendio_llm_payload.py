"""Testes do payload analítico para LLM (sem rede)."""

from __future__ import annotations

from datetime import date

from forest_pipelines.social.llm.payloads.focos_incendio import (
    build_focos_incendio_llm_payload,
    payload_to_prompt_block,
)


def _minimal_spec() -> dict:
    return {
        "schema_version": 2,
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
            "ytd_months": 3,
            "published_at_label": "Mar 2026",
            "source": "INPE (teste)",
        },
    }


def test_ytd_sums_and_mom_vs_mes() -> None:
    spec = _minimal_spec()
    ref = date(2026, 3, 15)
    p = build_focos_incendio_llm_payload(spec, ref)
    assert p["metrics"]["ytd_acumulado_ate_mes"]["soma_focos_ano_atual"] == 100 + 110 + 120
    assert p["metrics"]["ytd_acumulado_ate_mes"]["soma_focos_ano_anterior_mesmo_periodo"] == 90 * 3
    assert p["metrics"]["mes_vs_mes"]["mes_rotulo"] == "Mar"
    assert p["metrics"]["mes_vs_mes"]["focos_ano_atual"] == 120
    assert p["metrics"]["mes_vs_mes"]["focos_ano_anterior"] == 90


def test_calendario_a_frente_flag() -> None:
    spec = _minimal_spec()
    ref = date(2026, 5, 1)
    p = build_focos_incendio_llm_payload(spec, ref)
    assert p["flags"]["calendario_a_frente_do_ultimo_mes_fechado"] is True

    ref_ok = date(2026, 3, 20)
    p2 = build_focos_incendio_llm_payload(spec, ref_ok)
    assert p2["flags"]["calendario_a_frente_do_ultimo_mes_fechado"] is False


def test_payload_to_prompt_block_roundtrip() -> None:
    spec = _minimal_spec()
    p = build_focos_incendio_llm_payload(spec, date(2026, 3, 1))
    s = payload_to_prompt_block(p)
    assert "focos_incendio_br_v1" in s
    assert "2026-03-01" in s
