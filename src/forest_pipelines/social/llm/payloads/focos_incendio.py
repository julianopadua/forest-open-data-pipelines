"""
Payload analítico (sem LLM) para o tópico focos de incêndio / BDQueimadas.

Regra de negócio: séries mensais são fechadas por mês civil; `ytd_months` no chart_spec
é o último mês com arquivo mensal agregado — não há granularidade diária.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any


def _pct_delta(new: float, old: float) -> float | None:
    if old == 0:
        return None
    return round(100.0 * (new - old) / old, 2)


def build_focos_incendio_llm_payload(
    spec: dict[str, Any],
    reference_date: date,
) -> dict[str, Any]:
    """
    Monta um dict JSON-serializável com métricas comparativas para prompts LLM.

    - Mês vs mês: último mês com dado (índice ytd_months) — ano atual vs anterior.
    - YTD: soma jan..último mês para ano atual, ano anterior e soma das médias mensais 5y.
    - Parcialidade: se o calendário (reference_date) já passou do último mês fechado
      na série do ano civil corrente, sinaliza que os dados ainda não fecharam esses meses.
    """
    meta = spec["metadata"]
    ly = int(meta["latest_year"])
    ytd_m = int(meta["ytd_months"])
    if ytd_m < 1 or ytd_m > 12:
        raise ValueError(f"ytd_months inválido: {ytd_m}")

    month_labels: list[str] = list(spec["month_labels"])
    cur = spec["series"]["current"]["values"]
    prev = spec["series"]["previous"]["values"]
    avg = spec["series"]["avg_5y"]["values"]

    idx_last = ytd_m - 1

    ytd_current = 0
    for i in range(ytd_m):
        v = cur[i]
        if v is not None:
            ytd_current += int(v)

    ytd_previous = sum(int(prev[i]) for i in range(ytd_m))
    ytd_avg_sum = sum(float(avg[i]) for i in range(ytd_m))

    mom_cur = cur[idx_last]
    mom_cur_int = int(mom_cur) if mom_cur is not None else None
    mom_prev = int(prev[idx_last])

    # Calendário à frente do último mês com arquivo publicado (mesmo ano civil da série atual).
    calendario_a_frente_do_ultimo_mes_fechado = (
        reference_date.year == ly and reference_date.month > ytd_m
    )

    nota_sobre_dados_parciais = ""
    if calendario_a_frente_do_ultimo_mes_fechado:
        nota_sobre_dados_parciais = (
            f"A série mensal disponível vai até {month_labels[idx_last]} de {ly}. "
            f"Na data de referência ({reference_date.isoformat()}) o calendário já avançou "
            "além desse fechamento: meses posteriores ainda não constam como fechados na base."
        )
    else:
        nota_sobre_dados_parciais = (
            "Agregação mensal (BDQueimadas): não há contagem dia a dia neste payload; "
            "comparativos são entre totais mensais ou YTD de meses já publicados."
        )

    return {
        "schema": "focos_incendio_br_v1",
        "reference_date": reference_date.isoformat(),
        "metadata": {
            "latest_year": ly,
            "previous_year": meta.get("previous_year"),
            "avg_window_years_from": meta.get("avg_window_years_from"),
            "avg_window_years_to": meta.get("avg_window_years_to"),
            "ytd_months": ytd_m,
            "published_at_label": meta.get("published_at_label"),
            "source": meta.get("source"),
        },
        "flags": {
            "calendario_a_frente_do_ultimo_mes_fechado": calendario_a_frente_do_ultimo_mes_fechado,
            "nota_sobre_dados_parciais": nota_sobre_dados_parciais,
        },
        "metrics": {
            "mes_vs_mes": {
                "mes_rotulo": month_labels[idx_last],
                "focos_ano_atual": mom_cur_int,
                "focos_ano_anterior": mom_prev,
                "variacao_percentual_vs_ano_anterior": (
                    None
                    if mom_cur_int is None
                    else _pct_delta(float(mom_cur_int), float(mom_prev))
                ),
            },
            "ytd_acumulado_ate_mes": {
                "ultimo_mes_incluso_rotulo": month_labels[idx_last],
                "soma_focos_ano_atual": ytd_current,
                "soma_focos_ano_anterior_mesmo_periodo": ytd_previous,
                "soma_medias_mensais_5_anos_acumulada": round(ytd_avg_sum, 2),
                "variacao_pct_ytd_vs_ano_anterior": _pct_delta(
                    float(ytd_current), float(ytd_previous)
                ),
                "variacao_pct_ytd_vs_media_5y_acumulada": _pct_delta(
                    float(ytd_current), ytd_avg_sum
                ),
            },
        },
    }


def payload_to_prompt_block(payload: dict[str, Any]) -> str:
    """Serialização legível para user prompt (pt-BR labels no JSON)."""
    return json.dumps(payload, ensure_ascii=False, indent=2)
