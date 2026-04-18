"""
Payload analítico (sem LLM) para o tópico focos de incêndio / BDQueimadas.

Regra de negócio: agregação mensal civil. O chart_spec usa apenas meses já encerrados
(``last_closed_month``); não há granularidade diária nem mês civil em curso na linha atual.
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

    - Mês vs mês: compara o **último mês civil fechado** (``last_closed_month``) —
      ano atual vs ano anterior naquele mês.
    - Acumulado: soma de jan até esse último mês fechado (ano atual, ano anterior,
      soma das médias mensais da janela de 5 anos).
    """
    meta = spec["metadata"]
    ly = int(meta["latest_year"])
    last_m = int(meta.get("last_closed_month", meta.get("ytd_months", 0)))
    if last_m < 1 or last_m > 12:
        raise ValueError(f"last_closed_month inválido: {last_m}")

    month_labels: list[str] = list(spec["month_labels"])
    cur = spec["series"]["current"]["values"]
    prev = spec["series"]["previous"]["values"]
    avg = spec["series"]["avg_5y"]["values"]

    idx_last = last_m - 1

    ytd_current = 0
    for i in range(last_m):
        v = cur[i]
        if v is not None:
            ytd_current += int(v)

    ytd_previous = sum(int(prev[i]) for i in range(last_m))
    ytd_avg_sum = sum(float(avg[i]) for i in range(last_m))

    mom_cur = cur[idx_last]
    mom_cur_int = int(mom_cur) if mom_cur is not None else None
    mom_prev = int(prev[idx_last])

    return {
        "schema": "focos_incendio_br_v2",
        "reference_date": reference_date.isoformat(),
        "metadata": {
            "latest_year": ly,
            "previous_year": meta.get("previous_year"),
            "avg_window_years_from": meta.get("avg_window_years_from"),
            "avg_window_years_to": meta.get("avg_window_years_to"),
            "last_closed_month": last_m,
            "published_at_label": meta.get("published_at_label"),
            "source": meta.get("source"),
            "escopo": (
                "Somente meses civis já encerrados; comparações mês a mês e acumulado "
                "no mesmo recorte (sem o mês civil em curso)."
            ),
        },
        "metrics": {
            "ultimo_mes_fechado_vs_mesmo_mes_ano_anterior": {
                "mes_rotulo": month_labels[idx_last],
                "focos_ano_atual": mom_cur_int,
                "focos_ano_anterior": mom_prev,
                "variacao_percentual_vs_ano_anterior": (
                    None
                    if mom_cur_int is None
                    else _pct_delta(float(mom_cur_int), float(mom_prev))
                ),
            },
            "acumulado_desde_jan_ate_ultimo_mes_fechado": {
                "ultimo_mes_incluso_rotulo": month_labels[idx_last],
                "soma_focos_ano_atual": ytd_current,
                "soma_focos_ano_anterior_mesmo_periodo": ytd_previous,
                "soma_medias_mensais_5_anos_acumulada": round(ytd_avg_sum, 2),
                "variacao_pct_acumulado_vs_ano_anterior": _pct_delta(
                    float(ytd_current), float(ytd_previous)
                ),
                "variacao_pct_acumulado_vs_media_5y_acumulada": _pct_delta(
                    float(ytd_current), ytd_avg_sum
                ),
            },
        },
    }


def payload_to_prompt_block(payload: dict[str, Any]) -> str:
    """Serialização legível para user prompt (pt-BR labels no JSON)."""
    return json.dumps(payload, ensure_ascii=False, indent=2)
