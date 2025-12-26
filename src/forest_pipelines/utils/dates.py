# src/forest_pipelines/utils/dates.py
from __future__ import annotations

from datetime import date


def yyyymm_to_period(yyyymm: str) -> str:
    """
    "202512" -> "2025-12"
    """
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"yyyymm inválido: {yyyymm}")
    return f"{yyyymm[:4]}-{yyyymm[4:]}"


def month_range_str(year: int, month: int) -> tuple[str, str]:
    """
    Retorna (YYYY-MM-01, YYYY-MM-last_day) como string, útil pra metadados futuros.
    """
    if not (1 <= month <= 12):
        raise ValueError("month deve estar entre 1 e 12")

    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    # end é o primeiro dia do próximo mês; "último dia" = end - 1
    last = end.toordinal() - 1
    end_date = date.fromordinal(last)
    return start.isoformat(), end_date.isoformat()
