# src/forest_pipelines/datasets/noticias_agricolas/dates.py
from __future__ import annotations

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

TZ_BR = ZoneInfo("America/Sao_Paulo")

_RE_PUBLICADO = re.compile(
    r"Publicado\s+em\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}):(\d{2})",
    re.IGNORECASE,
)


def parse_published_line(datas_text: str) -> datetime | None:
    """
    Parse 'Publicado em DD/MM/YYYY HH:MM' from div.datas inner text.
    """
    raw = " ".join(datas_text.split())
    m = _RE_PUBLICADO.search(raw)
    if not m:
        return None
    d, h, mi = m.group(1), int(m.group(2)), int(m.group(3))
    try:
        return datetime.strptime(d, "%d/%m/%Y").replace(hour=h, minute=mi, tzinfo=TZ_BR)
    except ValueError:
        return None


def combine_listing_datetime(date_ddmmyyyy: str, time_hhmm: str) -> datetime | None:
    """
    Fallback: listing h3 date + span.hora as local time in America/Sao_Paulo.
    """
    try:
        d = datetime.strptime(date_ddmmyyyy.strip(), "%d/%m/%Y")
    except ValueError:
        return None
    parts = time_hhmm.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    try:
        return d.replace(hour=h, minute=m, tzinfo=TZ_BR)
    except ValueError:
        return None


def to_iso8601_z(dt: datetime) -> str:
    """Normalize to UTC and emit ISO 8601 with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_BR)
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")
