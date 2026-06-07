"""Listagem e download dos arquivos mensais Brasil (dataserver INPE COIDS)."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from forest_pipelines.http import stream_download

RE_MENSAL = re.compile(r"focos_mensal_br_(\d{6})\.(csv|zip)$", re.IGNORECASE)

DEFAULT_MENSAL_BASE_URL = (
    "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/"
)


def extract_mensal_links(base_url: str) -> list[tuple[int, str, str]]:
    """Lista (yyyymm, filename, url absoluta) ordenada por yyyymm."""
    r = requests.get(base_url, timeout=120)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    found: dict[int, tuple[str, str]] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        filename = href.split("/")[-1]
        m = RE_MENSAL.search(filename)
        if not m:
            continue
        yyyymm = int(m.group(1))
        full = urljoin(base_url, href)
        found[yyyymm] = (filename, full)
    return sorted(((k, v[0], v[1]) for k, v in found.items()), key=lambda x: x[0])


def filter_by_calendar_year(
    items: list[tuple[int, str, str]],
    year: int,
) -> list[tuple[int, str, str]]:
    return [t for t in items if t[0] // 100 == year]


def yyyymm_to_month(yyyymm: int) -> int:
    return int(yyyymm % 100)


def ensure_mensal_files_for_year(
    *,
    base_url: str,
    year: int,
    cache_dir: Path,
    skip_download: bool,
    months: Iterable[int] | None = None,
    force_download_months: Iterable[int] | None = None,
) -> list[tuple[int, Path]]:
    """
    Garante arquivos locais para cada mês disponível do ano civil.
    Retorna lista (mês 1-12, caminho local) ordenada por mês.
    """
    month_filter = _normalize_month_filter(months)
    force_months = _normalize_month_filter(force_download_months) or set()
    items = filter_by_calendar_year(extract_mensal_links(base_url), year)
    if month_filter is not None:
        items = [item for item in items if yyyymm_to_month(item[0]) in month_filter]
    if not items:
        if month_filter == set():
            return []
        raise FileNotFoundError(
            f"Nenhum focos_mensal_br_YYYYMM para o ano {year} em {base_url}"
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    out: list[tuple[int, Path]] = []

    for yyyymm, filename, url in items:
        month = yyyymm_to_month(yyyymm)
        local = cache_dir / filename
        if local.exists() and (skip_download or month not in force_months):
            out.append((month, local))
            continue
        if skip_download:
            raise FileNotFoundError(
                f"Arquivo mensal ausente no cache: {local}. "
                "Rode sem --skip-mensal-download para baixar."
            )
        stream_download(url, local)
        out.append((month, local))

    out.sort(key=lambda x: x[0])
    return out


def _normalize_month_filter(months: Iterable[int] | None) -> set[int] | None:
    if months is None:
        return None
    normalized = {int(month) for month in months}
    invalid = [month for month in normalized if month < 1 or month > 12]
    if invalid:
        raise ValueError(f"Mês inválido em filtro mensal: {sorted(invalid)}")
    return normalized
