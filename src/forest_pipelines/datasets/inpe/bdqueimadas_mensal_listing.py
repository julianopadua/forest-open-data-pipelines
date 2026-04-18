"""Listagem e download dos arquivos mensais Brasil (dataserver INPE COIDS)."""

from __future__ import annotations

import re
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
) -> list[tuple[int, Path]]:
    """
    Garante arquivos locais para cada mês disponível do ano civil.
    Retorna lista (mês 1-12, caminho local) ordenada por mês.
    """
    items = filter_by_calendar_year(extract_mensal_links(base_url), year)
    if not items:
        raise FileNotFoundError(
            f"Nenhum focos_mensal_br_YYYYMM para o ano {year} em {base_url}"
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    out: list[tuple[int, Path]] = []

    for yyyymm, filename, url in items:
        month = yyyymm_to_month(yyyymm)
        local = cache_dir / filename
        if local.exists():
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
