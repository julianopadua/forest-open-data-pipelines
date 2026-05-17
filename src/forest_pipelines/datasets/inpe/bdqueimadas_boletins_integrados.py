# src/forest_pipelines/datasets/inpe/bdqueimadas_boletins_integrados.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
import yaml
from bs4 import BeautifulSoup

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import profiled_item

RE_YEAR_DIR = re.compile(r"^(19|20)\d{2}$")
RE_BOLETIM_PDF = re.compile(r"(?P<month>0[1-9]|1[0-2])_(?P<year>(19|20)\d{2})\.pdf$", re.IGNORECASE)


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_url: str
    bucket_prefix: str


@dataclass(frozen=True)
class BoletimResource:
    period: str
    year: str
    month: str
    filename: str
    url: str


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=raw.get("id", "inpe_bdqueimadas_boletins_integrados"),
        title=raw.get("title", "INPE - BDQueimadas - Boletins Integrados"),
        source_url=raw.get("source_url", "https://dataserver-coids.inpe.br/queimadas/queimadas/Boletins-Integrados/"),
        bucket_prefix=raw.get("bucket_prefix", "inpe/bdqueimadas/boletins_integrados"),
    )


def _filename_from_url(url: str) -> str:
    return Path(unquote(urlparse(url).path)).name


def parse_boletim_pdf_link(href: str, base_url: str) -> BoletimResource | None:
    full_url = urljoin(base_url, href)
    filename = _filename_from_url(full_url)
    match = RE_BOLETIM_PDF.search(filename)
    if not match:
        return None
    year = match.group("year")
    month = match.group("month")
    return BoletimResource(
        period=f"{year}-{month}",
        year=year,
        month=month,
        filename=filename,
        url=full_url,
    )


def _soup_from_url(url: str) -> BeautifulSoup:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    if "html" not in content_type.lower() and content_type:
        raise ValueError(f"URL não retornou HTML: {url} ({content_type})")
    return BeautifulSoup(response.text, "html.parser")


def extract_year_directory_urls(source_url: str) -> list[tuple[str, str]]:
    soup = _soup_from_url(source_url)
    found: dict[str, str] = {}
    for link in soup.find_all("a", href=True):
        href = str(link["href"]).strip()
        if href in {"../", ".."} or href.startswith(("#", "mailto:", "javascript:")):
            continue
        full_url = urljoin(source_url, href)
        segment = Path(unquote(urlparse(full_url).path.rstrip("/"))).name
        if RE_YEAR_DIR.fullmatch(segment):
            found[segment] = full_url.rstrip("/") + "/"
    return sorted(found.items(), key=lambda item: item[0], reverse=True)


def extract_pdf_urls(source_url: str) -> list[BoletimResource]:
    year_dirs = extract_year_directory_urls(source_url)
    found: dict[str, BoletimResource] = {}

    pages = [url for _, url in year_dirs] or [source_url]
    for page_url in pages:
        soup = _soup_from_url(page_url)
        for link in soup.find_all("a", href=True):
            resource = parse_boletim_pdf_link(str(link["href"]), page_url)
            if resource:
                found[resource.url] = resource

    return sorted(found.values(), key=lambda item: item.period, reverse=True)


def validate_source_urls(resources: list[BoletimResource]) -> None:
    if not resources:
        raise RuntimeError("Nenhum PDF público encontrado na URL fonte.")

    invalid = [resource.url for resource in resources if not resource.url.startswith(("http://", "https://"))]
    if invalid:
        raise RuntimeError(
            "URL fonte não é viável para catálogo URL-only; links relativos/privados encontrados: "
            + ", ".join(invalid[:3])
        )


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "inpe/bdqueimadas_boletins_integrados")

    logger.info("Explorando boletins integrados INPE: %s", cfg.source_url)
    all_resources = extract_pdf_urls(cfg.source_url)
    validate_source_urls(all_resources)
    limit = latest_months if latest_months and latest_months > 0 else len(all_resources)
    selected_resources = all_resources[:limit]

    items: list[dict[str, Any]] = []
    for resource in selected_resources:
        logger.info("Indexando URL do boletim integrado %s: %s", resource.period, resource.url)

        items.append(
            profiled_item(
                source_url=resource.url,
                filename=resource.filename,
                period=resource.period,
                title=f"Boletim integrado {resource.month}/{resource.year}",
                logger=logger,
            )
        )

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "source_agency": "INPE - Programa Queimadas",
            "notes": "Boletins integrados mensais de queimadas publicados em PDF pelo Dataserver COIDS/INPE.",
            "custom_tags": {
                "total_items": len(items),
                "start_period": items[-1]["period"] if items else None,
                "end_period": items[0]["period"] if items else None,
            },
        },
    )
