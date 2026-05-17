# src/forest_pipelines/datasets/mma/cnuc_unidades_conservacao.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import requests
import yaml

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import profiled_item, profile_source_url

CKAN_SHOW_TMPL = "https://dados.mma.gov.br/api/3/action/package_show?id={package_id}"
ALLOWED_NETLOC = "dados.mma.gov.br"
PERIOD_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_url: str
    bucket_prefix: str
    ckan_package_id: str


def load_dataset_cfg(datasets_dir: Path) -> DatasetCfg:
    path = datasets_dir / "mma" / "cnuc_unidades_conservacao.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=raw.get("id", "mma_cnuc_unidades_conservacao"),
        title=raw.get("title", "MMA - CNUC - Unidades de Conservação"),
        source_url=raw.get(
            "source_url",
            "https://dados.mma.gov.br/dataset/unidadesdeconservacao",
        ),
        bucket_prefix=raw.get("bucket_prefix", "mma/cnuc/unidades_conservacao"),
        ckan_package_id=raw.get("ckan_package_id", "unidadesdeconservacao"),
    )


def _netloc_key(netloc: str) -> str:
    n = netloc.lower()
    return n[4:] if n.startswith("www.") else n


def is_allowed_download_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    return _netloc_key(parsed.netloc) == ALLOWED_NETLOC


def fetch_ckan_package(package_id: str, timeout: int = 60) -> dict[str, Any]:
    url = CKAN_SHOW_TMPL.format(package_id=package_id)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    envelope = response.json()
    if not envelope.get("success"):
        raise RuntimeError(f"CKAN package_show falhou: {envelope!r}")
    result = envelope.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("CKAN package_show retornou result inesperado")
    return result


def period_from_resource(resource: dict[str, Any]) -> str:
    #prefere data de atualizacao do recurso para ordenacao no portal
    for key in ("last_modified", "created"):
        raw = resource.get(key)
        if isinstance(raw, str):
            m = PERIOD_DATE_RE.match(raw.strip())
            if m:
                return m.group(1)
    url = str(resource.get("url") or "")
    fn = Path(unquote(urlparse(url).path)).name
    ym = re.search(r"(20\d{2})[-_](\d{2})", fn)
    if ym:
        return f"{ym.group(1)}-{ym.group(2)}-15"
    yonly = re.search(r"(20\d{2})", fn)
    if yonly:
        return f"{yonly.group(1)}-06-15"
    return "2018-01-01"


def pick_tabular_dictionary_pdf(resources: list[dict[str, Any]]) -> dict[str, Any] | None:
    #dicionario principal das tabelas csv (nao o do shapefile)
    for res in resources:
        fmt = str(res.get("format") or "").upper()
        name = str(res.get("name") or "")
        url = str(res.get("url") or "")
        if fmt != "PDF":
            continue
        if "shapefile" in name.lower():
            continue
        if "dicion" in name.lower() and "conserva" in name.lower():
            return res
    return None


def build_manifest_items(
    resources: list[dict[str, Any]],
    *,
    skip_urls: set[str],
    logger: Any,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for res in resources:
        url = str(res.get("url") or "").strip()
        if not url or url in skip_urls:
            continue
        if not is_allowed_download_url(url):
            continue
        filename = Path(unquote(urlparse(url).path)).name or "download"
        title = str(res.get("name") or filename).strip() or filename
        items.append(
            profiled_item(
                source_url=url,
                filename=filename,
                period=period_from_resource(res),
                title=title,
                logger=logger,
            )
        )
    #mais recente primeiro (portal ordena por periodo; iso ajuda)
    items.sort(key=lambda it: it["period"], reverse=True)
    return items


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir)
    logger.info("CKAN package_show MMA CNUC: %s", cfg.ckan_package_id)
    package = fetch_ckan_package(cfg.ckan_package_id)
    resources_raw = package.get("resources") or []
    if not isinstance(resources_raw, list):
        raise RuntimeError("resources CKAN inesperado")

    resources: list[dict[str, Any]] = [r for r in resources_raw if isinstance(r, dict)]
    warnings: list[str] = []
    allowed: list[dict[str, Any]] = []
    for res in resources:
        url = str(res.get("url") or "").strip()
        if not url:
            continue
        if not is_allowed_download_url(url):
            warnings.append(
                "Recurso fora de dados.mma.gov.br omitido do manifest: "
                + str(res.get("name") or url)[:120]
            )
            continue
        allowed.append(res)

    dict_res = pick_tabular_dictionary_pdf(allowed)
    skip_urls: set[str] = set()
    metadata_file: dict[str, Any] | None = None
    if dict_res:
        durl = str(dict_res["url"]).strip()
        dname = Path(unquote(urlparse(durl).path)).name
        skip_urls.add(durl)
        metadata_file = {
            "filename": dname,
            "source_url": durl,
            **profile_source_url(durl, filename=dname, logger=logger),
        }

    items = build_manifest_items(allowed, skip_urls=skip_urls, logger=logger)

    if not items and not metadata_file:
        raise RuntimeError("Nenhum recurso publico dados.mma.gov.br encontrado no pacote CKAN.")

    meta_release: dict[str, Any] = {}
    modified = package.get("metadata_modified")
    if isinstance(modified, str) and len(modified) >= 10:
        meta_release["last_release_iso"] = modified[:10] + "T00:00:00Z"

    status: Any = "success"
    if warnings:
        status = "success_partial_fallback"

    meta: dict[str, Any] = {
        "source_agency": "MMA - Ministério do Meio Ambiente e Mudança do Clima (CNUC)",
        "notes": "Indexação URL-only via API CKAN (package_show). Downloads diretos em dados.mma.gov.br; recursos em outros domínios ficam de fora do manifest.",
        "custom_tags": {
            "ckan_package_id": cfg.ckan_package_id,
            "indexed_resource_count": len(items),
            "gov_br_context_url": "https://www.gov.br/pt-br/servicos/obter-informacoes-sobre-as-unidades-de-conservacao-ambiental-nacionais",
        },
    }
    if metadata_file:
        meta["metadata_file"] = metadata_file
    if meta_release:
        meta["release"] = meta_release

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta,
        generation_status=status,
        warnings=warnings,
    )
