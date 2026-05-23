from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

import requests
import yaml

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import ProfileOptions, profiled_item, profile_source_url

CKAN_SHOW_TMPL = "https://dados.cvm.gov.br/api/3/action/package_show?id={package_id}"
ALLOWED_NETLOC = "dados.cvm.gov.br"
METADATA_HINT_RE = re.compile(r"(meta|metadado|dicionario|dicionário|layout|readme)", re.IGNORECASE)
DEFAULT_PERIOD_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?:[-_]?([01]\d))?(?!\d)")


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    ckan_package_id: str
    source_dataset_url: str
    bucket_prefix: str
    source_agency: str = "CVM - Comissão de Valores Mobiliários"
    notes: str = "Indexação URL-only via API CKAN da CVM. Downloads apontam para URLs oficiais em dados.cvm.gov.br."
    include_meta: bool = True
    filename_include: tuple[str, ...] = field(default_factory=tuple)
    filename_exclude: tuple[str, ...] = field(default_factory=tuple)
    period_regex: str | None = None
    latest_items: int | None = None
    latest_months: int | None = None
    max_items: int | None = None
    profile_timeout_s: int = 180
    max_archive_members: int = 8


def make_sync(dataset_id: str) -> Callable[..., dict[str, Any]]:
    def sync(
        settings: Any,
        storage: Any,
        logger: Any,
        latest_months: int | None = None,
    ) -> dict[str, Any]:
        return sync_dataset(
            dataset_id=dataset_id,
            settings=settings,
            storage=storage,
            logger=logger,
            latest_months=latest_months,
        )

    return sync


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / "cvm" / f"{dataset_id.removeprefix('cvm_')}.yml"
    if not path.exists():
        path = datasets_dir / f"{dataset_id}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Config CVM não encontrada para {dataset_id}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    ds_id = str(raw.get("id") or dataset_id)
    title = str(raw.get("title") or ds_id)
    package_id = str(raw.get("ckan_package_id") or raw.get("dataset_slug") or "").strip()
    source_dataset_url = str(raw.get("source_dataset_url") or raw.get("source_url") or "").strip()
    if not source_dataset_url and package_id:
        source_dataset_url = f"https://dados.cvm.gov.br/dataset/{package_id}"
    if not package_id:
        package_id = source_dataset_url.rstrip("/").split("/")[-1]
    if not package_id or not source_dataset_url:
        raise ValueError(f"Config CVM inválida: faltam ckan_package_id/source_url em {path}")

    bucket_prefix = str(raw.get("bucket_prefix") or "").strip()
    if not bucket_prefix:
        raise ValueError(f"Config CVM inválida: falta bucket_prefix em {path}")

    return DatasetCfg(
        id=ds_id,
        title=title,
        ckan_package_id=package_id,
        source_dataset_url=source_dataset_url,
        bucket_prefix=bucket_prefix,
        source_agency=str(raw.get("source_agency") or "CVM - Comissão de Valores Mobiliários"),
        notes=str(
            raw.get("notes")
            or "Indexação URL-only via API CKAN da CVM. Downloads apontam para URLs oficiais em dados.cvm.gov.br."
        ),
        include_meta=bool(raw.get("include_meta", True)),
        filename_include=tuple(str(v) for v in raw.get("filename_include") or ()),
        filename_exclude=tuple(str(v) for v in raw.get("filename_exclude") or ()),
        period_regex=str(raw["period_regex"]) if raw.get("period_regex") else None,
        latest_items=_optional_int(raw.get("latest_items")),
        latest_months=_optional_int(raw.get("latest_months")),
        max_items=_optional_int(raw.get("max_items")),
        profile_timeout_s=int(raw.get("profile_timeout_s") or 180),
        max_archive_members=int(raw.get("max_archive_members") or 8),
    )


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def fetch_ckan_package(package_id: str, timeout: int = 60) -> dict[str, Any]:
    response = requests.get(CKAN_SHOW_TMPL.format(package_id=package_id), timeout=timeout)
    response.raise_for_status()
    envelope = response.json()
    if not envelope.get("success"):
        raise RuntimeError(f"CKAN package_show CVM falhou: {envelope!r}")
    result = envelope.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("CKAN package_show CVM retornou result inesperado")
    return result


def is_allowed_download_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower() == ALLOWED_NETLOC


def filename_from_resource(resource: dict[str, Any]) -> str:
    url = str(resource.get("url") or "")
    name = Path(unquote(urlparse(url).path)).name
    return name or str(resource.get("name") or "download")


def _matches_any(patterns: tuple[str, ...], text: str) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def resource_allowed(resource: dict[str, Any], cfg: DatasetCfg) -> bool:
    url = str(resource.get("url") or "").strip()
    if not url or not is_allowed_download_url(url):
        return False
    target = " ".join(
        [
            filename_from_resource(resource),
            str(resource.get("name") or ""),
            str(resource.get("description") or ""),
            str(resource.get("format") or ""),
        ]
    )
    if cfg.filename_include and not _matches_any(cfg.filename_include, target):
        return False
    if cfg.filename_exclude and _matches_any(cfg.filename_exclude, target):
        return False
    return True


def is_metadata_resource(resource: dict[str, Any]) -> bool:
    target = " ".join(
        [
            filename_from_resource(resource),
            str(resource.get("name") or ""),
            str(resource.get("description") or ""),
        ]
    )
    return bool(METADATA_HINT_RE.search(target))


def period_from_resource(resource: dict[str, Any], cfg: DatasetCfg) -> str:
    target = " ".join([filename_from_resource(resource), str(resource.get("name") or "")])
    if cfg.period_regex:
        match = re.search(cfg.period_regex, target)
        if match:
            groups = match.groups()
            if len(groups) >= 2 and groups[1]:
                return f"{groups[0]}-{groups[1]}"
            return groups[0]
    match = DEFAULT_PERIOD_RE.search(target)
    if match:
        year, month = match.groups()
        return f"{year}-{month}" if month else year
    modified = str(resource.get("last_modified") or resource.get("created") or "").strip()
    if len(modified) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", modified):
        return modified[:10]
    return "current"


def select_resources(resources: list[dict[str, Any]], cfg: DatasetCfg, latest_months: int | None) -> list[dict[str, Any]]:
    allowed = [resource for resource in resources if resource_allowed(resource, cfg)]
    allowed.sort(key=lambda resource: (period_from_resource(resource, cfg), filename_from_resource(resource)), reverse=True)
    limit = latest_months or cfg.latest_months or cfg.latest_items or cfg.max_items
    if limit:
        allowed = allowed[:limit]
    return allowed


def build_metadata_file(
    resource: dict[str, Any] | None,
    *,
    logger: Any,
    options: ProfileOptions,
) -> dict[str, Any] | None:
    if resource is None:
        return None
    url = str(resource.get("url") or "").strip()
    filename = filename_from_resource(resource)
    return {
        "filename": filename,
        "source_url": url,
        **profile_source_url(url, filename=filename, logger=logger, options=options),
    }


def build_items(
    resources: list[dict[str, Any]],
    *,
    skip_urls: set[str],
    cfg: DatasetCfg,
    logger: Any,
    options: ProfileOptions,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for resource in resources:
        url = str(resource.get("url") or "").strip()
        if url in skip_urls:
            continue
        filename = filename_from_resource(resource)
        title = str(resource.get("name") or filename).strip()
        kind = "meta" if is_metadata_resource(resource) else "data"
        items.append(
            profiled_item(
                source_url=url,
                filename=filename,
                period=period_from_resource(resource, cfg),
                title=title,
                kind=kind,
                logger=logger,
                options=options,
            )
        )
    items.sort(key=lambda item: (str(item.get("period") or ""), str(item.get("filename") or "")), reverse=True)
    return items


def sync_dataset(
    *,
    dataset_id: str,
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, dataset_id)
    logger.info("CKAN package_show CVM: %s", cfg.ckan_package_id)
    package = fetch_ckan_package(cfg.ckan_package_id)
    resources_raw = package.get("resources") or []
    if not isinstance(resources_raw, list):
        raise RuntimeError("resources CKAN CVM inesperado")
    resources = [resource for resource in resources_raw if isinstance(resource, dict)]
    selected = select_resources(resources, cfg, latest_months)

    if not selected:
        raise RuntimeError(f"Nenhum recurso publico CVM encontrado para {cfg.id}")

    options = ProfileOptions(
        timeout_s=cfg.profile_timeout_s,
        max_archive_members=cfg.max_archive_members,
    )
    metadata_resource = next((resource for resource in selected if cfg.include_meta and is_metadata_resource(resource)), None)
    metadata_file = build_metadata_file(metadata_resource, logger=logger, options=options)
    skip_urls = {str(metadata_resource.get("url"))} if metadata_resource else set()
    items = build_items(selected, skip_urls=skip_urls, cfg=cfg, logger=logger, options=options)

    warnings: list[str] = []
    omitted = len(resources) - len(selected)
    if omitted > 0:
        warnings.append(f"{omitted} recurso(s) CKAN fora dos filtros do dataset foram omitidos do manifest.")

    meta: dict[str, Any] = {
        "source_agency": cfg.source_agency,
        "notes": cfg.notes,
        "custom_tags": {
            "ckan_package_id": cfg.ckan_package_id,
            "ckan_resource_count": len(resources),
            "indexed_resource_count": len(items) + (1 if metadata_file else 0),
        },
    }
    if metadata_file:
        meta["metadata_file"] = metadata_file
    modified = package.get("metadata_modified")
    if isinstance(modified, str) and len(modified) >= 10:
        meta["release"] = {"last_release_iso": modified[:10] + "T00:00:00Z"}

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_dataset_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta,
        warnings=warnings,
    )
