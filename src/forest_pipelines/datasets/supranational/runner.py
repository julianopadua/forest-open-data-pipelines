from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

import requests
import yaml

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import ProfileOptions, now_iso, profiled_item, warning

SUPRANATIONAL_DATASET_IDS: tuple[str, ...] = (
    "world_bank_wdi_bulk",
    "un_wpp_2024_bulk_csv",
    "jrc_ghsl_smod_r2023a",
    "jrc_ghsl_pop_r2023a",
    "jrc_gsw_v2021_aggregated",
    "nasa_power_ag_daily_reference_points",
    "oecd_sdd_stes_cli",
    "energydata_offshore_wind_technical_potential",
    "energydata_brazil_road_network",
    "faostat_qcl",
    "faostat_tcl",
    "faostat_rl",
    "faostat_lc",
    "faostat_fo",
    "faostat_em",
    "faostat_ei",
    "faostat_be",
    "ilo_ilostat_indicator_bulk_rds",
    "unesco_uis_bdds_education",
    "who_gho_air_pollution_pm25",
)

FILE_SUFFIXES = {
    ".csv",
    ".csv.gz",
    ".gz",
    ".geojson",
    ".json",
    ".kml",
    ".rds",
    ".tif",
    ".tiff",
    ".xls",
    ".xlsx",
    ".zip",
}
FILE_FORMATS = {"CSV", "GEOJSON", "JSON", "KML", "RDS", "TIF", "TIFF", "XLS", "XLSX", "ZIP"}
USER_AGENT = "ForestOpenDataDiscovery/1.0 (+https://institutoforest.org)"
BLOCKED_URL_MARKERS = (
    "datastore_search",
    "/api/3/action/datastore",
    "/vis?",
    "preview",
    "map_key",
    "token=",
    "signature=",
)


@dataclass(frozen=True)
class ResourceCfg:
    title: str
    source_url: str
    filename: str
    period: str = "Atual"
    source_page_url: str | None = None
    profile_mode: str | None = None
    format: str | None = None


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    protocol: str
    source_dataset_url: str
    bucket_prefix: str
    source_agency: str
    notes: str
    allowed_hosts: tuple[str, ...]
    accepted_license_ids: tuple[str, ...] = field(default_factory=tuple)
    source_license_url: str | None = None
    source_terms_url: str | None = None
    geographic_scope: str | None = None
    temporal_granularity: str | None = None
    dataset_version: str | None = None
    profile_mode: str = "headers"
    profile_timeout_s: int = 180
    max_archive_members: int = 8
    resources: tuple[ResourceCfg, ...] = field(default_factory=tuple)
    ckan_api_url: str | None = None
    ckan_package_id: str | None = None
    resource_include: tuple[str, ...] = field(default_factory=tuple)
    resource_exclude: tuple[str, ...] = field(default_factory=tuple)
    faostat_catalog_url: str | None = None
    faostat_dataset_code: str | None = None


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


def sync_dataset(
    *,
    dataset_id: str,
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, dataset_id)
    if cfg.protocol in {"static_files", "get_api", "rds_bulk"}:
        items = [_item_from_resource(cfg, resource, logger) for resource in cfg.resources]
        warnings: list[str] = []
    elif cfg.protocol == "ckan_files":
        items, warnings = _items_from_ckan(cfg, logger)
    elif cfg.protocol == "bulk_catalog":
        items, warnings = _items_from_faostat_catalog(cfg, logger)
    else:
        raise ValueError(f"Unsupported supranational protocol: {cfg.protocol}")

    meta = {
        "source_agency": cfg.source_agency,
        "notes": cfg.notes,
        "custom_tags": _custom_tags(cfg),
    }
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_dataset_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta,
        warnings=warnings,
    )


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / "supranational" / f"{dataset_id}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Config supranational not found for {dataset_id}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    resources = tuple(
        ResourceCfg(
            title=str(resource.get("title") or resource.get("filename") or ""),
            source_url=str(resource.get("source_url") or ""),
            filename=str(resource.get("filename") or filename_from_url(str(resource.get("source_url") or ""))),
            period=str(resource.get("period") or "Atual"),
            source_page_url=resource.get("source_page_url"),
            profile_mode=resource.get("profile_mode"),
            format=resource.get("format"),
        )
        for resource in raw.get("resources") or ()
    )
    cfg = DatasetCfg(
        id=str(raw.get("id") or dataset_id),
        title=str(raw.get("title") or dataset_id),
        protocol=str(raw.get("protocol") or "static_files"),
        source_dataset_url=str(raw.get("source_dataset_url") or raw.get("source_url") or ""),
        bucket_prefix=str(raw.get("bucket_prefix") or ""),
        source_agency=str(raw.get("source_agency") or ""),
        notes=str(raw.get("notes") or ""),
        allowed_hosts=tuple(str(host).lower() for host in raw.get("allowed_hosts") or ()),
        accepted_license_ids=tuple(str(value) for value in raw.get("accepted_license_ids") or ()),
        source_license_url=raw.get("source_license_url"),
        source_terms_url=raw.get("source_terms_url"),
        geographic_scope=raw.get("geographic_scope"),
        temporal_granularity=raw.get("temporal_granularity"),
        dataset_version=raw.get("dataset_version"),
        profile_mode=str(raw.get("profile_mode") or "headers"),
        profile_timeout_s=int(raw.get("profile_timeout_s") or 180),
        max_archive_members=int(raw.get("max_archive_members") or 8),
        resources=resources,
        ckan_api_url=raw.get("ckan_api_url"),
        ckan_package_id=raw.get("ckan_package_id"),
        resource_include=tuple(str(value) for value in raw.get("resource_include") or ()),
        resource_exclude=tuple(str(value) for value in raw.get("resource_exclude") or ()),
        faostat_catalog_url=raw.get("faostat_catalog_url"),
        faostat_dataset_code=raw.get("faostat_dataset_code"),
    )
    _validate_cfg(cfg)
    return cfg


def _validate_cfg(cfg: DatasetCfg) -> None:
    if cfg.id not in SUPRANATIONAL_DATASET_IDS:
        raise ValueError(f"Unsupported supranational dataset id: {cfg.id}")
    if not cfg.source_dataset_url or not cfg.bucket_prefix:
        raise ValueError(f"Invalid config for {cfg.id}: missing source_dataset_url or bucket_prefix")
    if not cfg.allowed_hosts:
        raise ValueError(f"Invalid config for {cfg.id}: missing allowed_hosts")
    _assert_allowed_url(cfg.source_dataset_url, cfg.allowed_hosts, allow_landing=True)
    for resource in cfg.resources:
        _assert_allowed_url(
            resource.source_url,
            cfg.allowed_hosts,
            allow_api=cfg.protocol == "get_api",
            allow_download_endpoint=cfg.protocol == "ckan_files",
        )
        if resource.source_page_url:
            _assert_allowed_url(resource.source_page_url, cfg.allowed_hosts, allow_landing=True)


def _custom_tags(cfg: DatasetCfg) -> dict[str, Any]:
    tags: dict[str, Any] = {
        "provider_family": "supranational",
        "access_protocol": cfg.protocol,
        "allowed_hosts": list(cfg.allowed_hosts),
    }
    optional = {
        "upstream_dataset_id": cfg.ckan_package_id or cfg.faostat_dataset_code,
        "source_license_url": cfg.source_license_url,
        "source_terms_url": cfg.source_terms_url,
        "geographic_scope": cfg.geographic_scope,
        "temporal_granularity": cfg.temporal_granularity,
        "dataset_version": cfg.dataset_version,
    }
    tags.update({key: value for key, value in optional.items() if value})
    if cfg.accepted_license_ids:
        tags["accepted_license_ids"] = list(cfg.accepted_license_ids)
    return tags


def _item_from_resource(cfg: DatasetCfg, resource: ResourceCfg, logger: Any) -> dict[str, Any]:
    _assert_allowed_url(
        resource.source_url,
        cfg.allowed_hosts,
        allow_api=cfg.protocol == "get_api",
        allow_download_endpoint=cfg.protocol == "ckan_files",
    )
    mode = resource.profile_mode or cfg.profile_mode
    base = {
        "source_url": resource.source_url,
        "filename": resource.filename,
        "period": resource.period,
        "title": resource.title,
        "extra": {"source_page_url": resource.source_page_url} if resource.source_page_url else None,
    }
    if mode == "download":
        return profiled_item(
            **base,
            logger=logger,
            options=ProfileOptions(
                timeout_s=cfg.profile_timeout_s,
                max_archive_members=cfg.max_archive_members,
            ),
        )
    item = {
        "kind": "data",
        "period": resource.period,
        "filename": resource.filename,
        "source_url": resource.source_url,
        "title": resource.title,
        **({"source_page_url": resource.source_page_url} if resource.source_page_url else {}),
        **_profile_headers(resource.source_url, resource.filename, mode=mode),
    }
    if resource.format and not item.get("format"):
        item["format"] = resource.format
    return item


def _profile_headers(source_url: str, filename: str, *, mode: str = "headers") -> dict[str, Any]:
    profile = {
        "format": _format_from_filename(filename),
        "profiled_at": now_iso(),
        "profile_status": "skipped",
        "profile_warnings": [
            warning(
                "profiling_skipped",
                "Content profiling was skipped; only source URL metadata was indexed.",
            )
        ],
    }
    if mode == "skip":
        return profile
    try:
        response = requests.head(
            source_url,
            allow_redirects=True,
            timeout=30,
            headers={"User-Agent": USER_AGENT},
        )
        response.raise_for_status()
    except Exception as exc:
        return {
            **profile,
            "profile_status": "failed",
            "profile_warnings": [
                warning("head_request_failed", f"Source URL metadata check failed: {type(exc).__name__}.")
            ],
        }
    size = response.headers.get("Content-Length")
    if size and str(size).isdigit():
        profile["size_bytes"] = int(str(size))
    profile["content_type"] = response.headers.get("Content-Type")
    profile["last_modified"] = response.headers.get("Last-Modified")
    return profile


def _items_from_ckan(cfg: DatasetCfg, logger: Any) -> tuple[list[dict[str, Any]], list[str]]:
    package = _fetch_json(str(cfg.ckan_api_url))
    if not package.get("success"):
        raise RuntimeError(f"CKAN package_show failed for {cfg.id}")
    result = package.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"CKAN package_show returned invalid result for {cfg.id}")
    _validate_ckan_package(cfg, result)
    resources = [res for res in result.get("resources") or () if _ckan_resource_allowed(cfg, res)]
    items = [
        _item_from_resource(
            cfg,
            ResourceCfg(
                title=str(resource.get("name") or filename_from_url(str(resource.get("url") or ""))),
                source_url=str(resource.get("url") or ""),
                filename=filename_from_url(str(resource.get("url") or "")),
                period=_period_from_text(" ".join([str(resource.get("name") or ""), str(resource.get("url") or "")])),
                source_page_url=cfg.source_dataset_url,
                profile_mode=cfg.profile_mode,
                format=str(resource.get("format") or "").lower() or None,
            ),
            logger,
        )
        for resource in resources
    ]
    if not items:
        raise RuntimeError(f"No accepted CKAN resources for {cfg.id}")
    omitted = len(result.get("resources") or []) - len(items)
    warnings = [f"{omitted} CKAN resource(s) were omitted by URL, license, or format filters."] if omitted else []
    return items, warnings


def _items_from_faostat_catalog(cfg: DatasetCfg, logger: Any) -> tuple[list[dict[str, Any]], list[str]]:
    if not cfg.faostat_catalog_url or not cfg.faostat_dataset_code:
        raise ValueError(f"FAOSTAT config incomplete for {cfg.id}")
    _assert_allowed_url(cfg.faostat_catalog_url, cfg.allowed_hosts)
    xml_text = _fetch_text(cfg.faostat_catalog_url)
    file_url = _faostat_file_location(xml_text, cfg.faostat_dataset_code)
    _assert_allowed_url(file_url, cfg.allowed_hosts)
    item = _item_from_resource(
        cfg,
        ResourceCfg(
            title=cfg.title,
            source_url=file_url,
            filename=filename_from_url(file_url),
            period=cfg.dataset_version or "Atual",
            source_page_url=cfg.source_dataset_url,
            profile_mode=cfg.profile_mode,
        ),
        logger,
    )
    return [item], []


def _validate_ckan_package(cfg: DatasetCfg, package: dict[str, Any]) -> None:
    if package.get("private") is True or str(package.get("state") or "").lower() not in {"", "active"}:
        raise RuntimeError(f"CKAN package is not public and active for {cfg.id}")
    if package.get("isopen") is not True:
        raise RuntimeError(f"CKAN package is not open for {cfg.id}")
    license_id = str(package.get("license_id") or "")
    if cfg.accepted_license_ids and license_id not in cfg.accepted_license_ids:
        raise RuntimeError(f"CKAN package license not accepted for {cfg.id}: {license_id}")


def _ckan_resource_allowed(cfg: DatasetCfg, resource: Any) -> bool:
    if not isinstance(resource, dict):
        return False
    if str(resource.get("state") or "active").lower() != "active":
        return False
    url = str(resource.get("url") or "").strip()
    if not _url_allowed(url, cfg.allowed_hosts, allow_download_endpoint=True):
        return False
    fmt = str(resource.get("format") or "").strip().upper()
    if not (_download_suffix_allowed(url) or ("/download/" in url.lower() and fmt in FILE_FORMATS)):
        return False
    target = " ".join(
        [
            filename_from_url(url),
            str(resource.get("name") or ""),
            str(resource.get("description") or ""),
            str(resource.get("format") or ""),
        ]
    )
    if cfg.resource_include and not _matches_any(cfg.resource_include, target):
        return False
    if cfg.resource_exclude and _matches_any(cfg.resource_exclude, target):
        return False
    return True


def _faostat_file_location(xml_text: str, dataset_code: str) -> str:
    root = ET.fromstring(xml_text)
    wanted = dataset_code.strip().upper()
    for node in root.iter():
        fields = {_strip_ns(child.tag): (child.text or "").strip() for child in list(node)}
        code = fields.get("DatasetCode") or fields.get("Code")
        file_location = fields.get("FileLocation") or fields.get("fileLocation")
        if code and code.strip().upper() == wanted and file_location:
            return file_location
    raise RuntimeError(f"FAOSTAT FileLocation not found for {dataset_code}")


def _fetch_json(url: str) -> dict[str, Any]:
    response = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected JSON object from {url}")
    return data


def _fetch_text(url: str) -> str:
    response = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def _assert_allowed_url(
    url: str,
    allowed_hosts: tuple[str, ...],
    *,
    allow_landing: bool = False,
    allow_api: bool = False,
    allow_download_endpoint: bool = False,
) -> None:
    if not _url_allowed(
        url,
        allowed_hosts,
        allow_landing=allow_landing,
        allow_api=allow_api,
        allow_download_endpoint=allow_download_endpoint,
    ):
        raise ValueError(f"URL is not accepted by supranational URL-only policy: {url}")


def _url_allowed(
    url: str,
    allowed_hosts: tuple[str, ...],
    *,
    allow_landing: bool = False,
    allow_api: bool = False,
    allow_download_endpoint: bool = False,
) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if parsed.scheme != "https" or not host:
        return False
    if host not in allowed_hosts:
        return False
    lowered = url.lower()
    if any(marker in lowered for marker in BLOCKED_URL_MARKERS):
        return False
    if allow_api and (parsed.query or "/api/" in parsed.path.lower()):
        return True
    if allow_download_endpoint and "/download/" in parsed.path.lower():
        return True
    return allow_landing or _download_suffix_allowed(url)


def _download_suffix_allowed(url: str) -> bool:
    path = unquote(urlparse(url).path).lower()
    return any(path.endswith(suffix) for suffix in FILE_SUFFIXES)


def _matches_any(patterns: tuple[str, ...], text: str) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def _period_from_text(value: str) -> str:
    match = re.search(r"(?<!\d)((?:19|20)\d{2})(?:[-_]?([01]\d))?(?!\d)", value)
    if not match:
        return "Atual"
    year, month = match.groups()
    return f"{year}-{month}" if month else year


def _format_from_filename(filename: str) -> str:
    if filename.lower().endswith(".csv.gz"):
        return "csv.gz"
    suffix = Path(filename).suffix.lower().lstrip(".")
    return suffix or "unknown"


def filename_from_url(url: str, fallback: str = "download") -> str:
    path = unquote(urlparse(url).path)
    name = Path(path).name
    return name or fallback


def _strip_ns(value: str) -> str:
    return value.rsplit("}", 1)[-1]
