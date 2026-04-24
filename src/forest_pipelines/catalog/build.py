"""Build + publish the consolidated catalogs consumed by the portal.

The portal fetches two catalog envelopes from Supabase Storage:

- catalog/open_data_catalog.json — all visible open-data datasets (base YAML + ANP compact)
- catalog/reports_catalog.json — all visible reports

This module is the sole producer of those files.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from forest_pipelines.catalog.anp_placement import anp_id_from_slug, placement_for_dataset

CATALOG_SCHEMA_VERSION = "1.0"
DEFAULT_CATALOG_BUCKET_PREFIX = "catalog"
ANP_CATALOG_DATASET_PATH = "anp/catalog/anp_catalog_compact.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Catalog config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Catalog YAML root must be a mapping: {path}")
    return data


def _dataset_entry_from_base(raw: dict[str, Any]) -> dict[str, Any]:
    required = ("id", "category_title", "subcategory_title", "source_id", "source_title",
                "slug", "title", "description", "manifest_path", "source_url")
    missing = [k for k in required if not raw.get(k)]
    if missing:
        raise ValueError(f"Base dataset missing required keys {missing}: {raw.get('id')}")

    entry = {
        "id": raw["id"],
        "category_title": raw["category_title"],
        "subcategory_title": raw["subcategory_title"],
        "source_id": raw["source_id"],
        "source_title": raw["source_title"],
        "slug": raw["slug"],
        "title": raw["title"],
        "description": raw["description"],
        "manifest_path": raw["manifest_path"],
        "source_url": raw["source_url"],
    }
    if raw.get("segment_title"):
        entry["segment_title"] = raw["segment_title"]
    return entry


_WHITESPACE_RE = re.compile(r"\s+")


def _dataset_entry_from_anp(ds: dict[str, Any]) -> dict[str, Any] | None:
    slug = ds.get("slug")
    if not slug:
        return None
    placement = placement_for_dataset(ds)
    notes = str(ds.get("notes_plain") or "")
    description = _WHITESPACE_RE.sub(" ", notes).strip()[:800]

    entry: dict[str, Any] = {
        "id": anp_id_from_slug(slug),
        "category_title": placement["category_title"],
        "subcategory_title": placement["subcategory_title"],
        "source_id": "anp",
        "source_title": "ANP",
        "slug": slug,
        "title": ds.get("title") or slug,
        "description": description,
        "manifest_path": ANP_CATALOG_DATASET_PATH,
        "source_url": f"https://dados.gov.br/dados/conjuntos-dados/{slug}",
    }
    if placement.get("segment_title"):
        entry["segment_title"] = placement["segment_title"]
    return entry


def build_open_data_catalog(
    *,
    base_config_path: Path,
    anp_compact_path: Path | None,
    warnings_bucket: list[str],
) -> dict[str, Any]:
    """Assemble the open-data catalog envelope.

    anp_compact_path is optional — if missing, only base datasets are included
    and a warning is added. This lets catalog publishes proceed even when the
    ANP pipeline has not been run locally.
    """
    base_cfg = _load_yaml(base_config_path)
    base_list = base_cfg.get("datasets") or []
    datasets: list[dict[str, Any]] = [_dataset_entry_from_base(d) for d in base_list]

    if anp_compact_path and anp_compact_path.is_file():
        with open(anp_compact_path, "r", encoding="utf-8") as f:
            envelope = json.load(f)
        for ds in envelope.get("datasets") or []:
            entry = _dataset_entry_from_anp(ds)
            if entry:
                datasets.append(entry)
    else:
        warnings_bucket.append(
            "ANP compact catalog not found; open-data catalog omits ANP datasets."
        )

    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "catalog_id": "open_data_catalog",
        "generated_at": _now_iso(),
        "generation_status": "success_partial_fallback" if warnings_bucket else "success",
        "warnings": list(warnings_bucket),
        "datasets": datasets,
    }


def _report_entry(raw: dict[str, Any]) -> dict[str, Any]:
    required = ("id", "slug", "title", "description", "source_title",
                "category_title", "manifest_path", "stable_report_path")
    missing = [k for k in required if not raw.get(k)]
    if missing:
        raise ValueError(f"Report entry missing required keys {missing}: {raw.get('id')}")

    entry: dict[str, Any] = {
        "id": raw["id"],
        "slug": raw["slug"],
        "title": raw["title"],
        "description": str(raw["description"]).strip(),
        "source_title": raw["source_title"],
        "category_title": raw["category_title"],
        "manifest_path": raw["manifest_path"],
        "stable_report_path": raw["stable_report_path"],
        "tags": list(raw.get("tags") or []),
    }
    for optional_key in (
        "source_portal_href",
        "source_dataset_url",
        "layout",
        "hero_image_src",
        "hero_image_credit_pt",
        "hero_image_credit_en",
        "related_article_url",
        "related_article_label_pt",
        "related_article_label_en",
    ):
        if raw.get(optional_key):
            entry[optional_key] = raw[optional_key]
    return entry


def build_reports_catalog(
    *,
    reports_config_path: Path,
    warnings_bucket: list[str],
) -> dict[str, Any]:
    cfg = _load_yaml(reports_config_path)
    entries = [_report_entry(r) for r in cfg.get("reports") or []]
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "catalog_id": "reports_catalog",
        "generated_at": _now_iso(),
        "generation_status": "success_partial_fallback" if warnings_bucket else "success",
        "warnings": list(warnings_bucket),
        "reports": entries,
    }


def publish_catalogs(
    storage: Any,
    *,
    open_data_envelope: dict[str, Any],
    reports_envelope: dict[str, Any],
    bucket_prefix: str = DEFAULT_CATALOG_BUCKET_PREFIX,
    logger: Any = None,
) -> dict[str, Any]:
    """Upload both catalog envelopes to Supabase Storage under bucket_prefix."""
    prefix = bucket_prefix.strip().rstrip("/")
    open_data_path = f"{prefix}/open_data_catalog.json"
    reports_path = f"{prefix}/reports_catalog.json"

    storage.upload_bytes(
        object_path=open_data_path,
        data=_to_bytes(open_data_envelope),
        content_type="application/json; charset=utf-8",
        upsert=True,
    )
    storage.upload_bytes(
        object_path=reports_path,
        data=_to_bytes(reports_envelope),
        content_type="application/json; charset=utf-8",
        upsert=True,
    )

    result = {
        "bucket_prefix": prefix,
        "paths": {
            "open_data_catalog": open_data_path,
            "reports_catalog": reports_path,
        },
        "public_urls": {
            "open_data_catalog": storage.public_url(open_data_path),
            "reports_catalog": storage.public_url(reports_path),
        },
    }
    if logger:
        logger.info("open_data_catalog.json publicado: %s", result["public_urls"]["open_data_catalog"])
        logger.info("reports_catalog.json publicado: %s", result["public_urls"]["reports_catalog"])
    return result


def _default_base_config_path(root: Path) -> Path:
    return root / "configs" / "catalog" / "open_data.yml"


def _default_reports_config_path(root: Path) -> Path:
    return root / "configs" / "catalog" / "reports.yml"


def _default_anp_compact_path(root: Path) -> Path:
    # Project-standard location produced by `anp-compact`.
    return root / "src" / "forest_pipelines" / "dados_abertos" / "anp_catalog_compact.json"


def build_catalogs_from_defaults(
    root: Path,
    *,
    anp_compact_override: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convenience wrapper: resolve all config paths from project root."""
    open_warnings: list[str] = []
    open_envelope = build_open_data_catalog(
        base_config_path=_default_base_config_path(root),
        anp_compact_path=anp_compact_override or _default_anp_compact_path(root),
        warnings_bucket=open_warnings,
    )
    reports_warnings: list[str] = []
    reports_envelope = build_reports_catalog(
        reports_config_path=_default_reports_config_path(root),
        warnings_bucket=reports_warnings,
    )
    return open_envelope, reports_envelope


__all__ = [
    "CATALOG_SCHEMA_VERSION",
    "DEFAULT_CATALOG_BUCKET_PREFIX",
    "ANP_CATALOG_DATASET_PATH",
    "build_open_data_catalog",
    "build_reports_catalog",
    "build_catalogs_from_defaults",
    "publish_catalogs",
]

# Iterable is imported for type-checking intent; not used in runtime paths.
_ = Iterable
