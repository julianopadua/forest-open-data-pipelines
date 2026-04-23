# src/forest_pipelines/datasets/noticias_agricolas/sync.py
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from forest_pipelines.datasets.noticias_agricolas.article_parser import (
    extract_source_article_id,
    parse_article_html,
)
from forest_pipelines.datasets.noticias_agricolas.http_client import ResilientHttpClient
from forest_pipelines.datasets.noticias_agricolas.list_parser import parse_category_list_html
from forest_pipelines.datasets.noticias_agricolas.merge import merge_listings_by_url, normalize_url_key
from forest_pipelines.datasets.noticias_agricolas.models import CategoryConfig, MergedListing
from forest_pipelines.datasets.noticias_agricolas.text_cleanup import first_useful_paragraph
from forest_pipelines.datasets.noticias_agricolas.validation import validate_feed_for_stable_publish


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    base_url: str
    source_key: str
    source_display_name: str
    categories: tuple[CategoryConfig, ...]
    items_per_category: int
    http_timeout_s: float
    http_max_retries: int
    request_delay_s: float
    max_workers: int
    min_items_for_stable_publish: int
    user_agent: str


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cats_raw = raw.get("categories") or []
    categories: list[CategoryConfig] = []
    for c in cats_raw:
        categories.append(
            CategoryConfig(
                slug=str(c["slug"]).strip(),
                label=str(c["label"]).strip(),
                url=str(c["url"]).strip(),
            )
        )

    return DatasetCfg(
        id=str(raw.get("id", dataset_id)),
        title=str(raw.get("title", dataset_id)),
        source_dataset_url=str(raw.get("source_dataset_url", "https://www.noticiasagricolas.com.br/")),
        bucket_prefix=str(raw["bucket_prefix"]),
        base_url=str(raw.get("base_url", "https://www.noticiasagricolas.com.br")).rstrip("/"),
        source_key=str(raw.get("source_key", "noticias_agricolas")),
        source_display_name=str(raw.get("source_name", "Notícias Agrícolas")),
        categories=tuple(categories),
        items_per_category=int(raw.get("items_per_category", 5)),
        http_timeout_s=float(raw.get("http_timeout_s", 30)),
        http_max_retries=int(raw.get("http_max_retries", 4)),
        request_delay_s=float(raw.get("request_delay_s", 0.35)),
        max_workers=min(5, max(1, int(raw.get("max_workers", 4)))),
        min_items_for_stable_publish=int(raw.get("min_items_for_stable_publish", 5)),
        user_agent=str(
            raw.get(
                "user_agent",
                "Mozilla/5.0 (compatible; forest-open-data-pipelines/0.1; news ingest)",
            )
        ),
    )


def _build_item_dict(
    *,
    cfg: DatasetCfg,
    merged: MergedListing,
    article_title: str,
    lead: str | None,
    excerpt: str,
    content_text: str,
    tags: list[str],
    image_url: str | None,
    published_at: str,
    scraped_at: str,
) -> dict[str, Any]:
    aid = extract_source_article_id(merged.url)
    return {
        "source": cfg.source_key,
        "source_name": cfg.source_display_name,
        "source_article_id": aid or "",
        "title": article_title,
        "url": merged.url,
        "category_slug": merged.primary_slug,
        "category_label": merged.primary_label,
        "categories": list(merged.category_slugs),
        "published_at": published_at,
        "lead": lead or "",
        "excerpt": excerpt,
        "content_text": content_text,
        "image_url": image_url,
        "tags": tags,
        "scraped_at": scraped_at,
        "rank_within_category": merged.rank_within_category,
    }


def _excerpt_from(lead: str | None, content_text: str) -> str:
    if lead and lead.strip():
        return lead.strip()
    fp = first_useful_paragraph(content_text)
    return fp or ""


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    _ = latest_months
    cfg = load_dataset_cfg(settings.datasets_dir, "noticias_agricolas_news")
    client = ResilientHttpClient(
        logger,
        timeout_s=cfg.http_timeout_s,
        max_retries=cfg.http_max_retries,
        delay_s=cfg.request_delay_s,
        user_agent=cfg.user_agent,
    )

    category_order = [c.slug for c in cfg.categories]
    list_rows: list[Any] = []
    failed_categories: list[str] = []

    for cat in cfg.categories:
        try:
            logger.info("Listando categoria %s (%s)", cat.slug, cat.url)
            html = client.get_text(cat.url)
            parsed = parse_category_list_html(
                html,
                category_slug=cat.slug,
                category_label=cat.label,
                base_url=cfg.base_url,
                limit=cfg.items_per_category,
            )
            logger.info("Categoria %s: %s itens na listagem", cat.slug, len(parsed))
            list_rows.extend(parsed)
        except Exception as e:  # noqa: BLE001
            failed_categories.append(cat.slug)
            logger.error("Falha ao listar categoria %s: %s", cat.slug, e)

    if failed_categories and len(failed_categories) == len(cfg.categories):
        raise RuntimeError(
            "Todas as categorias falharam na listagem; manifest estável não será atualizado."
        )

    merged_map = merge_listings_by_url(list_rows, category_order)
    logger.info("URLs únicas após deduplicação: %s", len(merged_map))

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def fetch_article(m: MergedListing) -> tuple[str, dict[str, Any] | None, str | None]:
        try:
            html = client.get_text(m.url)
            art = parse_article_html(
                html,
                url=m.url,
                base_url=cfg.base_url,
                listing_date_ddmmyyyy=m.listing_date_ddmmyyyy,
                listing_time_hhmm=m.listing_time_hhmm,
            )
            title_f = (art.title or "").strip() or (m.title_from_listing or "").strip()
            excerpt = _excerpt_from(art.lead, art.content_text)
            pub = art.published_at_iso
            if not pub:
                logger.warning("Sem published_at para %s; item descartado.", m.url)
                return (normalize_url_key(m.url), None, None)
            item = _build_item_dict(
                cfg=cfg,
                merged=m,
                article_title=title_f,
                lead=art.lead,
                excerpt=excerpt,
                content_text=art.content_text,
                tags=art.tags,
                image_url=art.image_url,
                published_at=pub,
                scraped_at=generated_at,
            )
            return (normalize_url_key(m.url), item, None)
        except Exception as e:  # noqa: BLE001
            logger.warning("Falha ao obter artigo %s: %s", m.url, e)
            return (normalize_url_key(m.url), None, str(e))

    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
        futs = {pool.submit(fetch_article, m): m for m in merged_map.values()}
        for fut in as_completed(futs):
            _key, item, _err = fut.result()
            if item:
                items.append(item)

    # Ordenar por published_at decrescente
    def sort_key(it: dict[str, Any]) -> str:
        return it.get("published_at") or ""

    items.sort(key=sort_key, reverse=True)

    blockers = validate_feed_for_stable_publish(items, min_items=cfg.min_items_for_stable_publish)
    if blockers:
        msg = "; ".join(blockers)
        logger.error("Validação falhou; manifest estável não será atualizado: %s", msg)
        raise RuntimeError(f"Validação do feed: {msg}")

    manifest_body: dict[str, Any] = {
        "schema_version": "1.0",
        "dataset_id": cfg.id,
        "title": cfg.title,
        "source_dataset_url": cfg.source_dataset_url,
        "bucket_prefix": cfg.bucket_prefix,
        "generated_at": generated_at,
        "generation_status": "success",
        "warnings": [],
        "items": items,
        "meta": {
            "source_agency": cfg.source_display_name,
            "custom_tags": {
                "source_key": cfg.source_key,
                "item_count": len(items),
                "categories_monitored": [
                    {"slug": c.slug, "label": c.label} for c in cfg.categories
                ],
            },
        },
    }

    raw_bytes = json.dumps(manifest_body, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        json.loads(raw_bytes.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError("JSON inválido após serialização") from e

    prefix = cfg.bucket_prefix.rstrip("/")
    now = datetime.now(timezone.utc)
    snap_dir = f"{prefix}/snapshots/{now:%Y}/{now:%m}/{now:%d}"
    snap_name = f"{now:%Y%m%dT%H%M%SZ}.json"
    snapshot_path = f"{snap_dir}/{snap_name}"

    logger.info("Enviando snapshot versionado: %s", snapshot_path)
    storage.upload_bytes(
        object_path=snapshot_path,
        data=raw_bytes,
        content_type="application/json",
        upsert=True,
    )

    stable_path = f"{prefix}/manifest.json"
    logger.info("Atualizando manifest estável: %s", stable_path)
    storage.upload_bytes(
        object_path=stable_path,
        data=raw_bytes,
        content_type="application/json",
        upsert=True,
    )

    logger.info("Manifest público: %s", storage.public_url(stable_path))

    return {**manifest_body, "_cli_skip_manifest_upload": True}
