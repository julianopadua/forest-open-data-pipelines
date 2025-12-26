# src/forest_pipelines/datasets/cvm/fi_inf_diario.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from bs4 import BeautifulSoup
import requests

from forest_pipelines.http import stream_download
from forest_pipelines.manifests.build_manifest import build_manifest

RE_ZIP = re.compile(r"inf_diario_fi_(\d{6})\.zip$", re.IGNORECASE)


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    latest_months: int


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return DatasetCfg(
        id=raw["id"],
        title=raw["title"],
        source_dataset_url=raw["source_dataset_url"],
        bucket_prefix=raw["bucket_prefix"],
        latest_months=int(raw.get("latest_months", 12)),
    )


def extract_resource_urls(dataset_url: str) -> list[str]:
    # Na página do dataset, os links diretos aparecem como anchors “resource-url-analytics”
    # (é exatamente o que você colou no HTML).
    html = requests.get(dataset_url, timeout=60).text
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for a in soup.select("a.resource-url-analytics"):
        href = a.get("href")
        if href and href.startswith("http"):
            urls.append(href)
    return sorted(set(urls))


def pick_latest_zip_urls(urls: list[str], latest_months: int) -> tuple[list[tuple[str, str]], str | None]:
    zips: list[tuple[str, str]] = []  # (period YYYY-MM, url)
    meta_url: str | None = None

    for u in urls:
        name = u.split("/")[-1]
        if name.lower().endswith(".txt") and "meta_inf_diario_fi" in name.lower():
            meta_url = u
            continue

        m = RE_ZIP.search(name)
        if not m:
            continue
        yyyymm = m.group(1)
        period = f"{yyyymm[:4]}-{yyyymm[4:]}"
        zips.append((period, u))

    zips.sort(key=lambda x: x[0], reverse=True)
    return zips[:latest_months], meta_url


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "cvm_fi_inf_diario")
    lm = latest_months or cfg.latest_months

    logger.info("Lendo resources do dataset: %s", cfg.source_dataset_url)
    urls = extract_resource_urls(cfg.source_dataset_url)

    zip_urls, meta_url = pick_latest_zip_urls(urls, lm)
    logger.info("Encontrados %d ZIPs (latest=%d). Meta=%s", len(zip_urls), lm, "sim" if meta_url else "não")

    items: list[dict[str, Any]] = []

    # Baixa + sobe os ZIPs
    for period, url in zip_urls:
        filename = url.split("/")[-1]
        local = settings.data_dir / "cvm_fi_inf_diario" / filename

        logger.info("Download: %s", url)
        dl = stream_download(url, local)

        object_path = f"{cfg.bucket_prefix}/data/{period}/{filename}"
        storage.upload_file(object_path, str(dl.file_path), "application/zip", upsert=True)
        public_url = storage.public_url(object_path)

        items.append(
            {
                "kind": "data",
                "period": period,
                "filename": filename,
                "sha256": dl.sha256,
                "size_bytes": dl.size_bytes,
                "storage_path": object_path,
                "public_url": public_url,
                "source_url": url,
            }
        )

    # Baixa + sobe o TXT (dicionário de dados)
    meta_obj: dict[str, Any] | None = None
    if meta_url:
        filename = meta_url.split("/")[-1]
        local = settings.data_dir / "cvm_fi_inf_diario" / filename

        logger.info("Download meta: %s", meta_url)
        dl = stream_download(meta_url, local)

        object_path = f"{cfg.bucket_prefix}/meta/{filename}"
        storage.upload_file(object_path, str(dl.file_path), "text/plain; charset=utf-8", upsert=True)
        public_url = storage.public_url(object_path)

        meta_obj = {
            "kind": "meta",
            "filename": filename,
            "sha256": dl.sha256,
            "size_bytes": dl.size_bytes,
            "storage_path": object_path,
            "public_url": public_url,
            "source_url": meta_url,
        }

    manifest = build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_dataset_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta_obj,
    )
    return manifest
