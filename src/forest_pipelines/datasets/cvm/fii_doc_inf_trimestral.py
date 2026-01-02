# src/forest_pipelines/datasets/cvm/fii_doc_inf_trimestral.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml
from bs4 import BeautifulSoup

from forest_pipelines.http import stream_download
from forest_pipelines.manifests.build_manifest import build_manifest

RE_YEAR_ZIP = re.compile(r"inf_trimestral_fii_(\d{4})\.zip$", re.IGNORECASE)


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    latest_years: int


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    ds_id = raw.get("id") or dataset_id
    title = raw.get("title") or ds_id

    source_dataset_url = raw.get("source_dataset_url")
    if not source_dataset_url:
        dataset_slug = raw.get("dataset_slug")
        if dataset_slug:
            source_dataset_url = f"https://dados.cvm.gov.br/dataset/{dataset_slug}"

    if not source_dataset_url:
        raise ValueError(
            "Config inválida: informe 'source_dataset_url' ou 'dataset_slug' "
            f"em {path.as_posix()}"
        )

    bucket_prefix = raw.get("bucket_prefix")
    if not bucket_prefix:
        raise ValueError(f"Config inválida: faltando 'bucket_prefix' em {path.as_posix()}")

    latest_years = int(raw.get("latest_years", 5))

    return DatasetCfg(
        id=ds_id,
        title=title,
        source_dataset_url=source_dataset_url,
        bucket_prefix=bucket_prefix,
        latest_years=latest_years,
    )


def extract_resource_urls(dataset_url: str) -> list[str]:
    r = requests.get(dataset_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    urls: list[str] = []
    for a in soup.select("a.resource-url-analytics"):
        href = a.get("href")
        if href and href.startswith("http"):
            urls.append(href)

    return sorted(set(urls))


def pick_latest_year_zip_urls(urls: list[str], latest_years: int) -> tuple[list[tuple[str, str]], str | None]:
    yearly: list[tuple[str, str]] = []
    meta_url: str | None = None

    for u in urls:
        name = u.split("/")[-1].split("?")[0]

        if name.lower().endswith(".zip") and "meta_inf_trimestral_fii" in name.lower():
            meta_url = u
            continue

        m = RE_YEAR_ZIP.search(name)
        if not m:
            continue

        year = m.group(1)
        yearly.append((year, u))

    yearly.sort(key=lambda x: x[0], reverse=True)
    return yearly[: max(1, latest_years)], meta_url


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,  # compat com CLI (atua como latest_years)
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "cvm/fii_doc_inf_trimestral")
    ly = latest_months or cfg.latest_years

    logger.info("Lendo resources do dataset: %s", cfg.source_dataset_url)
    urls = extract_resource_urls(cfg.source_dataset_url)

    zip_urls, meta_url = pick_latest_year_zip_urls(urls, ly)
    logger.info(
        "Encontrados %d ZIPs (latest_years=%d). Meta=%s",
        len(zip_urls),
        ly,
        "sim" if meta_url else "não",
    )

    items: list[dict[str, Any]] = []

    for year, url in zip_urls:
        filename = url.split("/")[-1].split("?")[0]
        local = settings.data_dir / "cvm_fii_doc_inf_trimestral" / filename

        logger.info("Download: %s", url)
        dl = stream_download(url, local)

        object_path = f"{cfg.bucket_prefix}/data/{year}/{filename}"
        storage.upload_file(object_path, str(dl.file_path), "application/zip", upsert=True)
        public_url = storage.public_url(object_path)

        items.append(
            {
                "kind": "data",
                "period": year,
                "filename": filename,
                "sha256": dl.sha256,
                "size_bytes": dl.size_bytes,
                "storage_path": object_path,
                "public_url": public_url,
                "source_url": url,
            }
        )

    meta_obj: dict[str, Any] | None = None
    if meta_url:
        filename = meta_url.split("/")[-1].split("?")[0]
        local = settings.data_dir / "cvm_fii_doc_inf_trimestral" / filename

        logger.info("Download meta: %s", meta_url)
        dl = stream_download(meta_url, local)

        object_path = f"{cfg.bucket_prefix}/meta/{filename}"
        storage.upload_file(object_path, str(dl.file_path), "application/zip", upsert=True)
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
