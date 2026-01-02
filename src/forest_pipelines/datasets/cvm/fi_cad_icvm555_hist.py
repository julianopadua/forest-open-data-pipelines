# src/forest_pipelines/datasets/cvm/fi_cad_icvm555_hist.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml
from bs4 import BeautifulSoup

from forest_pipelines.http import stream_download
from forest_pipelines.manifests.build_manifest import build_manifest


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str


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

    return DatasetCfg(
        id=ds_id,
        title=title,
        source_dataset_url=source_dataset_url,
        bucket_prefix=bucket_prefix,
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


def find_by_filename(urls: list[str], filename: str) -> str:
    for u in urls:
        name = u.split("/")[-1].split("?")[0]
        if name.lower() == filename.lower():
            return u
    raise ValueError(f"Não encontrei resource URL para '{filename}' na página do dataset.")


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,  # compat CLI (não usado)
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "cvm/fi_cad_icvm555_hist")

    data_filename = "cad_fi_hist.zip"
    meta_filename = "meta_cad_fi.zip"

    logger.info("Lendo resources do dataset: %s", cfg.source_dataset_url)
    urls = extract_resource_urls(cfg.source_dataset_url)

    data_url = find_by_filename(urls, data_filename)
    meta_url = find_by_filename(urls, meta_filename)

    items: list[dict[str, Any]] = []

    local = settings.data_dir / "cvm_fi_cad_icvm555_hist" / data_filename
    logger.info("Download: %s", data_url)
    dl = stream_download(data_url, local)

    object_path = f"{cfg.bucket_prefix}/data/atual/{data_filename}"
    storage.upload_file(object_path, str(dl.file_path), "application/zip", upsert=True)
    public_url = storage.public_url(object_path)

    items.append(
        {
            "kind": "data",
            "period": "Atual",
            "filename": data_filename,
            "sha256": dl.sha256,
            "size_bytes": dl.size_bytes,
            "storage_path": object_path,
            "public_url": public_url,
            "source_url": data_url,
        }
    )

    meta_local = settings.data_dir / "cvm_fi_cad_icvm555_hist" / meta_filename
    logger.info("Download meta: %s", meta_url)
    meta_dl = stream_download(meta_url, meta_local)

    meta_path = f"{cfg.bucket_prefix}/meta/{meta_filename}"
    storage.upload_file(meta_path, str(meta_dl.file_path), "application/zip", upsert=True)
    meta_public_url = storage.public_url(meta_path)

    meta_obj = {
        "kind": "meta",
        "filename": meta_filename,
        "sha256": meta_dl.sha256,
        "size_bytes": meta_dl.size_bytes,
        "storage_path": meta_path,
        "public_url": meta_public_url,
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
