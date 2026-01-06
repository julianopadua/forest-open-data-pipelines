# src/forest_pipelines/datasets/cvm/fi_doc_entrega.py

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

# Regex para capturar: fi_entrega_documento_202501.zip
RE_ZIP = re.compile(r"fi_entrega_documento_(\d{6})\.zip$", re.IGNORECASE)


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    latest_months: int


def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    # Carrega o YAML. O caminho deve bater com a estrutura de pastas: configs/datasets/cvm/fi_doc_entrega.yml
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    ds_id = raw.get("id") or dataset_id
    title = raw.get("title") or ds_id
    
    # Prioriza a URL explícita no YAML, senão monta pelo slug
    source_dataset_url = raw.get("source_dataset_url")
    if not source_dataset_url:
        dataset_slug = raw.get("dataset_slug")
        if dataset_slug:
            source_dataset_url = f"https://dados.cvm.gov.br/dataset/{dataset_slug}"

    if not source_dataset_url:
        raise ValueError(f"Config inválida: informe 'source_dataset_url' em {path.as_posix()}")

    bucket_prefix = raw.get("bucket_prefix")
    if not bucket_prefix:
        raise ValueError(f"Config inválida: faltando 'bucket_prefix' em {path.as_posix()}")

    latest_months = int(raw.get("latest_months", 12))

    return DatasetCfg(
        id=ds_id,
        title=title,
        source_dataset_url=source_dataset_url,
        bucket_prefix=bucket_prefix,
        latest_months=latest_months,
    )


def extract_resource_urls(dataset_url: str) -> list[str]:
    """
    Busca todos os links com a classe 'resource-url-analytics' na página do CKAN.
    """
    r = requests.get(dataset_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

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

        # Captura o metadado específico: meta_fi_entrega_documento.txt
        if name.lower().endswith(".txt") and "meta_fi_entrega_documento" in name.lower():
            meta_url = u
            continue

        # Captura os dados mensais
        m = RE_ZIP.search(name)
        if not m:
            continue

        yyyymm = m.group(1)
        period = f"{yyyymm[:4]}-{yyyymm[4:]}"
        zips.append((period, u))

    # Ordena do mais recente para o mais antigo
    zips.sort(key=lambda x: x[0], reverse=True)
    return zips[:latest_months], meta_url


def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    # O dataset_id passado aqui deve ser o caminho relativo dentro de configs/datasets
    cfg = load_dataset_cfg(settings.datasets_dir, "cvm/fi_doc_entrega")
    lm = latest_months or cfg.latest_months

    logger.info("Lendo resources do dataset: %s", cfg.source_dataset_url)
    urls = extract_resource_urls(cfg.source_dataset_url)

    zip_urls, meta_url = pick_latest_zip_urls(urls, lm)
    logger.info("Encontrados %d ZIPs (latest=%d). Meta=%s", len(zip_urls), lm, "sim" if meta_url else "não")

    items: list[dict[str, Any]] = []

    # Download e Upload dos dados mensais
    for period, url in zip_urls:
        filename = url.split("/")[-1]
        # Salva localmente em pasta temporária (usando _ ao invés de / para nome de pasta local)
        local_folder = settings.data_dir / "cvm_fi_doc_entrega"
        local_folder.mkdir(parents=True, exist_ok=True)
        local = local_folder / filename

        logger.info("Download: %s", url)
        dl = stream_download(url, local)

        # No bucket, respeita o prefixo definido no YAML (ex: cvm/fi/doc/entrega/data/...)
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

    # Download e Upload do metadado
    meta_obj: dict[str, Any] | None = None
    if meta_url:
        filename = meta_url.split("/")[-1]
        local_folder = settings.data_dir / "cvm_fi_doc_entrega"
        local_folder.mkdir(parents=True, exist_ok=True)
        local = local_folder / filename

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

    # Gera o manifesto
    manifest = build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_dataset_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta_obj,
    )
    return manifest