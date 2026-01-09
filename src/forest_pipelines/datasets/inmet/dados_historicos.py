# src/forest_pipelines/datasets/inmet/dados_historicos.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
import yaml
from bs4 import BeautifulSoup

from forest_pipelines.http import stream_download
from forest_pipelines.manifests.build_manifest import build_manifest

# Regex para capturar o ano: 2024.zip
RE_ZIP_YEAR = re.compile(r"(\d{4})\.zip$", re.IGNORECASE)

@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_url: str
    bucket_prefix: str

def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=raw.get("id", "inmet_dados_historicos"),
        title=raw.get("title", "INMET Dados Históricos"),
        source_url=raw.get("source_url", "https://portal.inmet.gov.br/dadoshistoricos"),
        bucket_prefix=raw.get("bucket_prefix", "inmet/dados_historicos")
    )

def extract_annual_zip_urls(source_url: str) -> list[tuple[str, str]]:
    """Extrai links da página e retorna lista de (ano, url) ordenada pelo mais recente"""
    r = requests.get(source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    found = []
    # Busca links dentro da estrutura de artigos listada no HTML do INMET
    for a in soup.find_all("a", href=True):
        href = a["href"]
        filename = href.split("/")[-1]
        match = RE_ZIP_YEAR.search(filename)
        if match:
            year = match.group(1)
            found.append((year, urljoin(source_url, href)))
    
    # Remove duplicatas e ordena decrescentemente pelo ano
    return sorted(list(set(found)), key=lambda x: x[0], reverse=True)

def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None, # Interpretado como N anos para este dataset
) -> dict[str, Any]:
    # 1. Carrega Configuração
    cfg = load_dataset_cfg(settings.datasets_dir, "inmet/dados_historicos")
    
    # 2. Descoberta de recursos
    logger.info("Explorando servidor INMET: %s", cfg.source_url)
    all_resources = extract_annual_zip_urls(cfg.source_url)
    
    # Filtra pelos N anos mais recentes se solicitado via CLI (--latest-months)
    limit = latest_months if latest_months else len(all_resources)
    selected_resources = all_resources[:limit]
    
    items: list[dict[str, Any]] = []
    
    # 3. Processamento de Downloads e Uploads
    for year, url in selected_resources:
        filename = f"{year}.zip"
        local_path = settings.data_dir / "inmet_historico" / filename
        
        logger.info(f"Processando Ano {year}: {filename}")
        dl = stream_download(url, local_path)
        
        # Estrutura de pastas padronizada: inmet/dados_historicos/data/2024/2024.zip
        object_path = f"{cfg.bucket_prefix}/data/{year}/{filename}"
        
        storage.upload_file(object_path, str(dl.file_path), "application/zip", upsert=True)
        
        items.append({
            "kind": "data",
            "period": year,
            "filename": filename,
            "sha256": dl.sha256,
            "size_bytes": dl.size_bytes,
            "public_url": storage.public_url(object_path),
            "source_url": url
        })

    # 4. Construção do Manifesto para o Portal Web
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "source": "INMET - Instituto Nacional de Meteorologia",
            "observation": "Dados históricos anuais de estações automáticas",
            "total_years_synced": len(items)
        }
    )