# src/forest_pipelines/datasets/inmet/dados_historicos.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from datetime import datetime

import requests
import yaml
from bs4 import BeautifulSoup
from forest_pipelines.manifests.build_manifest import build_manifest

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

def get_remote_file_size(url: str, logger: Any) -> int:
    """Faz um HEAD request para obter o Content-Length sem baixar o arquivo"""
    try:
        # allow_redirects=True é vital pois o INMET pode redirecionar para o servidor de arquivos
        r = requests.head(url, allow_redirects=True, timeout=15)
        return int(r.headers.get("Content-Length", 0))
    except Exception as e:
        logger.warning(f"Não foi possível obter o tamanho para {url}: {e}")
        return 0

def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    **kwargs
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "inmet/dados_historicos")
    
    logger.info("Indexando links e tamanhos do INMET: %s", cfg.source_url)
    r = requests.get(cfg.source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    items = []
    # Busca links dentro da estrutura de artigos listada no HTML
    for a in soup.find_all("a", href=True):
        href = a["href"]
        filename = href.split("/")[-1]
        match = RE_ZIP_YEAR.search(filename)
        
        if match:
            year = match.group(1)
            full_url = urljoin(cfg.source_url, href)
            
            logger.info(f"Obtendo metadados do arquivo de {year}...")
            # Pega o tamanho real via HEAD request
            size_bytes = get_remote_file_size(full_url, logger)
            
            items.append({
                "kind": "data",
                "period": year,
                "filename": filename,
                "sha256": "external", 
                "size_bytes": size_bytes, # Agora com o valor real!
                "public_url": full_url,
                "source_url": full_url
            })

    # Ordena para o mais recente ficar no topo
    items.sort(key=lambda x: x["period"], reverse=True)

    # Gera o manifesto e sobe para o bucket
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "source": "INMET - Instituto Nacional de Meteorologia",
            "observation": "Links indexados com metadados de tamanho obtidos via HTTP HEAD.",
            "total_items": len(items),
            "generated_at": datetime.utcnow().isoformat()
        }
    )