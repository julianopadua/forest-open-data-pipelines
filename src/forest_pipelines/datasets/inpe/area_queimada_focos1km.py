# src/forest_pipelines/datasets/inpe/area_queimada_focos1km.py
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

# Regex para capturar ano e mês: focos1km_202401.tif
RE_TIF_PERIOD = re.compile(r"focos1km_(\d{4})(\d{2})\.tif$", re.IGNORECASE)

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
        id=raw.get("id", "inpe_area_queimada_focos1km"),
        title=raw.get("title", "INPE - Área Queimada - FOCOS 1km"),
        source_url=raw.get("source_url", "https://dataserver-coids.inpe.br/queimadas/queimadas/area_queimada/FOCOS1km/tif/"),
        bucket_prefix=raw.get("bucket_prefix", "inpe/area_queimada/focos1km")
    )

def get_remote_metadata(url: str, logger: Any) -> dict[str, Any]:
    """Obtém metadados do arquivo via HEAD request (tamanho e data)"""
    try:
        r = requests.head(url, allow_redirects=True, timeout=15)
        return {
            "size": int(r.headers.get("Content-Length", 0)),
            "last_modified": r.headers.get("Last-Modified", "")
        }
    except Exception as e:
        logger.warning(f"Falha ao obter metadados para {url}: {e}")
        return {"size": 0, "last_modified": ""}

def sync(
    settings: Any,
    storage: Any,
    logger: Any,
    **kwargs
) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "inpe/area_queimada_focos1km")
    
    # 1. Tentar carregar manifesto atual para comparação incremental
    current_manifest = {}
    try:
        manifest_url = storage.public_url(f"{cfg.bucket_prefix}/manifest.json")
        res = requests.get(manifest_url)
        if res.ok:
            current_manifest = res.json()
            logger.info("Manifesto anterior carregado.")
    except:
        logger.info("Iniciando nova indexação (sem manifesto anterior).")

    existing_items = {it["period"]: it for it in current_manifest.get("items", [])}

    # 2. Explorar o servidor INPE
    logger.info("Indexando links do Dataserver INPE: %s", cfg.source_url)
    r = requests.get(cfg.source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    items = []
    has_changes = False

    # 3. Processar links da página
    for a in soup.find_all("a", href=True):
        href = a["href"]
        filename = href.split("/")[-1]
        match = RE_TIF_PERIOD.search(filename)
        
        if match:
            year, month = match.groups()
            period = f"{year}-{month}"
            full_url = urljoin(cfg.source_url, href)
            
            # HEAD request para metadados reais
            remote_meta = get_remote_metadata(full_url, logger)
            
            # Verificação de mudança
            existing = existing_items.get(period)
            if existing and existing.get("size_bytes") == remote_meta["size"]:
                logger.info(f"[-] {period}: Sem alterações detectadas.")
                items.append(existing)
                continue

            # Novo item ou item atualizado
            has_changes = True
            logger.info(f"[+] {period}: Novo link indexado ({filename}).")
            
            items.append({
                "kind": "data",
                "period": period,
                "filename": filename,
                "sha256": "external", 
                "size_bytes": remote_meta["size"],
                "public_url": full_url,
                "source_url": full_url,
                "updated_at": datetime.utcnow().isoformat()
            })

    # 4. Finalização
    if not has_changes and current_manifest:
        logger.info("Nenhuma alteração nos arquivos do INPE. Sincronização finalizada.")
        return current_manifest

    items.sort(key=lambda x: x["period"], reverse=True)

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "source": "INPE - Programa Queimadas",
            "observation": "Links indexados via Dataserver COIDS com verificação HEAD.",
            "total_items": len(items),
            "generated_at": datetime.utcnow().isoformat()
        }
    )