# src/forest_pipelines/datasets/eia/petroleum_weekly.py
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
        id=raw.get("id", dataset_id),
        title=raw.get("title", "Weekly Petroleum Status Report"),
        source_url=raw.get("source_url", "https://www.eia.gov/petroleum/supply/weekly/"),
        bucket_prefix=raw.get("bucket_prefix", "eia/petroleum_weekly")
    )

def scrape_eia_content(source_url: str) -> dict[str, Any]:
    """
    Extrai metadados de release e mapeia todos os arquivos por horário.
    """
    r = requests.get(source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 1. Extração de Datas (Para o Front-end)
    header = soup.select_one(".release-dates")
    metadata = {
        "week_ending": "",
        "release_date": "",
        "next_release_date": ""
    }
    if header:
        text = header.get_text(separator="|").split("|")
        # Lógica simples de busca nos spans baseada no seu HTML
        for span in header.select(".responsive-container"):
            txt = span.get_text().strip()
            if "Data for week ending" in txt:
                metadata["week_ending"] = txt.replace("Data for week ending", "").strip()
            elif "Release Date:" in txt:
                metadata["release_date"] = span.select_one(".date").get_text().strip() if span.select_one(".date") else ""
            elif "Next Release Date:" in txt:
                metadata["next_release_date"] = span.select_one(".date").get_text().strip() if span.select_one(".date") else ""

    # 2. Mapeamento da Tabela
    files = []
    tables = soup.select("div.basic-table table")
    
    for table in tables:
        rows = table.select("tbody tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5: continue # Pula linhas de título/header interno
            
            # A estrutura da EIA:
            # Col 0: Número | Col 1: Nome | Col 2 & 3: 10:30 AM | Col 4: 1:00 PM
            title = cols[1].get_text().strip()
            
            # Links das 10:30 AM (Colunas 2 e 3)
            for col_idx in [2, 3]:
                link = cols[col_idx].find("a")
                if link and link.get("href"):
                    files.append({
                        "title": title,
                        "url": urljoin(source_url, link.get("href")),
                        "release_time": "10:30 AM"
                    })
            
            # Links das 1:00 PM (Coluna 4)
            link_pm = cols[4].find("a")
            if link_pm and link_pm.get("href"):
                files.append({
                    "title": title,
                    "url": urljoin(source_url, link_pm.get("href")),
                    "release_time": "01:00 PM"
                })

    return {"metadata": metadata, "files": files}

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/petroleum_weekly")
    
    logger.info("Iniciando scrap da EIA: %s", cfg.source_url)
    scraped = scrape_eia_content(cfg.source_url)
    
    items = []
    # Usamos a data de release para organizar as pastas no bucket
    folder_date = scraped["metadata"]["release_date"].replace(",", "").replace(" ", "_")

    for file_info in scraped["files"]:
        url = file_info["url"]
        filename = url.split("/")[-1]
        
        # Evita baixar arquivos duplicados se houver (mesma URL para nomes diferentes)
        local_path = settings.data_dir / "eia" / filename
        
        logger.info(f"Baixando [{file_info['release_time']}] {filename}")
        dl = stream_download(url, local_path)
        
        # Estrutura: eia/petroleum_weekly/data/Dec_31_2025/1030AM/file.csv
        time_slug = file_info["release_time"].replace(" ", "").replace(":", "")
        object_path = f"{cfg.bucket_prefix}/data/{folder_date}/{time_slug}/{filename}"
        
        storage.upload_file(object_path, str(dl.file_path), "application/octet-stream", upsert=True)
        
        items.append({
            "kind": "data",
            "title": file_info["title"],
            "release_time": file_info["release_time"],
            "sha256": dl.sha256,
            "size_bytes": dl.size_bytes,
            "public_url": storage.public_url(object_path),
            "source_url": url
        })

    # Construção do Manifesto com a "Última Atualização" no topo
    manifest = build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "kind": "metadata",
            "last_eia_update": scraped["metadata"]["release_date"],
            "week_ending": scraped["metadata"]["week_ending"],
            "next_release": scraped["metadata"]["next_release_date"]
        }
    )
    
    return manifest