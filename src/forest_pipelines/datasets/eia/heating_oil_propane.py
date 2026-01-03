# src/forest_pipelines/datasets/eia/heating_oil_propane.py
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
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

def parse_iso_date(date_str: str) -> str:
    """Converte '12/31/2025' para '2025-12-31'"""
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str

def scrape_metadata(soup: BeautifulSoup) -> dict[str, str]:
    """Extrai datas de release da página e converte para ISO"""
    meta = {"release_date": "", "next_release": ""}
    
    # Busca 'Release Date: 12/31/2025'
    update_td = soup.find("td", class_="Update")
    if update_td:
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", update_td.get_text())
        if match:
            meta["release_date"] = parse_iso_date(match.group(1))
            
    # Busca 'Next Release Date: 1/7/2026'
    next_td = soup.find("td", string=re.compile(r"Next Release Date", re.I))
    if next_td:
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", next_td.get_text())
        if match:
            meta["next_release"] = parse_iso_date(match.group(1))
            
    return meta

def get_combinations(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    """Mapeia as 8 combinações tratando o HTML mal formado da EIA"""
    combos = []
    
    drop1 = soup.find("select", {"name": "Drop1"})
    if not drop1:
        return []

    # O HTML da EIA não fecha as tags <option>. 
    # Pegamos o string imediato de cada tag para evitar concatenar tudo.
    series_options = drop1.find_all("option")
    
    for s_opt in series_options:
        # Pega apenas o texto direto da opção, sem os descendentes
        raw_s_name = s_opt.next_element.strip()
        # Slugify: "Residential Heating Oil" -> "residential_heating_oil"
        series_slug = re.sub(r'[^a-z0-9]+', '_', raw_s_name.lower()).strip('_')
        
        base_filename_part = s_opt["value"] # ex: pet_pri_wfr_a_EPD2F_PRS_dpgal_w.htm
        
        # Geramos as duas variações de período (Weekly e Monthly) para cada série
        for freq_suffix, freq_label in [("_w.htm", "weekly"), ("_m.htm", "monthly")]:
            # Ajusta a URL: troca _w.htm por _m.htm se necessário
            current_url_part = re.sub(r'_[wm]\.htm$', freq_suffix, base_filename_part, flags=re.I)
            full_url = urljoin(base_url, current_url_part)
            
            combos.append({
                "filename": f"{series_slug}_{freq_label}.xls",
                "url": full_url,
                "display_name": f"{raw_s_name} ({freq_label.capitalize()})"
            })
            
    return combos

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    # Importante: busca na subpasta eia/
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/heating_oil_propane")
    
    r = requests.get(cfg.source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    meta_dates = scrape_metadata(soup)
    combinations = get_combinations(soup, cfg.source_url)
    
    items = []
    for combo in combinations:
        logger.info("Processando: %s", combo["display_name"])
        
        # Acessa a página da combinação para achar o link do XLS histórico
        try:
            res = requests.get(combo["url"], timeout=30)
            c_soup = BeautifulSoup(res.text, "html.parser")
            xls_link = c_soup.find("a", href=re.compile(r"\.xls$", re.I))
            
            if not xls_link:
                logger.warning("XLS não encontrado para %s", combo["display_name"])
                continue
                
            download_url = urljoin(combo["url"], xls_link["href"])
            local_path = settings.data_dir / "eia_heating_oil" / combo["filename"]
            dl = stream_download(download_url, local_path)
            
            # Caminho: eia/heating_oil_propane/data/YYYY-MM-DD/serie_periodo.xls
            object_path = f"{cfg.bucket_prefix}/data/{meta_dates['release_date']}/{combo['filename']}"
            storage.upload_file(object_path, str(dl.file_path), "application/vnd.ms-excel", upsert=True)
            
            items.append({
                "kind": "data",
                "title": combo["display_name"],
                "filename": combo["filename"],
                "sha256": dl.sha256,
                "size_bytes": dl.size_bytes,
                "public_url": storage.public_url(object_path),
                "source_url": download_url
            })
        except Exception as e:
            logger.error("Falha em %s: %s", combo["display_name"], str(e))

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "last_release_iso": meta_dates["release_date"],
            "next_release_iso": meta_dates["next_release"],
            "total_files": len(items),
            "scraped_at": datetime.utcnow().isoformat()
        }
    )

def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=raw.get("id", "eia_heating_oil_propane"),
        title=raw.get("title", "Heating Oil and Propane Prices"),
        source_url=raw.get("source_url", "https://www.eia.gov/dnav/pet/pet_pri_wfr_a_EPD2F_prs_dpgal_w.htm"),
        bucket_prefix=raw.get("bucket_prefix", "eia/heating_oil_propane")
    )