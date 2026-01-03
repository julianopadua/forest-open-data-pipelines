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
        clean_date = date_str.strip()
        dt = datetime.strptime(clean_date, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str

def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    
    return DatasetCfg(
        id=raw.get("id", "eia_heating_oil_propane"),
        title=raw.get("title", "Weekly Heating Oil and Propane Prices"),
        source_url=raw.get("source_url", "https://www.eia.gov/dnav/pet/pet_pri_wfr_a_EPD2F_prs_dpgal_w.htm"),
        bucket_prefix=raw.get("bucket_prefix", "eia/heating_oil_propane")
    )

def scrape_metadata(soup: BeautifulSoup) -> dict[str, str]:
    """Extrai datas de release da página e converte para ISO"""
    meta = {"release_date": "", "next_release": ""}
    
    # Busca por 'Release Date: 12/31/2025'
    update_td = soup.find("td", class_="Update")
    if update_td:
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", update_td.get_text())
        if match:
            meta["release_date"] = parse_iso_date(match.group(1))
            
    # Busca por 'Next Release Date: 1/7/2026'
    next_td = soup.find("td", class_="Footnotes", string=re.compile(r"Next Release Date"))
    if next_td:
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", next_td.get_text())
        if match:
            meta["next_release"] = parse_iso_date(match.group(1))
            
    return meta

def get_combinations(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    """Mapeia as 8 combinações de Series e Period"""
    combos = []
    
    # 1. Pegar as Séries (Residential/Wholesale Heating Oil/Propane)
    series_options = soup.find("select", {"name": "Drop1"}).find_all("option")
    # 2. Pegar os Períodos (Weekly/Monthly)
    period_options = soup.find("select", {"name": "DropF"}).find_all("option")
    
    for s_opt in series_options:
        series_name = s_opt.get_text(strip=True).lower().replace(" ", "_")
        series_url_part = s_opt["value"] # ex: pet_pri_wfr_a_EPD2F_PRS_dpgal_w.htm
        
        for p_opt in period_options:
            period_name = p_opt.get_text(strip=True).lower() # weekly ou monthly
            
            # A EIA troca o sufixo '_w.htm' por '_m.htm' para mensal
            current_url_part = series_url_part
            if period_name == "monthly":
                current_url_part = series_url_part.replace("_w.htm", "_m.htm")
                
            full_url = urljoin(base_url, current_url_part)
            
            # Nome fixo de qualidade: residential_heating_oil_weekly
            fixed_filename = f"{series_name}_{period_name}.xls"
            
            combos.append({
                "filename": fixed_filename,
                "url": full_url,
                "display_name": f"{s_opt.get_text(strip=True)} ({p_opt.get_text(strip=True)})"
            })
            
    return combos

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/heating_oil_propane")
    
    # 1. Pegar a página base para extrair metadados e combinações
    r = requests.get(cfg.source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    meta_dates = scrape_metadata(soup)
    logger.info("Release Date detectada: %s", meta_dates["release_date"])
    
    combinations = get_combinations(soup, cfg.source_url)
    items = []

    # 2. Iterar sobre as 8 combinações
    for combo in combinations:
        logger.info("Processando: %s", combo["display_name"])
        
        # Precisamos entrar em cada página para pegar o link do XLS
        # porque o nome do arquivo XLS muda (ex: PET_PRI_WFR_A_EPD2F_PRS_DPGAL_W.xls)
        res = requests.get(combo["url"], timeout=30)
        c_soup = BeautifulSoup(res.text, "html.parser")
        
        xls_link = c_soup.find("a", href=re.compile(r"\.xls$"))
        if not xls_link:
            logger.warning("Link XLS não encontrado para %s", combo["display_name"])
            continue
            
        download_url = urljoin(combo["url"], xls_link["href"])
        
        # Download local
        local_path = settings.data_dir / "eia_heating_oil" / combo["filename"]
        dl = stream_download(download_url, local_path)
        
        # Upload para o Bucket: eia/heating_oil_propane/data/2025-12-31/residential_heating_oil_weekly.xls
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

    # 3. Manifesto Final
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "last_release_iso": meta_dates["release_date"],
            "next_release_iso": meta_dates["next_release"],
            "total_combinations": len(items),
            "scraped_at": datetime.utcnow().isoformat()
        }
    )