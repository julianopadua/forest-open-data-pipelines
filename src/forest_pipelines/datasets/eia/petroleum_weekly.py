# src/forest_pipelines/datasets/eia/petroleum_weekly.py
from __future__ import annotations

import re
import unicodedata
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

def slugify(value: str) -> str:
    """Converte 'U.S. Petroleum Balance Sheet' em 'us_petroleum_balance_sheet'"""
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[-\s]+', '_', value)

def parse_eia_date(date_str: str) -> str:
    """Converte 'Dec. 31, 2025' ou 'Jan. 7, 2026' em '2025-12-31'"""
    clean_date = date_str.replace('.', '').strip()
    try:
        dt = datetime.strptime(clean_date, "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str  # Fallback caso o formato mude

def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    
    return DatasetCfg(
        id=raw.get("id", "eia_petroleum_weekly"),
        title=raw.get("title", "Weekly Petroleum Status Report"),
        source_url=raw.get("source_url", "https://www.eia.gov/petroleum/supply/weekly/"),
        bucket_prefix=raw.get("bucket_prefix", "eia/petroleum_weekly")
    )

def scrape_eia_content(source_url: str) -> dict[str, Any]:
    r = requests.get(source_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 1. Metadados de Release (Datas Limpas)
    metadata = {"week_ending_raw": "", "release_date_iso": "", "next_release_date_iso": ""}
    header = soup.select_one(".release-dates")
    if header:
        for span in header.select(".responsive-container"):
            txt = span.get_text().strip()
            if "Data for week ending" in txt:
                metadata["week_ending_raw"] = txt.replace("Data for week ending", "").strip()
            elif "Release Date:" in txt:
                raw_date = span.select_one(".date").get_text().strip() if span.select_one(".date") else ""
                metadata["release_date_iso"] = parse_eia_date(raw_date)
            elif "Next Release Date:" in txt:
                raw_date = span.select_one(".date").get_text().strip() if span.select_one(".date") else ""
                metadata["next_release_date_iso"] = parse_eia_date(raw_date)

    # 2. Mapeamento de Arquivos com Nomes Fixos Sluggificados
    files = []
    tables = soup.select("div.basic-table table")
    for table in tables:
        rows = table.select("tbody tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5 or "terminated" in row.get('class', []): 
                continue 
            
            raw_title = cols[1].get_text().strip()
            name_slug = slugify(raw_title)

            # Colunas 2 e 3 (10:30 AM)
            for col_idx in [2, 3]:
                link = cols[col_idx].find("a")
                if link and link.get("href"):
                    url = urljoin(source_url, link.get("href"))
                    ext = url.split(".")[-1].split("?")[0]
                    files.append({
                        "fixed_name": f"{name_slug}.{ext}",
                        "display_title": raw_title,
                        "url": url,
                        "release_time": "10:30"
                    })
            
            # Coluna 4 (01:00 PM)
            link_pm = cols[4].find("a")
            if link_pm and link_pm.get("href"):
                url = urljoin(source_url, link_pm.get("href"))
                ext = url.split(".")[-1].split("?")[0]
                files.append({
                    "fixed_name": f"{name_slug}.{ext}",
                    "display_title": raw_title,
                    "url": url,
                    "release_time": "13:00"
                })

    return {"metadata": metadata, "files": files}

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/petroleum_weekly")
    scraped = scrape_eia_content(cfg.source_url)
    
    release_date = scraped["metadata"]["release_date_iso"]
    logger.info("Sincronizando EIA - Release: %s", release_date)

    items = []
    for f in scraped["files"]:
        # Download
        local_path = settings.data_dir / "eia" / f["fixed_name"]
        dl = stream_download(f["url"], local_path)

        # Storage Path: eia/petroleum_weekly/data/2025-12-31/10-30/us_petroleum_balance_sheet.csv
        time_folder = f["release_time"].replace(":", "-")
        object_path = f"{cfg.bucket_prefix}/data/{release_date}/{time_folder}/{f['fixed_name']}"
        
        # Determinar Content-Type
        ctype = "application/pdf" if f["fixed_name"].endswith(".pdf") else "application/octet-stream"
        if f["fixed_name"].endswith(".csv"): ctype = "text/csv"
        
        storage.upload_file(object_path, str(dl.file_path), ctype, upsert=True)
        
        items.append({
            "kind": "data",
            "title": f["display_title"],
            "filename": f["fixed_name"],
            "release_time": f["release_time"],
            "sha256": dl.sha256,
            "size_bytes": dl.size_bytes,
            "public_url": storage.public_url(object_path),
            "source_url": f["url"]
        })

    # Manifesto Final (Aparece no Front automaticamente)
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "last_release_iso": release_date,
            "week_ending": scraped["metadata"]["week_ending_raw"],
            "next_release_iso": scraped["metadata"]["next_release_date_iso"],
            "scraped_at": datetime.utcnow().isoformat()
        }
    )