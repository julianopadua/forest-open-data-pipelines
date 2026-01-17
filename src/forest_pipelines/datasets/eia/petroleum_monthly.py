from __future__ import annotations

import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin

import requests
import yaml
from bs4 import BeautifulSoup
from forest_pipelines.manifests.build_manifest import build_manifest

@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_url: str
    bucket_prefix: str

def load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg:
    """Loads configuration from the yaml file."""
    path = datasets_dir / f"{dataset_id}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=raw.get("id", "eia_petroleum_monthly"),
        title=raw.get("title", "Petroleum Supply Monthly"),
        source_url=raw.get("source_url", "https://www.eia.gov/petroleum/supply/monthly/"),
        bucket_prefix=raw.get("bucket_prefix", "eia/petroleum_monthly")
    )

def retry_request(retries: int = 3, backoff: int = 2):
    """Decorator for exponential backoff on requests."""
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    last_exception = e
                    wait = backoff * (2 ** i)
                    # Attempt to find logger in args or kwargs
                    logger = kwargs.get('logger') or (args[1] if len(args) > 1 else None)
                    if logger:
                        logger.warning(f"Request failed (attempt {i+1}/{retries}). Retrying in {wait}s... Error: {e}")
                    time.sleep(wait)
            raise last_exception
        return wrapper
    return decorator

@retry_request(retries=3)
def safe_get(url: str, logger: Any, timeout: int = 30):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r

@retry_request(retries=2)
def safe_head(url: str, logger: Any):
    r = requests.head(url, allow_redirects=True, timeout=15)
    r.raise_for_status()
    return r

def extract_xls_link(sub_page_url: str, logger: Any) -> str | None:
    """Extracts the direct .xls link from the sub-page structure."""
    try:
        r = safe_get(sub_page_url, logger)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Priority 1: Link with class 'crumb' and specific text
        for a in soup.find_all("a", class_="crumb", href=True):
            if "Download Series History" in a.get_text() or ".xls" in a["href"]:
                return urljoin(sub_page_url, a["href"])
        
        # Priority 2: Regex fallback for any .xls file
        fallback = soup.find("a", href=re.compile(r"\.xls$", re.IGNORECASE))
        if fallback:
            logger.info(f"Fallback XLS found for {sub_page_url}")
            return urljoin(sub_page_url, fallback["href"])
            
    except Exception as e:
        logger.error(f"Error parsing sub-page {sub_page_url}: {e}")
    return None

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/petroleum_monthly")
    logger.info(f"Starting indexing for {cfg.title}")

    try:
        r = safe_get(cfg.source_url, logger)
    except Exception as e:
        logger.critical(f"Main index unreachable: {e}")
        raise

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("div.basic-table table")
    
    if not table:
        logger.error("Structure Error: Table not found in 'div.basic-table'.")
        raise ValueError("Target table is missing from EIA page.")

    items = []
    links = table.select("tbody tr td a[href*='/dnav/pet/']")
    
    for a in links:
        display_title = a.get_text().strip()
        sub_page_url = urljoin(cfg.source_url, a["href"])
        
        try:
            logger.info(f"Checking sub-page: {display_title}")
            direct_url = extract_xls_link(sub_page_url, logger)
            
            if not direct_url:
                logger.warning(f"No XLS link for: {display_title}")
                continue

            # Metadata head request
            size_bytes = 0
            try:
                h = safe_head(direct_url, logger)
                size_bytes = int(h.headers.get("Content-Length", 0))
            except Exception:
                logger.warning(f"Size unknown for {direct_url}")

            items.append({
                "kind": "data",
                "title": display_title,
                "filename": direct_url.split("/")[-1],
                "sha256": "external",
                "size_bytes": size_bytes,
                "public_url": direct_url,
                "source_url": sub_page_url
            })
        except Exception as e:
            logger.error(f"Dataset '{display_title}' failed: {e}")
            continue 

    logger.info(f"Indexing complete. {len(items)} items found.")
    
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "total_items": len(items),
            "status": "success" if len(items) == len(links) else "partial",
            "scraped_at": datetime.utcnow().isoformat()
        }
    )