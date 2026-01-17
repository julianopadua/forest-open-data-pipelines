# src/forest_pipelines/datasets/eia/petroleum_monthly.py
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

def retry_request(retries: int = 3, backoff: int = 2):
    """Decorator for simple exponential backoff on requests."""
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    last_exception = e
                    wait = backoff * (2 ** i)
                    # We assume the last arg or a kwarg 'logger' is passed
                    logger = kwargs.get('logger') or args[-1]
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
    try:
        r = safe_get(sub_page_url, logger)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Priority 1: The 'crumb' class link you identified
        for a in soup.find_all("a", class_="crumb", href=True):
            if "Download Series History" in a.get_text() or ".xls" in a["href"]:
                return urljoin(sub_page_url, a["href"])
        
        # Priority 2: Fallback - any link ending in _cur.xls
        fallback = soup.find("a", href=re.compile(r"\.xls$"))
        if fallback:
            logger.info(f"Used regex fallback for XLS link at {sub_page_url}")
            return urljoin(sub_page_url, fallback["href"])
            
    except Exception as e:
        logger.error(f"Critical failure parsing sub-page {sub_page_url}: {e}")
    return None

def sync(settings: Any, storage: Any, logger: Any, **kwargs) -> dict[str, Any]:
    cfg = load_dataset_cfg(settings.datasets_dir, "eia/petroleum_monthly")
    logger.info(f"Starting sync for {cfg.title}")

    try:
        r = safe_get(cfg.source_url, logger)
    except Exception as e:
        logger.critical(f"Could not reach main index page: {e}")
        raise

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("div.basic-table table")
    
    if not table:
        logger.error("HTML Structure changed: 'div.basic-table table' not found.")
        raise ValueError("Main data table missing.")

    items = []
    links = table.select("tbody tr td a[href*='/dnav/pet/']")
    logger.info(f"Found {len(links)} potential datasets to index.")

    for a in links:
        display_title = a.get_text().strip()
        sub_page_url = urljoin(cfg.source_url, a["href"])
        
        try:
            logger.info(f"Indexing: {display_title}")
            direct_url = extract_xls_link(sub_page_url, logger)
            
            if not direct_url:
                logger.warning(f"Skipping {display_title}: No download link found on sub-page.")
                continue

            # Get metadata
            size_bytes = 0
            try:
                h = safe_head(direct_url, logger)
                size_bytes = int(h.headers.get("Content-Length", 0))
            except Exception:
                logger.warning(f"Could not determine size for {direct_url}")

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
            logger.error(f"Failed to process dataset '{display_title}': {e}")
            continue # Move to next dataset instead of crashing the whole pipeline

    logger.info(f"Successfully indexed {len(items)} datasets.")
    
    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta={
            "total_items": len(items),
            "status": "partial_success" if len(items) < len(links) else "success",
            "scraped_at": datetime.utcnow().isoformat()
        }
    )