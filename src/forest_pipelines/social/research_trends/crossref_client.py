"""Minimal Crossref client used for DOI/metadata validation of OpenAlex works."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

LOG = logging.getLogger(__name__)

CROSSREF_BASE = "https://api.crossref.org"


@dataclass(slots=True)
class CrossrefClient:
    mailto: str
    cache_dir: Path
    request_timeout: int = 20
    sleep_between_calls: float = 0.1

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, doi: str) -> Path:
        safe = doi.replace("/", "_").replace(":", "_")
        return self.cache_dir / f"crossref_{safe}.json"

    def fetch_work(self, doi: str) -> dict[str, Any] | None:
        doi = doi.strip()
        if not doi:
            return None
        cache = self._cache_path(doi)
        if cache.exists():
            return json.loads(cache.read_text())

        url = f"{CROSSREF_BASE}/works/{doi}"
        headers = {"User-Agent": f"forest-pipelines/0.1 (mailto:{self.mailto})"}
        try:
            resp = requests.get(url, headers=headers, timeout=self.request_timeout)
        except requests.RequestException as exc:
            LOG.warning("crossref.request_error doi=%s err=%s", doi, exc)
            return None
        time.sleep(self.sleep_between_calls)
        if resp.status_code == 404:
            LOG.info("crossref.not_found doi=%s", doi)
            return None
        if resp.status_code != 200:
            LOG.warning(
                "crossref.bad_status doi=%s status=%d body=%s",
                doi,
                resp.status_code,
                resp.text[:120],
            )
            return None
        message = resp.json().get("message")
        if not message:
            return None
        cache.write_text(json.dumps(message, ensure_ascii=False, indent=2))
        return message
