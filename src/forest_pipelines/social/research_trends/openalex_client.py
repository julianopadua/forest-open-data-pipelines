"""Minimal OpenAlex client for the research-trends pipeline.

OpenAlex requires no API key. The "polite pool" is opted into via a
`mailto` parameter on every request — supply it to avoid rate limits.

Docs: https://docs.openalex.org/
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import requests

LOG = logging.getLogger(__name__)

OPENALEX_BASE = "https://api.openalex.org"


@dataclass(slots=True)
class OpenAlexClient:
    mailto: str
    cache_dir: Path
    per_page: int = 200
    request_timeout: int = 30
    sleep_between_calls: float = 0.2

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / f"openalex_{name}.json"

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        params = {**params, "mailto": self.mailto}
        url = f"{OPENALEX_BASE}{path}"
        resp = requests.get(url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()
        time.sleep(self.sleep_between_calls)
        return resp.json()

    def iter_works(
        self,
        *,
        search: str | None = None,
        filter_str: str | None = None,
        sort: str | None = None,
        cache_key: str,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield Work records page-by-page using cursor pagination.

        Caches the assembled list of works to disk so reruns are cheap and the
        pipeline is reproducible offline. The cache key MUST encode every
        request parameter that affects the result set (search, filter, sort,
        date window); otherwise stale caches will mask updated data.
        """
        cache = self._cache_path(cache_key)
        if cache.exists():
            LOG.info("openalex.cache_hit key=%s path=%s", cache_key, cache)
            data = json.loads(cache.read_text())
            yield from data.get("works", [])
            return

        cursor = "*"
        page = 0
        all_works: list[dict[str, Any]] = []
        meta: dict[str, Any] = {}
        while True:
            page += 1
            params: dict[str, Any] = {
                "per-page": self.per_page,
                "cursor": cursor,
            }
            if search:
                params["search"] = search
            if filter_str:
                params["filter"] = filter_str
            if sort:
                params["sort"] = sort
            payload = self._get("/works", params)
            results = payload.get("results", []) or []
            meta = payload.get("meta", meta) or meta
            all_works.extend(results)
            LOG.info(
                "openalex.page key=%s page=%d size=%d total=%s",
                cache_key,
                page,
                len(results),
                meta.get("count"),
            )
            cursor = (meta or {}).get("next_cursor")
            if not cursor or not results:
                break
            if max_pages and page >= max_pages:
                LOG.warning("openalex.max_pages_reached key=%s pages=%d", cache_key, page)
                break

        cache.write_text(
            json.dumps({"meta": meta, "works": all_works}, ensure_ascii=False, indent=2)
        )
        yield from all_works

    def count_by_year(
        self,
        *,
        search: str | None = None,
        filter_str: str | None = None,
        cache_key: str,
    ) -> dict[int, int]:
        """Return {year: total works} via OpenAlex group_by — one cheap request.

        Use this when you need the long-term yearly trend without paging through
        every work. Independent of `iter_works` cache.
        """
        cache = self._cache_path(f"groupby_{cache_key}")
        if cache.exists():
            raw = json.loads(cache.read_text())
            return {int(k): int(v) for k, v in raw.items()}

        params: dict[str, Any] = {"group_by": "publication_year", "per-page": 200}
        if search:
            params["search"] = search
        if filter_str:
            params["filter"] = filter_str
        payload = self._get("/works", params)
        result: dict[int, int] = {}
        for group in payload.get("group_by") or []:
            key = group.get("key")
            try:
                year = int(key)
            except (TypeError, ValueError):
                continue
            if year < 1900 or year > 3000:
                continue
            result[year] = int(group.get("count") or 0)
        cache.write_text(
            json.dumps({str(k): v for k, v in sorted(result.items())}, indent=2)
        )
        LOG.info("openalex.group_by_year key=%s years=%d", cache_key, len(result))
        return result
