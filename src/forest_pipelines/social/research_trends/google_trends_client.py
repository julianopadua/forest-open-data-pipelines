"""Google Trends client (via the unofficial pytrends library).

Google Trends has no official API. pytrends is community-maintained and can
get rate-limited or blocked. The pipeline tolerates failure: if a fresh pull
fails and no cache exists, the corresponding chart is skipped.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class GoogleTrendsClient:
    cache_dir: Path
    geo: str = "BR"
    timeframe: str = "today 10-y"
    hl: str = "pt-BR"

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, terms: list[str]) -> Path:
        sig = "|".join(sorted(terms)) + f"|{self.geo}|{self.timeframe}"
        digest = hashlib.sha1(sig.encode("utf-8")).hexdigest()[:12]
        return self.cache_dir / f"google_trends_{digest}.json"

    def interest_over_time(
        self, terms: list[str], *, force_refresh: bool = False
    ) -> dict[str, Any] | None:
        """Return a dict {term: [{date, value}]} or None when unavailable."""
        cache = self._cache_path(terms)
        if cache.exists() and not force_refresh:
            LOG.info("google_trends.cache_hit path=%s", cache)
            return json.loads(cache.read_text())

        try:
            from pytrends.request import TrendReq  # type: ignore
        except ImportError:
            LOG.warning("google_trends.pytrends_missing — install pytrends")
            return None

        try:
            client = TrendReq(hl=self.hl, tz=180)
            client.build_payload(terms, geo=self.geo, timeframe=self.timeframe)
            frame = client.interest_over_time()
        except Exception as exc:  # noqa: BLE001 - pytrends raises broad errors
            LOG.warning("google_trends.fetch_failed terms=%s err=%s", terms, exc)
            return None

        if frame is None or frame.empty:
            LOG.warning("google_trends.empty_response terms=%s", terms)
            return None

        if "isPartial" in frame.columns:
            frame = frame.drop(columns=["isPartial"])

        data: dict[str, list[dict[str, Any]]] = {}
        for term in terms:
            if term not in frame.columns:
                continue
            data[term] = [
                {"date": idx.strftime("%Y-%m-%d"), "value": int(val)}
                for idx, val in frame[term].items()
            ]

        payload = {
            "geo": self.geo,
            "timeframe": self.timeframe,
            "series": data,
        }
        cache.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload
