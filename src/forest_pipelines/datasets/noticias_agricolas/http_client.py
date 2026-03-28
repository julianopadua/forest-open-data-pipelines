# src/forest_pipelines/datasets/noticias_agricolas/http_client.py
from __future__ import annotations

import random
import time
from typing import Any

import requests

RETRY_STATUSES = {429, 500, 502, 503, 504}


class ResilientHttpClient:
    """
    Conservative HTTP GET with retries, timeout, and post-request delay.
    """

    def __init__(
        self,
        logger: Any,
        *,
        timeout_s: float = 30.0,
        max_retries: int = 4,
        delay_s: float = 0.35,
        delay_jitter_s: float = 0.08,
        user_agent: str = (
            "Mozilla/5.0 (compatible; forest-open-data-pipelines/0.1; +https://example.invalid)"
        ),
    ) -> None:
        self._logger = logger
        self._timeout_s = timeout_s
        self._max_retries = max(1, max_retries)
        self._delay_s = delay_s
        self._delay_jitter_s = delay_jitter_s
        self._headers = {
            "User-Agent": user_agent.strip(),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
            "Connection": "keep-alive",
        }

    def get_text(self, url: str) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = requests.get(
                    url,
                    headers=self._headers,
                    timeout=self._timeout_s,
                )
                if resp.status_code in RETRY_STATUSES:
                    wait = min(2**attempt, 30) + random.uniform(0, self._delay_jitter_s)
                    if self._logger:
                        self._logger.warning(
                            "HTTP %s para %s (tentativa %s); aguardando %.2fs",
                            resp.status_code,
                            url,
                            attempt,
                            wait,
                        )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                text = resp.text
                self._sleep_after_request()
                return text
            except requests.RequestException as e:
                last_exc = e
                wait = min(2**attempt, 20) + random.uniform(0, self._delay_jitter_s)
                if self._logger:
                    self._logger.warning(
                        "Falha de rede em %s (tentativa %s): %s; aguardando %.2fs",
                        url,
                        attempt,
                        e,
                        wait,
                    )
                time.sleep(wait)
        raise RuntimeError(f"Falha ao obter {url}") from last_exc

    def _sleep_after_request(self) -> None:
        jitter = random.uniform(0, self._delay_jitter_s)
        time.sleep(self._delay_s + jitter)
