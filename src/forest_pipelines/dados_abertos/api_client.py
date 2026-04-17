# src/forest_pipelines/dados_abertos/api_client.py
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests

# CKAN DataStore API (público, sem cookie de sessão)
API_BASE_URL = "https://dados.gov.br/api/3/action/package_search"
CKAN_ROWS_PER_PAGE = 50

# Centralized browser-like headers (WAF / bot mitigation)
BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://dados.gov.br/",
}


def build_buscar_url(*, offset: int, org_id: str) -> str:
    params = {
        "fq": f"organization:{org_id}",
        "start": offset,
        "rows": CKAN_ROWS_PER_PAGE,
    }
    return f"{API_BASE_URL}?{urlencode(params)}"


@dataclass(frozen=True)
class FetchResult:
    payload: dict[str, Any] | None
    status_code: int | None
    request_url: str
    final_url: str | None
    error: str | None


def fetch_json_with_retries(
    url: str,
    *,
    timeout_s: float = 60.0,
    max_attempts: int = 3,
) -> FetchResult:
    """
    GET with browser headers, follow redirects explicitly, JSON parse, retries with backoff.
    """
    last_error: str | None = None
    request_url = url

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url,
                headers=BROWSER_HEADERS,
                timeout=timeout_s,
                allow_redirects=True,
            )
            status = resp.status_code
            final = resp.url

            if status >= 400:
                reason = getattr(resp, "reason", None) or ""
                last_error = f"HTTP {status}" + (f" {reason}" if reason else "")
                if attempt < max_attempts:
                    time.sleep(2 * attempt)
                continue

            try:
                payload = resp.json()
            except json.JSONDecodeError as e:
                last_error = f"JSON inválido: {e}"
                if attempt < max_attempts:
                    time.sleep(2 * attempt)
                continue

            return FetchResult(
                payload=payload if isinstance(payload, dict) else None,
                status_code=status,
                request_url=request_url,
                final_url=final,
                error=None,
            )
        except requests.RequestException as e:
            last_error = str(e)
            if attempt < max_attempts:
                time.sleep(2 * attempt)

    return FetchResult(
        payload=None,
        status_code=None,
        request_url=request_url,
        final_url=None,
        error=last_error or "falha desconhecida",
    )
