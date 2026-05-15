"""HTTP client for the Forest Open Data API v1."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import httpx

from .models import DatasetManifest, DatasetSummary, OpenDataItem, ReportSummary

DEFAULT_BASE_URL = "https://institutoforest.org/api/v1"
USER_AGENT = "forest-data/0.1.0 (+https://institutoforest.org/docs/api/v1)"


class ForestDataError(Exception):
    """Base error for the SDK."""


class NotFoundError(ForestDataError):
    """Raised when the API returns a 404 problem response."""


class UpstreamError(ForestDataError):
    """Raised when the API or storage layer is unavailable."""


def _check(resp: httpx.Response) -> dict[str, Any]:
    if resp.status_code == 404:
        try:
            problem = resp.json()
            raise NotFoundError(problem.get("detail") or "not found")
        except ValueError as exc:
            raise NotFoundError("not found") from exc
    if resp.status_code >= 500:
        raise UpstreamError(f"upstream returned {resp.status_code}: {resp.text[:200]}")
    if resp.status_code >= 400:
        raise ForestDataError(f"request failed ({resp.status_code}): {resp.text[:200]}")
    return resp.json()


class Client:
    """Synchronous client for the Forest Open Data API v1."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = (
            base_url
            or os.environ.get("FOREST_API_BASE_URL")
            or DEFAULT_BASE_URL
        ).rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            follow_redirects=True,
        )

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _get(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        return _check(self._client.get(url))

    def list_datasets(self) -> list[DatasetSummary]:
        body = self._get("/catalog")
        return [DatasetSummary.from_dict(d) for d in body.get("datasets", [])]

    def list_reports(self) -> list[ReportSummary]:
        body = self._get("/catalog/reports")
        return [ReportSummary.from_dict(r) for r in body.get("reports", [])]

    def get_dataset(self, id_or_slug: str) -> DatasetManifest:
        body = self._get(f"/datasets/{id_or_slug}")
        return DatasetManifest.from_dict(body["manifest"])

    def get_report(self, id_or_slug: str) -> dict[str, Any]:
        body = self._get(f"/reports/{id_or_slug}")
        return body["manifest"]

    def get_dataset_items(self, id_or_slug: str) -> list[OpenDataItem]:
        body = self._get(f"/datasets/{id_or_slug}/items")
        return [OpenDataItem.from_dict(it) for it in body.get("items", [])]

    def download(
        self,
        id_or_slug: str,
        path: str | os.PathLike[str] = ".",
        *,
        verify_sha256: bool = True,
        chunk_size: int = 1024 * 1024,
    ) -> list[Path]:
        """Download every file in a dataset to `path`. Returns the local file paths."""
        manifest = self.get_dataset(id_or_slug)
        target = Path(path) / manifest.dataset_id
        target.mkdir(parents=True, exist_ok=True)

        out: list[Path] = []
        for item in manifest.items:
            local = target / item.filename
            self._download_one(item, local, verify_sha256=verify_sha256, chunk_size=chunk_size)
            out.append(local)
        return out

    def _download_one(
        self,
        item: OpenDataItem,
        local: Path,
        *,
        verify_sha256: bool,
        chunk_size: int,
    ) -> None:
        h = hashlib.sha256()
        with self._client.stream("GET", item.public_url) as resp:
            if resp.status_code != 200:
                raise UpstreamError(
                    f"download failed for {item.filename}: HTTP {resp.status_code}"
                )
            with local.open("wb") as fh:
                for chunk in resp.iter_bytes(chunk_size):
                    fh.write(chunk)
                    if verify_sha256:
                        h.update(chunk)
        if verify_sha256 and h.hexdigest() != item.sha256:
            local.unlink(missing_ok=True)
            raise ForestDataError(
                f"sha256 mismatch for {item.filename}: "
                f"expected {item.sha256}, got {h.hexdigest()}"
            )
