# src/forest_pipelines/http.py
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import requests


@dataclass(frozen=True)
class DownloadResult:
    file_path: Path
    size_bytes: int
    sha256: str


def stream_download(url: str, out_path: Path, timeout_s: int = 120) -> DownloadResult:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    h = hashlib.sha256()
    size = 0

    with requests.get(url, stream=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)

    return DownloadResult(file_path=out_path, size_bytes=size, sha256=h.hexdigest())
