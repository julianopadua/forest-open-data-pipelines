# src/forest_pipelines/storage/supabase_storage.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from supabase import create_client


@dataclass
class SupabaseStorage:
    supabase_url: str
    service_role_key: str
    bucket: str
    logger: Any

    @classmethod
    def from_env(cls, logger: Any, bucket_open_data: str) -> "SupabaseStorage":
        url = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not url or not key:
            raise RuntimeError("Env vars faltando: SUPABASE_URL e/ou SUPABASE_SERVICE_ROLE_KEY")
        return cls(url, key, bucket_open_data, logger)

    @property
    def client(self):
        return create_client(self.supabase_url, self.service_role_key)

    def upload_file(self, object_path: str, local_path: str, content_type: str, upsert: bool = True) -> None:
        upsert_str = "true" if upsert else "false"
        with open(local_path, "rb") as f:
            resp = (
                self.client.storage
                .from_(self.bucket)
                .upload(
                    file=f,
                    path=object_path,
                    file_options={"content-type": content_type, "upsert": upsert_str},
                )
            )
        # algumas versões retornam dict; outras retornam objeto - só loga “best effort”
        self.logger.info("Upload: %s -> %s (resp=%s)", local_path, object_path, str(resp)[:200])

    def upload_bytes(self, object_path: str, data: bytes, content_type: str, upsert: bool = True) -> None:
        upsert_str = "true" if upsert else "false"
        resp = (
            self.client.storage
            .from_(self.bucket)
            .upload(
                file=data,
                path=object_path,
                file_options={"content-type": content_type, "upsert": upsert_str},
            )
        )
        self.logger.info("Upload bytes: %s (resp=%s)", object_path, str(resp)[:200])

    def public_url(self, object_path: str) -> str:
        # padrão documentado pela própria Supabase para buckets públicos :contentReference[oaicite:2]{index=2}
        base = self.supabase_url.rstrip("/")
        path = object_path.lstrip("/")
        return f"{base}/storage/v1/object/public/{self.bucket}/{path}"
