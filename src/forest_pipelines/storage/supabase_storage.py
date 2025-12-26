# src/forest_pipelines/storage/supabase_storage.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from supabase import create_client


@dataclass
class SupabaseStorage:
    supabase_url: str
    service_role_key: str = field(repr=False)  # <-- NÃO aparece em traceback/locals
    bucket: str = "open-data"
    logger: Any = None

    @classmethod
    def from_env(cls, logger: Any, bucket_open_data: str) -> "SupabaseStorage":
        supabase_url = os.getenv("SUPABASE_URL", "").strip()
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

        if not supabase_url or not service_role_key:
            raise RuntimeError("Env vars faltando: SUPABASE_URL e/ou SUPABASE_SERVICE_ROLE_KEY")

        bucket = (bucket_open_data or "").strip()
        if not bucket:
            raise RuntimeError("Bucket inválido. Verifique SUPABASE_BUCKET_OPEN_DATA ou configs/app.yml")

        return cls(
            supabase_url=supabase_url,
            service_role_key=service_role_key,
            bucket=bucket,
            logger=logger,
        )

    @property
    def client(self):
        # cria client sob demanda; a key fica só no objeto (repr oculto)
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

        if self.logger:
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

        if self.logger:
            self.logger.info("Upload bytes: %s (resp=%s)", object_path, str(resp)[:200])

    def public_url(self, object_path: str) -> str:
        base = self.supabase_url.rstrip("/")
        path = object_path.lstrip("/")
        return f"{base}/storage/v1/object/public/{self.bucket}/{path}"
