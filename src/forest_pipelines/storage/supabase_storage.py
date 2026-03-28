# src/forest_pipelines/storage/supabase_storage.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any

from supabase import create_client


@dataclass
class SupabaseStorage:
    supabase_url: str
    service_role_key: str = field(repr=False)
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

        supabase_url = supabase_url.rstrip("/") + "/"

        return cls(
            supabase_url=supabase_url,
            service_role_key=service_role_key,
            bucket=bucket,
            logger=logger,
        )

    @cached_property
    def client(self):
        return create_client(self.supabase_url, self.service_role_key)

    def upload_file(self, object_path: str, local_path: str, content_type: str, upsert: bool = True) -> None:
        upsert_str = "true" if upsert else "false"
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
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
                    self.logger.info(
                        "Upload: %s -> %s (attempt=%s, resp=%s)",
                        local_path,
                        object_path,
                        attempt,
                        str(resp)[:200],
                    )
                return
            except Exception as e:  # noqa: BLE001
                last_error = e
                if self.logger:
                    self.logger.warning(
                        "Falha no upload de arquivo %s (attempt=%s). erro=%s",
                        object_path,
                        attempt,
                        e,
                    )
                if attempt < 3:
                    time.sleep(2 * attempt)

        raise RuntimeError(f"Falha ao subir arquivo para {object_path}") from last_error

    def upload_bytes(self, object_path: str, data: bytes, content_type: str, upsert: bool = True) -> None:
        upsert_str = "true" if upsert else "false"
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
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
                    self.logger.info(
                        "Upload bytes: %s (attempt=%s, resp=%s)",
                        object_path,
                        attempt,
                        str(resp)[:200],
                    )
                return
            except Exception as e:  # noqa: BLE001
                last_error = e
                if self.logger:
                    self.logger.warning(
                        "Falha no upload bytes %s (attempt=%s). erro=%s",
                        object_path,
                        attempt,
                        e,
                    )
                if attempt < 3:
                    time.sleep(2 * attempt)

        raise RuntimeError(f"Falha ao subir bytes para {object_path}") from last_error

    def download_bytes(self, object_path: str) -> bytes | None:
        try:
            data = (
                self.client.storage
                .from_(self.bucket)
                .download(object_path)
            )

            if self.logger:
                self.logger.info("Download bytes: %s", object_path)

            if isinstance(data, (bytes, bytearray)):
                return bytes(data)

            return data if isinstance(data, bytes) else None
        except Exception as e:  # noqa: BLE001
            if self.logger:
                self.logger.info("Download ausente ou indisponível: %s (erro=%s)", object_path, e)
            return None

    def public_url(self, object_path: str) -> str:
        base = self.supabase_url.rstrip("/")
        path = object_path.lstrip("/")
        return f"{base}/storage/v1/object/public/{self.bucket}/{path}"