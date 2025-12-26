# scripts/backfill_cvm_inf_diario.py
from __future__ import annotations

import json

from forest_pipelines.logging_ import get_logger
from forest_pipelines.settings import load_settings
from forest_pipelines.storage.supabase_storage import SupabaseStorage
from forest_pipelines.registry.datasets import get_dataset_runner


def main() -> None:
    settings = load_settings("configs/app.yml")
    logger = get_logger(settings.logs_dir, "backfill_cvm_fi_inf_diario")

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    runner = get_dataset_runner("cvm_fi_inf_diario")

    # backfill “grande”
    manifest = runner(
        settings=settings,
        storage=storage,
        logger=logger,
        latest_months=60,
    )

    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    manifest_path = f"{manifest['bucket_prefix'].rstrip('/')}/manifest.json"

    storage.upload_bytes(
        object_path=manifest_path,
        data=manifest_bytes,
        content_type="application/json",
        upsert=True,
    )

    logger.info("Manifest publicado: %s", storage.public_url(manifest_path))


if __name__ == "__main__":
    main()
