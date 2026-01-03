# src/forest_pipelines/cli.py
from __future__ import annotations
import json
import typer
from forest_pipelines.logging_ import get_logger
from forest_pipelines.registry.datasets import get_dataset_runner
from forest_pipelines.settings import load_settings
from forest_pipelines.storage.supabase_storage import SupabaseStorage

# no_args_is_help ajuda a diagnosticar erros de comando
app = typer.Typer(add_completion=False, no_args_is_help=True)

@app.command()
def sync(
    dataset_id: str = typer.Argument(..., help="ID do dataset (ex: eia_petroleum_weekly)"),
    config_path: str = typer.Option("configs/app.yml", help="Caminho do config principal"),
    latest_months: int | None = typer.Option(None, help="Sobrescreve latest_months"),
) -> None:
    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, dataset_id)

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    runner = get_dataset_runner(dataset_id)
    
    # Chama o sync do dataset passando os parâmetros
    manifest = runner(
        settings=settings,
        storage=storage,
        logger=logger,
        latest_months=latest_months,
    )

    # Publicação do Manifesto
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    manifest_path = f"{manifest['bucket_prefix'].rstrip('/')}/manifest.json"

    storage.upload_bytes(
        object_path=manifest_path,
        data=manifest_bytes,
        content_type="application/json",
        upsert=True,
    )

    logger.info("Manifest publicado: %s", storage.public_url(manifest_path))
    logger.info("Sincronização concluída com sucesso!")

if __name__ == "__main__":
    app()