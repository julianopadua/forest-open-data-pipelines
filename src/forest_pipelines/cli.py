# src/forest_pipelines/cli.py
from __future__ import annotations

import json

import typer

from forest_pipelines.audits.registry import get_audit_runner
from forest_pipelines.logging_ import get_logger
from forest_pipelines.registry.datasets import get_dataset_runner
from forest_pipelines.reports.publish.supabase import publish_report_package
from forest_pipelines.reports.registry.reports import get_report_runner
from forest_pipelines.settings import load_settings
from forest_pipelines.storage.supabase_storage import SupabaseStorage

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

    manifest = runner(
        settings=settings,
        storage=storage,
        logger=logger,
        latest_months=latest_months,
    )

    skip_cli_manifest = bool(manifest.pop("_cli_skip_manifest_upload", False))

    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    manifest_path = f"{manifest['bucket_prefix'].rstrip('/')}/manifest.json"

    if not skip_cli_manifest:
        storage.upload_bytes(
            object_path=manifest_path,
            data=manifest_bytes,
            content_type="application/json",
            upsert=True,
        )

    logger.info("Manifest publicado: %s", storage.public_url(manifest_path))
    logger.info("Sincronização concluída com sucesso!")


@app.command("build-report")
def build_report(
    report_id: str = typer.Argument(..., help="ID do report (ex: bdqueimadas_overview)"),
    config_path: str = typer.Option("configs/app.yml", help="Caminho do config principal"),
) -> None:
    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, f"reports/{report_id}")

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    runner = get_report_runner(report_id)
    package = runner(
        settings=settings,
        storage=storage,
        logger=logger,
    )

    publication = publish_report_package(
        storage=storage,
        package=package,
        logger=logger,
    )

    logger.info("Manifest do report: %s", publication["public_urls"]["manifest"])
    logger.info("Report live: %s", publication["public_urls"]["live_report"])
    logger.info("Build do report concluído com sucesso!")


@app.command("audit-dataset")
def audit_dataset(
    dataset_id: str = typer.Argument(..., help="ID do dataset para auditoria (ex: inpe_bdqueimadas_focos)"),
    config_path: str = typer.Option("configs/app.yml", help="Caminho do config principal"),
) -> None:
    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, f"audits/{dataset_id}")

    runner = get_audit_runner(dataset_id)
    result = runner(
        settings=settings,
        logger=logger,
    )

    logger.info("Auditoria concluída.")
    logger.info("Markdown: %s", result["readme_path"])
    logger.info("JSON resumo: %s", result["summary_json_path"])


if __name__ == "__main__":
    app()
