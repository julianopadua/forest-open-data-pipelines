# src/forest_pipelines/cli.py
from __future__ import annotations

import json

import typer

from forest_pipelines.audits.registry import get_audit_runner
from forest_pipelines.cli_help import (
    ANP_CATALOG_DOC,
    ANP_COMPACT_DOC,
    AUDIT_DATASET_DOC,
    BUILD_REPORT_DOC,
    SYNC_DOC,
    build_app_help,
    short_command_summary,
)
from forest_pipelines.logging_ import get_logger
from forest_pipelines.registry.datasets import get_dataset_runner
from forest_pipelines.reports.publish.supabase import publish_report_package
from forest_pipelines.reports.registry.reports import get_report_runner
from forest_pipelines.settings import load_settings
from forest_pipelines.storage.supabase_storage import SupabaseStorage

app = typer.Typer(
    name="forest-pipelines",
    help=build_app_help(),
    add_completion=False,
    no_args_is_help=True,
)


@app.command(
    "anp-catalog",
    rich_help_panel="Dados abertos",
    help=ANP_CATALOG_DOC,
    short_help=short_command_summary(ANP_CATALOG_DOC),
)
def anp_catalog_cmd(
    org_id: str = typer.Option(
        "88609f8c-a0ee-46eb-9294-f2175a6b561e",
        "--org-id",
        help="UUID da organização no CKAN (filtro fq=organization:<UUID>). Padrão: ANP.",
    ),
    offset_start: int = typer.Option(
        0,
        "--offset-start",
        help="Parâmetro CKAN `start` (deslocamento da página; incremento automático a cada lote).",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Máximo de datasets a processar (corta a paginação cedo; útil para teste de fumaça).",
    ),
    output_dir: str | None = typer.Option(
        None,
        "--output-dir",
        help="Diretório de saída para anp_catalogo_supabase.json e anp_catalogo_supabase.csv (padrão: cwd).",
    ),
) -> None:
    from pathlib import Path

    from forest_pipelines.dados_abertos.anp_catalog import run_anp_catalog

    out = Path(output_dir).resolve() if output_dir else None
    code = run_anp_catalog(
        org_id=org_id,
        offset_start=offset_start,
        limit=limit,
        output_dir=out,
    )
    raise typer.Exit(code=code)


@app.command(
    "anp-compact",
    rich_help_panel="Dados abertos",
    help=ANP_COMPACT_DOC,
    short_help=short_command_summary(ANP_COMPACT_DOC),
)
def anp_compact_cmd(
    input_json: str = typer.Argument(
        ...,
        help="Snapshot do portal: JSON com 'registros' (lista) e opcionalmente 'totalRegistros'.",
    ),
    output_json: str = typer.Option(
        "anp_catalog_compact.json",
        "--output",
        "-o",
        help="Arquivo JSON de saída (envelope com schema_version, generated_at, datasets[]).",
    ),
    validate: bool = typer.Option(
        True,
        "--validate/--no-validate",
        help="Validar o envelope contra o JSON Schema v1 (pacote jsonschema). Use --no-validate para só gerar o arquivo.",
    ),
) -> None:
    from pathlib import Path

    from forest_pipelines.dados_abertos.anp_catalog_compact import (
        load_anp_snapshot,
        transform_anp_snapshot,
        validate_compact_envelope,
        write_compact_catalog,
    )

    inp = Path(input_json).resolve()
    out = Path(output_json).resolve()
    if not inp.is_file():
        raise typer.BadParameter(f"Arquivo não encontrado: {inp}")

    data = load_anp_snapshot(inp)
    envelope = transform_anp_snapshot(data)
    write_compact_catalog(out, envelope)
    if validate:
        validate_compact_envelope(envelope)
    typer.echo(f"Escrito: {out}")


@app.command(
    "sync",
    rich_help_panel="Pipelines e storage",
    help=SYNC_DOC,
    short_help=short_command_summary(SYNC_DOC),
)
def sync(
    dataset_id: str = typer.Argument(
        ...,
        help="ID registrado (veja lista em forest-pipelines --help), ex.: eia_petroleum_weekly.",
    ),
    config_path: str = typer.Option(
        "configs/app.yml",
        "--config-path",
        help="YAML principal: diretórios de dados, logs, datasets_dir, bucket Supabase.",
    ),
    latest_months: int | None = typer.Option(
        None,
        "--latest-months",
        help="Recorte temporal em meses quando o runner do dataset suporta; caso contrário é ignorado.",
    ),
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


@app.command(
    "build-report",
    rich_help_panel="Relatórios",
    help=BUILD_REPORT_DOC,
    short_help=short_command_summary(BUILD_REPORT_DOC),
)
def build_report(
    report_id: str = typer.Argument(
        ...,
        help="ID do relatório registrado (veja forest-pipelines --help).",
    ),
    config_path: str = typer.Option(
        "configs/app.yml",
        "--config-path",
        help="YAML principal (paths, bucket Supabase para publicação do pacote).",
    ),
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


@app.command(
    "audit-dataset",
    rich_help_panel="Auditorias",
    help=AUDIT_DATASET_DOC,
    short_help=short_command_summary(AUDIT_DATASET_DOC),
)
def audit_dataset(
    dataset_id: str = typer.Argument(
        ...,
        help="ID com auditoria registrada (veja forest-pipelines --help).",
    ),
    config_path: str = typer.Option(
        "configs/app.yml",
        "--config-path",
        help="YAML principal (paths, ex.: docs_dir para saída da auditoria).",
    ),
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
