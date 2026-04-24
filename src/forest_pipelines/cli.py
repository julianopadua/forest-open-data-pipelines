# src/forest_pipelines/cli.py
from __future__ import annotations

import json

import typer

from forest_pipelines.audits.registry import get_audit_runner
from forest_pipelines.cli_help import (
    ANP_CATALOG_DOC,
    ANP_COMPACT_DOC,
    ANP_PUBLISH_DOC,
    AUDIT_DATASET_DOC,
    BUILD_REPORT_DOC,
    PUBLISH_CATALOG_DOC,
    SYNC_DOC,
    build_app_help,
    short_command_summary,
)
from forest_pipelines.logging_ import get_logger
from forest_pipelines.registry.datasets import get_dataset_runner
from forest_pipelines.reports.publish.supabase import publish_report_package
from forest_pipelines.reports.registry.reports import get_report_runner
from forest_pipelines.dados_abertos.publish_anp_catalog import DEFAULT_ANP_CATALOG_PREFIX
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
    "anp-publish",
    rich_help_panel="Dados abertos",
    help=ANP_PUBLISH_DOC,
    short_help=short_command_summary(ANP_PUBLISH_DOC),
)
def anp_publish_cmd(
    compact_json: str = typer.Argument(
        ...,
        help="Arquivo JSON do envelope compacto (schema_version, generated_at, datasets).",
    ),
    config_path: str = typer.Option(
        "configs/app.yml",
        "--config-path",
        help="YAML principal (bucket Supabase via supabase.bucket_open_data_env).",
    ),
    bucket_prefix: str = typer.Option(
        DEFAULT_ANP_CATALOG_PREFIX,
        "--bucket-prefix",
        help="Prefixo dentro do bucket (sem barra final); padrão: anp/catalog.",
    ),
    validate: bool = typer.Option(
        True,
        "--validate/--no-validate",
        help="Validar envelope com JSON Schema v1 antes do upload.",
    ),
) -> None:
    from pathlib import Path

    from forest_pipelines.dados_abertos.anp_catalog_compact import validate_compact_envelope
    from forest_pipelines.dados_abertos.publish_anp_catalog import publish_anp_catalog_compact

    path = Path(compact_json).resolve()
    if not path.is_file():
        raise typer.BadParameter(f"Arquivo não encontrado: {path}")

    envelope = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(envelope, dict):
        raise typer.BadParameter("JSON raiz deve ser um objeto")
    if validate:
        validate_compact_envelope(envelope)

    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, "anp/publish")

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    manifest = publish_anp_catalog_compact(
        storage,
        envelope,
        logger,
        bucket_prefix=bucket_prefix,
    )
    typer.echo(manifest["public_urls"]["catalog"])


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
    force: bool = typer.Option(
        False,
        "--force",
        help="Sobrescreve o report publicado sem pedir confirmação, mesmo que o período não tenha mudado.",
    ),
    scope: str = typer.Option(
        "",
        "--scope",
        help="Escopo de carga: 'current' (só ano corrente, rápido) ou 'full' (histórico completo). "
             "Se omitido, exibe prompt interativo.",
    ),
    no_llm: bool = typer.Option(
        False,
        "--no-llm",
        help="Pula a geração via LLM e usa o fallback determinístico, independentemente do config.",
    ),
) -> None:
    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, f"reports/{report_id}")

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    # --- Smart overwrite check ---
    if not force:
        existing_meta = _fetch_existing_report_meta(storage, report_id, logger)
        if existing_meta is not None:
            last_period = existing_meta.get("latest_period") or existing_meta.get("latest_period", "?")
            generated_at = existing_meta.get("generated_at", "?")
            typer.echo(
                f"\nReport '{report_id}' já foi publicado anteriormente.\n"
                f"  Período coberto : {last_period}\n"
                f"  Gerado em       : {generated_at}\n"
            )
            should_continue = typer.confirm("Deseja regenerar e sobrescrever?", default=False)
            if not should_continue:
                typer.echo("Operação cancelada. Use --force para pular esta confirmação.")
                raise typer.Exit()

    # --- Scope selection ---
    scope_lower = scope.strip().lower()
    if scope_lower in ("current", "full"):
        current_year_only = scope_lower == "current"
        typer.echo(f"→ Modo: {'apenas ano corrente' if current_year_only else 'histórico completo'}.\n")
    else:
        typer.echo(
            "\nEscolha o escopo de carga:\n"
            "  [1] Apenas ano corrente — carrega só o ZIP mais recente (rápido, ideal p/ atualização mensal)\n"
            "  [2] Todos os anos configurados — histórico completo (mais lento, recomendado p/ primeira execução)\n"
        )
        scope_choice = typer.prompt("Opção (1 ou 2)", default="1")
        current_year_only = scope_choice.strip() == "1"
        typer.echo(f"→ Modo: {'apenas ano corrente' if current_year_only else 'histórico completo'}.\n")

    if no_llm:
        typer.echo("→ LLM desabilitado: usando fallback determinístico.\n")

    runner = get_report_runner(report_id)
    package = runner(
        settings=settings,
        storage=storage,
        logger=logger,
        current_year_only=current_year_only,
        skip_llm=no_llm,
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
    "publish-catalog",
    rich_help_panel="Pipelines e storage",
    help=PUBLISH_CATALOG_DOC,
    short_help=short_command_summary(PUBLISH_CATALOG_DOC),
)
def publish_catalog_cmd(
    config_path: str = typer.Option(
        "configs/app.yml",
        "--config-path",
        help="YAML principal (bucket Supabase via supabase.bucket_open_data_env).",
    ),
    bucket_prefix: str = typer.Option(
        "catalog",
        "--bucket-prefix",
        help="Prefixo dentro do bucket (sem barra final); padrão: catalog.",
    ),
    anp_compact: str | None = typer.Option(
        None,
        "--anp-compact",
        help="Caminho para anp_catalog_compact.json. Padrão: <root>/anp_catalog_compact.json.",
    ),
) -> None:
    from pathlib import Path

    from forest_pipelines.catalog.build import (
        build_catalogs_from_defaults,
        publish_catalogs,
    )

    settings = load_settings(config_path)
    logger = get_logger(settings.logs_dir, "catalog/publish")

    override = Path(anp_compact).resolve() if anp_compact else None
    open_envelope, reports_envelope = build_catalogs_from_defaults(
        settings.root,
        anp_compact_override=override,
    )

    logger.info(
        "Catálogo open-data: %d datasets, status=%s, warnings=%d",
        len(open_envelope.get("datasets", [])),
        open_envelope.get("generation_status"),
        len(open_envelope.get("warnings", [])),
    )
    logger.info(
        "Catálogo reports: %d reports, status=%s",
        len(reports_envelope.get("reports", [])),
        reports_envelope.get("generation_status"),
    )

    storage = SupabaseStorage.from_env(
        logger=logger,
        bucket_open_data=settings.supabase_bucket_open_data,
    )

    result = publish_catalogs(
        storage=storage,
        open_data_envelope=open_envelope,
        reports_envelope=reports_envelope,
        bucket_prefix=bucket_prefix,
        logger=logger,
    )
    typer.echo(result["public_urls"]["open_data_catalog"])
    typer.echo(result["public_urls"]["reports_catalog"])


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


def _fetch_existing_report_meta(storage: Any, report_id: str, logger: Any) -> dict | None:
    """Try to fetch the published manifest for a report to check if it already exists."""
    from typing import Any as _Any  # noqa: PLC0415
    try:
        from forest_pipelines.reports.registry.reports import get_report_runner  # noqa: PLC0415
        # Derive bucket prefix from the registry config (same logic as publish)
        from forest_pipelines.reports.definitions.base import load_report_cfg  # noqa: PLC0415
        from pathlib import Path  # noqa: PLC0415

        # We need settings to get reports_dir — load minimal config
        from forest_pipelines.settings import load_settings as _ls  # noqa: PLC0415
        settings = _ls("configs/app.yml")
        cfg = load_report_cfg(settings.reports_dir, report_id)
        bucket_prefix = cfg.bucket_prefix.rstrip("/")
        manifest_path = f"{bucket_prefix}/manifest.json"

        raw = storage.download_bytes(manifest_path)
        if raw is None:
            return None
        manifest = json.loads(raw)
        meta = manifest.get("meta", {})
        meta["generated_at"] = manifest.get("generated_at")
        return meta
    except Exception as e:  # noqa: BLE001
        logger.debug("Não foi possível verificar report existente: %s", e)
        return None


if __name__ == "__main__":
    app()
