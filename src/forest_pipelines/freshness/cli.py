from __future__ import annotations

import json
from datetime import timezone
from pathlib import Path
from typing import Any

import typer

from forest_pipelines.freshness.classifier import classify_presets, write_classifications_csv
from forest_pipelines.freshness.config import load_watch_config
from forest_pipelines.freshness.models import utc_now
from forest_pipelines.freshness.report import write_report
from forest_pipelines.freshness.storage import append_observations, load_observations, write_latest_snapshot
from forest_pipelines.freshness.watch import collect_watch_signals, observation_log

app = typer.Typer(
    name="freshness",
    help="Observa sinais leves de atualizacao de fontes para cadencia social.",
    add_completion=False,
    no_args_is_help=True,
)

DEFAULT_CONFIG = Path("configs/freshness/watch.yml")
DEFAULT_HISTORY = Path("data/freshness_watch/observations.csv")
DEFAULT_LATEST = Path("data/freshness_watch/latest.json")
DEFAULT_REPORT = Path("data/freshness_watch/reports/social_cadence.md")
DEFAULT_CLASSIFICATION = Path("data/freshness_watch/classification.csv")


@app.command("watch")
def watch_cmd(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="YAML de fontes observadas."),
    history: Path = typer.Option(DEFAULT_HISTORY, "--history", help="CSV append-only de observacoes."),
    latest: Path = typer.Option(DEFAULT_LATEST, "--latest", help="Snapshot JSON com o estado atual."),
    timeout_s: int | None = typer.Option(None, "--timeout-s", help="Timeout HTTP por request."),
) -> None:
    observed_at = utc_now().astimezone(timezone.utc)
    watch_config = load_watch_config(config)
    typer.echo(
        observation_log(
            "freshness_watch_start",
            config=str(config),
            watches=len(watch_config.watches),
        )
    )
    records = collect_watch_signals(
        watch_config,
        observed_at=observed_at,
        timeout_s=timeout_s,
    )
    observations = append_observations(history, records, observed_at=observed_at)
    all_observations = load_observations(history)
    write_latest_snapshot(latest, all_observations)
    typer.echo(
        observation_log(
            "freshness_watch_complete",
            observations=len(observations),
            changed=sum(1 for item in observations if item.changed),
            no_signal=sum(1 for item in observations if item.status != "ok"),
            history=str(history),
            latest=str(latest),
        )
    )


@app.command("classify-presets")
def classify_cmd(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="YAML de fontes observadas."),
    history: Path = typer.Option(DEFAULT_HISTORY, "--history", help="CSV append-only de observacoes."),
    out: Path | None = typer.Option(None, "--out", help="CSV opcional para classificacoes."),
) -> None:
    load_watch_config(config)
    classifications = classify_presets(history)
    if out:
        write_classifications_csv(out, classifications)
        typer.echo(observation_log("freshness_classify_complete", presets=len(classifications), out=str(out)))
        return
    typer.echo(json.dumps([item.as_row() for item in classifications], ensure_ascii=False, indent=2))


@app.command("report")
def report_cmd(
    history: Path = typer.Option(DEFAULT_HISTORY, "--history", help="CSV append-only de observacoes."),
    output_format: str = typer.Option("md", "--format", help="Formato de saida: md ou csv."),
    out: Path = typer.Option(DEFAULT_REPORT, "--out", help="Arquivo de saida do relatorio."),
) -> None:
    classifications = classify_presets(history)
    write_report(out, classifications, output_format=output_format)
    typer.echo(
        observation_log(
            "freshness_report_complete",
            format=output_format,
            presets=len(classifications),
            out=str(out),
        )
    )


def rows_as_jsonable(rows: list[Any]) -> list[dict[str, str]]:
    return [row.as_row() for row in rows]
