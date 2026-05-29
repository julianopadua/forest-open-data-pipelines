from __future__ import annotations

from pathlib import Path

from forest_pipelines.freshness.classifier import CadenceClassification, write_classifications_csv


def write_report(
    path: str | Path,
    classifications: list[CadenceClassification],
    *,
    output_format: str,
) -> None:
    if output_format == "csv":
        write_classifications_csv(path, classifications)
        return
    if output_format != "md":
        raise ValueError("Unsupported freshness report format. Use md or csv.")
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown_report(classifications), encoding="utf-8")


def render_markdown_report(classifications: list[CadenceClassification]) -> str:
    lines = [
        "# Freshness social cadence",
        "",
        "Relatorio deterministico gerado a partir do historico local de observacoes de freshness.",
        "",
        "| Preset | Watch IDs | Cadencia sugerida | Confianca | Ultima atualizacao da fonte | Ultima observacao | Mediana dias | Mudancas | Metodo | Avisos |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in classifications:
        lines.append(
            "| "
            + " | ".join(
                [
                    item.preset,
                    ", ".join(item.watch_ids),
                    item.suggested_cadence,
                    item.confidence,
                    item.last_source_modified_at,
                    item.last_observed_at,
                    item.median_interval_days,
                    str(item.changes_observed),
                    ", ".join(item.signal_methods),
                    "; ".join(item.warnings),
                ]
            )
            + " |"
        )
    if not classifications:
        lines.append("| nenhum |  | insufficient_data | low |  |  |  | 0 |  | sem observacoes |")
    lines.extend(
        [
            "",
            "## Leitura operacional",
            "",
            "- `daily`, `weekly` e `monthly` indicam cadencia provavel para revisar ou gerar presets sociais.",
            "- `ad_hoc` indica fonte estavel ou sem mudancas suficientes para agenda fixa.",
            "- `insufficient_data` indica que o watcher ainda precisa acumular historico.",
            "- Este relatorio nao dispara posts e nao chama LLM.",
        ]
    )
    return "\n".join(lines) + "\n"
