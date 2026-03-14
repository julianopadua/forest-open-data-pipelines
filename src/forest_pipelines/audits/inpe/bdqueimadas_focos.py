# src/forest_pipelines/audits/inpe/bdqueimadas_focos.py
from __future__ import annotations

import json
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from forest_pipelines.audits.markdown import render_bullets, render_table
from forest_pipelines.audits.utils import (
    count_member_rows,
    detect_delimiter,
    distinct_preview,
    extract_year_from_name,
    fmt_int,
    fmt_pct,
    infer_series_kind,
    json_ready,
    now_iso,
    pick_archive_member,
    pick_best_column,
    read_header_columns,
    read_sample,
    safe_pct,
    top_schema_signature,
)

DATETIME_CANDIDATES = [
    "data_pas",
    "data_hora_gmt",
    "data_hora",
    "datahora",
    "data",
    "date",
]

STATE_CANDIDATES = [
    "estado",
    "uf",
    "estado_sigla",
    "state",
]

SAMPLE_ROWS_PER_FILE = 2000


def run_audit(
    settings: Any,
    logger: Any,
) -> dict[str, Any]:
    dataset_id = "inpe_bdqueimadas_focos"
    base_dir = settings.data_dir / "inpe_bdqueimadas"
    output_dir = settings.docs_dir / "audits" / "inpe" / "bdqueimadas_focos"
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_files = sorted(base_dir.glob("focos_br_ref_*.zip"))
    if not zip_files:
        raise FileNotFoundError(f"Nenhum ZIP encontrado em: {base_dir.resolve()}")

    file_profiles: list[dict[str, Any]] = []
    column_presence: Counter[str] = Counter()
    column_types: dict[str, Counter[str]] = defaultdict(Counter)
    column_samples: dict[str, list[str]] = defaultdict(list)
    all_column_lists: list[list[str]] = []
    detected_datetime_columns: Counter[str] = Counter()
    detected_state_columns: Counter[str] = Counter()

    for zip_path in zip_files:
        logger.info("Auditando arquivo: %s", zip_path.name)

        with zipfile.ZipFile(zip_path) as zf:
            member = pick_archive_member(zf)
            delimiter = detect_delimiter(zf, member)
            columns = read_header_columns(zf, member, delimiter)
            sample_df = read_sample(zf, member, delimiter, SAMPLE_ROWS_PER_FILE)
            row_count = count_member_rows(zf, member)

        datetime_col = pick_best_column(columns, DATETIME_CANDIDATES)
        state_col = pick_best_column(columns, STATE_CANDIDATES)

        if datetime_col:
            detected_datetime_columns[datetime_col] += 1
        if state_col:
            detected_state_columns[state_col] += 1

        per_column_type_map: dict[str, str] = {}
        per_column_samples_map: dict[str, list[str]] = {}

        for col in columns:
            column_presence[col] += 1

            if col in sample_df.columns:
                inferred = infer_series_kind(sample_df[col])
                sample_values = distinct_preview(sample_df[col], limit=4)
            else:
                inferred = "missing_in_sample"
                sample_values = []

            per_column_type_map[col] = inferred
            per_column_samples_map[col] = sample_values
            column_types[col][inferred] += 1

            for item in sample_values:
                if item not in column_samples[col]:
                    column_samples[col].append(item)
                if len(column_samples[col]) >= 6:
                    column_samples[col] = column_samples[col][:6]

        year = extract_year_from_name(zip_path.name)

        profile = {
            "zip_name": zip_path.name,
            "year": year,
            "member_name": member,
            "delimiter": delimiter,
            "row_count": row_count,
            "sample_rows_read": min(len(sample_df), SAMPLE_ROWS_PER_FILE),
            "column_count": len(columns),
            "columns": columns,
            "datetime_column_detected": datetime_col,
            "state_column_detected": state_col,
            "column_types": per_column_type_map,
            "column_samples": per_column_samples_map,
        }
        file_profiles.append(profile)
        all_column_lists.append(columns)

    modal_schema, modal_schema_freq = top_schema_signature(all_column_lists)
    files_total = len(file_profiles)

    intersection_columns = sorted(
        col for col, freq in column_presence.items() if freq == files_total
    )
    union_columns = sorted(column_presence.keys())

    divergent_files: list[dict[str, Any]] = []
    for profile in file_profiles:
        cols = profile["columns"]
        added = [c for c in cols if c not in modal_schema]
        missing = [c for c in modal_schema if c not in cols]

        if added or missing:
            divergent_files.append(
                {
                    "year": profile["year"],
                    "zip_name": profile["zip_name"],
                    "column_count": profile["column_count"],
                    "datetime_column_detected": profile["datetime_column_detected"],
                    "state_column_detected": profile["state_column_detected"],
                    "added_vs_modal_schema": ", ".join(added) if added else "-",
                    "missing_vs_modal_schema": ", ".join(missing) if missing else "-",
                }
            )

    column_summary_rows: list[dict[str, Any]] = []
    for col in union_columns:
        presence_count = column_presence[col]
        presence_pct = safe_pct(presence_count, files_total)

        type_counter = column_types[col]
        dominant_types = ", ".join(
            f"{kind} ({count})"
            for kind, count in type_counter.most_common(3)
        )

        sample_preview = ", ".join(column_samples[col][:4]) if column_samples[col] else "-"

        column_summary_rows.append(
            {
                "column": col,
                "present_in_files": presence_count,
                "presence_pct": fmt_pct(presence_pct),
                "dominant_types": dominant_types,
                "sample_values": sample_preview,
            }
        )

    inventory_rows = [
        {
            "year": profile["year"],
            "zip_name": profile["zip_name"],
            "row_count": fmt_int(int(profile["row_count"])),
            "column_count": profile["column_count"],
            "datetime_column_detected": profile["datetime_column_detected"] or "-",
            "state_column_detected": profile["state_column_detected"] or "-",
        }
        for profile in sorted(file_profiles, key=lambda x: (x["year"] or 0))
    ]

    summary = {
        "dataset_id": dataset_id,
        "generated_at": now_iso(),
        "base_dir": str(base_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "files_total": files_total,
        "years_detected": [p["year"] for p in sorted(file_profiles, key=lambda x: (x["year"] or 0))],
        "datetime_column_frequency": dict(detected_datetime_columns),
        "state_column_frequency": dict(detected_state_columns),
        "intersection_columns": intersection_columns,
        "union_columns": union_columns,
        "modal_schema": {
            "frequency": modal_schema_freq,
            "columns": modal_schema,
        },
        "files": file_profiles,
        "divergent_files": divergent_files,
        "column_summary": column_summary_rows,
    }

    readme_path = output_dir / "README.md"
    summary_json_path = output_dir / "summary.json"

    readme_path.write_text(
        _render_markdown(summary),
        encoding="utf-8",
    )
    summary_json_path.write_text(
        json.dumps(json_ready(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "dataset_id": dataset_id,
        "readme_path": str(readme_path.resolve()),
        "summary_json_path": str(summary_json_path.resolve()),
    }


def _render_markdown(summary: dict[str, Any]) -> str:
    files_total = int(summary["files_total"])
    modal_schema = summary["modal_schema"]["columns"]
    modal_schema_freq = int(summary["modal_schema"]["frequency"])
    divergent_files = summary["divergent_files"]
    intersection_columns = summary["intersection_columns"]
    union_columns = summary["union_columns"]

    dt_freq = summary["datetime_column_frequency"]
    state_freq = summary["state_column_frequency"]

    dt_desc = (
        ", ".join(f"{k} ({v})" for k, v in sorted(dt_freq.items(), key=lambda x: (-x[1], x[0])))
        if dt_freq
        else "Nenhuma coluna temporal reconhecida."
    )
    state_desc = (
        ", ".join(f"{k} ({v})" for k, v in sorted(state_freq.items(), key=lambda x: (-x[1], x[0])))
        if state_freq
        else "Nenhuma coluna de UF reconhecida."
    )

    findings = [
        f"Foram auditados {files_total} arquivos ZIP do BDQueimadas localizados em `{summary['base_dir']}`.",
        f"O esquema modal apareceu em {modal_schema_freq} arquivo(s).",
        f"A interseção de colunas entre todos os arquivos contém {len(intersection_columns)} coluna(s).",
        f"A união de colunas observadas contém {len(union_columns)} coluna(s).",
        f"Detecção de coluna temporal: {dt_desc}",
        f"Detecção de coluna de UF/estado: {state_desc}",
    ]

    if divergent_files:
        findings.append(
            f"Foram detectados {len(divergent_files)} arquivo(s) com divergência em relação ao esquema modal."
        )
    else:
        findings.append("Nenhum arquivo divergiu do esquema modal observado.")

    sections: list[str] = []
    sections.append("# Auditoria de esquema do BDQueimadas")
    sections.append("")
    sections.append("## Escopo")
    sections.append("")
    sections.append(
        "Esta auditoria inspeciona localmente os arquivos ZIP anuais do BDQueimadas, "
        "identificando estrutura de colunas, colunas temporais e geográficas detectáveis, "
        "tipos inferidos a partir de amostras e diferenças de esquema entre anos."
    )
    sections.append("")
    sections.append("## Metadados")
    sections.append("")
    sections.append(
        render_table(
            [
                {
                    "dataset_id": summary["dataset_id"],
                    "generated_at": summary["generated_at"],
                    "files_total": files_total,
                    "base_dir": summary["base_dir"],
                    "output_dir": summary["output_dir"],
                }
            ],
            ["dataset_id", "generated_at", "files_total", "base_dir", "output_dir"],
        )
    )
    sections.append("")
    sections.append("## Achados principais")
    sections.append("")
    sections.append(render_bullets(findings))
    sections.append("")
    sections.append("## Inventário por arquivo")
    sections.append("")
    sections.append(
        render_table(
            summary_inventory_rows(summary),
            [
                "year",
                "zip_name",
                "row_count",
                "column_count",
                "datetime_column_detected",
                "state_column_detected",
            ],
        )
    )
    sections.append("")
    sections.append("## Esquema modal")
    sections.append("")
    sections.append(
        f"O esquema modal apareceu em **{modal_schema_freq}** arquivo(s) e possui "
        f"**{len(modal_schema)}** coluna(s)."
    )
    sections.append("")
    sections.append(render_bullets([f"`{col}`" for col in modal_schema]))
    sections.append("")
    sections.append("## Colunas presentes em todos os arquivos")
    sections.append("")
    sections.append(render_bullets([f"`{col}`" for col in intersection_columns]))
    sections.append("")
    sections.append("## Resumo por coluna")
    sections.append("")
    sections.append(
        render_table(
            summary["column_summary"],
            ["column", "present_in_files", "presence_pct", "dominant_types", "sample_values"],
        )
    )
    sections.append("")
    sections.append("## Arquivos divergentes em relação ao esquema modal")
    sections.append("")
    if divergent_files:
        sections.append(
            render_table(
                divergent_files,
                [
                    "year",
                    "zip_name",
                    "column_count",
                    "datetime_column_detected",
                    "state_column_detected",
                    "added_vs_modal_schema",
                    "missing_vs_modal_schema",
                ],
            )
        )
    else:
        sections.append("Nenhum arquivo divergente.")
    sections.append("")
    sections.append("## Observações para construção de reports")
    sections.append("")
    sections.append(
        render_bullets(
            [
                "A coluna temporal detectada deve ser priorizada na configuração do report quando houver estabilidade suficiente entre os anos.",
                "A coluna geográfica detectada para UF/estado pode ser usada para tabelas comparativas e agregações regionais.",
                "O arquivo `summary.json` gerado junto deste Markdown pode ser consumido por utilitários internos para preparar builders de reports.",
                "Esta auditoria usa leitura de cabeçalho, contagem de linhas e amostragem de dados; ela não substitui validação semântica integral da base.",
            ]
        )
    )

    return "\n".join(sections) + "\n"


def summary_inventory_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for profile in sorted(summary["files"], key=lambda x: (x["year"] or 0)):
        rows.append(
            {
                "year": profile["year"],
                "zip_name": profile["zip_name"],
                "row_count": fmt_int(int(profile["row_count"])),
                "column_count": profile["column_count"],
                "datetime_column_detected": profile["datetime_column_detected"] or "-",
                "state_column_detected": profile["state_column_detected"] or "-",
            }
        )
    return rows
