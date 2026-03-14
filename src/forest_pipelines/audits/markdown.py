# src/forest_pipelines/audits/markdown.py
from __future__ import annotations

from typing import Any


def _escape_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def render_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        header = "| " + " | ".join(columns) + " |"
        sep = "| " + " | ".join("---" for _ in columns) + " |"
        return "\n".join([header, sep])

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(_escape_cell(row.get(col)) for col in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body])


def render_bullets(items: list[str]) -> str:
    if not items:
        return "- Nenhum item."
    return "\n".join(f"- {item}" for item in items)
