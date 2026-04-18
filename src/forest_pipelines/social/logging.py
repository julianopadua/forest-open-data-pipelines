# src/forest_pipelines/social/logging.py
from __future__ import annotations

import json
import logging
from pathlib import Path

from forest_pipelines.llm.router import RoutedTextResult
from forest_pipelines.logging_ import get_logger

_REPO_ROOT = Path(__file__).resolve().parents[3]


def get_social_bdqueimadas_logger(logs_dir: Path | None = None) -> logging.Logger:
    """Logger de arquivo + stdout em ``<repo>/logs/social/bdqueimadas/<ano>/<mês>/<dia>.log``."""
    base = logs_dir if logs_dir is not None else _REPO_ROOT / "logs"
    return get_logger(base, "social/bdqueimadas")


def log_stage(logger: logging.Logger, stage: str, payload: dict) -> None:
    """Uma linha JSON por etapa do fluxo (``stage`` + dados resumidos)."""
    line = json.dumps({"event": "stage", "stage": stage, **payload}, ensure_ascii=False)
    logger.info("%s", line)


def log_llm_roundtrip(
    logger: logging.Logger,
    *,
    topic_id: str,
    component: str,
    system_prompt: str,
    user_prompt: str,
    result: RoutedTextResult,
) -> None:
    """Registra prompt completo enviado ao provedor e resposta (texto + modelo + bruto)."""
    line = json.dumps(
        {
            "event": "llm_roundtrip",
            "topic": topic_id,
            "component": component,
            "request": {
                "system": system_prompt,
                "user": user_prompt,
            },
            "response": {
                "model": result.model,
                "text": result.text,
                "raw_text": result.raw_text,
            },
        },
        ensure_ascii=False,
    )
    logger.info("%s", line)
