# src/forest_pipelines/reports/llm/base.py
from __future__ import annotations

from typing import Any

from forest_pipelines.reports.definitions.base import ReportLLMCfg


def maybe_generate_analysis_blocks(
    llm_cfg: ReportLLMCfg,
    prompt_context: dict[str, Any],
    fallback_blocks: dict[str, str],
    logger: Any,
) -> dict[str, str]:
    if not llm_cfg.enabled:
        return fallback_blocks

    logger.warning(
        "LLM habilitada para provider=%s model=%s, mas a integração ainda não foi implementada. "
        "Usando fallback determinístico.",
        llm_cfg.provider,
        llm_cfg.model,
    )
    return fallback_blocks