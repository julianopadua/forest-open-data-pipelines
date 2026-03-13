# src/forest_pipelines/reports/llm/base.py
from __future__ import annotations

import json
from typing import Any

from forest_pipelines.llm.router import generate_json
from forest_pipelines.reports.definitions.base import ReportLLMCfg


def maybe_generate_analysis_blocks(
    settings: Any,
    llm_cfg: ReportLLMCfg,
    report_id: str,
    prompt_context: dict[str, Any],
    fallback_blocks: dict[str, str],
    logger: Any,
) -> dict[str, str]:
    if not llm_cfg.enabled:
        return fallback_blocks

    required_keys = list(fallback_blocks.keys())

    system_prompt = (
        "Você é um analista descritivo de dados públicos. "
        "Sua função é redigir observações curtas, prudentes, não especulativas e fiéis ao contexto fornecido. "
        "Não invente causalidade, não extrapole tendências além do que foi explicitamente descrito. "
        "Responda exclusivamente com um objeto JSON puro, sem markdown, sem comentários, "
        f"contendo exatamente estas chaves: {required_keys}."
    )

    user_prompt = (
        f"report_id: {report_id}\n"
        f"max_chars_por_bloco: {llm_cfg.max_chars_per_block}\n"
        "contexto_estruturado:\n"
        f"{json.dumps(prompt_context, ensure_ascii=False, indent=2)}\n\n"
        "Instruções de saída:\n"
        "- headline: frase principal de síntese factual.\n"
        "- overview: leitura geral curta da janela recente.\n"
        "- comparison: comparação objetiva entre períodos mencionados.\n"
        "- limitations: ressalva metodológica curta e prudente.\n"
        "Responda apenas com JSON."
    )

    try:
        routed = generate_json(
            llm_settings=settings.llm,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            required_keys=required_keys,
            model_override=llm_cfg.model,
        )

        logger.info("LLM routed com sucesso. model=%s report_id=%s", routed.model, report_id)

        clean: dict[str, str] = {}
        for key in required_keys:
            value = routed.data.get(key, fallback_blocks[key])
            text = str(value).strip()
            if not text:
                text = fallback_blocks[key]
            clean[key] = text[: llm_cfg.max_chars_per_block].strip()

        return clean
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Falha no pipeline LLM do report %s. Usando fallback determinístico. erro=%s",
            report_id,
            e,
        )
        return fallback_blocks