# src/forest_pipelines/reports/llm/base.py
from __future__ import annotations

import json
from typing import Any

from forest_pipelines.llm.router import generate_json
from forest_pipelines.reports.definitions.base import ReportLLMCfg

SUPPORTED_REPORT_LOCALES = ("pt", "en")


def maybe_generate_analysis_blocks(
    settings: Any,
    llm_cfg: ReportLLMCfg,
    report_id: str,
    prompt_context: dict[str, Any],
    fallback_blocks: dict[str, Any],
    logger: Any,
) -> dict[str, dict[str, str]]:
    normalized_fallback = _normalize_fallback_blocks(fallback_blocks)
    required_keys = list(normalized_fallback.keys())

    if not llm_cfg.enabled:
        return normalized_fallback

    localized_results: dict[str, dict[str, str]] = {}

    for locale in SUPPORTED_REPORT_LOCALES:
        locale_fallback = {
            key: normalized_fallback[key][locale]
            for key in required_keys
        }

        try:
            system_prompt, user_prompt = _build_prompts(
                locale=locale,
                report_id=report_id,
                prompt_context=prompt_context,
                required_keys=required_keys,
                max_chars_per_block=llm_cfg.max_chars_per_block,
            )

            routed = generate_json(
                llm_settings=settings.llm,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                required_keys=required_keys,
                model_override=llm_cfg.model,
            )

            logger.info(
                "LLM routed com sucesso. model=%s report_id=%s locale=%s",
                routed.model,
                report_id,
                locale,
            )

            clean: dict[str, str] = {}
            for key in required_keys:
                value = routed.data.get(key, locale_fallback[key])
                text = str(value).strip()
                if not text:
                    text = locale_fallback[key]
                clean[key] = text[: llm_cfg.max_chars_per_block].strip()

            localized_results[locale] = clean

        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Falha no pipeline LLM do report %s para locale=%s. "
                "Usando fallback determinístico. erro=%s",
                report_id,
                locale,
                e,
            )
            localized_results[locale] = locale_fallback

    return {
        key: {
            locale: localized_results[locale][key]
            for locale in SUPPORTED_REPORT_LOCALES
        }
        for key in required_keys
    }


def _normalize_fallback_blocks(
    fallback_blocks: dict[str, Any],
) -> dict[str, dict[str, str]]:
    normalized: dict[str, dict[str, str]] = {}

    for key, value in fallback_blocks.items():
        if isinstance(value, dict):
            pt = str(value.get("pt") or value.get("en") or "").strip()
            en = str(value.get("en") or value.get("pt") or "").strip()
        else:
            text = str(value).strip()
            pt = text
            en = text

        normalized[key] = {
            "pt": pt,
            "en": en,
        }

    return normalized


def _build_prompts(
    locale: str,
    report_id: str,
    prompt_context: dict[str, Any],
    required_keys: list[str],
    max_chars_per_block: int,
) -> tuple[str, str]:
    context_json = json.dumps(prompt_context, ensure_ascii=False, indent=2)

    if locale == "pt":
        system_prompt = (
            "Você é um analista descritivo de dados públicos. "
            "Sua função é redigir observações curtas, prudentes, não especulativas e fiéis ao contexto fornecido. "
            "Não invente causalidade, não extrapole tendências além do que foi explicitamente descrito. "
            "Responda exclusivamente com um objeto JSON puro, sem markdown, sem comentários, "
            f"contendo exatamente estas chaves: {required_keys}."
        )

        user_prompt = (
            f"report_id: {report_id}\n"
            f"max_chars_por_bloco: {max_chars_per_block}\n"
            "contexto_estruturado:\n"
            f"{context_json}\n\n"
            "Instruções de saída:\n"
            "- headline: frase principal de síntese factual.\n"
            "- overview: leitura geral curta da janela recente.\n"
            "- comparison: comparação objetiva entre períodos mencionados.\n"
            "- limitations: ressalva metodológica curta e prudente.\n"
            "Responda apenas com JSON."
        )

        return system_prompt, user_prompt

    system_prompt = (
        "You are a descriptive analyst of public data. "
        "Your job is to write short, careful, non-speculative observations faithful to the provided context. "
        "Do not invent causality and do not extrapolate trends beyond what was explicitly described. "
        "Respond exclusively with a pure JSON object, with no markdown and no comments, "
        f"containing exactly these keys: {required_keys}."
    )

    user_prompt = (
        f"report_id: {report_id}\n"
        f"max_chars_per_block: {max_chars_per_block}\n"
        "structured_context:\n"
        f"{context_json}\n\n"
        "Output instructions:\n"
        "- headline: main factual synthesis sentence.\n"
        "- overview: short general reading of the recent window.\n"
        "- comparison: objective comparison between the mentioned periods.\n"
        "- limitations: short and careful methodological caveat.\n"
        "Respond with JSON only."
    )

    return system_prompt, user_prompt