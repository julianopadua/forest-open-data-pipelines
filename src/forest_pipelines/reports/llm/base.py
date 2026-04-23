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
            "Você é um analista descritivo de dados públicos de incêndios e focos de calor. "
            "Sua função é redigir observações factuais, calibradas e não especulativas, fiéis exclusivamente "
            "aos números fornecidos no contexto. Não invente causalidade, não atribua causas climáticas ou "
            "humanas sem que estejam explicitadas, e não extrapole tendências além do período descrito. "
            "Escreva como analista de dados — objetivo, direto, sem sensacionalismo. "
            "Responda exclusivamente com um objeto JSON puro, sem markdown, sem comentários, "
            f"contendo exatamente estas chaves: {required_keys}."
        )

        user_prompt = (
            f"report_id: {report_id}\n"
            f"max_chars_por_bloco: {max_chars_per_block}\n"
            "contexto_estruturado:\n"
            f"{context_json}\n\n"
            "Instruções de saída — use SOMENTE os números em 'monthly_analysis' do contexto:\n"
            "- headline: frase principal de síntese factual com o dado do mês fechado mais recente "
            "(focos, variação % vs ano anterior e vs média histórica) e o acumulado do ano corrente. "
            "Mencione o mês e o ano explicitamente.\n"
            "- overview: leitura geral da janela de análise recente. "
            "Compare os últimos 12 meses com os 12 meses anteriores (variação %). "
            "Comente a posição do ano corrente em relação à série histórica disponível, sem especular sobre causas.\n"
            "- comparison: comparação estruturada em quatro pontos: "
            "(1) mês mais recente vs mesmo mês do ano anterior (% e absoluto); "
            "(2) mês mais recente vs média do mesmo mês nos últimos 5 anos (% e absoluto); "
            "(3) acumulado jan–mês_atual do ano corrente vs mesmo período do ano anterior (% e absoluto); "
            "(4) acumulado jan–mês_atual vs média acumulada dos últimos 5 anos (% e absoluto). "
            "Cite os valores numéricos precisos de cada comparação.\n"
            "- limitations: ressalva metodológica curta — ano corrente pode estar incompleto, "
            "a leitura é descritiva e não estabelece causalidade.\n"
            "Responda apenas com JSON."
        )

        return system_prompt, user_prompt

    system_prompt = (
        "You are a descriptive analyst of public wildfire and hotspot data. "
        "Your job is to write factual, calibrated, non-speculative observations strictly faithful to the numbers "
        "provided in the context. Do not invent causality, do not attribute climatic or human causes unless "
        "explicitly stated, and do not extrapolate trends beyond the described period. "
        "Write as a data analyst — objective, direct, no sensationalism. "
        "Respond exclusively with a pure JSON object, with no markdown and no comments, "
        f"containing exactly these keys: {required_keys}."
    )

    user_prompt = (
        f"report_id: {report_id}\n"
        f"max_chars_per_block: {max_chars_per_block}\n"
        "structured_context:\n"
        f"{context_json}\n\n"
        "Output instructions — use ONLY the numbers in 'monthly_analysis' from the context:\n"
        "- headline: main factual synthesis with data from the latest closed month "
        "(hotspot count, % change vs previous year and vs historical average) and year-to-date total. "
        "Mention the month and year explicitly.\n"
        "- overview: general reading of the recent analysis window. "
        "Compare the latest 12 months with the prior 12 months (% change). "
        "Comment on where the current year stands relative to the available historical series, without speculating about causes.\n"
        "- comparison: structured comparison in four points: "
        "(1) latest month vs same month of previous year (% and absolute); "
        "(2) latest month vs average of the same month over the last 5 years (% and absolute); "
        "(3) YTD Jan–current_month of current year vs same period of previous year (% and absolute); "
        "(4) YTD Jan–current_month vs cumulative 5-year average for the same period (% and absolute). "
        "Cite the precise numerical values for each comparison.\n"
        "- limitations: short methodological caveat — current year may be incomplete, "
        "the reading is descriptive and does not establish causality.\n"
        "Respond with JSON only."
    )

    return system_prompt, user_prompt