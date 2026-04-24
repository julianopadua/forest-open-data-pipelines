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
    skip_llm: bool = False,
) -> dict[str, dict[str, str]]:
    normalized_fallback = _normalize_fallback_blocks(fallback_blocks)
    required_keys = list(normalized_fallback.keys())

    if not llm_cfg.enabled or skip_llm:
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
            "- comparison: texto corrido (sem listas nem marcadores) que aborde, em prosa fluida, "
            "as seguintes comparações na ordem dada: "
            "o mês mais recente frente ao mesmo mês do ano anterior (valores absolutos e variação %); "
            "o mesmo mês frente à média histórica dos últimos 5 anos (valores e %); "
            "o acumulado jan–mês_atual do ano corrente versus mesmo período do ano anterior (valores e %); "
            "e esse acumulado frente à média acumulada dos últimos 5 anos (valores e %). "
            "Use frases completas com verbos, conectivos e valores numéricos precisos; "
            "evite enumerações, traços ou qualquer formatação de lista.\n"
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
        "- comparison: flowing prose (no bullet points, no numbered lists) covering "
        "in order: the latest month vs the same month of the previous year (absolute values and % change); "
        "that month vs the 5-year historical average for the same month (values and %); "
        "the YTD Jan–current_month total of the current year vs the same period of the previous year (values and %); "
        "and that YTD total vs the cumulative 5-year average for the same period (values and %). "
        "Write in complete sentences with connectives and precise numbers; "
        "avoid enumerations, dashes, or any list formatting.\n"
        "- limitations: short methodological caveat — current year may be incomplete, "
        "the reading is descriptive and does not establish causality.\n"
        "Respond with JSON only."
    )

    return system_prompt, user_prompt