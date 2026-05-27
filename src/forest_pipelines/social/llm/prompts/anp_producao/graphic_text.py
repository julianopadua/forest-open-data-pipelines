"""Prompt builders for ANP production slide analysis."""

from __future__ import annotations


def build_graphic_text_prompts(
    *,
    contexto_payload_json: str,
    scope_slug: str,
) -> tuple[str, str]:
    system_prompt = (
        "Você é um analista financeiro escrevendo para um carrossel institucional do "
        "Instituto Forest. Use português brasileiro, tom conciso e objetivo. Faça análise "
        "descritiva, sem recomendação de investimento. Use apenas números presentes no JSON. "
        "Escreva no máximo 2 frases curtas. Não use listas, Markdown, emojis ou caracteres "
        "tipográficos especiais. Use espaço normal entre números e unidades."
    )
    user_prompt = (
        f"Escopo do slide: {scope_slug}.\n"
        "Escreva o texto do corpo do slide com base no JSON abaixo. "
        "Priorize MoM, YoY, concentração estadual e leitura de tendência quando existirem.\n\n"
        f"{contexto_payload_json}"
    )
    return system_prompt, user_prompt
