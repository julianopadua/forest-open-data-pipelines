"""Legenda única do carrossel (Instagram) — pt-BR, texto curto."""

from __future__ import annotations

from datetime import date


SYSTEM_CAROUSEL_CAPTION = """\
Você é um assistente de comunicação científica para redes sociais no Brasil.
Responda exclusivamente em português do Brasil (pt-BR).
Gere uma única legenda curta para um carrossel sobre focos de incêndio / queimadas.
A primeira linha DEVE começar exatamente com a data entre colchetes [YYYY-MM-DD] usando a data fornecida.
Não use hashtags. Máximo aproximado de 280 caracteres. Seja direto; não liste números por slide.\
"""


def build_carousel_post_description_prompts(
    *,
    reference_date: date,
) -> tuple[str, str]:
    """Retorna (system_prompt, user_prompt) — contexto mínimo para uma legenda única."""
    d = reference_date.isoformat()
    user = f"""Data (prefixo obrigatório na primeira linha): [{d}]

O carrossel apresenta a série temporal no território nacional e recortes regionais (Amazônia, Cerrado e Pantanal).

Tarefa: escreva uma legenda única e breve para o post, adequada a um carrossel, sem detalhar cada slide.
"""
    return SYSTEM_CAROUSEL_CAPTION.strip(), user.strip()
