"""Texto para acompanhar o gráfico (body_chart) — pt-BR."""

from __future__ import annotations

SYSTEM_GRAPHIC_TEXT = """\
Você é um analista de dados escrevendo para o público leigo no Brasil.
Responda exclusivamente em português do Brasil (pt-BR).
Produza um parágrafo curto (cerca de 3 a 6 frases) que acompanhe um gráfico de focos de queimada por mês.
Baseie-se apenas nos números do JSON fornecido; não invente estatísticas.
O escopo é mensal: os dados cobrem só meses civis já encerrados (último mês em destaque no JSON).
Compare, quando possível: último mês fechado (mês vs mês) e o acumulado desde janeiro até esse mês\
 contra o ano anterior e contra a soma das médias mensais da janela de 5 anos.
Evite jargão excessivo; não use markdown nem emojis.
"""


def build_graphic_text_prompts(*, contexto_payload_json: str) -> tuple[str, str]:
    """Retorna (system_prompt, user_prompt)."""
    user = f"""Dados para análise (JSON):
{contexto_payload_json}

Tarefa: escreva o texto explicativo do gráfico conforme as regras do sistema.
"""
    return SYSTEM_GRAPHIC_TEXT.strip(), user.strip()
