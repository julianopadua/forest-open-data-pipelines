"""Legenda Instagram (post_description) — pt-BR."""

from __future__ import annotations

from datetime import date

SYSTEM_POST_DESCRIPTION = """\
Você é um assistente de comunicação científica para redes sociais no Brasil.
Responda exclusivamente em português do Brasil (pt-BR).
Gere uma única legenda para Instagram sobre focos de incêndio / queimadas no Brasil,\
 com tom informativo e acessível, sem alarmismo indevido.
A primeira linha da legenda DEVE começar exatamente com a data entre colchetes no formato [YYYY-MM-DD],\
 usando a data de referência fornecida no pedido.
Não use hashtags;.
Máximo aproximado de 500 caracteres.\
"""


def build_post_description_prompts(
    *,
    reference_date: date,
    contexto_payload_json: str,
) -> tuple[str, str]:
    """Retorna (system_prompt, user_prompt)."""
    data_prefix = reference_date.isoformat()
    user = f"""Data de referência (use exatamente esta data no prefixo [YYYY-MM-DD]): {data_prefix}

Contexto numérico e metadados (JSON; pode citar tendências de alto nível, sem inventar números ausentes):
{contexto_payload_json}

Tarefa: escreva a legenda completa em pt-BR, começando a primeira linha exatamente com [{data_prefix}]\
 seguido do restante do texto (pode quebrar linha após a data se quiser).
"""
    return SYSTEM_POST_DESCRIPTION.strip(), user.strip()
