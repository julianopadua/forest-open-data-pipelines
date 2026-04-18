"""Registro de tópicos e componentes de texto para posts sociais."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from forest_pipelines.llm.router import RoutedTextResult, generate_text
from forest_pipelines.settings import LLMSettings
from forest_pipelines.social.llm.payloads.focos_incendio import (
    build_focos_incendio_llm_payload,
    payload_to_prompt_block,
)
from forest_pipelines.social.llm.prompts.focos_incendio_br.graphic_text import (
    build_graphic_text_prompts,
)
from forest_pipelines.social.llm.prompts.focos_incendio_br.post_description import (
    build_post_description_prompts,
)
from forest_pipelines.social.logging import log_llm_roundtrip, log_stage

TOPIC_FOCOS_INCENDIO_BR = "focos_incendio_br"


def run_topic_components(
    topic_id: str,
    spec: dict[str, Any],
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Executa todos os componentes de texto de um tópico.

    Retorna dict com chaves de componente (ex.: post_description, graphic_text) e,
    por componente, {"text", "model"}.
    """
    if topic_id == TOPIC_FOCOS_INCENDIO_BR:
        return _run_focos_incendio_br(
            spec, reference_date, llm_settings, topic_id=topic_id, logger=logger
        )

    raise ValueError(f"Tópico social LLM desconhecido: {topic_id!r}")


def _run_focos_incendio_br(
    spec: dict[str, Any],
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    topic_id: str,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    payload = build_focos_incendio_llm_payload(spec, reference_date)
    block = payload_to_prompt_block(payload)
    if logger:
        log_stage(
            logger,
            "llm_payload_ready",
            {
                "topic": topic_id,
                "reference_date": reference_date.isoformat(),
                "payload_keys": sorted(payload.keys()),
                "context_json_chars": len(block),
            },
        )

    sys1, usr1 = build_post_description_prompts(
        reference_date=reference_date,
        contexto_payload_json=block,
    )
    r1: RoutedTextResult = generate_text(llm_settings, sys1, usr1)
    if logger:
        log_llm_roundtrip(
            logger,
            topic_id=topic_id,
            component="post_description",
            system_prompt=sys1,
            user_prompt=usr1,
            result=r1,
        )

    sys2, usr2 = build_graphic_text_prompts(contexto_payload_json=block)
    r2: RoutedTextResult = generate_text(llm_settings, sys2, usr2)
    if logger:
        log_llm_roundtrip(
            logger,
            topic_id=topic_id,
            component="graphic_text",
            system_prompt=sys2,
            user_prompt=usr2,
            result=r2,
        )

    return {
        "post_description": {"text": r1.text, "model": r1.model},
        "graphic_text": {"text": r2.text, "model": r2.model},
    }
