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
from forest_pipelines.social.llm.prompts.focos_incendio_br.carousel_post_description import (
    build_carousel_post_description_prompts,
)
from forest_pipelines.social.llm.prompts.focos_incendio_br.graphic_text import (
    build_graphic_text_prompts,
)
from forest_pipelines.social.llm.prompts.focos_incendio_br.post_description import (
    build_post_description_prompts,
)
from forest_pipelines.social.logging import log_llm_roundtrip, log_stage

TOPIC_FOCOS_INCENDIO_BR = "focos_incendio_br"
COMPONENT_POST_DESCRIPTION = "post_description"
COMPONENT_GRAPHIC_TEXT = "graphic_text"
DEFAULT_COMPONENTS = (COMPONENT_POST_DESCRIPTION, COMPONENT_GRAPHIC_TEXT)


def generate_carousel_instagram_caption(
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    topic_id: str = TOPIC_FOCOS_INCENDIO_BR,
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """Uma única legenda para o carrossel (contexto mínimo)."""
    sys_u, usr = build_carousel_post_description_prompts(reference_date=reference_date)
    r = generate_text(llm_settings, sys_u, usr)
    if logger:
        log_llm_roundtrip(
            logger,
            topic_id=topic_id,
            component="carousel_post_description",
            system_prompt=sys_u,
            user_prompt=usr,
            result=r,
            scope="carousel",
        )
    return {"text": r.text, "model": r.model}


def generate_graphic_text_for_carousel_scope(
    spec: dict[str, Any],
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    biome_label_pt: str,
    escopo_nacional: bool,
    scope_slug: str,
    topic_id: str = TOPIC_FOCOS_INCENDIO_BR,
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """Texto do slide (body_chart) para um recorte do carrossel."""
    payload = build_focos_incendio_llm_payload(
        spec, reference_date, biome=biome_label_pt
    )
    block = payload_to_prompt_block(payload)
    if logger:
        log_stage(
            logger,
            "llm_payload_ready",
            {
                "topic": topic_id,
                "reference_date": reference_date.isoformat(),
                "scope": scope_slug,
                "payload_keys": sorted(payload.keys()),
                "context_json_chars": len(block),
            },
        )

    sys2, usr2 = build_graphic_text_prompts(
        contexto_payload_json=block,
        biome_label_pt=biome_label_pt,
        escopo_nacional=escopo_nacional,
    )
    r2: RoutedTextResult = generate_text(llm_settings, sys2, usr2)
    if logger:
        log_llm_roundtrip(
            logger,
            topic_id=topic_id,
            component=COMPONENT_GRAPHIC_TEXT,
            system_prompt=sys2,
            user_prompt=usr2,
            result=r2,
            scope=scope_slug,
        )
    return {"text": r2.text, "model": r2.model}


def run_topic_components(
    topic_id: str,
    spec: dict[str, Any],
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    components: tuple[str, ...] = DEFAULT_COMPONENTS,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Executa todos os componentes de texto de um tópico.

    Retorna dict com chaves de componente (ex.: post_description, graphic_text) e,
    por componente, {"text", "model"}.
    """
    if topic_id == TOPIC_FOCOS_INCENDIO_BR:
        return _run_focos_incendio_br(
            spec,
            reference_date,
            llm_settings,
            topic_id=topic_id,
            components=components,
            logger=logger,
        )

    raise ValueError(f"Tópico social LLM desconhecido: {topic_id!r}")


def _run_focos_incendio_br(
    spec: dict[str, Any],
    reference_date: date,
    llm_settings: LLMSettings,
    *,
    topic_id: str,
    components: tuple[str, ...],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    requested = tuple(dict.fromkeys(components))
    invalid = [name for name in requested if name not in DEFAULT_COMPONENTS]
    if invalid:
        raise ValueError(f"Componentes LLM desconhecidos: {invalid}")

    meta = spec.get("metadata") or {}
    biome_label = str(meta.get("biome_label_pt", "Brasil (Nacional)"))
    escopo_nacional = meta.get("biome_scope") == "nacional"

    payload = build_focos_incendio_llm_payload(
        spec, reference_date, biome=biome_label
    )
    block = payload_to_prompt_block(payload)
    if logger:
        log_stage(
            logger,
            "llm_payload_ready",
            {
                "topic": topic_id,
                "reference_date": reference_date.isoformat(),
                "components": list(requested),
                "payload_keys": sorted(payload.keys()),
                "context_json_chars": len(block),
            },
        )

    out: dict[str, Any] = {}

    if COMPONENT_POST_DESCRIPTION in requested:
        sys1, usr1 = build_post_description_prompts(
            reference_date=reference_date,
            contexto_payload_json=block,
        )
        r1: RoutedTextResult = generate_text(llm_settings, sys1, usr1)
        if logger:
            log_llm_roundtrip(
                logger,
                topic_id=topic_id,
                component=COMPONENT_POST_DESCRIPTION,
                system_prompt=sys1,
                user_prompt=usr1,
                result=r1,
            )
        out[COMPONENT_POST_DESCRIPTION] = {"text": r1.text, "model": r1.model}

    if COMPONENT_GRAPHIC_TEXT in requested:
        sys2, usr2 = build_graphic_text_prompts(
            contexto_payload_json=block,
            biome_label_pt=biome_label,
            escopo_nacional=escopo_nacional,
        )
        r2: RoutedTextResult = generate_text(llm_settings, sys2, usr2)
        if logger:
            log_llm_roundtrip(
                logger,
                topic_id=topic_id,
                component=COMPONENT_GRAPHIC_TEXT,
                system_prompt=sys2,
                user_prompt=usr2,
                result=r2,
            )
        out[COMPONENT_GRAPHIC_TEXT] = {"text": r2.text, "model": r2.model}

    return out
