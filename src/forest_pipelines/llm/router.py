# src/forest_pipelines/llm/router.py
from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from forest_pipelines.llm.groq_client import GroqClient, GroqParams
from forest_pipelines.settings import LLMSettings


@dataclass(frozen=True)
class RoutedJSONResult:
    data: dict[str, Any]
    model: str
    raw_text: str


@dataclass(frozen=True)
class RoutedTextResult:
    text: str
    model: str
    raw_text: str


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def _validate_non_empty_text(text: str) -> None:
    if not text.strip():
        raise ValueError("Resposta do modelo vazia.")


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()

    fence_match = _JSON_FENCE_RE.search(stripped)
    if fence_match:
        candidate = fence_match.group(1).strip()
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    first = stripped.find("{")
    last = stripped.rfind("}")
    if first != -1 and last != -1 and last > first:
        candidate = stripped[first : last + 1]
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    raise ValueError("Não foi possível extrair um objeto JSON válido da resposta do modelo.")


def _validate_required_keys(data: dict[str, Any], required_keys: list[str]) -> None:
    missing = [k for k in required_keys if k not in data]
    if missing:
        raise KeyError(f"Resposta do modelo sem chaves obrigatórias: {missing}")


async def _generate_json_async(
    llm_settings: LLMSettings,
    system_prompt: str,
    user_prompt: str,
    required_keys: list[str],
    model_override: str | None = None,
) -> RoutedJSONResult:
    if llm_settings.provider != "groq":
        raise NotImplementedError(
            f"Provider LLM não suportado nesta versão: {llm_settings.provider}"
        )

    api_key = os.getenv(llm_settings.api_key_env, "").strip()
    if not api_key:
        raise RuntimeError(
            f"Env var da LLM ausente ou vazia: {llm_settings.api_key_env}"
        )

    candidate_models: list[str] = []
    if model_override:
        candidate_models.append(model_override)
    candidate_models.extend(m for m in llm_settings.preferred_models if m and m != model_override)

    if not candidate_models:
        raise RuntimeError("Nenhum modelo configurado para roteamento LLM.")

    client = GroqClient(api_key=api_key, timeout_s=llm_settings.timeout_s)

    try:
        last_error: Exception | None = None

        for model in candidate_models:
            try:
                raw_text, _headers = await client.chat_raw(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    params=GroqParams(
                        temperature=llm_settings.temperature,
                        top_p=llm_settings.top_p,
                        max_completion_tokens=llm_settings.max_completion_tokens,
                        stream=llm_settings.stream,
                        service_tier=llm_settings.service_tier,
                        reasoning_effort=llm_settings.reasoning_effort,
                    ),
                )

                parsed = _extract_json_object(raw_text)
                _validate_required_keys(parsed, required_keys)

                return RoutedJSONResult(
                    data=parsed,
                    model=model,
                    raw_text=raw_text,
                )
            except Exception as e:  # noqa: BLE001
                last_error = e
                continue

        raise RuntimeError("Falha em todos os modelos candidatos.") from last_error
    finally:
        await client.close()


async def _generate_text_async(
    llm_settings: LLMSettings,
    system_prompt: str,
    user_prompt: str,
    model_override: str | None = None,
) -> RoutedTextResult:
    if llm_settings.provider != "groq":
        raise NotImplementedError(
            f"Provider LLM não suportado nesta versão: {llm_settings.provider}"
        )

    api_key = os.getenv(llm_settings.api_key_env, "").strip()
    if not api_key:
        raise RuntimeError(
            f"Env var da LLM ausente ou vazia: {llm_settings.api_key_env}"
        )

    candidate_models: list[str] = []
    if model_override:
        candidate_models.append(model_override)
    candidate_models.extend(m for m in llm_settings.preferred_models if m and m != model_override)

    if not candidate_models:
        raise RuntimeError("Nenhum modelo configurado para roteamento LLM.")

    client = GroqClient(api_key=api_key, timeout_s=llm_settings.timeout_s)

    try:
        last_error: Exception | None = None

        for model in candidate_models:
            try:
                raw_text, _headers = await client.chat_raw(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    params=GroqParams(
                        temperature=llm_settings.temperature,
                        top_p=llm_settings.top_p,
                        max_completion_tokens=llm_settings.max_completion_tokens,
                        stream=llm_settings.stream,
                        service_tier=llm_settings.service_tier,
                        reasoning_effort=llm_settings.reasoning_effort,
                    ),
                )

                text = (raw_text or "").strip()
                _validate_non_empty_text(text)
                return RoutedTextResult(text=text, model=model, raw_text=raw_text)
            except Exception as e:  # noqa: BLE001
                last_error = e
                continue

        raise RuntimeError("Falha em todos os modelos candidatos.") from last_error
    finally:
        await client.close()


def generate_text(
    llm_settings: LLMSettings,
    system_prompt: str,
    user_prompt: str,
    model_override: str | None = None,
) -> RoutedTextResult:
    return asyncio.run(
        _generate_text_async(
            llm_settings=llm_settings,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model_override=model_override,
        )
    )


def generate_json(
    llm_settings: LLMSettings,
    system_prompt: str,
    user_prompt: str,
    required_keys: list[str],
    model_override: str | None = None,
) -> RoutedJSONResult:
    return asyncio.run(
        _generate_json_async(
            llm_settings=llm_settings,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            required_keys=required_keys,
            model_override=model_override,
        )
    )