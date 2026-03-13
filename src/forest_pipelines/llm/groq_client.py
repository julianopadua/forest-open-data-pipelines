# src/forest_pipelines/llm/groq_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from groq import AsyncGroq


@dataclass(frozen=True)
class GroqParams:
    temperature: float
    top_p: float
    max_completion_tokens: int
    stream: bool
    service_tier: str | None
    reasoning_effort: str | None


def _effective_reasoning_effort(model: str, requested: str | None) -> str | None:
    if not requested:
        return None

    m = model.strip()

    if m in {"openai/gpt-oss-20b", "openai/gpt-oss-120b"}:
        if requested in {"low", "medium", "high"}:
            return requested
        if requested == "default":
            return "medium"
        return None

    if m in {"qwen/qwen3-32b"}:
        if requested in {"none", "default"}:
            return requested
        if requested in {"low", "medium", "high"}:
            return "default"
        return None

    return None


class GroqClient:
    def __init__(self, api_key: str, timeout_s: float = 90.0) -> None:
        self._client = AsyncGroq(
            api_key=api_key,
            timeout=timeout_s,
            max_retries=0,
        )

    async def close(self) -> None:
        await self._client.close()

    async def chat_raw(
        self,
        model: str,
        messages: list[dict[str, str]],
        params: GroqParams,
    ) -> tuple[str, Mapping[str, str]]:
        if params.stream:
            raise RuntimeError("stream=True não é suportado nesta integração de reports.")

        eff_reason = _effective_reasoning_effort(model, params.reasoning_effort)

        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": params.temperature,
            "top_p": params.top_p,
            "max_completion_tokens": params.max_completion_tokens,
            "stream": False,
        }

        if params.service_tier:
            kwargs["service_tier"] = params.service_tier

        if eff_reason:
            kwargs["reasoning_effort"] = eff_reason

        raw = await self._client.chat.completions.with_raw_response.create(**kwargs)
        headers = raw.headers
        completion = await raw.parse()

        message = completion.choices[0].message
        text = message.content or ""

        if isinstance(text, list):
            joined: list[str] = []
            for item in text:
                if isinstance(item, str):
                    joined.append(item)
                elif isinstance(item, dict) and "text" in item:
                    joined.append(str(item["text"]))
            text = "\n".join(joined)

        return str(text), headers