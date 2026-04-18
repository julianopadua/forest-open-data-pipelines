from __future__ import annotations

from datetime import date

import pytest

from forest_pipelines.llm.router import _validate_non_empty_text
from forest_pipelines.settings import LLMSettings
from forest_pipelines.social.llm.registry import run_topic_components


def _minimal_spec() -> dict:
    return {
        "schema_version": 3,
        "month_labels": [
            "Jan",
            "Fev",
            "Mar",
            "Abr",
            "Mai",
            "Jun",
            "Jul",
            "Ago",
            "Set",
            "Out",
            "Nov",
            "Dez",
        ],
        "series": {
            "current": {"key": "current", "label": "2026", "values": [100, 110, 120, None, None, None, None, None, None, None, None, None]},
            "previous": {"key": "previous", "label": "2025", "values": [90] * 12},
            "avg_5y": {
                "key": "avg_5y",
                "label": "Média 2021–2025 (por mês)",
                "values": [95.0, 100.0, 105.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
        },
        "metadata": {
            "latest_year": 2026,
            "previous_year": 2025,
            "avg_window_years_from": 2021,
            "avg_window_years_to": 2025,
            "last_closed_month": 3,
            "published_at_label": "Mar 2026",
            "source": "INPE (teste)",
            "biome_scope": "nacional",
            "biome_label_pt": "Brasil (Nacional)",
        },
    }


def _llm_settings() -> LLMSettings:
    return LLMSettings(
        provider="groq",
        api_key_env="GROQ_API_KEY",
        temperature=0.2,
        top_p=1.0,
        max_completion_tokens=700,
        stream=False,
        service_tier=None,
        reasoning_effort="medium",
        timeout_s=90.0,
        preferred_models=("model-a", "model-b"),
    )


def test_validate_non_empty_text_rejects_empty() -> None:
    with pytest.raises(ValueError, match="vazia"):
        _validate_non_empty_text("")


def test_run_topic_components_can_limit_to_single_component(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_generate_text(llm_settings: LLMSettings, system_prompt: str, user_prompt: str) -> object:
        calls.append(user_prompt)

        class _Result:
            text = "texto ok"
            model = "model-a"
            raw_text = "texto ok"

        return _Result()

    monkeypatch.setattr("forest_pipelines.social.llm.registry.generate_text", fake_generate_text)

    out = run_topic_components(
        "focos_incendio_br",
        _minimal_spec(),
        date(2026, 4, 18),
        _llm_settings(),
        components=("post_description",),
    )

    assert list(out.keys()) == ["post_description"]
    assert len(calls) == 1
