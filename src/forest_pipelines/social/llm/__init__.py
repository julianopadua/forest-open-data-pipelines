"""Geração de textos para posts sociais via Groq (tópicos registrados)."""

from forest_pipelines.social.llm.registry import TOPIC_FOCOS_INCENDIO_BR, run_topic_components

__all__ = ["TOPIC_FOCOS_INCENDIO_BR", "run_topic_components"]
