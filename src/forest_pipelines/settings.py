# src/forest_pipelines/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class LLMSettings:
    provider: str
    api_key_env: str
    temperature: float
    top_p: float
    max_completion_tokens: int
    stream: bool
    service_tier: str | None
    reasoning_effort: str | None
    timeout_s: float
    preferred_models: tuple[str, ...]


@dataclass(frozen=True)
class Settings:
    root: Path
    data_dir: Path
    logs_dir: Path
    docs_dir: Path
    datasets_dir: Path
    reports_dir: Path
    supabase_bucket_open_data: str
    llm: LLMSettings


def load_settings(config_path: str) -> Settings:
    load_dotenv()
    root = Path(config_path).resolve().parent.parent

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    data_dir = (root / cfg["app"]["data_dir"]).resolve()
    logs_dir = (root / cfg["app"]["logs_dir"]).resolve()
    docs_dir = (root / cfg["app"].get("docs_dir", "docs")).resolve()
    datasets_dir = (root / cfg["datasets_dir"]).resolve()
    reports_dir = (root / cfg.get("reports_dir", "configs/reports")).resolve()

    bucket_env = cfg["supabase"]["bucket_open_data_env"]
    bucket = os.getenv(bucket_env, "open-data")

    llm_cfg = cfg.get("llm", {}) or {}
    preferred_models = llm_cfg.get("preferred_models", []) or []

    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        root=root,
        data_dir=data_dir,
        logs_dir=logs_dir,
        docs_dir=docs_dir,
        datasets_dir=datasets_dir,
        reports_dir=reports_dir,
        supabase_bucket_open_data=bucket,
        llm=LLMSettings(
            provider=str(llm_cfg.get("provider", "groq")).strip(),
            api_key_env=str(llm_cfg.get("api_key_env", "GROQ_API_KEY")).strip(),
            temperature=float(llm_cfg.get("temperature", 0.2)),
            top_p=float(llm_cfg.get("top_p", 1.0)),
            max_completion_tokens=int(llm_cfg.get("max_completion_tokens", 700)),
            stream=bool(llm_cfg.get("stream", False)),
            service_tier=llm_cfg.get("service_tier"),
            reasoning_effort=llm_cfg.get("reasoning_effort"),
            timeout_s=float(llm_cfg.get("timeout_s", 90.0)),
            preferred_models=tuple(str(m).strip() for m in preferred_models if str(m).strip()),
        ),
    )
