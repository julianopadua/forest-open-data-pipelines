# src/forest_pipelines/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    root: Path
    data_dir: Path
    logs_dir: Path
    datasets_dir: Path
    supabase_bucket_open_data: str


def load_settings(config_path: str) -> Settings:
    load_dotenv()
    root = Path(config_path).resolve().parent.parent  # .../configs/app.yml -> repo root

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    data_dir = (root / cfg["app"]["data_dir"]).resolve()
    logs_dir = (root / cfg["app"]["logs_dir"]).resolve()
    datasets_dir = (root / cfg["datasets_dir"]).resolve()

    bucket_env = cfg["supabase"]["bucket_open_data_env"]
    bucket = os.getenv(bucket_env, "open-data")

    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        root=root,
        data_dir=data_dir,
        logs_dir=logs_dir,
        datasets_dir=datasets_dir,
        supabase_bucket_open_data=bucket,
    )
