# src/forest_pipelines/reports/definitions/base.py
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class LocalizedText(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pt: str
    en: str


def localized_text_dict(
    value: str | LocalizedText | None,
    *,
    fallback_pt: str | None = None,
    fallback_en: str | None = None,
) -> dict[str, str] | None:
    if value is None:
        if fallback_pt is None and fallback_en is None:
            return None

        pt = (fallback_pt or fallback_en or "").strip()
        en = (fallback_en or fallback_pt or "").strip()
        return {"pt": pt, "en": en}

    if isinstance(value, LocalizedText):
        pt = value.pt.strip()
        en = value.en.strip()

        if not pt and en:
            pt = en
        if not en and pt:
            en = pt

        return {"pt": pt, "en": en}

    text = str(value).strip()
    pt = (fallback_pt if fallback_pt is not None else text).strip()
    en = (fallback_en if fallback_en is not None else text).strip()

    if not pt and en:
        pt = en
    if not en and pt:
        en = pt

    return {"pt": pt, "en": en}


class ReportDatasetCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    local_relative_dir: str
    file_glob: str = "*.zip"
    recent_years: int | None = None


class ReportColumnsCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")

    datetime_candidates: list[str] = Field(
        default_factory=lambda: [
            "data_hora_gmt",
            "data_hora",
            "datahora",
            "data",
            "date",
        ]
    )
    state_candidates: list[str] = Field(
        default_factory=lambda: [
            "estado",
            "uf",
            "estado_sigla",
            "state",
        ]
    )


class ReportDisplayCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")

    monthly_points: int = 24
    annual_years: int = 8
    top_states_limit: int = 10


class ReportEditorialCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")

    publish_generated_as_live: bool = True
    overrides_file: str | None = None


class ReportLLMCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    provider: str = "groq"
    model: str | None = None
    max_chars_per_block: int = 700


class ReportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    title: str | LocalizedText
    bucket_prefix: str
    source_label: str | LocalizedText
    summary: str | LocalizedText | None = None
    dataset: ReportDatasetCfg
    columns: ReportColumnsCfg = Field(default_factory=ReportColumnsCfg)
    display: ReportDisplayCfg = Field(default_factory=ReportDisplayCfg)
    editorial: ReportEditorialCfg = Field(default_factory=ReportEditorialCfg)
    llm: ReportLLMCfg = Field(default_factory=ReportLLMCfg)

    def resolve_overrides_path(self, root: Path) -> Path | None:
        if not self.editorial.overrides_file:
            return None
        return (root / self.editorial.overrides_file).resolve()


def load_report_cfg(reports_dir: Path, report_id: str) -> ReportConfig:
    path = reports_dir / f"{report_id}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Config do report não encontrada: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    return ReportConfig.model_validate(raw)