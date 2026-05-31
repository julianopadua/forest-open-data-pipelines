from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import yaml

from forest_pipelines.datasets.inpe.coids_directory import CoidsEntry, discover_files, parse_last_modified
from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import profiled_item


RE_YEAR = re.compile(r"(?P<year>(19|20)\d{2})")
RE_YYYYMM = re.compile(r"(?P<year>(19|20)\d{2})(?P<month>0[1-9]|1[0-2])")
RE_YYYYMMDD = re.compile(r"(?P<year>(19|20)\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])")
RE_10MIN = re.compile(
    r"(?P<date>(19|20)\d{6})_(?P<hour>[0-2]\d[0-5]\d)"
)
RE_STATE_SEGMENT = re.compile(r"/EstadosBr_sat_ref/(?P<uf>[A-Z]{2})/", re.IGNORECASE)


@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_url: str
    bucket_prefix: str
    period_strategy: str
    recursive: bool = False
    max_depth: int = 4
    notes: str = ""
    kind: str = "data"


def make_sync(config_name: str) -> Callable[..., dict[str, Any]]:
    def sync(
        settings: Any,
        storage: Any,
        logger: Any,
        latest_months: int | None = None,
    ) -> dict[str, Any]:
        cfg = load_dataset_cfg(settings.datasets_dir, f"inpe/{config_name}")
        return sync_from_cfg(
            cfg=cfg,
            logger=logger,
            latest_months=latest_months,
        )

    return sync


def load_dataset_cfg(datasets_dir: Path, dataset_key: str) -> DatasetCfg:
    path = datasets_dir / f"{dataset_key}.yml"
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DatasetCfg(
        id=str(raw["id"]),
        title=str(raw["title"]),
        source_url=str(raw["source_url"]),
        bucket_prefix=str(raw["bucket_prefix"]),
        period_strategy=str(raw.get("period_strategy", "filename_date")),
        recursive=bool(raw.get("recursive", False)),
        max_depth=int(raw.get("max_depth", 4)),
        notes=str(raw.get("notes", "")),
        kind=str(raw.get("kind", "data")),
    )


def sync_from_cfg(
    *,
    cfg: DatasetCfg,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    logger.info("Indexando COIDS INPE: %s", cfg.source_url)
    resources = discover_files(
        cfg.source_url,
        recursive=cfg.recursive,
        max_depth=cfg.max_depth,
    )
    resources = sorted(resources, key=lambda entry: entry_period(entry, cfg.period_strategy), reverse=True)
    limit = latest_months if latest_months and latest_months > 0 else len(resources)
    selected = resources[:limit]

    items: list[dict[str, Any]] = []
    warnings: list[str] = []
    for entry in selected:
        period = entry_period(entry, cfg.period_strategy)
        logger.info("Perfilando %s: %s", period, entry.url)
        item = profiled_item(
            source_url=entry.url,
            filename=entry.filename,
            period=period,
            title=item_title(entry, period),
            logger=logger,
        )
        item["kind"] = cfg.kind
        if entry.last_modified_label and "release_time" not in item:
            item["release_time"] = entry.last_modified_label
        if entry.size_label:
            item["source_size_label"] = entry.size_label
        items.append(item)

    if not items:
        warnings.append(f"Nenhum arquivo publico encontrado em {cfg.source_url}")

    return build_manifest(
        dataset_id=cfg.id,
        title=cfg.title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        warnings=warnings,
        meta={
            "source_agency": "INPE - Programa Queimadas",
            "notes": cfg.notes,
            "custom_tags": {
                "total_items": len(items),
                "recursive": cfg.recursive,
                "period_strategy": cfg.period_strategy,
            },
        },
    )


def item_title(entry: CoidsEntry, period: str) -> str:
    return f"{entry.filename} ({period})"


def entry_period(entry: CoidsEntry, strategy: str) -> str:
    filename = entry.filename
    if strategy == "annual_state":
        state = _state_from_url(entry.url)
        year = _year_from_name(filename)
        return f"{state}/{year}" if state else year
    if strategy == "annual":
        return _year_from_name(filename)
    if strategy == "monthly":
        return _monthly_from_name(filename)
    if strategy == "daily":
        return _daily_from_name(filename)
    if strategy == "ten_min":
        return _ten_min_from_name(filename)
    if strategy == "modified_or_name":
        modified = parse_last_modified(entry.last_modified_label)
        if modified is not None:
            return modified.strftime("%Y-%m-%d %H:%M:%S")
        return Path(filename).stem
    return Path(filename).stem


def _year_from_name(filename: str) -> str:
    match = RE_YEAR.search(filename)
    if not match:
        return Path(filename).stem
    return match.group("year")


def _monthly_from_name(filename: str) -> str:
    match = RE_YYYYMM.search(filename)
    if not match:
        return Path(filename).stem
    return f"{match.group('year')}-{match.group('month')}"


def _daily_from_name(filename: str) -> str:
    match = RE_YYYYMMDD.search(filename)
    if not match:
        return Path(filename).stem
    return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"


def _ten_min_from_name(filename: str) -> str:
    match = RE_10MIN.search(filename)
    if not match:
        return _daily_from_name(filename)
    date_text = match.group("date")
    return f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]} {match.group('hour')}"


def _state_from_url(url: str) -> str | None:
    match = RE_STATE_SEGMENT.search(url)
    if not match:
        return None
    return match.group("uf").upper()
