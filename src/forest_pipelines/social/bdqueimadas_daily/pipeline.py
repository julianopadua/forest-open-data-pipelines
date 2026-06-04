from __future__ import annotations

import argparse
import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from forest_pipelines.datasets.inpe.coids_directory import fetch_directory_entries
from forest_pipelines.settings import load_settings
from forest_pipelines.social.logging import (
    get_social_bdqueimadas_daily_logger,
    log_llm_json_roundtrip,
    log_stage,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
APP_ROOT = REPO_ROOT / "apps" / "social-post-templates"
DEFAULT_SOURCE_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/"
DEFAULT_DATA_DIR = REPO_ROOT / "data" / "inpe_bdqueimadas_daily"
DEFAULT_GENERATED_ROOT = APP_ROOT / "public" / "generated"
DEFAULT_OUT_DIR = DEFAULT_GENERATED_ROOT / "bdqueimadas" / "daily"
DEFAULT_MANIFEST = APP_ROOT / "examples" / "bdqueimadas-daily-social.manifest.json"
DEFAULT_PUBLIC_MANIFEST = APP_ROOT / "public" / "examples" / "bdqueimadas-daily-social.manifest.json"
DEFAULT_LLM_JSON = DEFAULT_OUT_DIR / "bdqueimadas-daily-social_llm.json"
DEFAULT_APP_CONFIG = REPO_ROOT / "configs" / "app.yml"
DEFAULT_LOGS_DIR = REPO_ROOT / "logs"
LLM_TOPIC_ID = "bdqueimadas_daily"
IBGE_BRAZIL_GEOJSON_URL = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR"
    "?formato=application/vnd.geo+json&qualidade=minima"
)
REFERENCE_SATELLITE = "AQUA_M-T"
SOURCE_LABEL = "Fonte: INPE BDQueimadas"
LLM_SLIDE_KEYS = ("daily", "states", "biomes", "map")
STATE_REGION_BY_NAME = {
    "ACRE": "Norte",
    "ALAGOAS": "Nordeste",
    "AMAPA": "Norte",
    "AMAZONAS": "Norte",
    "BAHIA": "Nordeste",
    "CEARA": "Nordeste",
    "DISTRITO FEDERAL": "Centro-Oeste",
    "ESPIRITO SANTO": "Sudeste",
    "GOIAS": "Centro-Oeste",
    "MARANHAO": "Nordeste",
    "MATO GROSSO": "Centro-Oeste",
    "MATO GROSSO DO SUL": "Centro-Oeste",
    "MINAS GERAIS": "Sudeste",
    "PARA": "Norte",
    "PARAIBA": "Nordeste",
    "PARANA": "Sul",
    "PERNAMBUCO": "Nordeste",
    "PIAUI": "Nordeste",
    "RIO DE JANEIRO": "Sudeste",
    "RIO GRANDE DO NORTE": "Nordeste",
    "RIO GRANDE DO SUL": "Sul",
    "RONDONIA": "Norte",
    "RORAIMA": "Norte",
    "SANTA CATARINA": "Sul",
    "SAO PAULO": "Sudeste",
    "SERGIPE": "Nordeste",
    "TOCANTINS": "Norte",
}
STATE_REGION_BY_UF = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte",
}
EXPECTED_COLUMNS = (
    "id",
    "lat",
    "lon",
    "data_hora_gmt",
    "satelite",
    "municipio",
    "estado",
    "pais",
    "municipio_id",
    "estado_id",
    "pais_id",
    "numero_dias_sem_chuva",
    "precipitacao",
    "risco_fogo",
    "bioma",
    "frp",
)


@dataclass(frozen=True)
class DailyResource:
    period: date
    filename: str
    url: str


def run(
    *,
    source_url: str = DEFAULT_SOURCE_URL,
    data_dir: Path = DEFAULT_DATA_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    emit_manifest: Path = DEFAULT_MANIFEST,
    as_of: date | None = None,
    days: int = 7,
    run_llm: bool = False,
    app_config: Path = DEFAULT_APP_CONFIG,
    out_social_llm: Path = DEFAULT_LLM_JSON,
    logs_dir: Path | None = DEFAULT_LOGS_DIR,
) -> dict[str, Any]:
    started_at = perf_counter()
    logger = get_social_bdqueimadas_daily_logger(logs_dir)
    log_stage(
        logger,
        "pipeline_start",
        {
            "source_url": source_url,
            "data_dir": str(data_dir),
            "run_llm": run_llm,
            "days": days,
            "as_of": as_of.isoformat() if as_of else None,
        },
    )
    resources = select_daily_window(
        extract_daily_links(source_url),
        as_of=as_of,
        days=days,
    )
    if not resources:
        raise RuntimeError("Nenhum CSV diário encontrado para a janela solicitada.")

    cached = [
        download_daily_resource(resource, data_dir / "raw", refresh_existing=True)
        for resource in resources
    ]
    frames: list[pd.DataFrame] = []
    read_warnings: list[str] = []
    for resource, path in zip(resources, cached, strict=True):
        frame = read_daily_csv(path)
        missing = [name for name in EXPECTED_COLUMNS if name not in frame.columns]
        if missing:
            read_warnings.append(f"{resource.filename}: colunas ausentes {missing}")
        frame["source_period"] = resource.period.isoformat()
        frame["source_url"] = resource.url
        frames.append(frame)

    raw_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    filtered = filter_reference_satellite(raw_df)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily_counts = build_daily_counts(filtered, resources)
    state_rank = top_n_with_other(filtered, "estado", top_n=4)
    biome_rank = top_n_with_other(filtered, "bioma", top_n=4)
    region_rank = build_region_rank(filtered)

    charts = {
        "daily": out_dir / "bdqueimadas-daily-7d.png",
        "states": out_dir / "bdqueimadas-daily-states.png",
        "biomes": out_dir / "bdqueimadas-daily-biomes.png",
        "map": out_dir / "bdqueimadas-daily-map.png",
    }
    render_bar_chart(
        daily_counts,
        charts["daily"],
        title="Focos por dia",
        label_key="date",
        value_key="value",
    )
    render_bar_chart(
        state_rank,
        charts["states"],
        title="Estados com mais focos",
        label_key="label",
        value_key="value",
    )
    render_bar_chart(
        biome_rank,
        charts["biomes"],
        title="Biomas com mais focos",
        label_key="label",
        value_key="value",
    )
    map_status = render_density_map(
        filtered,
        charts["map"],
        geojson_cache=data_dir / "geo" / "brasil.geojson",
    )

    payload = build_llm_payload(
        resources=resources,
        daily_counts=daily_counts,
        state_rank=state_rank,
        biome_rank=biome_rank,
        region_rank=region_rank,
        total_focos=int(len(filtered)),
        total_raw_rows=int(len(raw_df)),
        map_status=map_status,
        warnings=read_warnings,
    )
    texts = deterministic_texts(payload)
    llm_error: str | None = None
    llm_model: str | None = None
    llm_models_by_slide: dict[str, str] = {}
    llm_errors_by_slide: dict[str, str] = {}
    if run_llm:
        log_stage(
            logger,
            "llm_run_start",
            {
                "component": "bdqueimadas_daily_slides",
                "slide_keys": list(LLM_SLIDE_KEYS),
                "window": payload.get("window"),
                "total_focos": payload.get("metrics", {}).get("total_focos_reference_satellite"),
            },
        )
        for slide_key in LLM_SLIDE_KEYS:
            component = f"bdqueimadas_daily_{slide_key}"
            log_stage(logger, "llm_slide_start", {"component": component, "slide_key": slide_key})
            try:
                result = run_llm_slide_text(payload, slide_key, app_config=app_config, logger=logger)
                slide_text = extract_llm_text_value(result.data, slide_key)
                if not slide_text:
                    raise ValueError("Resposta LLM sem campo text preenchido.")
                texts["slides"][slide_key] = normalize_visible_text(slide_text)
                llm_models_by_slide[slide_key] = result.model
                log_stage(
                    logger,
                    "llm_slide_ok",
                    {
                        "component": component,
                        "slide_key": slide_key,
                        "model": result.model,
                    },
                )
            except Exception as exc:
                llm_errors_by_slide[slide_key] = str(exc)
                log_stage(
                    logger,
                    "llm_slide_failed",
                    {
                        "component": component,
                        "slide_key": slide_key,
                        "error": str(exc),
                    },
                )
        unique_models = sorted(set(llm_models_by_slide.values()))
        llm_model = ", ".join(unique_models) if unique_models else None
        llm_error = json.dumps(llm_errors_by_slide, ensure_ascii=False) if llm_errors_by_slide else None
        log_stage(
            logger,
            "llm_run_done",
            {
                "component": "bdqueimadas_daily_slides",
                "slides_ok": sorted(llm_models_by_slide.keys()),
                "slides_failed": sorted(llm_errors_by_slide.keys()),
                "model": llm_model,
            },
        )
    else:
        log_stage(logger, "llm_skipped", {"reason": "run_llm=false"})
    texts = normalize_text_bundle(texts)

    generated_in_seconds = max(1, int(round(perf_counter() - started_at)))
    asset_version = datetime.now().strftime("%Y%m%d%H%M%S")
    sidecar = {
        "schema_version": 1,
        "topic": "bdqueimadas_daily",
        "reference_satellite": REFERENCE_SATELLITE,
        "payload": payload,
        "texts": texts,
        "llm_model": llm_model,
        "llm_error": llm_error,
        "llm_models_by_slide": llm_models_by_slide,
        "llm_errors_by_slide": llm_errors_by_slide,
        "generated_in_seconds": generated_in_seconds,
        "asset_version": asset_version,
        "charts": {key: str(path.resolve()) for key, path in charts.items()},
    }
    out_social_llm.parent.mkdir(parents=True, exist_ok=True)
    out_social_llm.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    write_manifest(
        emit_manifest,
        charts=charts,
        texts=texts,
        payload=payload,
        generated_in_seconds=generated_in_seconds,
        asset_version=asset_version,
    )
    log_stage(
        logger,
        "pipeline_done",
        {
            "manifest": str(emit_manifest),
            "sidecar": str(out_social_llm),
            "llm_model": llm_model,
            "llm_error": llm_error,
            "window": payload.get("window"),
        },
    )
    return sidecar


def extract_daily_links(source_url: str) -> list[DailyResource]:
    resources: list[DailyResource] = []
    for entry in fetch_directory_entries(source_url):
        filename = entry.filename
        if not filename.startswith("focos_diario_br_") or not filename.endswith(".csv"):
            continue
        stem = Path(filename).stem
        date_text = stem.rsplit("_", 1)[-1]
        try:
            period = datetime.strptime(date_text, "%Y%m%d").date()
        except ValueError:
            continue
        resources.append(DailyResource(period=period, filename=filename, url=entry.url))
    return sorted(resources, key=lambda item: item.period)


def select_daily_window(
    resources: list[DailyResource],
    *,
    as_of: date | None,
    days: int,
    today: date | None = None,
) -> list[DailyResource]:
    if not resources:
        return []
    if as_of is None:
        current_day = today or date.today()
        complete_resources = [resource for resource in resources if resource.period < current_day]
        ref = complete_resources[-1].period if complete_resources else resources[-1].period
    else:
        ref = as_of
    candidates = [resource for resource in resources if resource.period <= ref]
    return candidates[-days:]


def download_daily_resource(
    resource: DailyResource,
    cache_dir: Path,
    *,
    refresh_existing: bool = False,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / resource.filename
    if target.exists() and not refresh_existing:
        return target
    try:
        response = requests.get(resource.url, timeout=180)
        response.raise_for_status()
        target.write_bytes(response.content)
    except Exception:
        if target.exists():
            return target
        raise
    return target


def read_daily_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=None, engine="python")
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    return frame


def filter_reference_satellite(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "satelite" not in frame.columns:
        return frame.iloc[0:0].copy()
    out = frame[frame["satelite"].astype(str).str.strip().eq(REFERENCE_SATELLITE)].copy()
    for col in ("lat", "lon"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def build_daily_counts(
    frame: pd.DataFrame,
    resources: list[DailyResource],
) -> list[dict[str, Any]]:
    counts = frame.groupby("source_period").size().to_dict() if "source_period" in frame.columns else {}
    return [
        {"date": resource.period.isoformat(), "value": int(counts.get(resource.period.isoformat(), 0))}
        for resource in resources
    ]


def top_n_with_other(frame: pd.DataFrame, column: str, *, top_n: int) -> list[dict[str, Any]]:
    if frame.empty or column not in frame.columns:
        return [{"label": "Outros", "value": 0}]
    series = frame[column].fillna("Sem informação").astype(str).str.strip()
    series = series.replace({"": "Sem informação"})
    counts = series.value_counts()
    top = counts.head(top_n)
    rows = [{"label": str(label), "value": int(value)} for label, value in top.items()]
    other = int(counts.iloc[top_n:].sum()) if len(counts) > top_n else 0
    rows.append({"label": "Outros", "value": other})
    return rows


def build_region_rank(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty or "estado" not in frame.columns:
        return [{"label": "Não identificada", "value": 0}]
    regions = frame["estado"].fillna("").astype(str).map(region_for_state)
    counts = regions.value_counts()
    return [{"label": str(label), "value": int(value)} for label, value in counts.items()]


def region_for_state(value: str) -> str:
    normalized = normalize_state_name(value)
    if not normalized:
        return "Não identificada"
    return STATE_REGION_BY_UF.get(normalized) or STATE_REGION_BY_NAME.get(normalized, "Não identificada")


def normalize_state_name(value: str) -> str:
    import unicodedata

    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.upper().strip().split())


def value_share(row: dict[str, Any] | None, total: int) -> float | None:
    if not row or total <= 0:
        return None
    return round(100.0 * int(row.get("value", 0)) / total, 2)


def format_pt_int(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_label_pt(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "Não identificado"
    if text.isupper():
        return text.title()
    return text


ACCENT_REPLACEMENTS = {
    "analise": "análise",
    "analises": "análises",
    "area": "área",
    "ate": "até",
    "bioma nao identificado": "bioma não identificado",
    "concentracao": "concentração",
    "diario": "diário",
    "estado nao identificado": "estado não identificado",
    "grafico": "gráfico",
    "graficos": "gráficos",
    "minimo": "mínimo",
    "nao": "não",
    "periodo": "período",
    "proximos": "próximos",
    "regiao": "região",
    "satelite": "satélite",
}


def restore_pt_br_accents(text: str) -> str:
    out = str(text)
    for src, dst in sorted(ACCENT_REPLACEMENTS.items(), key=lambda item: len(item[0]), reverse=True):
        out = re.sub(rf"\b{re.escape(src)}\b", dst, out, flags=re.IGNORECASE)
    return out


def strip_emojis(text: str) -> str:
    out: list[str] = []
    for ch in text:
        if unicodedata.category(ch) in {"So", "Sk", "Cs"}:
            continue
        if ord(ch) in range(0x1F300, 0x1FAFF):
            continue
        out.append(ch)
    return re.sub(r"\s{2,}", " ", "".join(out)).strip()


def normalize_visible_text(text: str) -> str:
    out = strip_emojis(text)
    out = restore_pt_br_accents(out)
    out = re.sub(r"\s*[\u2013\u2014]\s*", ": ", out)
    out = re.sub(r"(\d+)\.(\d+)%", r"\1,\2%", out)

    def repl_count(match: re.Match[str]) -> str:
        return f"{int(match.group(1)):,}".replace(",", ".")

    return re.sub(r"\b(\d{4,})(?=\s+(?:focos|registros)\b)", repl_count, out)


def normalize_text_bundle(texts: dict[str, Any]) -> dict[str, Any]:
    slides = texts.get("slides") if isinstance(texts.get("slides"), dict) else {}
    return {
        **texts,
        "instagram_caption": normalize_visible_text(str(texts.get("instagram_caption", ""))),
        "slides": {
            key: normalize_visible_text(str(value))
            for key, value in slides.items()
        },
    }


def build_payload_highlights(
    *,
    daily_counts: list[dict[str, Any]],
    state_rank: list[dict[str, Any]],
    biome_rank: list[dict[str, Any]],
    region_rank: list[dict[str, Any]],
    total_focos: int,
) -> dict[str, Any]:
    max_day = max(daily_counts, key=lambda row: int(row["value"])) if daily_counts else None
    min_day = min(daily_counts, key=lambda row: int(row["value"])) if daily_counts else None
    top_state = state_rank[0] if state_rank else None
    top_biome = biome_rank[0] if biome_rank else None
    top_region = region_rank[0] if region_rank else None
    return {
        "cover": {
            "total_focos_reference_satellite": total_focos,
            "days_observed": len(daily_counts),
            "max_day": max_day,
            "min_day": min_day,
            "daily_counts": daily_counts,
        },
        "daily": {
            "daily_counts": daily_counts,
            "max_day": max_day,
            "min_day": min_day,
        },
        "states": {
            "top_states": state_rank,
            "top_state": top_state,
            "top_state_share_pct": value_share(top_state, total_focos),
        },
        "biomes": {
            "top_biomes": biome_rank,
            "top_biome": top_biome,
            "top_biome_share_pct": value_share(top_biome, total_focos),
        },
        "map": {
            "region_counts": region_rank,
            "top_region_by_focus_count": top_region,
            "top_region_share_pct": value_share(top_region, total_focos),
            "aggregation_note": "Região agregada por estado. Não é uma densidade por área.",
        },
    }


def render_bar_chart(
    rows: list[dict[str, Any]],
    out_path: Path,
    *,
    title: str,
    label_key: str,
    value_key: str,
) -> None:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    labels = [str(row[label_key]) for row in rows]
    values = [int(row[value_key]) for row in rows]
    fig, ax = plt.subplots(figsize=(10.8, 6.2), dpi=100)
    fig.patch.set_alpha(0)
    ax.set_facecolor((0, 0, 0, 0))
    bars = ax.bar(labels, values, color="#f97316")
    ax.set_title(title, color="white", fontsize=18, pad=12)
    ax.set_ylabel("Focos", color="white")
    ax.tick_params(axis="x", colors="white", labelrotation=18)
    ax.tick_params(axis="y", colors="white")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda value, _: f"{int(value):,}".replace(",", ".")))
    for spine in ax.spines.values():
        spine.set_color("white")
    for bar, value in zip(bars, values, strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:,}".replace(",", "."),
            ha="center",
            va="bottom",
            color="white",
            fontsize=11,
        )
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, transparent=True, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def render_density_map(
    frame: pd.DataFrame,
    out_path: Path,
    *,
    geojson_cache: Path,
) -> dict[str, Any]:
    import matplotlib.pyplot as plt

    geojson, geo_error = load_brazil_geojson(geojson_cache)
    fig, ax = plt.subplots(figsize=(10.8, 6.2), dpi=100)
    fig.patch.set_alpha(0)
    ax.set_facecolor((0, 0, 0, 0))
    ax.set_xlim(-74.5, -33.0)
    ax.set_ylim(-34.5, 6.0)
    ax.set_aspect("equal", adjustable="box")
    ax.axis("off")

    polygon_count = 0
    if geojson:
        for polygon in iter_geojson_polygons(geojson):
            polygon_count += 1
            xs = [point[0] for point in polygon]
            ys = [point[1] for point in polygon]
            ax.plot(xs, ys, color="white", linewidth=0.8, alpha=0.9)
    else:
        ax.plot(
            [-74.5, -33.0, -33.0, -74.5, -74.5],
            [-34.5, -34.5, 6.0, 6.0, -34.5],
            color="white",
            linewidth=0.8,
            alpha=0.55,
        )

    points = frame.dropna(subset=["lon", "lat"]) if {"lon", "lat"}.issubset(frame.columns) else frame.iloc[0:0]
    if not points.empty:
        hb = ax.hexbin(
            points["lon"],
            points["lat"],
            gridsize=46,
            cmap="YlOrRd",
            mincnt=1,
            linewidths=0,
            alpha=0.82,
        )
        cbar = fig.colorbar(hb, ax=ax, shrink=0.74, pad=0.02)
        cbar.ax.yaxis.set_tick_params(color="white")
        plt.setp(cbar.ax.get_yticklabels(), color="white")
        cbar.set_label("Focos", color="white")

    ax.text(
        0.01,
        0.02,
        "Densidade de focos nos ultimos 7 dias",
        transform=ax.transAxes,
        color="white",
        fontsize=13,
        ha="left",
    )
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, transparent=True, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)
    return {"geojson_ok": geojson is not None, "geojson_error": geo_error, "polygon_count": polygon_count}


def load_brazil_geojson(cache_path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8")), None
    try:
        response = requests.get(IBGE_BRAZIL_GEOJSON_URL, timeout=120)
        response.raise_for_status()
        data = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return data, None
    except Exception as exc:
        return None, str(exc)


def iter_geojson_polygons(geojson: dict[str, Any]) -> list[list[tuple[float, float]]]:
    polygons: list[list[tuple[float, float]]] = []
    features = geojson.get("features") if geojson.get("type") == "FeatureCollection" else [geojson]
    for feature in features or []:
        geometry = feature.get("geometry") if isinstance(feature, dict) and feature.get("type") == "Feature" else feature
        if not isinstance(geometry, dict):
            continue
        gtype = geometry.get("type")
        coords = geometry.get("coordinates")
        if gtype == "Polygon":
            polygons.extend(_polygon_rings(coords))
        elif gtype == "MultiPolygon":
            for polygon in coords or []:
                polygons.extend(_polygon_rings(polygon))
    return polygons


def _polygon_rings(coords: Any) -> list[list[tuple[float, float]]]:
    rings: list[list[tuple[float, float]]] = []
    for ring in coords or []:
        points: list[tuple[float, float]] = []
        for pair in ring:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                points.append((float(pair[0]), float(pair[1])))
        if points:
            rings.append(points)
    return rings


def build_llm_payload(
    *,
    resources: list[DailyResource],
    daily_counts: list[dict[str, Any]],
    state_rank: list[dict[str, Any]],
    biome_rank: list[dict[str, Any]],
    region_rank: list[dict[str, Any]],
    total_focos: int,
    total_raw_rows: int,
    map_status: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema": "bdqueimadas_daily_v1",
        "reference_satellite": REFERENCE_SATELLITE,
        "window": {
            "start_date": resources[0].period.isoformat(),
            "end_date": resources[-1].period.isoformat(),
            "days": len(resources),
        },
        "source_urls": [{"date": item.period.isoformat(), "url": item.url} for item in resources],
        "metrics": {
            "total_focos_reference_satellite": total_focos,
            "total_rows_all_satellites": total_raw_rows,
            "daily_counts": daily_counts,
            "top_states": state_rank,
            "top_biomes": biome_rank,
            "top_regions": region_rank,
        },
        "slide_context": build_payload_highlights(
            daily_counts=daily_counts,
            state_rank=state_rank,
            biome_rank=biome_rank,
            region_rank=region_rank,
            total_focos=total_focos,
        ),
        "map_status": map_status,
        "warnings": warnings,
    }


def deterministic_texts(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload["metrics"]
    window = payload["window"]
    slide_context = payload.get("slide_context", {})
    daily = slide_context.get("daily", {})
    states = slide_context.get("states", {})
    biomes = slide_context.get("biomes", {})
    map_context = slide_context.get("map", {})
    total = int(metrics["total_focos_reference_satellite"])
    max_day = daily.get("max_day") or {}
    min_day = daily.get("min_day") or {}
    top_state = states.get("top_state") or {}
    top_biome = biomes.get("top_biome") or {}
    top_region = map_context.get("top_region_by_focus_count") or {}
    return {
        "instagram_caption": (
            f"Entre {window['start_date']} e {window['end_date']}, o satélite "
            f"{REFERENCE_SATELLITE} registrou {format_pt_int(total)} focos de calor no Brasil."
        ),
        "slides": {
            "cover": (
                f"Foram {format_pt_int(total)} focos em {window['days']} dias. "
                f"O pico ocorreu em {max_day.get('date', 'data não informada')} "
                f"com {format_pt_int(int(max_day.get('value', 0)))} registros."
            ),
            "daily": (
                f"O maior dia foi {max_day.get('date', 'data não informada')}, "
                f"com {format_pt_int(int(max_day.get('value', 0)))} focos. "
                f"O menor foi {min_day.get('date', 'data não informada')}, "
                f"com {format_pt_int(int(min_day.get('value', 0)))}."
            ),
            "states": (
                f"{format_label_pt(top_state.get('label', 'Estado não identificado'))} liderou o ranking, "
                f"com {format_pt_int(int(top_state.get('value', 0)))} focos "
                f"({states.get('top_state_share_pct', 0)}% do recorte)."
            ),
            "biomes": (
                f"{format_label_pt(top_biome.get('label', 'Bioma não identificado'))} concentrou "
                f"{format_pt_int(int(top_biome.get('value', 0)))} focos "
                f"({biomes.get('top_biome_share_pct', 0)}% do recorte)."
            ),
            "map": (
                f"Na agregação por estado, a região {format_label_pt(top_region.get('label', 'não identificada'))} "
                f"somou {format_pt_int(int(top_region.get('value', 0)))} focos "
                f"({map_context.get('top_region_share_pct', 0)}% do recorte)."
            ),
        },
        "comments": [
            "Texto determinístico gerado sem LLM ou usado como fallback.",
        ],
    }


def build_slide_llm_prompts(slide_key: str, payload: dict[str, Any]) -> tuple[str, str]:
    if slide_key not in LLM_SLIDE_KEYS:
        raise ValueError(f"Slide LLM inválido: {slide_key}")
    instructions = {
        "daily": (
            "Explique a distribuição diária. Mencione o dia com mais focos, "
            "o dia com menos focos e a sequência de contagens por dia quando couber."
        ),
        "states": (
            "Explique o ranking de estados. Mencione o estado líder, sua participação "
            "no recorte e os demais estados principais quando couber."
        ),
        "biomes": (
            "Explique o ranking de biomas. Mencione o bioma líder, sua participação "
            "no recorte e os demais biomas principais quando couber."
        ),
        "map": (
            "Explique a agregação regional do mapa. Mencione a região com mais focos "
            "e deixe claro que a região foi agregada por estado, sem chamar isso de densidade por área."
        ),
    }
    context = {
        "reference_satellite": payload.get("reference_satellite"),
        "window": payload.get("window"),
        "slide_context": payload.get("slide_context", {}).get(slide_key, {}),
    }
    system_prompt = (
        "Você é um analista de dados ambientais. Responda somente com JSON válido. "
        "Retorne exatamente a chave text. Use português brasileiro com acentuação ortográfica correta. "
        "Use linguagem objetiva, sem afirmar causalidade e sem extrapolar os dados. "
        "Escreva um parágrafo curto para caber em slide quadrado. "
        "Não gere caption, capa, CTA, comentários ou campos extras. "
        "Não use emoji, emoticon, pictograma, símbolo decorativo ou travessão."
    )
    user_prompt = (
        f"Gere o texto do slide {slide_key} do carrossel diário de focos de calor no Brasil. "
        f"{instructions[slide_key]} "
        "Retorne JSON no formato {\"text\":\"...\"}. Contexto:\n"
        + json.dumps(context, ensure_ascii=False, indent=2)
    )
    return system_prompt, user_prompt


def run_llm_slide_text(
    payload: dict[str, Any],
    slide_key: str,
    *,
    app_config: Path,
    logger: logging.Logger | None = None,
):
    from forest_pipelines.llm.router import generate_json

    settings = load_settings(str(app_config))
    system_prompt, user_prompt = build_slide_llm_prompts(slide_key, payload)
    result = generate_json(
        settings.llm,
        system_prompt,
        user_prompt,
        required_keys=["text"],
    )
    if logger is not None:
        log_llm_json_roundtrip(
            logger,
            topic_id=LLM_TOPIC_ID,
            component=f"bdqueimadas_daily_{slide_key}",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            result=result,
            scope=slide_key,
        )
    return result


def build_llm_prompts(payload: dict[str, Any]) -> tuple[str, str]:
    return build_slide_llm_prompts("daily", payload)


def extract_llm_text_value(value: Any, slide_key: str | None = None) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict):
        text = value.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()
        if slide_key:
            slides = value.get("slides")
            if isinstance(slides, dict):
                nested = extract_llm_text_value(slides.get(slide_key))
                if nested:
                    return nested
    return None


def normalize_llm_texts(data: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    slides = data.get("slides") if isinstance(data.get("slides"), dict) else {}
    clean_slides = dict(fallback["slides"])
    for key in clean_slides:
        value = extract_llm_text_value(slides.get(key))
        if value:
            clean_slides[key] = normalize_visible_text(value)
    comments = data.get("comments")
    return {
        "instagram_caption": normalize_visible_text(str(fallback["instagram_caption"]).strip()),
        "slides": clean_slides,
        "comments": comments if isinstance(comments, list) else fallback["comments"],
    }


def public_generated_url(path: Path, asset_version: str) -> str:
    public_root = APP_ROOT / "public"
    try:
        rel = path.resolve().relative_to(public_root.resolve()).as_posix()
    except ValueError:
        rel = f"generated/{path.name}"
    return f"/{rel}?v={asset_version}"


def write_manifest(
    path: Path,
    *,
    charts: dict[str, Path],
    texts: dict[str, Any],
    payload: dict[str, Any],
    generated_in_seconds: int,
    asset_version: str,
) -> None:
    window = payload["window"]
    slides_text = texts.get("slides", {})
    slides = [
        {
            "type": "cover",
            "slots": {
                "series_label": "Monitor Diário",
                "title": "Focos de calor no Brasil",
                "summary": slides_text.get("cover", ""),
            },
            "slotStyles": {
                "title": {"fontSize": 118, "lineHeight": 0.98},
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "caption": "Focos por dia",
                "image_url": public_generated_url(charts["daily"], asset_version),
                "source": SOURCE_LABEL,
                "body_text": slides_text.get("daily", ""),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "caption": "Estados com mais focos",
                "image_url": public_generated_url(charts["states"], asset_version),
                "source": SOURCE_LABEL,
                "body_text": slides_text.get("states", ""),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "caption": "Biomas com mais focos",
                "image_url": public_generated_url(charts["biomes"], asset_version),
                "source": SOURCE_LABEL,
                "body_text": slides_text.get("biomes", ""),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "caption": "Mapa de densidade",
                "image_url": public_generated_url(charts["map"], asset_version),
                "source": SOURCE_LABEL,
                "body_text": slides_text.get("map", ""),
            },
        },
        {
            "type": "cta",
            "slots": {
                "cta_kicker": f"Esse post foi gerado em {generated_in_seconds} segundos",
                "cta_headline": "Análise assistida por IA",
                "cta_subline": (
                    "Use como apoio, não como autoridade. "
                    "A análise descritiva foi feita por IA a partir de dados abertos. "
                    "Veja mais dados e análises em institutoforest.org."
                ),
                "cta_url": "institutoforest.org",
            },
        },
    ]
    manifest = {
        "theme": "red",
        "runId": "bdqueimadas-daily-social",
        "globalSlots": {
            "topic_tag": "Queimadas & Focos",
            "published_at": f"{window['start_date']} a {window['end_date']}",
        },
        "instagram_caption_draft": texts.get("instagram_caption", ""),
        "slides": slides,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    if path.resolve() == DEFAULT_MANIFEST.resolve():
        DEFAULT_PUBLIC_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_PUBLIC_MANIFEST.write_text(text, encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera carrossel diário BDQueimadas.")
    parser.add_argument("--source-url", default=DEFAULT_SOURCE_URL)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--emit-manifest", type=Path, nargs="?", const=DEFAULT_MANIFEST, default=DEFAULT_MANIFEST)
    parser.add_argument("--as-of", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--llm", action="store_true")
    parser.add_argument("--app-config", type=Path, default=DEFAULT_APP_CONFIG)
    parser.add_argument("--out-social-llm", type=Path, default=DEFAULT_LLM_JSON)
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=DEFAULT_LOGS_DIR,
        help="Diretorio base de logs (subcaminho social/bdqueimadas-daily/<ano>/<mes>/).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()
    args = _parse_args(argv)
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        sidecar = run(
            source_url=args.source_url,
            data_dir=args.data_dir,
            out_dir=args.out_dir,
            emit_manifest=args.emit_manifest,
            as_of=as_of,
            days=args.days,
            run_llm=args.llm,
            app_config=args.app_config,
            out_social_llm=args.out_social_llm,
            logs_dir=args.logs_dir,
        )
    except Exception as exc:
        print(f"Erro: {exc}")
        return 1
    print(f"OK: {args.emit_manifest}")
    print(f"Textos e payload: {args.out_social_llm.resolve()}")
    print(f"Log: {args.logs_dir.resolve()}/social/bdqueimadas-daily/")
    print(f"Janela: {sidecar['payload']['window']['start_date']} a {sidecar['payload']['window']['end_date']}")
    return 0
