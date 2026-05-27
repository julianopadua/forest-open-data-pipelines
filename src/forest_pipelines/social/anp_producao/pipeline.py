"""Build the ANP petroleum and natural gas production social deck."""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import shutil
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from forest_pipelines.settings import load_settings
from forest_pipelines.social.llm.registry import generate_graphic_text_for_anp_scope

LOG = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
APP_ROOT = REPO_ROOT / "apps" / "social-post-templates"
DEFAULT_DATA_DIR = REPO_ROOT / "data" / "anp_producao_petroleo_gas"
DEFAULT_OUT_DIR = APP_ROOT / "public" / "generated"
DEFAULT_MANIFEST_PATH = APP_ROOT / "examples" / "anp-producao-petroleo-gas.manifest.json"
DEFAULT_PUBLIC_MANIFEST_PATH = (
    APP_ROOT / "public" / "examples" / "anp-producao-petroleo-gas.manifest.json"
)
DEFAULT_APP_CONFIG = REPO_ROOT / "configs" / "app.yml"
ANP_LANDING_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "producao-de-petroleo-e-gas-natural-por-estado-e-localizacao"
)
ANP_COMPACT_PATH = (
    REPO_ROOT / "src" / "forest_pipelines" / "dados_abertos" / "anp_catalog_compact.json"
)

MONTH_TO_NUMBER = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}
MONTH_LABELS_PT = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]


@dataclass(frozen=True, slots=True)
class ResourceDef:
    key: str
    label: str
    match_tokens: tuple[str, ...]
    filename: str
    unit: str
    preferred_value_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DataProfile:
    key: str
    file: str
    size_bytes: int
    rows: int
    columns: list[str]
    value_column: str
    period_min: str
    period_max: str
    uf_count: int
    location_values: list[str]
    null_values: int


@dataclass(frozen=True, slots=True)
class PeriodMetrics:
    latest_period: pd.Timestamp
    previous_period: pd.Timestamp
    yoy_period: pd.Timestamp
    petroleo_m3: float
    petroleo_mom_pct: float | None
    petroleo_yoy_pct: float | None
    gas_1000m3: float
    gas_mom_pct: float | None
    gas_yoy_pct: float | None


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    data_dir: Path
    out_dir: Path
    manifest_path: Path
    public_manifest_path: Path
    landing_url: str
    llm: bool
    refresh: bool
    recent_months_national: int
    recent_months_state: int


RESOURCE_DEFS: tuple[ResourceDef, ...] = (
    ResourceDef(
        key="petroleo_m3",
        label="Produção de petróleo",
        match_tokens=("producao-petroleo",),
        filename="producao-petroleo-m3.csv",
        unit="m3",
        preferred_value_columns=("PRODUÇÃO",),
    ),
    ResourceDef(
        key="lgn_m3",
        label="Produção de LGN",
        match_tokens=("producao-lgn",),
        filename="producao-lgn-m3.csv",
        unit="m3",
        preferred_value_columns=("PRODUÇÃO",),
    ),
    ResourceDef(
        key="gas_natural_1000m3",
        label="Produção de gás natural",
        match_tokens=("producao-gas-natural",),
        filename="producao-gas-natural-1000m3.csv",
        unit="1000 m3",
        preferred_value_columns=("PRODUÇÃO",),
    ),
    ResourceDef(
        key="reinjecao_1000m3",
        label="Reinjeção de gás natural",
        match_tokens=("reinjecao-gn",),
        filename="reinjecao-gn-1000m3.csv",
        unit="1000 m3",
        preferred_value_columns=("REINJETADO",),
    ),
    ResourceDef(
        key="queima_perda_1000m3",
        label="Queima e perda de gás natural",
        match_tokens=("queima-e-perda",),
        filename="queima-e-perda-gn-1000m3.csv",
        unit="1000 m3",
        preferred_value_columns=("QUEIMADO", "QUEIMA E PERDA"),
    ),
    ResourceDef(
        key="consumo_proprio_1000m3",
        label="Consumo próprio de gás natural",
        match_tokens=("consumo-proprio",),
        filename="consumo-proprio-gn1000m3.csv",
        unit="1000 m3",
        preferred_value_columns=("CONSUMO", "CONSUMO PRÓPRIO"),
    ),
    ResourceDef(
        key="gn_disponivel_1000m3",
        label="Gás natural disponível",
        match_tokens=("gn-disponivel",),
        filename="gn-disponivel-1000m3.csv",
        unit="1000 m3",
        preferred_value_columns=("DISPONÍVEL",),
    ),
)


def normalize_source_url(url: str) -> str:
    value = html.unescape(str(url or "").strip())
    if value.startswith("http://=https://"):
        return value.removeprefix("http://=")
    if value.startswith("https://=https://"):
        return value.removeprefix("https://=")
    return value


def discover_resource_urls_from_html(page_html: str) -> dict[str, str]:
    urls: dict[str, str] = {}
    for raw in re.findall(r'href=["\\\']([^"\\\']+)["\\\']', page_html, flags=re.IGNORECASE):
        url = normalize_source_url(raw)
        low = url.lower()
        if "ppgn-el" not in low or "metadados" in low:
            continue
        for item in RESOURCE_DEFS:
            if any(token in low for token in item.match_tokens):
                urls[item.key] = url
    return urls


def discover_resource_urls_from_compact(compact_path: Path = ANP_COMPACT_PATH) -> dict[str, str]:
    if not compact_path.exists():
        return {}
    data = json.loads(compact_path.read_text(encoding="utf-8"))
    packages = data.get("packages") or data.get("datasets") or []
    target = None
    for package in packages:
        if package.get("slug") == "producao-de-petroleo-e-gas-natural-por-estado-e-localizacao":
            target = package
            break
    if not target:
        return {}
    urls: dict[str, str] = {}
    for resource in target.get("resources") or []:
        if resource.get("kind") != "data":
            continue
        url = normalize_source_url(str(resource.get("url") or ""))
        low = url.lower()
        for item in RESOURCE_DEFS:
            if any(token in low for token in item.match_tokens):
                urls[item.key] = url
    return urls


def fetch_landing_html(landing_url: str, timeout_s: int = 60) -> str:
    req = urllib.request.Request(
        landing_url,
        headers={"User-Agent": "forest-open-data-pipelines/0.1"},
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8", errors="replace")


def discover_resource_urls(landing_url: str) -> dict[str, str]:
    try:
        urls = discover_resource_urls_from_html(fetch_landing_html(landing_url))
    except Exception as exc:
        LOG.warning("anp_producao.discovery_html_failed error=%s", exc)
        urls = {}
    fallback = discover_resource_urls_from_compact()
    merged = {**fallback, **urls}
    missing = [item.key for item in RESOURCE_DEFS if item.key not in merged]
    if missing:
        raise RuntimeError(f"Não foi possível descobrir URLs ANP para: {missing}")
    return merged


def _download(url: str, target: Path, timeout_s: int = 120) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "forest-open-data-pipelines/0.1"},
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp, target.open("wb") as dst:
        shutil.copyfileobj(resp, dst)


def ensure_resource_files(
    urls: dict[str, str], data_dir: Path, *, refresh: bool
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    data_dir.mkdir(parents=True, exist_ok=True)
    for item in RESOURCE_DEFS:
        target = data_dir / item.filename
        if refresh or not target.exists():
            _download(urls[item.key], target)
        paths[item.key] = target
    return paths


def _value_column(columns: list[str], item: ResourceDef) -> str:
    for candidate in item.preferred_value_columns:
        if candidate in columns:
            return candidate
    return columns[-1]


def _parse_numeric(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    has_comma = text.str.contains(",", regex=False)
    text = text.where(~has_comma, text.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    return pd.to_numeric(text, errors="coerce")


def load_resource_frame(path: Path, item: ResourceDef) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    value_col = _value_column(list(df.columns), item)
    df["ANO_INT"] = pd.to_numeric(df["ANO"], errors="coerce").astype("Int64")
    df["MES_NUM"] = df["MÊS"].map(MONTH_TO_NUMBER).astype("Int64")
    if df["ANO_INT"].isna().any() or df["MES_NUM"].isna().any():
        raise ValueError(f"Período inválido em {path.name}")
    df["PERIODO"] = pd.to_datetime(
        {
            "year": df["ANO_INT"].astype(int),
            "month": df["MES_NUM"].astype(int),
            "day": 1,
        },
        errors="coerce",
    )
    df["VALOR"] = _parse_numeric(df[value_col])
    df.attrs["value_column"] = value_col
    df.attrs["resource_key"] = item.key
    return df


def profile_frame(key: str, path: Path, df: pd.DataFrame) -> DataProfile:
    locations = []
    if "LOCALIZAÇÃO" in df.columns:
        locations = sorted(str(v) for v in df["LOCALIZAÇÃO"].dropna().unique().tolist())
    return DataProfile(
        key=key,
        file=path.name,
        size_bytes=path.stat().st_size,
        rows=int(len(df)),
        columns=[str(c) for c in df.columns if c not in {"ANO_INT", "MES_NUM", "PERIODO", "VALOR"}],
        value_column=str(df.attrs["value_column"]),
        period_min=str(df["PERIODO"].min().date()),
        period_max=str(df["PERIODO"].max().date()),
        uf_count=int(df["UNIDADE DA FEDERAÇÃO"].nunique()),
        location_values=locations,
        null_values=int(df["VALOR"].isna().sum()),
    )


def national_series(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    out = df.groupby("PERIODO", as_index=False)["VALOR"].sum().sort_values("PERIODO")
    return out.rename(columns={"VALOR": value_name})


def latest_common_positive_metrics(petroleum: pd.DataFrame, gas: pd.DataFrame) -> PeriodMetrics:
    combo = national_series(petroleum, "petroleo_m3").merge(
        national_series(gas, "gas_1000m3"),
        on="PERIODO",
        how="inner",
    )
    combo = combo[(combo["petroleo_m3"] > 0) & (combo["gas_1000m3"] > 0)].sort_values("PERIODO")
    if len(combo) < 2:
        raise RuntimeError("Série ANP sem meses positivos suficientes para MoM e YoY.")
    latest = combo.iloc[-1]
    previous = combo.iloc[-2]
    yoy_period = latest["PERIODO"] - pd.DateOffset(years=1)
    yoy_match = combo[combo["PERIODO"].eq(yoy_period)]
    if yoy_match.empty:
        raise RuntimeError(f"Sem período YoY comparável para {latest['PERIODO']}.")
    yoy = yoy_match.iloc[0]
    return PeriodMetrics(
        latest_period=latest["PERIODO"],
        previous_period=previous["PERIODO"],
        yoy_period=yoy["PERIODO"],
        petroleo_m3=float(latest["petroleo_m3"]),
        petroleo_mom_pct=pct_delta(float(latest["petroleo_m3"]), float(previous["petroleo_m3"])),
        petroleo_yoy_pct=pct_delta(float(latest["petroleo_m3"]), float(yoy["petroleo_m3"])),
        gas_1000m3=float(latest["gas_1000m3"]),
        gas_mom_pct=pct_delta(float(latest["gas_1000m3"]), float(previous["gas_1000m3"])),
        gas_yoy_pct=pct_delta(float(latest["gas_1000m3"]), float(yoy["gas_1000m3"])),
    )


def pct_delta(new: float, old: float) -> float | None:
    if old == 0:
        return None
    return round((new / old - 1.0) * 100.0, 2)


def state_series_with_other(
    df: pd.DataFrame,
    *,
    latest_period: pd.Timestamp,
    recent_months: int,
    top_n: int = 8,
) -> pd.DataFrame:
    state = df.groupby(["PERIODO", "UNIDADE DA FEDERAÇÃO"], as_index=False)["VALOR"].sum()
    latest = state[state["PERIODO"].eq(latest_period)].sort_values("VALOR", ascending=False)
    top_states = latest[latest["VALOR"] > 0].head(top_n)["UNIDADE DA FEDERAÇÃO"].tolist()
    start = latest_period - pd.DateOffset(months=recent_months - 1)
    recent = state[(state["PERIODO"] >= start) & (state["PERIODO"] <= latest_period)].copy()
    recent["series"] = recent["UNIDADE DA FEDERAÇÃO"].where(
        recent["UNIDADE DA FEDERAÇÃO"].isin(top_states),
        "OUTROS",
    )
    return recent.groupby(["PERIODO", "series"], as_index=False)["VALOR"].sum().sort_values(["PERIODO", "series"])


def state_share_rows(df: pd.DataFrame, latest_period: pd.Timestamp, limit: int = 8) -> list[dict[str, Any]]:
    latest = (
        df[df["PERIODO"].eq(latest_period)]
        .groupby("UNIDADE DA FEDERAÇÃO", as_index=False)["VALOR"]
        .sum()
        .sort_values("VALOR", ascending=False)
    )
    latest = latest[latest["VALOR"] > 0]
    total = float(latest["VALOR"].sum())
    rows: list[dict[str, Any]] = []
    for _, row in latest.head(limit).iterrows():
        value = float(row["VALOR"])
        rows.append(
            {
                "uf": str(row["UNIDADE DA FEDERAÇÃO"]),
                "value": round(value, 2),
                "share_pct": round(value / total * 100.0, 2) if total else None,
            }
        )
    return rows


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value:+.1f}%"


def _format_month(ts: pd.Timestamp) -> str:
    return f"{MONTH_LABELS_PT[int(ts.month) - 1]} {int(ts.year)}"


def _compact_value(value: float, *, kind: str) -> str:
    if kind == "petroleo":
        return f"{value / 1_000_000:.1f} mi m3"
    return f"{value / 1_000_000:.1f} bi m3"


def build_panel_payloads(
    frames: dict[str, pd.DataFrame],
    profiles: list[DataProfile],
    metrics: PeriodMetrics,
) -> dict[str, dict[str, Any]]:
    petroleum_states = state_share_rows(frames["petroleo_m3"], metrics.latest_period, limit=8)
    gas_states = state_share_rows(frames["gas_natural_1000m3"], metrics.latest_period, limit=8)
    source_profile = {p.key: asdict(p) for p in profiles}
    common = {
        "source": ANP_LANDING_URL,
        "latest_period": str(metrics.latest_period.date()),
        "previous_period": str(metrics.previous_period.date()),
        "yoy_period": str(metrics.yoy_period.date()),
    }
    return {
        "national": {
            **common,
            "panel": "national",
            "title": "Produção nacional",
            "metrics": {
                "petroleo_m3": round(metrics.petroleo_m3, 2),
                "petroleo_mom_pct": metrics.petroleo_mom_pct,
                "petroleo_yoy_pct": metrics.petroleo_yoy_pct,
                "gas_1000m3": round(metrics.gas_1000m3, 2),
                "gas_mom_pct": metrics.gas_mom_pct,
                "gas_yoy_pct": metrics.gas_yoy_pct,
            },
            "profiles": source_profile,
        },
        "petroleo_uf": {
            **common,
            "panel": "petroleo_uf",
            "title": "Petróleo por estado",
            "top_states": petroleum_states,
            "latest_total_m3": round(metrics.petroleo_m3, 2),
        },
        "gas_uf": {
            **common,
            "panel": "gas_uf",
            "title": "Gás natural por estado",
            "top_states": gas_states,
            "latest_total_1000m3": round(metrics.gas_1000m3, 2),
        },
    }


def fallback_text(scope: str, payload: dict[str, Any]) -> str:
    if scope == "national":
        m = payload["metrics"]
        return (
            f"Em {_format_month(pd.Timestamp(payload['latest_period']))}, petróleo somou "
            f"{_compact_value(float(m['petroleo_m3']), kind='petroleo')} "
            f"({_format_pct(m['petroleo_mom_pct'])} MoM, {_format_pct(m['petroleo_yoy_pct'])} YoY). "
            f"Gás natural atingiu {_compact_value(float(m['gas_1000m3']), kind='gas')} "
            f"({_format_pct(m['gas_mom_pct'])} MoM, {_format_pct(m['gas_yoy_pct'])} YoY)."
        )
    top = payload.get("top_states") or []
    if not top:
        return "Sem dados estaduais positivos no último mês comum da série."
    first = top[0]
    second = top[1] if len(top) > 1 else None
    if second:
        return (
            f"{first['uf']} lidera o recorte com {first['share_pct']:.1f}% do total. "
            f"{second['uf']} aparece em seguida, com {second['share_pct']:.1f}%. "
            "A produção segue concentrada, com baixa dispersão entre os demais estados."
        )
    return f"{first['uf']} concentra {first['share_pct']:.1f}% do total no último mês disponível."


def sanitize_slide_text(text: str, max_chars: int = 320) -> str:
    value = str(text or "")
    replacements = {
        "\u00a0": " ",
        "\u2007": " ",
        "\u2009": " ",
        "\u202f": " ",
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    value = value.replace("1000m3", "mil m3").replace("1000 m3", "mil m3")
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) <= max_chars:
        return value
    clipped = value[:max_chars].rstrip()
    last_period = clipped.rfind(".")
    if last_period >= max_chars // 2:
        return clipped[: last_period + 1]
    return clipped.rstrip(" ,.;:") + "."


def maybe_generate_texts(
    payloads: dict[str, dict[str, Any]], *, use_llm: bool, app_config: Path = DEFAULT_APP_CONFIG
) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    if not use_llm:
        return {
            key: {"text": fallback_text(key, payload), "model": "deterministic"}
            for key, payload in payloads.items()
        }
    load_dotenv()
    settings = load_settings(str(app_config))
    for key, payload in payloads.items():
        safe_fallback = fallback_text(key, payload)
        try:
            out[key] = generate_graphic_text_for_anp_scope(
                payload,
                settings.llm,
                scope_slug=key,
            )
            out[key]["text"] = sanitize_slide_text(out[key]["text"])
            if key != "national" and _state_text_restates_volume(out[key]["text"]):
                out[key] = {
                    "text": safe_fallback,
                    "model": f"{out[key]['model']}:validated-fallback",
                }
        except Exception as exc:
            LOG.warning("anp_producao.llm_failed scope=%s error=%s", key, exc)
            out[key] = {"text": safe_fallback, "model": "deterministic"}
    return out


def _state_text_restates_volume(text: str) -> bool:
    low = text.lower()
    return "m3" in low or "m³" in low or "metro cúbico" in low or "metros cúbicos" in low


def _setup_matplotlib_cache(out_dir: Path) -> None:
    cache_root = out_dir.parent / ".cache"
    mpl_root = out_dir.parent / ".mplconfig"
    cache_root.mkdir(parents=True, exist_ok=True)
    mpl_root.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root))
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_root))


def render_charts(
    frames: dict[str, pd.DataFrame],
    metrics: PeriodMetrics,
    out_dir: Path,
    *,
    recent_months_national: int,
    recent_months_state: int,
) -> dict[str, Path]:
    _setup_matplotlib_cache(out_dir)
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    out_dir.mkdir(parents=True, exist_ok=True)
    bg = "#071526"
    fg = "#D8ECFF"
    grid = "#27445F"
    petroleum_color = "#4A9EFF"
    gas_color = "#7CE3C3"

    paths: dict[str, Path] = {}

    pet = national_series(frames["petroleo_m3"], "petroleo_m3")
    gas = national_series(frames["gas_natural_1000m3"], "gas_1000m3")
    combo = pet.merge(gas, on="PERIODO", how="inner")
    combo = combo[
        (combo["PERIODO"] <= metrics.latest_period)
        & (combo["petroleo_m3"] > 0)
        & (combo["gas_1000m3"] > 0)
    ].tail(recent_months_national)

    fig, ax1 = plt.subplots(figsize=(10.8, 6.2), dpi=100)
    fig.patch.set_facecolor(bg)
    ax1.set_facecolor(bg)
    ax2 = ax1.twinx()
    ax1.plot(combo["PERIODO"], combo["petroleo_m3"] / 1_000_000, color=petroleum_color, linewidth=2.8)
    ax2.plot(combo["PERIODO"], combo["gas_1000m3"] / 1_000_000, color=gas_color, linewidth=2.8)
    ax1.set_title("Brasil: produção de petróleo e gás natural", loc="left", color="#FFFFFF", fontsize=18)
    ax1.set_ylabel("Petróleo, milhões m3", color=fg)
    ax2.set_ylabel("Gás natural, bilhões m3", color=fg)
    ax1.grid(color=grid, alpha=0.45)
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    for ax in (ax1, ax2):
        ax.tick_params(colors=fg)
        for spine in ax.spines.values():
            spine.set_color(fg)
    ax1.legend(["Petróleo"], loc="upper left", frameon=False, labelcolor=fg)
    ax2.legend(["Gás natural"], loc="upper right", frameon=False, labelcolor=fg)
    fig.tight_layout()
    paths["national"] = out_dir / "anp-producao-national.png"
    fig.savefig(paths["national"], facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)

    def render_state(df: pd.DataFrame, title: str, ylabel: str, filename: str, divisor: float) -> Path:
        series = state_series_with_other(
            df,
            latest_period=metrics.latest_period,
            recent_months=recent_months_state,
        )
        fig_s, ax = plt.subplots(figsize=(10.8, 6.2), dpi=100)
        fig_s.patch.set_facecolor(bg)
        ax.set_facecolor(bg)
        palette = [
            "#4A9EFF",
            "#7CE3C3",
            "#F7C948",
            "#F97066",
            "#B692F6",
            "#64D2FF",
            "#A6E3A1",
            "#F5A97F",
            "#9AA7B5",
        ]
        for idx, (name, group) in enumerate(series.groupby("series")):
            ax.plot(group["PERIODO"], group["VALOR"] / divisor, linewidth=1.9, label=name, color=palette[idx % len(palette)])
        ax.set_title(title, loc="left", color="#FFFFFF", fontsize=18)
        ax.set_ylabel(ylabel, color=fg)
        ax.grid(color=grid, alpha=0.45)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{value:.1f}"))
        ax.tick_params(colors=fg)
        for spine in ax.spines.values():
            spine.set_color(fg)
        ax.legend(fontsize=8, ncols=3, frameon=False, labelcolor=fg)
        fig_s.tight_layout()
        path = out_dir / filename
        fig_s.savefig(path, facecolor=fig_s.get_facecolor(), bbox_inches="tight")
        plt.close(fig_s)
        return path

    paths["petroleo_uf"] = render_state(
        frames["petroleo_m3"],
        "Petróleo por estado, top UFs e outros",
        "Milhões m3",
        "anp-producao-petroleo-ufs.png",
        1_000_000,
    )
    paths["gas_uf"] = render_state(
        frames["gas_natural_1000m3"],
        "Gás natural por estado, top UFs e outros",
        "Bilhões m3",
        "anp-producao-gas-ufs.png",
        1_000_000,
    )
    return paths


def write_chart_specs(
    out_dir: Path,
    payloads: dict[str, dict[str, Any]],
    profiles: list[DataProfile],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for key, payload in payloads.items():
        (out_dir / f"chart_spec-anp-producao-{key}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    (out_dir / "anp-producao-source-profile.json").write_text(
        json.dumps([asdict(p) for p in profiles], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_manifest(
    chart_paths: dict[str, Path],
    payloads: dict[str, dict[str, Any]],
    texts: dict[str, dict[str, str]],
    metrics: PeriodMetrics,
) -> dict[str, Any]:
    published_at = _format_month(metrics.latest_period)

    def img(key: str) -> str:
        return f"/generated/{chart_paths[key].name}"

    slides = [
        {
            "type": "cover",
            "slots": {
                "topic_tag": "ANP Energia",
                "published_at": published_at,
                "series_label": "Séries mensais",
                "title": "Produção de petróleo e gás natural no Brasil",
                "summary": (
                    f"Dados oficiais ANP por estado e localização. Último mês comum positivo: "
                    f"{published_at}."
                ),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "topic_tag": "ANP Energia",
                "published_at": published_at,
                "caption": "Brasil: petróleo (m3) e gás natural (mil m3) · ANP",
                "image_url": img("national"),
                "body_text": texts["national"]["text"],
            },
            "generation": {"ok": True, "model": texts["national"]["model"], "error": None},
        },
        {
            "type": "body_chart",
            "slots": {
                "topic_tag": "ANP Energia",
                "published_at": published_at,
                "caption": "Petróleo por estado · ANP",
                "image_url": img("petroleo_uf"),
                "body_text": texts["petroleo_uf"]["text"],
            },
            "generation": {"ok": True, "model": texts["petroleo_uf"]["model"], "error": None},
        },
        {
            "type": "body_chart",
            "slots": {
                "topic_tag": "ANP Energia",
                "published_at": published_at,
                "caption": "Gás natural por estado · ANP",
                "image_url": img("gas_uf"),
                "body_text": texts["gas_uf"]["text"],
            },
            "generation": {"ok": True, "model": texts["gas_uf"]["model"], "error": None},
        },
        {
            "type": "cta",
            "slots": {
                "topic_tag": "ANP Energia",
                "published_at": published_at,
                "cta_kicker": "Quer continuar acompanhando?",
                "cta_headline": "Mais dados abertos de energia",
                "cta_subline": "Acesse séries, fontes oficiais e metadados no Forest.",
                "cta_url": "institutoforest.org",
            },
        },
    ]
    global_slots = {
        "topic_tag": "ANP Energia",
        "published_at": published_at,
    }
    for slide in slides:
        slots = slide.get("slots", {})
        slots.pop("topic_tag", None)
        slots.pop("published_at", None)
    return {
        "theme": "navy",
        "runId": "anp-producao-petroleo-gas",
        "sizes": {
            "topicTagPx": 24,
            "datePx": 26,
            "pageNumberPx": 24,
            "logoHeightPx": 54,
        },
        "globalSlots": global_slots,
        "slides": slides,
        "sources": {
            "anp_landing_url": ANP_LANDING_URL,
            "latest_period": str(metrics.latest_period.date()),
        },
        "summary_metrics": payloads["national"]["metrics"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def run(config: PipelineConfig) -> dict[str, Any]:
    LOG.info("anp_producao.start data_dir=%s llm=%s", config.data_dir, config.llm)
    urls = discover_resource_urls(config.landing_url)
    paths = ensure_resource_files(urls, config.data_dir, refresh=config.refresh)
    frames: dict[str, pd.DataFrame] = {}
    profiles: list[DataProfile] = []
    defs_by_key = {item.key: item for item in RESOURCE_DEFS}
    for key, path in paths.items():
        frame = load_resource_frame(path, defs_by_key[key])
        frames[key] = frame
        profiles.append(profile_frame(key, path, frame))
    metrics = latest_common_positive_metrics(
        frames["petroleo_m3"],
        frames["gas_natural_1000m3"],
    )
    payloads = build_panel_payloads(frames, profiles, metrics)
    texts = maybe_generate_texts(payloads, use_llm=config.llm)
    chart_paths = render_charts(
        frames,
        metrics,
        config.out_dir,
        recent_months_national=config.recent_months_national,
        recent_months_state=config.recent_months_state,
    )
    write_chart_specs(config.out_dir, payloads, profiles)
    manifest = build_manifest(chart_paths, payloads, texts, metrics)
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.public_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    config.public_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG.info("anp_producao.done manifest=%s", config.manifest_path)
    return manifest


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m forest_pipelines.social.anp_producao",
        description="Build the ANP petroleum and natural gas social carousel.",
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--public-manifest-path", type=Path, default=DEFAULT_PUBLIC_MANIFEST_PATH)
    parser.add_argument("--landing-url", default=ANP_LANDING_URL)
    parser.add_argument("--llm", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--recent-months-national", type=int, default=120)
    parser.add_argument("--recent-months-state", type=int, default=60)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = PipelineConfig(
        data_dir=args.data_dir,
        out_dir=args.out_dir,
        manifest_path=args.manifest_path,
        public_manifest_path=args.public_manifest_path,
        landing_url=args.landing_url,
        llm=args.llm,
        refresh=args.refresh,
        recent_months_national=args.recent_months_national,
        recent_months_state=args.recent_months_state,
    )
    run(config)
    return 0
