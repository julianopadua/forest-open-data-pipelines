"""Monthly BDQueimadas chart: ano civil atual (só meses já fechados) vs série em CSV anual (data_pas)."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from forest_pipelines.settings import load_settings
from forest_pipelines.social.logging import get_social_bdqueimadas_logger, log_stage

from forest_pipelines.datasets.inpe.bdqueimadas_mensal_listing import (
    DEFAULT_MENSAL_BASE_URL,
    ensure_mensal_files_for_year,
)
from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    BIOME_LABELS,
    INPE_REFERENCE_SATELLITE,
    _pick_member,
    build_year_payload_from_csv,
    count_focos_rows_brasil_file,
)
from forest_pipelines.reports.builders.bdqueimadas_overview import (
    DEFAULT_BIOME_CANDIDATES,
    DEFAULT_DATETIME_CANDIDATES,
    DEFAULT_SATELLITE_CANDIDATES,
    DEFAULT_STATE_CANDIDATES,
    _select_annual_reference_csv_files,
    _select_zip_files,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_SUBDIR = Path("data") / "inpe_bdqueimadas"
DEFAULT_MENSAL_CACHE_DIR = DEFAULT_DATA_SUBDIR / "mensal"
DEFAULT_PLOT_SOURCES_METADATA_NAME = "bdqueimadas_plot_sources.json"
DEFAULT_OUT_DIR = (
    REPO_ROOT / "apps" / "social-post-templates" / "public" / "generated"
)
DEFAULT_CHART_PNG = DEFAULT_OUT_DIR / "bdqueimadas-chart.png"
DEFAULT_CHART_SPEC = DEFAULT_OUT_DIR / "chart_spec.json"
DEFAULT_SOCIAL_LLM_JSON = DEFAULT_OUT_DIR / "social_llm.json"
DEFAULT_APP_CONFIG = REPO_ROOT / "configs" / "app.yml"
DEFAULT_MANIFEST_PATH = (
    REPO_ROOT / "apps" / "social-post-templates" / "examples" / "bdqueimadas-social.manifest.json"
)
PUBLIC_MANIFEST_COPY = (
    REPO_ROOT
    / "apps"
    / "social-post-templates"
    / "public"
    / "examples"
    / "bdqueimadas-social.manifest.json"
)

# Carrossel fixo: Nacional + três biomas (ordem do manifest de 6 slides).
BDQUEIMADAS_CAROUSEL_SCOPES: tuple[str | None, ...] = (
    None,
    "Amazônia",
    "Cerrado",
    "Pantanal",
)

_SCOPE_TO_INPE_BIOME: dict[str, str] = {
    "Amazônia": "AMAZÔNIA",
    "Cerrado": "CERRADO",
    "Pantanal": "PANTANAL",
}

MONTH_LABELS_PT = [
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
]

def format_published_at_pt(month: int, year: int) -> str:
    """Ex.: Abr 2026 — alinhado ao último mês civil já encerrado na série atual."""
    if month < 1 or month > 12:
        return f"{year}"
    return f"{MONTH_LABELS_PT[month - 1]} {year}"


def last_closed_month_for_calendar_year(reference_date: date, year: int) -> int:
    """
    Último mês civil já encerrado no ano ``year``, na linha do tempo de ``reference_date``.

    Ex.: em 18 de abril de 2026 → 3 (março); em 1º de maio de 2026 → 4 (abril).
    Em janeiro, antes de fechar o mês anterior no mesmo ano civil, retorna 0.
    Se ``reference_date.year > year``, o ano do gráfico já terminou → 12.
    Se ``reference_date.year < year``, o ano do gráfico ainda não começou → 0.
    """
    if reference_date.year > year:
        return 12
    if reference_date.year < year:
        return 0
    return reference_date.month - 1


def carousel_scope_to_slug(scope: str | None) -> str:
    """Slug estável para nomes de arquivo (ASCII)."""
    if scope is None:
        return "nacional"
    return {"Amazônia": "amazonia", "Cerrado": "cerrado", "Pantanal": "pantanal"}.get(
        scope,
        scope.lower().replace(" ", "-"),
    )


def carousel_scope_to_inpe_biome_key(scope: str | None) -> str | None:
    """Chave de bioma como no CSV INPE (maiúsculas); None = território nacional."""
    if scope is None:
        return None
    return _SCOPE_TO_INPE_BIOME.get(scope)


def carousel_biome_label_pt(scope: str | None) -> str:
    """Rótulo em pt-BR para metadata/LLM (Nacional vs bioma)."""
    if scope is None:
        return "Brasil"
    key = carousel_scope_to_inpe_biome_key(scope)
    if key and key in BIOME_LABELS:
        return str(BIOME_LABELS[key]["pt"])
    return str(scope)


def _trim_payload_monthly_to_inferred_year(payload: dict[str, Any]) -> dict[str, Any]:
    """Descarta meses cujo ano civil do agregado não bate com o ano do arquivo (nome)."""
    inf = payload.get("inferred_year")
    if inf is None:
        return payload
    inf_int = int(inf)
    monthly: list[dict[str, Any]] = []
    for row in payload.get("monthly_all", []):
        try:
            if int(row.get("year", 0)) == inf_int:
                monthly.append(row)
        except (TypeError, ValueError):
            continue
    return {**payload, "monthly_all": monthly}


def _monthly_all_payloads_to_df_dedupe(
    payloads: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Junta séries mensais de vários ZIPs sem somar duplicatas (period, year).

    Dois arquivos para o mesmo ano civil (ex.: cópias de focos_br_ref_2025.zip)
    geravam contagem ~2x; mantém a primeira ocorrência (ordem de _select_zip_files).
    """
    rows: list[dict[str, Any]] = []
    for item in payloads:
        for row in item.get("monthly_all", []):
            rows.append(dict(row))
    if not rows:
        return pd.DataFrame(columns=["period", "year", "value"])
    df = pd.DataFrame(rows)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype(int)
    df["period"] = df["period"].astype(str)
    df = df.drop_duplicates(subset=["period", "year"], keep="first")
    return df.sort_values(["period"]).reset_index(drop=True)


def _monthly_by_biome_payloads_to_df_dedupe(
    payloads: list[dict[str, Any]],
    inpe_biome_key: str,
) -> pd.DataFrame:
    """Série mensal para um bioma INPE (dedupe period,year como na série nacional)."""
    target = str(inpe_biome_key).strip().upper()
    rows: list[dict[str, Any]] = []
    for item in payloads:
        for row in item.get("monthly_by_biome", []):
            b = str(row.get("biome", "")).strip().upper()
            if b == target:
                rows.append(dict(row))
    if not rows:
        return pd.DataFrame(columns=["period", "year", "value"])
    df = pd.DataFrame(rows)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype(int)
    df["period"] = df["period"].astype(str)
    df = df.drop_duplicates(subset=["period", "year"], keep="first")
    return df.sort_values(["period"]).reset_index(drop=True)


def _collect_year_payloads(
    *,
    recent_years: int | None,
    anual_dir: Path,
    csv_glob: str,
    satellite_candidates: list[str] | None,
    reference_satellite: str | None,
) -> list[dict[str, Any]]:
    datetime_candidates = list(DEFAULT_DATETIME_CANDIDATES)
    state_candidates = list(DEFAULT_STATE_CANDIDATES)
    biome_candidates = list(DEFAULT_BIOME_CANDIDATES)
    sat_cands = (
        satellite_candidates
        if satellite_candidates is not None
        else list(DEFAULT_SATELLITE_CANDIDATES)
    )
    csv_files = _select_annual_reference_csv_files(
        anual_dir,
        csv_glob=csv_glob,
        recent_years=recent_years,
    )
    payloads: list[dict[str, Any]] = []
    for csv_path in csv_files:
        payloads.append(
            _trim_payload_monthly_to_inferred_year(
                build_year_payload_from_csv(
                    csv_path,
                    datetime_candidates,
                    state_candidates,
                    biome_candidates,
                    satellite_candidates=sat_cands,
                    reference_satellite=reference_satellite,
                )
            )
        )
    return payloads


def extract_annual_reference_csvs(
    data_dir: Path,
    *,
    file_glob: str,
    recent_years: int | None,
    out_dir: Path | None = None,
) -> dict[str, Path]:
    """
    Extrai o CSV principal de cada ZIP focos_br_ref_*.zip para data_dir/anual/*.csv
    (inspeção manual e conferência com o portal).
    """
    base_out = out_dir if out_dir is not None else data_dir / "anual"
    base_out.mkdir(parents=True, exist_ok=True)
    zip_files = _select_zip_files(
        base_dir=data_dir,
        file_glob=file_glob,
        recent_years=recent_years,
    )
    written: dict[str, Path] = {}
    for zp in zip_files:
        with zipfile.ZipFile(zp) as zf:
            member = _pick_member(zf)
            target = base_out / f"{zp.stem}.csv"
            with zf.open(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
        written[zp.name] = target.resolve()
    return written


def load_monthly_all_df(
    data_dir: Path,
    *,
    recent_years: int | None = None,
    anual_dir: Path | None = None,
    anual_subdir: str = "anual",
    csv_glob: str = "focos_br_ref_*.csv",
    satellite_candidates: list[str] | None = None,
    reference_satellite: str | None = None,
) -> pd.DataFrame:
    """
    Série histórica lida dos CSV em ``anual/`` (extraídos do ZIP ou copiados):
    contagens mensais por ``data_pas`` (ISO), alinhadas a SQL/DuckDB no mesmo arquivo.
    """
    base_anual = anual_dir if anual_dir is not None else (data_dir / anual_subdir)
    csv_files = _select_annual_reference_csv_files(
        base_anual,
        csv_glob=csv_glob,
        recent_years=recent_years,
    )
    if not csv_files:
        raise FileNotFoundError(
            f"Nenhum CSV em {base_anual.resolve()} com glob {csv_glob!r}. "
            "Extraia os ZIPs (make bdqueimadas-social-assets gera <data-dir>/anual/*.csv) "
            "ou copie focos_br_ref_YYYY.csv para essa pasta."
        )

    payloads = _collect_year_payloads(
        recent_years=recent_years,
        anual_dir=base_anual,
        csv_glob=csv_glob,
        satellite_candidates=satellite_candidates,
        reference_satellite=reference_satellite,
    )

    monthly_all_df = _monthly_all_payloads_to_df_dedupe(payloads)
    if monthly_all_df.empty:
        raise RuntimeError("Agregação mensal vazia após leitura dos CSVs em anual/.")
    return monthly_all_df


def compute_chart_spec(
    monthly_all_df: pd.DataFrame,
    *,
    current_year: int,
    current_year_monthly_counts: dict[int, int],
    reference_date: date,
    reference_satellite: str | None = None,
    biome_scope: str = "nacional",
    biome_label_pt: str = "Brasil (Nacional)",
) -> dict[str, Any]:
    if not current_year_monthly_counts:
        raise RuntimeError(
            "Sem dados mensais do ano civil atual. "
            "Verifique rede/cache em data/inpe_bdqueimadas/mensal/."
        )

    previous_year = current_year - 1
    last_closed = last_closed_month_for_calendar_year(reference_date, current_year)
    if last_closed < 1:
        raise RuntimeError(
            f"Nenhum mês civil encerrado no ano {current_year} em relação à data "
            f"{reference_date.isoformat()}. A série do ano atual só inclui meses já fechados "
            "(ex.: em janeiro use --as-of a partir de fevereiro, ou --current-year no ano anterior)."
        )
    for m in range(1, last_closed + 1):
        if m not in current_year_monthly_counts:
            raise RuntimeError(
                f"Falta arquivo mensal para o mês {m} (até o último mês fechado, {last_closed}). "
                f"Disponíveis: {sorted(current_year_monthly_counts.keys())}."
            )

    df = monthly_all_df.copy()
    df["month"] = pd.to_datetime(df["period"], errors="coerce").dt.month
    df = df.dropna(subset=["month"])
    df["month"] = df["month"].astype(int)

    # Rótulos do eixo X: um por mês (abrev. PT), sem letras duplicadas (J/M).
    labels = list(MONTH_LABELS_PT)

    series_current: list[int | None] = []
    series_previous: list[int] = []
    series_avg_5y: list[float] = []

    y0 = previous_year - 4
    y1 = previous_year

    for m in range(1, 13):
        if m <= last_closed:
            series_current.append(int(current_year_monthly_counts[m]))
        else:
            series_current.append(None)

        pr = df[(df["year"] == previous_year) & (df["month"] == m)]["value"]
        series_previous.append(int(pr.iloc[0]) if len(pr) else 0)

        win = df[
            (df["year"] >= y0)
            & (df["year"] <= y1)
            & (df["month"] == m)
        ]["value"]
        series_avg_5y.append(float(win.mean()) if len(win) else 0.0)

    published_at_label = format_published_at_pt(last_closed, current_year)

    source = (
        "INPE BDQueimadas (mensal COIDS + agregação ZIP anual"
        + (f"; satélite {reference_satellite}" if reference_satellite else "")
        + ")"
    )
    meta: dict[str, Any] = {
        "latest_year": current_year,
        "previous_year": previous_year,
        "avg_window_years_from": y0,
        "avg_window_years_to": y1,
        "last_closed_month": last_closed,
        "reference_date": reference_date.isoformat(),
        "unit": "focos",
        "source": source,
        "published_at_label": published_at_label,
        "biome_scope": biome_scope,
        "biome_label_pt": biome_label_pt,
    }
    if reference_satellite:
        meta["reference_satellite"] = reference_satellite

    return {
        "schema_version": 3,
        "month_labels": labels,
        "series": {
            "current": {
                "key": "current",
                "label": str(current_year),
                "values": series_current,
            },
            "previous": {
                "key": "previous",
                "label": str(previous_year),
                "values": series_previous,
            },
            "avg_5y": {
                "key": "avg_5y",
                "label": f"Média {y0}–{y1} (por mês)",
                "values": series_avg_5y,
            },
        },
        "metadata": meta,
    }


def render_chart_png(
    spec: dict[str, Any],
    out_path: Path,
    *,
    width_px: int = 1080,
    height_px: int = 620,
    dpi: int = 100,
) -> None:
    import matplotlib.ticker as mticker
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    from matplotlib.ticker import MaxNLocator

    _WHITE = "#ffffff"
    _CUR = "#2ecc9a"
    _PREV = "#94a3b8"
    _AVG_FILL = "#1a7a5e"

    labels = spec["month_labels"]
    cur_raw = spec["series"]["current"]["values"]
    prev = spec["series"]["previous"]["values"]
    avg = spec["series"]["avg_5y"]["values"]
    avg_legend_label = spec["series"]["avg_5y"]["label"]
    meta = spec["metadata"]
    ly = meta["latest_year"]
    py = meta.get("previous_year")

    cur = np.array(
        [np.nan if v is None else float(v) for v in cur_raw],
        dtype=float,
    )

    x = np.arange(len(labels))
    fig_w = width_px / dpi
    fig_h = height_px / dpi
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_alpha(0.0)
    ax.set_facecolor((0, 0, 0, 0))

    ax.fill_between(
        x,
        0,
        avg,
        color=_AVG_FILL,
        alpha=0.35,
        linewidth=0,
    )
    ax.plot(
        x,
        cur,
        color=_CUR,
        linewidth=2.8,
    )
    if py is not None:
        ax.plot(
            x,
            prev,
            color=_PREV,
            linewidth=1.8,
            linestyle="--",
        )

    valid_idx = np.flatnonzero(~np.isnan(cur))
    li: int | None = None
    if valid_idx.size > 0:
        li = int(valid_idx[-1])
        ax.scatter(
            [x[li]],
            [cur[li]],
            color=_CUR,
            s=72,
            zorder=6,
            edgecolors=_CUR,
            linewidths=0,
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12, color=_WHITE, rotation=0, ha="center")
    ax.set_xlabel("Mês", fontsize=14, color=_WHITE, labelpad=8)
    ax.set_ylabel("Nº de focos", fontsize=12, color=_WHITE, labelpad=6)

    # Não usar MaxNLocator no eixo X: ele reduz o número de ticks e some rótulos de meses.
    ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=9))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _pos: f"{int(v):,}".replace(",", "."))
    )

    ax.tick_params(axis="x", labelsize=12, colors=_WHITE)
    ax.tick_params(axis="y", labelsize=11, colors=_WHITE)
    ax.grid(False)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(_WHITE)
    ax.spines["bottom"].set_color(_WHITE)

    # Ordem: ano corrente → ano anterior → média 5 anos (matplotlib não garante ordem pelo plot).
    legend_handles: list = [
        Line2D([0], [0], color=_CUR, linewidth=2.8, linestyle="-", label=str(ly)),
    ]
    if py is not None:
        legend_handles.append(
            Line2D(
                [0],
                [0],
                color=_PREV,
                linewidth=1.8,
                linestyle="--",
                label=str(py),
            )
        )
    legend_handles.append(
        Patch(
            facecolor=_AVG_FILL,
            edgecolor="none",
            linewidth=0,
            alpha=0.35,
            label=avg_legend_label,
        )
    )
    # Legenda colada ao canto superior direito da área do gráfico (coordenadas do eixo).
    ax.legend(
        handles=legend_handles,
        frameon=True,
        facecolor=(0.1, 0.14, 0.12),
        edgecolor=(0.0, 0.0, 0.0, 0.0),
        fontsize=11,
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
        borderaxespad=0.2,
        labelcolor=_WHITE,
        framealpha=0.55,
    )

    # Valor no último ponto (mesmo da bolinha): rótulo acima + seta até o marcador.
    if li is not None:
        last_val = int(cur[li])
        val_lbl = f"{last_val:,}".replace(",", ".")
        ymin, ymax = ax.get_ylim()
        yspan = ymax - ymin if ymax > ymin else 1.0
        # Espaço extra no topo para o texto e a seta não serem cortados.
        headroom = yspan * 0.14
        ax.set_ylim(ymin, ymax + headroom)

        # Meses finais: desloca o texto um pouco à esquerda para não colidir com a legenda.
        tx = float(x[li])
        if li >= 9:
            tx = max(x[li] - 0.55, -0.2)
        ty = float(cur[li]) + yspan * 0.11

        ax.annotate(
            val_lbl,
            xy=(float(x[li]), float(cur[li])),
            xytext=(tx, ty),
            textcoords="data",
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight="bold",
            color=_WHITE,
            zorder=8,
            clip_on=False,
            arrowprops={
                "arrowstyle": "-|>",
                "color": _CUR,
                "lw": 1.6,
                "shrinkA": 0,
                "shrinkB": 6,
                "mutation_scale": 12,
                "connectionstyle": "arc3,rad=0",
            },
        )

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        out_path,
        dpi=dpi,
        transparent=True,
        bbox_inches="tight",
        pad_inches=0.12,
    )
    plt.close(fig)


def _period_str_to_month(period: str) -> int:
    """Ex.: '2025-01' -> 1."""
    s = str(period).strip()
    if "-" in s:
        return int(s.split("-")[-1])
    return int(s)


def _monthly_all_payload_to_by_calendar_month(
    monthly_all: list[dict[str, Any]],
    *,
    data_year: int,
) -> dict[str, int]:
    """Mapeia mês civil 1–12 -> focos (só linhas cujo year bate com data_year)."""
    out: dict[str, int] = {}
    for row in monthly_all:
        try:
            y = int(row.get("year", 0))
        except (TypeError, ValueError):
            continue
        if y != data_year:
            continue
        try:
            m = _period_str_to_month(str(row.get("period", "")))
        except (TypeError, ValueError):
            continue
        if 1 <= m <= 12:
            out[str(m)] = int(row.get("value", 0))
    return out


def collect_plot_sources_metadata(
    *,
    data_dir: Path,
    recent_years: int | None,
    anual_dir: Path,
    csv_glob: str,
    zip_glob_for_extract: str,
    satellite_candidates: list[str],
    reference_satellite: str | None,
    current_year: int,
    reference_date: date,
    last_closed_month: int,
    mensal_files: list[tuple[int, Path]],
    chart_spec_path: Path,
) -> dict[str, Any]:
    """
    Metadados: CSVs anuais em anual/ (contagem por mês, data_pas) e mensais do ano corrente.
    """
    dt_c = list(DEFAULT_DATETIME_CANDIDATES)
    st_c = list(DEFAULT_STATE_CANDIDATES)
    bio_c = list(DEFAULT_BIOME_CANDIDATES)
    ref = reference_satellite or None

    csv_files = _select_annual_reference_csv_files(
        anual_dir,
        csv_glob=csv_glob,
        recent_years=recent_years,
    )

    annual_entries: list[dict[str, Any]] = []
    for csv_path in sorted(csv_files, key=lambda p: p.name):
        payload = _trim_payload_monthly_to_inferred_year(
            build_year_payload_from_csv(
                csv_path,
                dt_c,
                st_c,
                bio_c,
                satellite_candidates=satellite_candidates,
                reference_satellite=ref,
            )
        )
        inf = payload.get("inferred_year")
        inf_int = int(inf) if inf is not None else None
        monthly_all = payload.get("monthly_all") or []
        by_month: dict[str, int] = {}
        if inf_int is not None:
            by_month = _monthly_all_payload_to_by_calendar_month(
                monthly_all,
                data_year=inf_int,
            )

        annual_entries.append(
            {
                "path": str(csv_path.resolve()),
                "filename": csv_path.name,
                "inferred_year": inf_int,
                "file_size_bytes": payload.get("file_size_bytes"),
                "row_count_brasil_valid_after_filters": payload.get("row_count"),
                "month_span_min": payload.get("month_span_min"),
                "month_span_max": payload.get("month_span_max"),
                "monthly_focos_by_calendar_month": by_month,
                "detected_columns": {
                    "datetime": payload.get("detected_datetime_column"),
                    "state": payload.get("detected_state_column"),
                    "biome": payload.get("detected_biome_column"),
                },
            }
        )

    mensal_entries: list[dict[str, Any]] = []
    for month, path in sorted(mensal_files, key=lambda t: t[0]):
        n_plot = count_focos_rows_brasil_file(
            path,
            dt_c,
            st_c,
            bio_c,
            satellite_candidates=satellite_candidates,
            reference_satellite=ref,
        )
        n_all = count_focos_rows_brasil_file(
            path,
            dt_c,
            st_c,
            bio_c,
            satellite_candidates=None,
            reference_satellite=None,
        )
        mensal_entries.append(
            {
                "calendar_month": month,
                "filename": path.name,
                "path": str(path.resolve()),
                "focos_count_used_in_chart": n_plot,
                "focos_count_all_rows_all_satellites": n_all,
            }
        )

    try:
        chart_rel = str(chart_spec_path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        chart_rel = str(chart_spec_path.resolve())

    return {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reference_satellite_filter": ref,
        "reference_satellite_constant": INPE_REFERENCE_SATELLITE,
        "current_year": current_year,
        "reference_date": reference_date.isoformat(),
        "last_closed_month": last_closed_month,
        "data_dir": str(data_dir.resolve()),
        "anual_dir": str(anual_dir.resolve()),
        "csv_anual_glob": csv_glob,
        "zip_glob_for_extract": zip_glob_for_extract,
        "recent_years_limit": recent_years,
        "chart_spec_json": chart_rel,
        "annual_reference_csvs": annual_entries,
        "mensal_current_year_files": mensal_entries,
        "notes_pt": (
            "Série anual: CSVs em anual/; datetime = data_pas (ISO YYYY-MM-DD HH:mm:ss) quando "
            "presente; agregação mensal = mesmo critério que COUNT(*) com filtro de mês em SQL. "
            "Sem somar duplicata (period,year) se houver mais de um CSV para o mesmo ano. "
            "Mensais: satélite conforme reference_satellite_filter quando a coluna existe. "
            "Linha do ano civil atual: só até o último mês civil já encerrado (last_closed_month), "
            "não o mês em curso."
        ),
    }


def write_plot_sources_metadata(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_bdqueimadas_social_assets(
    *,
    data_dir: Path,
    out_png: Path,
    out_json: Path,
    recent_years: int | None,
    emit_manifest: Path | None,
    mensal_base_url: str,
    current_year: int | None,
    mensal_cache_dir: Path,
    skip_mensal_download: bool,
    satellite_candidates: list[str] | None = None,
    reference_satellite: str | None = None,
    file_glob: str = "focos_br_ref_*.zip",
    csv_glob: str = "focos_br_ref_*.csv",
    metadata_out: Path | None = None,
    extract_anual_csvs: bool = True,
    anual_extract_dir: Path | None = None,
    run_llm: bool = False,
    llm_components: tuple[str, ...] | None = None,
    reference_date: date | None = None,
    app_config: Path | None = None,
    out_social_llm: Path | None = None,
    logs_dir: Path | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    cy = current_year if current_year is not None else date.today().year
    ref_d = reference_date if reference_date is not None else date.today()

    log = logger if logger is not None else get_social_bdqueimadas_logger(logs_dir)
    base_logs = logs_dir if logs_dir is not None else REPO_ROOT / "logs"
    log_stage(
        log,
        "pipeline_start",
        {
            "logs_dir": str(base_logs.resolve()),
            "data_dir": str(data_dir.resolve()),
            "current_year_arg": current_year,
            "recent_years": recent_years,
            "run_llm": run_llm,
            "llm_components": list(llm_components) if llm_components is not None else None,
            "extract_anual_csvs": extract_anual_csvs,
            "skip_mensal_download": skip_mensal_download,
            "reference_date": ref_d.isoformat(),
            "out_png": str(out_png.resolve()),
            "out_json": str(out_json.resolve()),
        },
    )

    sat_cands = satellite_candidates if satellite_candidates is not None else list(
        DEFAULT_SATELLITE_CANDIDATES
    )
    ref_sat = (
        reference_satellite
        if reference_satellite is not None
        else INPE_REFERENCE_SATELLITE
    )

    anual_dir = anual_extract_dir if anual_extract_dir is not None else data_dir / "anual"

    if extract_anual_csvs:
        written = extract_annual_reference_csvs(
            data_dir,
            file_glob=file_glob,
            recent_years=recent_years,
            out_dir=anual_dir,
        )
        log_stage(
            log,
            "extract_anual_done",
            {
                "anual_dir": str(anual_dir.resolve()),
                "zip_count": len(written),
                "zips": sorted(written.keys()),
            },
        )
    else:
        log_stage(log, "extract_anual_skipped", {"anual_dir": str(anual_dir.resolve())})

    year_payloads = _collect_year_payloads(
        recent_years=recent_years,
        anual_dir=anual_dir,
        csv_glob=csv_glob,
        satellite_candidates=sat_cands,
        reference_satellite=ref_sat,
    )
    monthly_nacional = _monthly_all_payloads_to_df_dedupe(year_payloads)
    if monthly_nacional.empty:
        raise RuntimeError("Agregação mensal vazia após leitura dos CSVs em anual/.")
    log_stage(
        log,
        "load_monthly_all_df_done",
        {
            "rows": int(len(monthly_nacional)),
            "columns": list(monthly_nacional.columns),
        },
    )

    mensal_files = ensure_mensal_files_for_year(
        base_url=mensal_base_url,
        year=cy,
        cache_dir=mensal_cache_dir,
        skip_download=skip_mensal_download,
    )
    log_stage(
        log,
        "mensal_files_ready",
        {
            "year": cy,
            "mensal_cache_dir": str(mensal_cache_dir.resolve()),
            "files": [
                {"month": m, "path": str(p.resolve())} for m, p in mensal_files
            ],
        },
    )

    dt_c = list(DEFAULT_DATETIME_CANDIDATES)
    st_c = list(DEFAULT_STATE_CANDIDATES)
    bio_c = list(DEFAULT_BIOME_CANDIDATES)

    carousel_scope_outcomes: list[dict[str, Any]] = []
    for scope in BDQUEIMADAS_CAROUSEL_SCOPES:
        slug = carousel_scope_to_slug(scope)
        inpe_key = carousel_scope_to_inpe_biome_key(scope)
        label_pt = carousel_biome_label_pt(scope)
        biome_scope_meta = "nacional" if scope is None else (inpe_key or "unknown")
        log_stage(
            log,
            "carousel_scope_start",
            {"slug": slug, "biome_label_pt": label_pt, "inpe_biome_key": inpe_key},
        )
        try:
            if scope is None:
                monthly_df = monthly_nacional
            else:
                monthly_df = _monthly_by_biome_payloads_to_df_dedupe(
                    year_payloads,
                    inpe_key or "",
                )
                if monthly_df.empty:
                    raise RuntimeError(
                        f"Série anual vazia para o bioma {label_pt} (INPE: {inpe_key})."
                    )

            scope_counts: dict[int, int] = {}
            for month, path in mensal_files:
                scope_counts[month] = count_focos_rows_brasil_file(
                    path,
                    dt_c,
                    st_c,
                    bio_c,
                    satellite_candidates=sat_cands,
                    reference_satellite=ref_sat,
                    biome_inpe_key=inpe_key,
                )

            scope_spec = compute_chart_spec(
                monthly_df,
                current_year=cy,
                current_year_monthly_counts=scope_counts,
                reference_date=ref_d,
                reference_satellite=ref_sat or None,
                biome_scope=biome_scope_meta,
                biome_label_pt=label_pt,
            )
            stem_png = out_png.stem
            stem_json = out_json.stem
            out_png_scope = out_png.parent / f"{stem_png}-{slug}{out_png.suffix}"
            out_json_scope = out_json.parent / f"{stem_json}-{slug}{out_json.suffix}"
            render_chart_png(scope_spec, out_png_scope)
            out_json_scope.parent.mkdir(parents=True, exist_ok=True)
            out_json_scope.write_text(
                json.dumps(scope_spec, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            if slug == "nacional":
                out_png.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(out_png_scope, out_png)
                shutil.copyfile(out_json_scope, out_json)

            smeta = scope_spec["metadata"]
            log_stage(
                log,
                "carousel_scope_ok",
                {
                    "slug": slug,
                    "chart_png": str(out_png_scope.resolve()),
                    "chart_spec_json": str(out_json_scope.resolve()),
                    "last_closed_month": smeta.get("last_closed_month"),
                },
            )
            carousel_scope_outcomes.append(
                {
                    "scope": scope,
                    "slug": slug,
                    "ok": True,
                    "spec": scope_spec,
                    "chart_png": out_png_scope,
                    "chart_json": out_json_scope,
                    "image_url": f"/generated/{out_png_scope.name}",
                    "biome_label_pt": label_pt,
                    "escopo_nacional": scope is None,
                }
            )
        except Exception as e:  # noqa: BLE001
            log.exception("carousel_scope_failed")
            log_stage(
                log,
                "carousel_scope_failed",
                {"slug": slug, "error": str(e)},
            )
            carousel_scope_outcomes.append(
                {
                    "scope": scope,
                    "slug": slug,
                    "ok": False,
                    "error": str(e),
                    "biome_label_pt": label_pt,
                    "escopo_nacional": scope is None,
                }
            )

    nacional_ok = next(
        (o for o in carousel_scope_outcomes if o.get("slug") == "nacional" and o.get("ok")),
        None,
    )
    spec = nacional_ok["spec"] if nacional_ok else next(
        (o["spec"] for o in carousel_scope_outcomes if o.get("ok")),
        None,
    )
    if spec is None:
        raise RuntimeError(
            "Nenhum recorte do carrossel (Nacional / biomas) foi gerado com sucesso."
        )
    meta0 = spec["metadata"]
    log_stage(
        log,
        "chart_spec_computed",
        {
            "latest_year": meta0.get("latest_year"),
            "last_closed_month": meta0.get("last_closed_month"),
            "reference_date": meta0.get("reference_date"),
            "published_at_label": meta0.get("published_at_label"),
            "avg_window": [
                meta0.get("avg_window_years_from"),
                meta0.get("avg_window_years_to"),
            ],
            "carousel_slugs": [o.get("slug") for o in carousel_scope_outcomes],
        },
    )
    log_stage(log, "render_chart_png_done", {"path": str(out_png.resolve())})
    log_stage(log, "chart_spec_json_written", {"path": str(out_json.resolve())})

    llm_graphic_text: str | None = None
    carousel_body_texts: dict[str, str] = {}
    carousel_body_models: dict[str, str] = {}
    post_caption_text: str | None = None
    post_caption_model: str | None = None

    if run_llm:
        cfg = app_config if app_config is not None else DEFAULT_APP_CONFIG
        settings = load_settings(str(cfg))
        from forest_pipelines.social.llm.registry import (
            COMPONENT_GRAPHIC_TEXT,
            COMPONENT_POST_DESCRIPTION,
            DEFAULT_COMPONENTS,
            TOPIC_FOCOS_INCENDIO_BR,
            generate_carousel_instagram_caption,
            generate_graphic_text_for_carousel_scope,
        )
        requested_components = llm_components if llm_components is not None else DEFAULT_COMPONENTS

        log_stage(
            log,
            "llm_run_start",
            {
                "topic": TOPIC_FOCOS_INCENDIO_BR,
                "reference_date": ref_d.isoformat(),
                "components": list(requested_components),
                "app_config": str(cfg.resolve()),
            },
        )

        if COMPONENT_POST_DESCRIPTION in requested_components:
            try:
                cap = generate_carousel_instagram_caption(
                    ref_d,
                    settings.llm,
                    topic_id=TOPIC_FOCOS_INCENDIO_BR,
                    logger=log,
                )
                post_caption_text = cap["text"]
                post_caption_model = cap["model"]
            except Exception as e:  # noqa: BLE001
                log.exception("carousel_caption_failed")
                log_stage(log, "carousel_caption_failed", {"error": str(e)})
                post_caption_text = ""
                post_caption_model = ""

        if COMPONENT_GRAPHIC_TEXT in requested_components:
            for outcome in carousel_scope_outcomes:
                slug = str(outcome.get("slug", ""))
                if not outcome.get("ok"):
                    carousel_body_texts[slug] = (
                        "Não foi possível gerar o texto deste slide (falha nos dados ou no gráfico). "
                        "Confira o log do pipeline."
                    )
                    carousel_body_models[slug] = ""
                    log_stage(
                        log,
                        "carousel_graphic_skipped",
                        {"slug": slug, "reason": outcome.get("error", "unknown")},
                    )
                    continue
                try:
                    ospec = outcome["spec"]
                    blabel = str(outcome.get("biome_label_pt", "Brasil (Nacional)"))
                    esc_n = bool(outcome.get("escopo_nacional"))
                    gt = generate_graphic_text_for_carousel_scope(
                        ospec,
                        ref_d,
                        settings.llm,
                        biome_label_pt=blabel,
                        escopo_nacional=esc_n,
                        scope_slug=slug,
                        topic_id=TOPIC_FOCOS_INCENDIO_BR,
                        logger=log,
                    )
                    carousel_body_texts[slug] = gt["text"]
                    carousel_body_models[slug] = gt["model"]
                    if slug == "nacional":
                        llm_graphic_text = gt["text"]
                except Exception as e:  # noqa: BLE001
                    log.exception("carousel_graphic_failed")
                    log_stage(
                        log,
                        "carousel_graphic_failed",
                        {"slug": slug, "error": str(e)},
                    )
                    carousel_body_texts[slug] = (
                        "Não foi possível gerar o texto automático deste slide. "
                        f"Erro: {e}"
                    )
                    carousel_body_models[slug] = ""

        sidecar = out_social_llm if out_social_llm is not None else DEFAULT_SOCIAL_LLM_JSON
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar_payload: dict[str, Any] = {
            "schema_version": 2,
            "topic": TOPIC_FOCOS_INCENDIO_BR,
            "reference_date": ref_d.isoformat(),
            "components": list(requested_components),
            "post_description": post_caption_text,
            "post_description_model": post_caption_model,
            "scopes": [],
        }
        for outcome in carousel_scope_outcomes:
            slug = str(outcome.get("slug", ""))
            entry: dict[str, Any] = {
                "slug": slug,
                "ok": bool(outcome.get("ok")),
                "biome_label_pt": outcome.get("biome_label_pt"),
                "error": outcome.get("error"),
                "graphic_text": carousel_body_texts.get(slug),
                "graphic_text_model": carousel_body_models.get(slug),
            }
            if outcome.get("ok") and outcome.get("chart_json"):
                entry["chart_spec_json"] = str(Path(outcome["chart_json"]).resolve())
            if outcome.get("ok") and outcome.get("chart_png"):
                entry["chart_png"] = str(Path(outcome["chart_png"]).resolve())
            sidecar_payload["scopes"].append(entry)
        sidecar.write_text(
            json.dumps(sidecar_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        log_stage(
            log,
            "social_llm_json_written",
            {"path": str(sidecar.resolve())},
        )

    if emit_manifest is not None:
        write_bdqueimadas_carousel_manifest(
            emit_manifest,
            carousel_scope_outcomes=carousel_scope_outcomes,
            carousel_body_texts=carousel_body_texts if run_llm else {},
            post_caption_text=post_caption_text if run_llm else None,
            meta_ref=spec["metadata"],
        )
        log_stage(
            log,
            "manifest_written",
            {
                "path": str(emit_manifest.resolve()),
                "slides": 6,
            },
        )
    else:
        log_stage(log, "manifest_skipped", {})

    meta_path = (
        metadata_out
        if metadata_out is not None
        else data_dir / "metadata" / DEFAULT_PLOT_SOURCES_METADATA_NAME
    )
    write_plot_sources_metadata(
        meta_path,
        collect_plot_sources_metadata(
            data_dir=data_dir,
            recent_years=recent_years,
            anual_dir=anual_dir,
            csv_glob=csv_glob,
            zip_glob_for_extract=file_glob,
            satellite_candidates=sat_cands,
            reference_satellite=ref_sat or None,
            current_year=cy,
            reference_date=ref_d,
            last_closed_month=int(meta0["last_closed_month"]),
            mensal_files=mensal_files,
            chart_spec_path=out_json,
        ),
    )
    log_stage(
        log,
        "plot_sources_metadata_written",
        {"path": str(meta_path.resolve())},
    )
    log_stage(log, "pipeline_done", {"ok": True})
    return spec


def write_bdqueimadas_carousel_manifest(
    path: Path,
    *,
    carousel_scope_outcomes: list[dict[str, Any]],
    carousel_body_texts: dict[str, str],
    post_caption_text: str | None,
    meta_ref: dict[str, Any],
) -> None:
    """Manifest com 6 slides: capa, 4× body_chart (Nacional + biomas), CTA."""
    ly = meta_ref["latest_year"]
    py = meta_ref.get("previous_year")
    y0 = meta_ref["avg_window_years_from"]
    y1 = meta_ref["avg_window_years_to"]
    lcm = int(meta_ref.get("last_closed_month", meta_ref.get("ytd_months", 1)))
    published_at = meta_ref.get("published_at_label", format_published_at_pt(lcm, ly))

    default_body = (
        "O gráfico resume a série mensal de focos. "
        "A faixa mais clara representa a média dos anos anteriores por mês; "
        "as linhas comparam o ano corrente com o ano anterior."
    )

    slides: list[dict[str, Any]] = [
        {
            "type": "cover",
            "slots": {
                "topic_tag": "Queimadas & Focos",
                "published_at": published_at,
                "series_label": "Séries Temporais",
                "title": "Focos de incêndio no Brasil",
                "summary": (
                    f"Brasil, Amazônia, Cerrado e Pantanal — "
                    f"{ly} vs "
                    f"{py if py is not None else '—'} "
                    f"e média por mês ({y0}–{y1})."
                ),
            },
        },
    ]

    generation_errors: list[dict[str, Any]] = []

    for outcome in carousel_scope_outcomes:
        slug = str(outcome.get("slug", ""))
        ok = bool(outcome.get("ok"))
        label = str(outcome.get("biome_label_pt", slug))
        body_txt = carousel_body_texts.get(slug)
        if not body_txt:
            body_txt = default_body
        if not ok:
            err = str(outcome.get("error", ""))
            body_txt = (
                "Não foi possível montar este slide automaticamente. "
                f"Detalhe: {err}"
            )
            generation_errors.append({"slug": slug, "error": err})

        image_url = "/generated/bdqueimadas-chart.png"
        if ok and outcome.get("chart_png"):
            image_url = f"/generated/{Path(outcome['chart_png']).name}"

        slides.append(
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": "Queimadas & Focos",
                    "published_at": published_at,
                    "caption": f"Focos por mês ({label}) · BDQueimadas, INPE",
                    "image_url": image_url,
                    "body_text": body_txt,
                },
                "generation": {
                    "ok": ok,
                    "error": None if ok else str(outcome.get("error", "")),
                },
            }
        )

    slides.append(
        {
            "type": "cta",
            "slots": {
                "topic_tag": "Queimadas & Focos",
                "published_at": published_at,
                "cta_kicker": "Continua acompanhando",
                "cta_headline": "Mais análises e dados",
                "cta_subline": "Acesse o portal de dados abertos do instituto.",
                "cta_url": "www.instituto-exemplo.org",
            },
        },
    )

    manifest: dict[str, Any] = {
        "theme": "green",
        "runId": "bdqueimadas-social",
        "sizes": {
            "topicTagPx": 24,
            "datePx": 26,
            "pageNumberPx": 24,
            "logoHeightPx": 54,
        },
        "slides": slides,
    }
    if post_caption_text is not None:
        manifest["instagram_caption_draft"] = post_caption_text
    if generation_errors:
        manifest["generation_errors"] = generation_errors

    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    if path.resolve() == DEFAULT_MANIFEST_PATH.resolve():
        PUBLIC_MANIFEST_COPY.parent.mkdir(parents=True, exist_ok=True)
        PUBLIC_MANIFEST_COPY.write_text(text, encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Carrossel BDQueimadas: quatro gráficos (Nacional, Amazônia, Cerrado, Pantanal) "
            "com ano atual (CSV mensal INPE), ano anterior e média de 5 anos (ZIPs anuais). "
            "Saídas sufixadas em public/generated/; cópias nacionais canônicas bdqueimadas-chart.png e chart_spec.json."
        )
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=REPO_ROOT / DEFAULT_DATA_SUBDIR,
        help="Pasta com focos_br_ref_*.zip (default: <repo>/data/inpe_bdqueimadas).",
    )
    p.add_argument(
        "--recent-years",
        type=int,
        default=None,
        metavar="N",
        help="Usar apenas os N ZIPs anuais mais recentes (default: todos).",
    )
    p.add_argument(
        "--mensal-base-url",
        default=DEFAULT_MENSAL_BASE_URL,
        help="URL da listagem mensal Brasil (default: dataserver INPE).",
    )
    p.add_argument(
        "--mensal-cache-dir",
        type=Path,
        default=REPO_ROOT / DEFAULT_MENSAL_CACHE_DIR,
        help="Cache dos CSV/ZIP mensais (default: <repo>/data/inpe_bdqueimadas/mensal).",
    )
    p.add_argument(
        "--current-year",
        type=int,
        default=None,
        metavar="YYYY",
        help="Ano civil da linha principal (default: ano atual do sistema).",
    )
    p.add_argument(
        "--skip-mensal-download",
        action="store_true",
        help="Usar só arquivos já presentes em --mensal-cache-dir (sem HTTP).",
    )
    p.add_argument(
        "--out-png",
        type=Path,
        default=DEFAULT_CHART_PNG,
        help=(
            "PNG canônico do recorte nacional (cópia do sufixo -nacional; default: "
            f"{DEFAULT_CHART_PNG})"
        ),
    )
    p.add_argument(
        "--out-json",
        type=Path,
        default=DEFAULT_CHART_SPEC,
        help=(
            "chart_spec.json canônico do recorte nacional (default: "
            f"{DEFAULT_CHART_SPEC})"
        ),
    )
    p.add_argument(
        "--emit-manifest",
        type=Path,
        nargs="?",
        const=DEFAULT_MANIFEST_PATH,
        default=None,
        help=(
            "Grava manifest do compositor com 6 slides (capa + 4 body_chart + CTA; default se a "
            f"flag for usada sem caminho: {DEFAULT_MANIFEST_PATH})"
        ),
    )
    p.add_argument(
        "--metadata-out",
        type=Path,
        default=None,
        help=(
            "JSON com metadados das bases usadas no gráfico (default: "
            "<--data-dir>/metadata/bdqueimadas_plot_sources.json)."
        ),
    )
    p.add_argument(
        "--file-glob",
        default="focos_br_ref_*.zip",
        help="Glob dos ZIPs anuais em --data-dir (default: focos_br_ref_*.zip).",
    )
    p.add_argument(
        "--csv-glob",
        default="focos_br_ref_*.csv",
        help="Glob dos CSVs em --anual-dir para a série histórica (default: focos_br_ref_*.csv).",
    )
    p.add_argument(
        "--no-extract-anual",
        action="store_true",
        help="Não extrair CSV de cada ZIP para <data-dir>/anual/.",
    )
    p.add_argument(
        "--anual-dir",
        type=Path,
        default=None,
        help="Pasta para CSVs extraídos dos ZIPs (default: <data-dir>/anual).",
    )
    p.add_argument(
        "--llm",
        action="store_true",
        help=(
            "Gera textos com Groq: uma legenda Instagram para o carrossel + quatro textos de slide "
            "(um por recorte). "
            f"Requer {DEFAULT_APP_CONFIG.name} e GROQ_API_KEY. "
            f"Grava {DEFAULT_SOCIAL_LLM_JSON.name} em public/generated/ (ou --out-social-llm)."
        ),
    )
    p.add_argument(
        "--llm-components",
        default="post_description,graphic_text",
        help=(
            "Componentes LLM separados por vírgula: post_description (legenda única do carrossel), "
            "graphic_text (quatro chamadas, uma por escopo). "
            "Valores aceitos: post_description, graphic_text (default: ambos)."
        ),
    )
    p.add_argument(
        "--as-of",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Data de referência: define o último mês civil fechado na linha do ano atual "
            "(ex.: abril → até março), além do prefixo [YYYY-MM-DD] na legenda LLM (default: hoje)."
        ),
    )
    p.add_argument(
        "--app-config",
        type=Path,
        default=None,
        help=f"Caminho para app.yml (default: <repo>/configs/app.yml). Usado com --llm.",
    )
    p.add_argument(
        "--out-social-llm",
        type=Path,
        default=None,
        help=f"Saída JSON com textos LLM (default: {DEFAULT_SOCIAL_LLM_JSON}).",
    )
    p.add_argument(
        "--logs-dir",
        type=Path,
        default=REPO_ROOT / "logs",
        help="Pasta base para logs (default: <repo>/logs).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()
    args = _parse_args(argv)
    llm_components = tuple(
        part.strip() for part in str(args.llm_components).split(",") if part.strip()
    )
    ref_date: date | None = None
    if args.as_of:
        ref_date = date.fromisoformat(args.as_of)
    run_logger = get_social_bdqueimadas_logger(args.logs_dir)
    try:
        spec = build_bdqueimadas_social_assets(
            data_dir=args.data_dir,
            out_png=args.out_png,
            out_json=args.out_json,
            recent_years=args.recent_years,
            emit_manifest=args.emit_manifest,
            mensal_base_url=args.mensal_base_url,
            current_year=args.current_year,
            mensal_cache_dir=args.mensal_cache_dir,
            skip_mensal_download=args.skip_mensal_download,
            file_glob=args.file_glob,
            csv_glob=args.csv_glob,
            metadata_out=args.metadata_out,
            extract_anual_csvs=not args.no_extract_anual,
            anual_extract_dir=args.anual_dir,
            run_llm=args.llm,
            llm_components=llm_components,
            reference_date=ref_date,
            app_config=args.app_config,
            out_social_llm=args.out_social_llm,
            logs_dir=args.logs_dir,
            logger=run_logger,
        )
    except Exception as e:  # noqa: BLE001
        run_logger.exception("pipeline_failed")
        print(f"Erro: {e}", file=sys.stderr)
        return 1

    meta = spec["metadata"]
    print(
        f"OK: {args.out_png} e {args.out_json} "
        f"(ano atual {meta['latest_year']}, média {meta['avg_window_years_from']}–{meta['avg_window_years_to']}, "
        f"publicação {meta.get('published_at_label', '')})"
    )
    if args.emit_manifest:
        print(f"Manifest: {args.emit_manifest}")
    if args.llm:
        p_llm = args.out_social_llm if args.out_social_llm is not None else DEFAULT_SOCIAL_LLM_JSON
        print(f"Textos LLM (Groq): {p_llm.resolve()}")
    meta_out = (
        args.metadata_out
        if args.metadata_out is not None
        else args.data_dir / "metadata" / DEFAULT_PLOT_SOURCES_METADATA_NAME
    )
    print(f"Metadados das bases: {meta_out.resolve()}")
    print(
        "Log do pipeline: "
        f"{(args.logs_dir / 'social' / 'bdqueimadas').resolve()}/"
        "<ano>/<mês>/<YYYY-MM-DD>.log"
    )
    if not args.no_extract_anual:
        an_dir = args.anual_dir if args.anual_dir is not None else args.data_dir / "anual"
        print(f"CSV anual extraído em: {an_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
