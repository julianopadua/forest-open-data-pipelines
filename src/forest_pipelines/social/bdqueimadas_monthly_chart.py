"""Monthly BDQueimadas comparison chart (YTD ano atual via mensal INPE vs ano anterior ZIP vs média 5 anos)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from forest_pipelines.datasets.inpe.bdqueimadas_mensal_listing import (
    DEFAULT_MENSAL_BASE_URL,
    ensure_mensal_files_for_year,
)
from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    INPE_REFERENCE_SATELLITE,
    _build_year_payload,
    consolidate_year_payloads,
    count_focos_rows_brasil_file,
)
from forest_pipelines.reports.builders.bdqueimadas_overview import (
    DEFAULT_BIOME_CANDIDATES,
    DEFAULT_DATETIME_CANDIDATES,
    DEFAULT_SATELLITE_CANDIDATES,
    DEFAULT_STATE_CANDIDATES,
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
    """Ex.: Abr 2026 — alinhado ao último mês com dado na série atual."""
    if month < 1 or month > 12:
        return f"{year}"
    return f"{MONTH_LABELS_PT[month - 1]} {year}"


def load_monthly_all_df(
    data_dir: Path,
    *,
    recent_years: int | None = None,
    file_glob: str = "focos_br_ref_*.zip",
    satellite_candidates: list[str] | None = None,
    reference_satellite: str | None = None,
) -> pd.DataFrame:
    base = data_dir
    zip_files = _select_zip_files(
        base_dir=base,
        file_glob=file_glob,
        recent_years=recent_years,
    )
    if not zip_files:
        raise FileNotFoundError(
            f"Nenhum ZIP encontrado em {base.resolve()} com glob {file_glob!r}."
        )

    datetime_candidates = list(DEFAULT_DATETIME_CANDIDATES)
    state_candidates = list(DEFAULT_STATE_CANDIDATES)
    biome_candidates = list(DEFAULT_BIOME_CANDIDATES)

    payloads: list[dict[str, Any]] = []
    for zp in zip_files:
        payloads.append(
            _build_year_payload(
                zp,
                datetime_candidates,
                state_candidates,
                biome_candidates,
                satellite_candidates=satellite_candidates,
                reference_satellite=reference_satellite,
            )
        )

    consolidated = consolidate_year_payloads(payloads)
    monthly_all_df = consolidated["monthly_all_df"]
    if monthly_all_df.empty:
        raise RuntimeError("Agregação mensal vazia após leitura dos ZIPs.")
    return monthly_all_df


def compute_chart_spec(
    monthly_all_df: pd.DataFrame,
    *,
    current_year: int,
    current_year_monthly_counts: dict[int, int],
    reference_satellite: str | None = None,
) -> dict[str, Any]:
    if not current_year_monthly_counts:
        raise RuntimeError(
            "Sem dados mensais do ano civil atual. "
            "Verifique rede/cache em data/inpe_bdqueimadas/mensal/."
        )

    previous_year = current_year - 1
    ytd_month = max(current_year_monthly_counts.keys())

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
        if m <= ytd_month:
            series_current.append(int(current_year_monthly_counts.get(m, 0)))
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

    published_at_label = format_published_at_pt(ytd_month, current_year)

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
        "ytd_months": ytd_month,
        "unit": "focos",
        "source": source,
        "published_at_label": published_at_label,
    }
    if reference_satellite:
        meta["reference_satellite"] = reference_satellite

    return {
        "schema_version": 2,
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
    ax.set_xticklabels(labels, fontsize=8, color=_WHITE, rotation=0, ha="center")
    ax.set_xlabel("Mês", fontsize=11, color=_WHITE, labelpad=6)
    ax.set_ylabel("Nº de focos", fontsize=11, color=_WHITE, labelpad=6)

    # Não usar MaxNLocator no eixo X: ele reduz o número de ticks e some rótulos de meses.
    ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=9))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _pos: f"{int(v):,}".replace(",", "."))
    )

    ax.tick_params(axis="both", labelsize=10, colors=_WHITE)
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
    ax.legend(
        handles=legend_handles,
        frameon=True,
        facecolor=(0.1, 0.14, 0.12),
        edgecolor=(0.0, 0.0, 0.0, 0.0),
        fontsize=9,
        loc="upper right",
        labelcolor=_WHITE,
        framealpha=0.4,
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
    file_glob: str,
    satellite_candidates: list[str],
    reference_satellite: str | None,
    current_year: int,
    mensal_files: list[tuple[int, Path]],
    chart_spec_path: Path,
) -> dict[str, Any]:
    """
    Metadados das bases usadas no gráfico: ZIPs anuais (contagem por mês) e
    CSVs mensais do ano corrente (contagem total por arquivo, com e sem filtro de satélite).
    """
    dt_c = list(DEFAULT_DATETIME_CANDIDATES)
    st_c = list(DEFAULT_STATE_CANDIDATES)
    bio_c = list(DEFAULT_BIOME_CANDIDATES)
    ref = reference_satellite or None

    zip_files = _select_zip_files(
        base_dir=data_dir,
        file_glob=file_glob,
        recent_years=recent_years,
    )

    annual_entries: list[dict[str, Any]] = []
    for zp in sorted(zip_files, key=lambda p: p.name):
        payload = _build_year_payload(
            zp,
            dt_c,
            st_c,
            bio_c,
            satellite_candidates=satellite_candidates,
            reference_satellite=ref,
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
                "path": str(zp.resolve()),
                "filename": zp.name,
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
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reference_satellite_filter": ref,
        "reference_satellite_constant": INPE_REFERENCE_SATELLITE,
        "current_year": current_year,
        "data_dir": str(data_dir.resolve()),
        "annual_zip_glob": file_glob,
        "recent_years_zip_limit": recent_years,
        "chart_spec_json": chart_rel,
        "annual_reference_zips": annual_entries,
        "mensal_current_year_files": mensal_entries,
        "notes_pt": (
            "Contagens do gráfico: linhas com datetime/UF/bioma válidos (Brasil); "
            "se existir coluna de satélite, aplica-se o filtro do reference_satellite_filter. "
            "Nos CSVs mensais, focos_count_all_rows_all_satellites é a soma de todas as linhas "
            "válidas sem filtro de satélite (comparar com o portal pode exigir o mesmo critério "
            "de satélite e escopo geográfico)."
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
    metadata_out: Path | None = None,
) -> dict[str, Any]:
    cy = current_year if current_year is not None else date.today().year

    sat_cands = satellite_candidates if satellite_candidates is not None else list(
        DEFAULT_SATELLITE_CANDIDATES
    )
    ref_sat = (
        reference_satellite
        if reference_satellite is not None
        else INPE_REFERENCE_SATELLITE
    )

    monthly = load_monthly_all_df(
        data_dir,
        recent_years=recent_years,
        file_glob=file_glob,
        satellite_candidates=sat_cands,
        reference_satellite=ref_sat,
    )

    mensal_files = ensure_mensal_files_for_year(
        base_url=mensal_base_url,
        year=cy,
        cache_dir=mensal_cache_dir,
        skip_download=skip_mensal_download,
    )

    dt_c = list(DEFAULT_DATETIME_CANDIDATES)
    st_c = list(DEFAULT_STATE_CANDIDATES)
    bio_c = list(DEFAULT_BIOME_CANDIDATES)

    current_year_monthly_counts: dict[int, int] = {}
    for month, path in mensal_files:
        current_year_monthly_counts[month] = count_focos_rows_brasil_file(
            path,
            dt_c,
            st_c,
            bio_c,
            satellite_candidates=sat_cands,
            reference_satellite=ref_sat,
        )

    spec = compute_chart_spec(
        monthly,
        current_year=cy,
        current_year_monthly_counts=current_year_monthly_counts,
        reference_satellite=ref_sat or None,
    )
    render_chart_png(spec, out_png)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps(spec, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if emit_manifest is not None:
        write_bdqueimadas_manifest(spec, emit_manifest)

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
            file_glob=file_glob,
            satellite_candidates=sat_cands,
            reference_satellite=ref_sat or None,
            current_year=cy,
            mensal_files=mensal_files,
            chart_spec_path=out_json,
        ),
    )
    return spec


def write_bdqueimadas_manifest(spec: dict[str, Any], path: Path) -> None:
    meta = spec["metadata"]
    ly = meta["latest_year"]
    py = meta.get("previous_year")
    y0 = meta["avg_window_years_from"]
    y1 = meta["avg_window_years_to"]
    published_at = meta.get("published_at_label", format_published_at_pt(meta["ytd_months"], ly))

    manifest = {
        "theme": "green",
        "runId": "bdqueimadas-social",
        "sizes": {
            "topicTagPx": 24,
            "datePx": 26,
            "pageNumberPx": 24,
            "logoHeightPx": 48,
        },
        "slides": [
            {
                "type": "cover",
                "slots": {
                    "topic_tag": "Queimadas & Clima",
                    "published_at": published_at,
                    "series_label": "Série temporal",
                    "title": "Focos de calor no Brasil",
                    "summary": (
                        f"Comparativo mês a mês: {ly} (parcial) vs "
                        f"{py if py is not None else '—'} "
                        f"e média por mês ({y0}–{y1})."
                    ),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": "Queimadas & Clima",
                    "published_at": published_at,
                    "caption": "Focos por mês (Brasil)",
                    "image_url": "/generated/bdqueimadas-chart.png",
                    "body_text": (
                        "O gráfico resume a série mensal de focos no território nacional. "
                        "A faixa mais clara representa a média dos anos anteriores por mês; "
                        "as linhas comparam o ano corrente com o ano anterior."
                    ),
                },
            },
            {
                "type": "cta",
                "slots": {
                    "topic_tag": "Queimadas & Clima",
                    "published_at": published_at,
                    "cta_kicker": "Continua acompanhando",
                    "cta_headline": "Mais análises e dados",
                    "cta_subline": "Acesse o portal de dados abertos do instituto.",
                    "cta_url": "www.instituto-exemplo.org",
                },
            },
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    if path.resolve() == DEFAULT_MANIFEST_PATH.resolve():
        PUBLIC_MANIFEST_COPY.parent.mkdir(parents=True, exist_ok=True)
        PUBLIC_MANIFEST_COPY.write_text(text, encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Gera PNG + chart_spec.json: ano atual (CSV mensal INPE), "
            "ano anterior e média 5 anos (ZIPs anuais locais)."
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
        help=f"Saída PNG (default: {DEFAULT_CHART_PNG})",
    )
    p.add_argument(
        "--out-json",
        type=Path,
        default=DEFAULT_CHART_SPEC,
        help=f"Saída chart_spec.json (default: {DEFAULT_CHART_SPEC})",
    )
    p.add_argument(
        "--emit-manifest",
        type=Path,
        nargs="?",
        const=DEFAULT_MANIFEST_PATH,
        default=None,
        help=(
            "Grava manifest do compositor (default se a flag for usada sem caminho: "
            f"{DEFAULT_MANIFEST_PATH})"
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
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
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
            metadata_out=args.metadata_out,
        )
    except Exception as e:  # noqa: BLE001
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
    meta_out = (
        args.metadata_out
        if args.metadata_out is not None
        else args.data_dir / "metadata" / DEFAULT_PLOT_SOURCES_METADATA_NAME
    )
    print(f"Metadados das bases: {meta_out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
