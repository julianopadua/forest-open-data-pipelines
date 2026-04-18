"""Monthly BDQueimadas comparison chart (YTD vs previous year vs 5y monthly average) for social templates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    _build_year_payload,
    consolidate_year_payloads,
)
from forest_pipelines.reports.builders.bdqueimadas_overview import (
    DEFAULT_BIOME_CANDIDATES,
    DEFAULT_DATETIME_CANDIDATES,
    DEFAULT_STATE_CANDIDATES,
    _select_zip_files,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_SUBDIR = Path("data") / "inpe_bdqueimadas"
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


def load_monthly_all_df(
    data_dir: Path,
    *,
    recent_years: int | None = None,
    file_glob: str = "focos_br_ref_*.zip",
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
            )
        )

    consolidated = consolidate_year_payloads(payloads)
    monthly_all_df = consolidated["monthly_all_df"]
    if monthly_all_df.empty:
        raise RuntimeError("Agregação mensal vazia após leitura dos ZIPs.")
    return monthly_all_df


def compute_chart_spec(monthly_all_df: pd.DataFrame) -> dict[str, Any]:
    df = monthly_all_df.copy()
    df["month"] = pd.to_datetime(df["period"], errors="coerce").dt.month
    df = df.dropna(subset=["month"])
    df["month"] = df["month"].astype(int)

    latest_year = int(df["year"].max())
    years_sorted = sorted(int(y) for y in df["year"].unique())
    prev_years = [y for y in years_sorted if y < latest_year]
    previous_year = prev_years[-1] if prev_years else None

    latest_rows = df[df["year"] == latest_year].sort_values("period")
    if latest_rows.empty:
        raise RuntimeError(f"Sem dados para o ano mais recente ({latest_year}).")

    max_month = int(latest_rows["month"].max())
    month_names = [
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
    labels = [month_names[m - 1] for m in range(1, max_month + 1)]

    series_current: list[int] = []
    series_previous: list[int] = []
    series_avg_5y: list[float] = []

    y0 = max(latest_year - 5, min(years_sorted))
    y1 = latest_year - 1

    for m in range(1, max_month + 1):
        cur = latest_rows[latest_rows["month"] == m]["value"]
        series_current.append(int(cur.iloc[0]) if len(cur) else 0)

        if previous_year is not None:
            pr = df[(df["year"] == previous_year) & (df["month"] == m)]["value"]
            series_previous.append(int(pr.iloc[0]) if len(pr) else 0)
        else:
            series_previous.append(0)

        win = df[
            (df["year"] >= y0)
            & (df["year"] <= y1)
            & (df["month"] == m)
        ]["value"]
        series_avg_5y.append(float(win.mean()) if len(win) else 0.0)

    meta = {
        "latest_year": latest_year,
        "previous_year": previous_year,
        "avg_window_years_from": y0,
        "avg_window_years_to": y1,
        "ytd_months": max_month,
        "unit": "focos",
        "source": "INPE BDQueimadas (agregação Brasil)",
    }

    return {
        "schema_version": 1,
        "month_labels": labels,
        "series": {
            "current": {
                "key": "current",
                "label": str(latest_year),
                "values": series_current,
            },
            "previous": {
                "key": "previous",
                "label": str(previous_year) if previous_year is not None else "—",
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
    import matplotlib.pyplot as plt
    import numpy as np

    labels = spec["month_labels"]
    cur = spec["series"]["current"]["values"]
    prev = spec["series"]["previous"]["values"]
    avg = spec["series"]["avg_5y"]["values"]
    meta = spec["metadata"]
    ly = meta["latest_year"]
    py = meta.get("previous_year")

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
        color="#1a7a5e",
        alpha=0.35,
        linewidth=0,
        label=spec["series"]["avg_5y"]["label"],
    )
    ax.plot(
        x,
        cur,
        color="#2ecc9a",
        linewidth=2.8,
        label=str(ly),
    )
    if py is not None:
        ax.plot(
            x,
            prev,
            color="#94a3b8",
            linewidth=1.8,
            linestyle="--",
            label=str(py),
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11, color="#334155")
    ax.tick_params(axis="y", labelsize=10, colors="#334155")
    ax.grid(axis="y", color="#cbd5e1", linestyle="-", linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#64748b")
    ax.spines["bottom"].set_color("#64748b")

    ax.legend(
        frameon=True,
        facecolor=(1, 1, 1, 0.92),
        edgecolor="#e2e8f0",
        fontsize=9,
        loc="upper left",
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


def build_bdqueimadas_social_assets(
    *,
    data_dir: Path,
    out_png: Path,
    out_json: Path,
    recent_years: int | None,
    emit_manifest: Path | None,
) -> dict[str, Any]:
    monthly = load_monthly_all_df(data_dir, recent_years=recent_years)
    spec = compute_chart_spec(monthly)
    render_chart_png(spec, out_png)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps(spec, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if emit_manifest is not None:
        write_bdqueimadas_manifest(spec, emit_manifest)
    return spec


def write_bdqueimadas_manifest(spec: dict[str, Any], path: Path) -> None:
    meta = spec["metadata"]
    ly = meta["latest_year"]
    py = meta.get("previous_year")
    y0 = meta["avg_window_years_from"]
    y1 = meta["avg_window_years_to"]

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
                    "published_at": "Dados abertos",
                    "series_label": "Série temporal",
                    "title": "Focos de calor no Brasil",
                    "summary": (
                        f"Comparativo mês a mês: {ly} (parcial) vs "
                        f"{py if py is not None else '—'} "
                        f"e média histórica por mês ({y0}–{y1})."
                    ),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": "Queimadas & Clima",
                    "published_at": "Dados abertos",
                    "caption": "Focos por mês (Brasil)",
                    "image_url": "/generated/bdqueimadas-chart.png",
                    "legend_current": f"● {ly} (YTD)",
                    "legend_previous": f"● {py} (mesmo período)" if py is not None else "—",
                    "legend_avg": f"■ Média {y0}–{y1} por mês calendário",
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
                    "published_at": "Dados abertos",
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
        description="Gera PNG + chart_spec.json a partir dos ZIPs locais do BDQueimadas."
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
        help="Usar apenas os N anos mais recentes (default: todos os ZIPs).",
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
        )
    except Exception as e:  # noqa: BLE001
        print(f"Erro: {e}", file=sys.stderr)
        return 1

    meta = spec["metadata"]
    print(
        f"OK: {args.out_png} e {args.out_json} "
        f"(anos {meta['latest_year']}, janela média {meta['avg_window_years_from']}–{meta['avg_window_years_to']})"
    )
    if args.emit_manifest:
        print(f"Manifest: {args.emit_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
