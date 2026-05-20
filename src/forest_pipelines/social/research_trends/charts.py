"""Matplotlib chart renderers for the research-trends deck.

All charts are rendered at 1080×620 px (DPI 100, figsize 10.8 × 6.2) to match
the social-post-template body-chart slot. The white theme palette is used so
the PNGs sit cleanly on the light card background.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# White theme palette — kept in sync with src/white/theme.css.
ACCENT = "#0B7B56"
ACCENT_FILL = "#D4EDE4"
TEXT_PRIMARY = "#0B1F17"
TEXT_SECONDARY = "#2C5F47"
TEXT_MUTED = "#8ABAAA"
BG = "#FFFFFF"
COMPARE = "#94a3b8"

FIG_W = 10.8
FIG_H = 6.2
DPI = 100


def _new_fig():
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(TEXT_MUTED)
    ax.tick_params(axis="both", colors=TEXT_SECONDARY, labelsize=11)
    ax.grid(axis="y", color=TEXT_MUTED, alpha=0.25, linewidth=0.8)
    return fig, ax


def _save(fig, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out, dpi=DPI, facecolor=BG, bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)


def render_publications_per_year(
    yearly_counts: dict[int, int],
    out: Path,
    *,
    title: str,
    highlight_year: int,
    window: int = 10,
) -> None:
    """Histogram of yearly publication counts over the last `window` years.

    `yearly_counts` is a {year: count} mapping. Missing years pad with zero so
    the x-axis is always full. `highlight_year` gets a stronger accent so it
    visually pops out of the bar series.
    """
    fig, ax = _new_fig()
    if not yearly_counts:
        _draw_empty(ax, title)
        _save(fig, out)
        return
    end_year = max(highlight_year, max(yearly_counts.keys()))
    years = list(range(end_year - (window - 1), end_year + 1))
    counts = [int(yearly_counts.get(y, 0)) for y in years]

    bar_colors = [ACCENT if y == highlight_year else ACCENT_FILL for y in years]
    bar_edges = [ACCENT if y == highlight_year else "none" for y in years]
    bars = ax.bar(
        years,
        counts,
        color=bar_colors,
        edgecolor=bar_edges,
        linewidth=1.5,
        width=0.72,
    )

    max_val = max(counts) if counts else 1
    headroom = max(max_val * 0.10, 1)
    ax.set_ylim(0, max_val + headroom)

    for year, bar, val in zip(years, bars, counts):
        if val <= 0:
            continue
        is_highlight = year == highlight_year
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + headroom * 0.18,
            f"{val:,}".replace(",", "."),
            ha="center",
            va="bottom",
            color=ACCENT if is_highlight else TEXT_SECONDARY,
            fontsize=11 if is_highlight else 10,
            fontweight="bold" if is_highlight else "normal",
        )

    ax.set_xticks(years)
    ax.set_xticklabels([str(y) for y in years], color=TEXT_SECONDARY)
    if highlight_year in years:
        idx = years.index(highlight_year)
        ax.get_xticklabels()[idx].set_color(ACCENT)
        ax.get_xticklabels()[idx].set_fontweight("bold")
    ax.set_title(title, color=TEXT_PRIMARY, fontsize=15, fontweight="bold", loc="left", pad=12)
    ax.set_ylabel("Trabalhos publicados", color=TEXT_SECONDARY, fontsize=11, labelpad=6)
    ax.margins(x=0.02)
    _save(fig, out)


def _draw_empty(ax, title: str) -> None:
    ax.set_title(title, color=TEXT_PRIMARY, fontsize=15, fontweight="bold", loc="left", pad=12)
    ax.text(
        0.5,
        0.5,
        "Sem dados disponíveis para o recorte atual.",
        ha="center",
        va="center",
        color=TEXT_MUTED,
        fontsize=14,
        transform=ax.transAxes,
    )
    ax.set_xticks([])
    ax.set_yticks([])


def render_top_bars(
    items: list[dict[str, Any]],
    out: Path,
    *,
    title: str,
    label_key: str = "label",
    value_key: str = "count",
) -> None:
    fig, ax = _new_fig()
    if not items:
        _draw_empty(ax, title)
        _save(fig, out)
        return
    items = list(reversed(items))  # so largest is at top
    labels = [str(it[label_key])[:48] for it in items]
    values = [int(it[value_key]) for it in items]
    bars = ax.barh(labels, values, color=ACCENT, alpha=0.9, edgecolor="none")
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + max(values) * 0.012,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}".replace(",", "."),
            va="center",
            ha="left",
            color=TEXT_SECONDARY,
            fontsize=10,
        )
    ax.set_title(title, color=TEXT_PRIMARY, fontsize=15, fontweight="bold", loc="left", pad=12)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=TEXT_MUTED, alpha=0.25, linewidth=0.8)
    ax.set_xlim(0, max(values) * 1.18 if values else 1)
    for tick in ax.get_yticklabels():
        tick.set_color(TEXT_PRIMARY)
    _save(fig, out)


def render_open_access_share(
    series: list[dict[str, Any]], out: Path, *, title: str
) -> None:
    fig, ax = _new_fig()
    if not series:
        _draw_empty(ax, title)
        _save(fig, out)
        return
    xs = [int(row["year"]) for row in series]
    oa = [float(row["oa_pct"]) for row in series]
    closed = [100.0 - val for val in oa]
    ax.fill_between(xs, oa, color=ACCENT, alpha=0.85, linewidth=0, label="Open Access")
    ax.fill_between(
        xs,
        oa,
        [100.0] * len(xs),
        color=COMPARE,
        alpha=0.35,
        linewidth=0,
        label="Acesso restrito",
    )
    ax.plot(xs, oa, color=ACCENT, linewidth=2.4)
    ax.set_ylim(0, 100)
    ax.set_ylabel("% de publicações", color=TEXT_SECONDARY, fontsize=11, labelpad=6)
    ax.set_title(title, color=TEXT_PRIMARY, fontsize=15, fontweight="bold", loc="left", pad=12)
    leg = ax.legend(
        loc="upper left",
        fontsize=10,
        frameon=True,
        facecolor=BG,
        edgecolor=TEXT_MUTED,
        labelcolor=TEXT_SECONDARY,
    )
    for text in leg.get_texts():
        text.set_color(TEXT_SECONDARY)
    ax.margins(x=0.02)
    _ = closed  # noqa: F841 — kept for legibility; values plotted via fill_between
    _save(fig, out)


def render_trends_vs_publications(
    trends_series: list[dict[str, Any]],
    pubs_series: list[dict[str, Any]],
    out: Path,
    *,
    title: str,
    trends_label: str,
) -> None:
    fig, ax = _new_fig()
    if not trends_series or not pubs_series:
        _draw_empty(ax, title)
        _save(fig, out)
        return
    tx = [row["date"] for row in trends_series]
    ty = [int(row["value"]) for row in trends_series]
    line1, = ax.plot(tx, ty, color=COMPARE, linewidth=1.6, alpha=0.85, label=trends_label)
    ax.set_ylabel("Google Trends (índice 0–100)", color=TEXT_SECONDARY, fontsize=11, labelpad=6)
    ax.set_ylim(0, max(105, max(ty) + 5 if ty else 105))
    ax.tick_params(axis="x", rotation=0)

    ax2 = ax.twinx()
    ax2.set_facecolor(BG)
    for spine in ("top",):
        ax2.spines[spine].set_visible(False)
    ax2.spines["right"].set_color(TEXT_MUTED)
    px = [int(row["year"]) for row in pubs_series]
    py = [int(row["count"]) for row in pubs_series]
    # Re-map publication years onto the same x-axis range by using their numeric position.
    line2, = ax2.plot(
        [str(y) + "-06-15" for y in px], py, color=ACCENT, linewidth=2.6, label="Publicações (OpenAlex)"
    )
    ax2.scatter([str(px[-1]) + "-06-15"] if px else [], [py[-1]] if py else [], color=ACCENT, s=46, zorder=5)
    ax2.set_ylabel("Publicações por ano", color=ACCENT, fontsize=11, labelpad=6)
    ax2.tick_params(axis="y", colors=ACCENT, labelsize=11)

    ax.set_title(title, color=TEXT_PRIMARY, fontsize=15, fontweight="bold", loc="left", pad=12)
    # Trim x-tick density.
    xticks = ax.get_xticks()
    if len(xticks) > 6:
        keep = xticks[:: max(1, len(xticks) // 6)]
        ax.set_xticks(keep)
    leg = ax.legend(
        handles=[line1, line2],
        loc="upper left",
        fontsize=10,
        frameon=True,
        facecolor=BG,
        edgecolor=TEXT_MUTED,
    )
    for text in leg.get_texts():
        text.set_color(TEXT_SECONDARY)
    _save(fig, out)
