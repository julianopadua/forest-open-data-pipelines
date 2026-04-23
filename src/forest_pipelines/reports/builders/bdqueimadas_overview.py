# src/forest_pipelines/reports/builders/bdqueimadas_overview.py
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    ALL_BIOMES_VALUE,
    INPE_REFERENCE_SATELLITE,
    biome_label_i18n,
    build_incremental_year_caches,
    combine_all_and_biome_records,
    consolidate_year_payloads,
    read_focos_subset_brasil_file,
    _df_to_records,
)
from forest_pipelines.reports.definitions.base import (
    ReportConfig,
    load_report_cfg,
    localized_text_dict,
)
from forest_pipelines.reports.llm.base import maybe_generate_analysis_blocks

RE_YEAR = re.compile(r"(\d{4})")
RE_MENSAL_CSV = re.compile(r"focos_mensal_br_(\d{4})(\d{2})\.(csv|zip)$", re.IGNORECASE)

DEFAULT_DATETIME_CANDIDATES = [
    "data_pas",
    "data_hora_gmt",
    "data_hora",
    "datahora",
    "data",
    "date",
]

DEFAULT_STATE_CANDIDATES = [
    "estado",
    "uf",
    "estado_sigla",
    "state",
]

DEFAULT_BIOME_CANDIDATES = [
    "bioma",
    "biome",
]

# Coluna de satélite nos CSVs INPE (ex.: bases mensais com vários sensores).
DEFAULT_SATELLITE_CANDIDATES = [
    "satelite",
    "satellite",
    "satélite",
]

SUPPORTED_REPORT_LOCALES = ["pt", "en"]
DEFAULT_REPORT_LOCALE = "pt"
REPORT_SCHEMA_VERSION = 2


def build_package(
    settings: Any,
    storage: Any,
    logger: Any,
    current_year_only: bool = False,
) -> dict[str, Any]:
    cfg = load_report_cfg(settings.reports_dir, "bdqueimadas_overview")

    datetime_candidates = _merge_candidates(
        cfg.columns.datetime_candidates,
        DEFAULT_DATETIME_CANDIDATES,
    )
    state_candidates = _merge_candidates(
        cfg.columns.state_candidates,
        DEFAULT_STATE_CANDIDATES,
    )
    biome_candidates = _merge_candidates(
        cfg.columns.biome_candidates,
        DEFAULT_BIOME_CANDIDATES,
    )

    zip_files = _select_zip_files(
        base_dir=settings.data_dir / cfg.dataset.local_relative_dir,
        file_glob=cfg.dataset.file_glob,
        recent_years=1 if current_year_only else cfg.dataset.recent_years,
    )

    if not zip_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado para o report em: "
            f"{(settings.data_dir / cfg.dataset.local_relative_dir).resolve()}"
        )

    cache_prefix = f"{cfg.bucket_prefix.rstrip('/')}/_cache"

    incremental = build_incremental_year_caches(
        storage=storage,
        cache_prefix=cache_prefix,
        zip_files=zip_files,
        datetime_candidates=datetime_candidates,
        state_candidates=state_candidates,
        biome_candidates=biome_candidates,
        logger=logger,
    )

    consolidated = consolidate_year_payloads(incremental["year_payloads"])

    monthly_all_df = consolidated["monthly_all_df"]
    monthly_by_biome_df = consolidated["monthly_by_biome_df"]
    annual_all_df = consolidated["annual_all_df"]
    annual_by_biome_df = consolidated["annual_by_biome_df"]
    state_year_all_df = consolidated["state_year_all_df"]
    state_year_by_biome_df = consolidated["state_year_by_biome_df"]
    state_month_all_df = consolidated["state_month_all_df"]
    available_biomes = consolidated["available_biomes"]
    yearly_file_stats = consolidated["yearly_file_stats"]
    total_rows_processed = consolidated["total_rows_processed"]

    if monthly_all_df.empty or annual_all_df.empty:
        raise RuntimeError("Não foi possível montar séries agregadas para o report BDQueimadas.")

    first_year = int(annual_all_df["year"].min())
    latest_year = int(annual_all_df["year"].max())
    previous_year = _find_previous_year(annual_all_df, latest_year)

    first_period = str(monthly_all_df["period"].iloc[0])
    latest_period = str(monthly_all_df["period"].iloc[-1])
    year_range = f"{first_year}-{latest_year}"

    available_years = [int(y) for y in annual_all_df["year"].tolist()]
    available_periods = [str(period) for period in monthly_all_df["period"].tolist()]

    default_monthly_window = monthly_all_df.tail(cfg.display.monthly_points).copy()
    default_annual_window = annual_all_df.tail(cfg.display.annual_years).copy()

    default_monthly_start = str(default_monthly_window["period"].iloc[0])
    default_monthly_end = str(default_monthly_window["period"].iloc[-1])
    default_annual_start = int(default_annual_window["year"].iloc[0])
    default_annual_end = int(default_annual_window["year"].iloc[-1])

    current_year_total = int(
        annual_all_df.loc[annual_all_df["year"] == latest_year, "value"].iloc[0]
    )
    previous_year_total = int(
        annual_all_df.loc[annual_all_df["year"] == previous_year, "value"].iloc[0]
    ) if previous_year is not None else 0

    recent_12m_total = int(monthly_all_df["value"].tail(12).sum())
    prior_12m_total = int(monthly_all_df["value"].iloc[-24:-12].sum()) if len(monthly_all_df) >= 24 else 0

    top_states_table = _build_top_states_table(
        state_year_series=state_year_all_df,
        latest_year=latest_year,
        previous_year=previous_year,
        limit=cfg.display.top_states_limit,
    )

    analysis_window_df = monthly_all_df.tail(cfg.analysis.recent_months).copy()
    analysis_window_start = str(analysis_window_df["period"].iloc[0])
    analysis_window_end = str(analysis_window_df["period"].iloc[-1])

    top_biomes_context = _build_top_biomes_context(
        annual_by_biome_df=annual_by_biome_df,
        latest_year=latest_year,
        previous_year=previous_year,
        limit=cfg.analysis.top_biomes_context_limit,
    )

    # --- Monthly metrics for static sections and enhanced LLM context ---
    latest_month_num = int(latest_period.split("-")[1]) if "-" in latest_period else 12
    latest_month_total = int(
        monthly_all_df.loc[monthly_all_df["period"] == latest_period, "value"].sum()
    )

    same_period_prev_year: str | None = None
    same_month_prev_year_total = 0
    if previous_year is not None:
        same_period_prev_year = f"{previous_year}-{latest_period[-2:]}"
        prev_vals = monthly_all_df.loc[monthly_all_df["period"] == same_period_prev_year, "value"]
        same_month_prev_year_total = int(prev_vals.sum()) if not prev_vals.empty else 0

    current_year_month_periods = [
        f"{latest_year}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)
    ]
    ytd_current_year = int(
        monthly_all_df.loc[monthly_all_df["period"].isin(current_year_month_periods), "value"].sum()
    )

    ytd_previous_year = 0
    if previous_year is not None:
        prev_year_month_periods = [
            f"{previous_year}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)
        ]
        ytd_previous_year = int(
            monthly_all_df.loc[monthly_all_df["period"].isin(prev_year_month_periods), "value"].sum()
        )

    five_avg_candidate_years = [y for y in available_years if latest_year - 5 <= y < latest_year]
    ytd_per_year: list[float] = []
    for yr in five_avg_candidate_years:
        yr_periods = [f"{yr}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)]
        yr_ytd = float(monthly_all_df.loc[monthly_all_df["period"].isin(yr_periods), "value"].sum())
        if yr_ytd > 0:
            ytd_per_year.append(yr_ytd)
    ytd_5yr_avg = round(sum(ytd_per_year) / len(ytd_per_year), 0) if ytd_per_year else None

    top_states_month_data = _build_top_states_month_merged(
        state_month_all_df=state_month_all_df,
        current_period=latest_period,
        previous_period=same_period_prev_year,
    )
    top_states_by_volume_month = _sort_top_states_month(
        top_states_month_data, sort_by="volume", limit=cfg.display.top_states_limit
    )
    top_states_by_variation_month = _sort_top_states_month(
        top_states_month_data, sort_by="variation", limit=cfg.display.top_states_limit
    )

    highlights = _build_highlights(
        latest_year=latest_year,
        previous_year=previous_year,
        current_year_total=current_year_total,
        previous_year_total=previous_year_total,
        recent_12m_total=recent_12m_total,
        prior_12m_total=prior_12m_total,
        latest_period=latest_period,
        total_rows_processed=total_rows_processed,
        file_count_used=len(zip_files),
        year_range=year_range,
    )

    # Last closed month (latest_period is the most recent closed month)
    last_closed_month = int(latest_period.split("-")[1]) if "-" in latest_period else 12

    # 5-year average window bounds
    avg_window_start = min(five_avg_candidate_years) if five_avg_candidate_years else latest_year - 5
    avg_window_end = max(five_avg_candidate_years) if five_avg_candidate_years else latest_year - 1

    # Latest month 5yr average for LLM context
    latest_month_5yr_avg_vals = []
    for yr in five_avg_candidate_years:
        yr_period = f"{yr}-{latest_period[-2:]}"
        yr_val = float(monthly_all_df.loc[monthly_all_df["period"] == yr_period, "value"].sum())
        if yr_val > 0:
            latest_month_5yr_avg_vals.append(yr_val)
    latest_month_5yr_avg = round(
        sum(latest_month_5yr_avg_vals) / len(latest_month_5yr_avg_vals), 0
    ) if latest_month_5yr_avg_vals else None

    analysis_context = {
        "coverage_first_year": first_year,
        "coverage_latest_year": latest_year,
        "coverage_year_range": year_range,
        "coverage_first_period": first_period,
        "coverage_latest_period": latest_period,
        "analysis_window_months": cfg.analysis.recent_months,
        "analysis_window_start_period": analysis_window_start,
        "analysis_window_end_period": analysis_window_end,
        "latest_year": latest_year,
        "previous_year": previous_year,
        "latest_period": latest_period,
        "current_year_total": current_year_total,
        "previous_year_total": previous_year_total,
        "recent_12m_total": recent_12m_total,
        "prior_12m_total": prior_12m_total,
        "total_rows_processed": total_rows_processed,
        "file_count_used": len(zip_files),
        "available_biomes": available_biomes,
        "cache_stats": incremental["cache_stats"],
        "top_states_current_year": [
            {
                "state": row["state"],
                "current_year_total": row["current_year_total"],
                "previous_year_total": row["previous_year_total"],
            }
            for row in top_states_table[: min(cfg.analysis.top_states_context_limit, len(top_states_table))]
        ],
        "top_biomes_current_year": top_biomes_context,
        "monthly_analysis": {
            "latest_period": latest_period,
            "latest_month_num": latest_month_num,
            "last_closed_month": last_closed_month,
            "avg_window_start": avg_window_start,
            "avg_window_end": avg_window_end,
            "latest_month_total": latest_month_total,
            "same_period_prev_year": same_period_prev_year,
            "same_month_prev_year_total": same_month_prev_year_total,
            "latest_month_pct_change_vs_prev_year": _safe_pct_change(latest_month_total, same_month_prev_year_total),
            "latest_month_5yr_avg": latest_month_5yr_avg,
            "latest_month_pct_change_vs_5yr_avg": _safe_pct_change(latest_month_total, latest_month_5yr_avg) if latest_month_5yr_avg else None,
            "ytd_current_year": ytd_current_year,
            "ytd_previous_year": ytd_previous_year,
            "ytd_pct_change": _safe_pct_change(ytd_current_year, ytd_previous_year),
            "ytd_5yr_avg": ytd_5yr_avg,
            "ytd_vs_5yr_avg_pct": _safe_pct_change(ytd_current_year, ytd_5yr_avg) if ytd_5yr_avg else None,
            "top_states_latest_month": [
                {
                    "state": row["state"],
                    "current_month_total": row["current_month_total"],
                    "previous_month_total": row["previous_month_total"],
                    "pct_change": row["pct_change"],
                }
                for row in top_states_by_volume_month[: min(cfg.analysis.top_states_context_limit, len(top_states_by_volume_month))]
            ],
        },
    }

    fallback_analysis = _build_fallback_analysis(
        first_year=first_year,
        latest_year=latest_year,
        previous_year=previous_year,
        current_year_total=current_year_total,
        previous_year_total=previous_year_total,
        recent_12m_total=recent_12m_total,
        prior_12m_total=prior_12m_total,
        latest_period=latest_period,
        total_rows_processed=total_rows_processed,
        file_count_used=len(zip_files),
        year_range=year_range,
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
        latest_month_total=latest_month_total,
        same_month_prev_year_total=same_month_prev_year_total,
        ytd_current_year=ytd_current_year,
        ytd_previous_year=ytd_previous_year,
    )

    analysis_blocks = maybe_generate_analysis_blocks(
        settings=settings,
        llm_cfg=cfg.llm,
        report_id=cfg.id,
        prompt_context=analysis_context,
        fallback_blocks=fallback_analysis,
        logger=logger,
    )

    title_i18n = localized_text_dict(cfg.title) or _localized("", "")
    source_label_i18n = localized_text_dict(cfg.source_label) or _localized("", "")
    summary_i18n = localized_text_dict(cfg.summary) if cfg.summary is not None else None

    # Load monthly CSV files for current year (more accurate than annual ZIP for incomplete year)
    mensal_dir = settings.data_dir / cfg.dataset.local_relative_dir / "mensal"
    mensal_counts = _load_mensal_counts_for_current_year(
        mensal_dir=mensal_dir,
        current_year=latest_year,
        datetime_candidates=datetime_candidates,
        state_candidates=state_candidates,
        biome_candidates=biome_candidates,
        satellite_candidates=DEFAULT_SATELLITE_CANDIDATES,
    )
    # Override last_closed_month from actual monthly files if available
    if mensal_counts["last_closed_month"] > 0:
        last_closed_month = mensal_counts["last_closed_month"]
        logger.info(
            "Usando dados mensais INPE COIDS: %d meses disponíveis para %d (último: %d)",
            len(mensal_counts["national"]),
            latest_year,
            last_closed_month,
        )

    # Monthly year comparison section data (biome + state filterable, period static)
    available_states = sorted(
        str(s) for s in state_month_all_df["state"].unique() if pd.notna(s)
    ) if not state_month_all_df.empty else []

    monthly_year_comparison_data = _build_monthly_year_comparison_records(
        monthly_all_df=monthly_all_df,
        monthly_by_biome_df=monthly_by_biome_df,
        state_month_all_df=state_month_all_df,
        latest_year=latest_year,
        previous_year=previous_year,
        five_avg_candidate_years=five_avg_candidate_years,
        last_closed_month=last_closed_month,
        mensal_counts=mensal_counts,
    )

    # Static monthly series: all historical months, __all__ biome only
    static_monthly_df = monthly_all_df[["period", "year", "value"]].copy()
    static_monthly_df["biome"] = ALL_BIOMES_VALUE
    static_monthly_records = _df_to_records(static_monthly_df)

    # Month label for column headers (e.g. "Abr/2026")
    latest_month_label_pt = _month_label_pt(latest_period)
    latest_month_label_en = _month_label_en(latest_period)
    prev_month_label_pt = _month_label_pt(same_period_prev_year) if same_period_prev_year else str(previous_year or "")
    prev_month_label_en = _month_label_en(same_period_prev_year) if same_period_prev_year else str(previous_year or "")

    monthly_series_records = combine_all_and_biome_records(
        all_df=monthly_all_df[["period", "year", "value"]],
        by_biome_df=monthly_by_biome_df[["period", "year", "biome", "value"]],
        sort_cols=["period"],
    )
    annual_totals_records = combine_all_and_biome_records(
        all_df=annual_all_df[["year", "value"]],
        by_biome_df=annual_by_biome_df[["year", "biome", "value"]],
        sort_cols=["year"],
    )
    state_year_records = combine_all_and_biome_records(
        all_df=state_year_all_df[["year", "state", "value"]],
        by_biome_df=state_year_by_biome_df[["year", "state", "biome", "value"]],
        sort_cols=["year", "state"],
    )

    generated_report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_id": cfg.id,
        "title": title_i18n,
        "source_label": source_label_i18n,
        "summary": summary_i18n,
        "available_locales": SUPPORTED_REPORT_LOCALES,
        "default_locale": DEFAULT_REPORT_LOCALE,
        "generated_at": _now_iso(),
        "publication_status": "generated",
        "dataset": {
            "dataset_id": cfg.dataset.dataset_id,
            "local_relative_dir": cfg.dataset.local_relative_dir,
            "file_count_used": len(zip_files),
            "total_rows_processed": total_rows_processed,
            "years_loaded": len(available_years),
            "available_biomes": available_biomes,
            "cache": incremental["cache_stats"],
        },
        "coverage": {
            "first_year": first_year,
            "latest_year": latest_year,
            "previous_year": previous_year,
            "first_period": first_period,
            "latest_period": latest_period,
            "year_range": year_range,
            "period_range": {
                "start": first_period,
                "end": latest_period,
            },
            "recent_years_loaded": cfg.dataset.recent_years,
        },
        "filters": {
            "biome": {
                "kind": "single_select",
                "label": _localized("Bioma", "Biome"),
                "default_value": ALL_BIOMES_VALUE,
                "all_value": ALL_BIOMES_VALUE,
                "options": [
                    {
                        "value": ALL_BIOMES_VALUE,
                        "label": _localized("Todos os biomas", "All biomes"),
                    },
                    *[
                        {
                            "value": biome,
                            "label": biome_label_i18n(biome),
                        }
                        for biome in available_biomes
                    ],
                ],
            },
            "period": {
                "kind": "range",
                "label": _localized("Período", "Period"),
                "granularities": ["year", "month"],
                "available_years": available_years,
                "available_periods": available_periods,
                "bounds": {
                    "year_start": first_year,
                    "year_end": latest_year,
                    "period_start": first_period,
                    "period_end": latest_period,
                },
            },
        },
        "analysis_scope": {
            "recent_months": cfg.analysis.recent_months,
            "window_start_period": analysis_window_start,
            "window_end_period": analysis_window_end,
            "biome_scope": ALL_BIOMES_VALUE,
        },
        "highlights": highlights,
        "analysis": analysis_blocks,
        "sections": [
            {
                "id": "monthly_year_comparison",
                "kind": "monthly_year_comparison",
                "is_static": True,
                "title": _localized(
                    f"Focos mensais por ano — comparativo {latest_year} vs {previous_year} vs média {avg_window_start}–{avg_window_end}",
                    f"Monthly hotspots by year — {latest_year} vs {previous_year} vs {avg_window_start}–{avg_window_end} average",
                ),
                "current_year": latest_year,
                "previous_year": previous_year,
                "avg_window_start": avg_window_start,
                "avg_window_end": avg_window_end,
                "last_closed_month": last_closed_month,
                "filterable_by": ["biome", "state"],
                "available_states": available_states,
                "data": monthly_year_comparison_data,
            },
            {
                "id": "monthly_series_static",
                "kind": "timeseries",
                "is_static": True,
                "highlight_year": latest_year,
                "title": _localized(
                    f"Focos mensais — histórico completo com destaque {latest_year}",
                    f"Monthly hotspots — full history highlighting {latest_year}",
                ),
                "x_key": "period",
                "y_key": "value",
                "biome_key": "biome",
                "filterable_by": [],
                "data": static_monthly_records,
            },
            {
                "id": "top_states_latest_month",
                "kind": "table",
                "is_static": True,
                "title": _localized(
                    f"Top UFs em focos — {latest_month_label_pt} vs {prev_month_label_pt}",
                    f"Top states by hotspots — {latest_month_label_en} vs {prev_month_label_en}",
                ),
                "columns": [
                    {"key": "state", "label": _localized("UF", "State")},
                    {"key": "current_month_total", "label": _localized(latest_month_label_pt, latest_month_label_en)},
                    {"key": "previous_month_total", "label": _localized(prev_month_label_pt, prev_month_label_en)},
                    {"key": "absolute_change", "label": _localized("Variação absoluta", "Absolute change")},
                    {"key": "pct_change", "label": _localized("Variação %", "% change")},
                ],
                "filterable_by": [],
                "data": top_states_by_volume_month,
            },
            {
                "id": "top_states_biggest_movers_month",
                "kind": "table",
                "is_static": True,
                "title": _localized(
                    f"Maiores variações em focos — {latest_month_label_pt} vs {prev_month_label_pt}",
                    f"Biggest movers in hotspots — {latest_month_label_en} vs {prev_month_label_en}",
                ),
                "columns": [
                    {"key": "state", "label": _localized("UF", "State")},
                    {"key": "current_month_total", "label": _localized(latest_month_label_pt, latest_month_label_en)},
                    {"key": "previous_month_total", "label": _localized(prev_month_label_pt, prev_month_label_en)},
                    {"key": "absolute_change", "label": _localized("Variação absoluta", "Absolute change")},
                    {"key": "pct_change", "label": _localized("Variação %", "% change")},
                ],
                "filterable_by": [],
                "data": top_states_by_variation_month,
            },
            {
                "id": "monthly_series",
                "kind": "timeseries",
                "title": _localized(
                    "Série mensal de focos (filtros)",
                    "Monthly hotspot series (filters)",
                ),
                "x_key": "period",
                "y_key": "value",
                "biome_key": "biome",
                "filterable_by": ["period", "biome"],
                "period_filter_granularity": "month",
                "default_view": {
                    "biome": ALL_BIOMES_VALUE,
                    "start_period": default_monthly_start,
                    "end_period": default_monthly_end,
                },
                "data": monthly_series_records,
            },
            {
                "id": "annual_totals",
                "kind": "bar",
                "title": _localized(
                    "Série anual de focos",
                    "Annual hotspot series",
                ),
                "x_key": "year",
                "y_key": "value",
                "biome_key": "biome",
                "filterable_by": ["period", "biome"],
                "period_filter_granularity": "year",
                "default_view": {
                    "biome": ALL_BIOMES_VALUE,
                    "start_year": default_annual_start,
                    "end_year": default_annual_end,
                },
                "data": annual_totals_records,
            },
            {
                "id": "top_states_current_vs_previous",
                "kind": "table",
                "title": _localized(
                    "Comparação por UF: ano selecionado vs ano anterior",
                    "State comparison: selected year vs previous year",
                ),
                "filterable_by": ["period", "biome"],
                "period_filter_granularity": "year",
                "comparison_strategy": "latest_vs_previous_year_within_filtered_range",
                "group_key": "state",
                "year_key": "year",
                "value_key": "value",
                "biome_key": "biome",
                "default_view": {
                    "biome": ALL_BIOMES_VALUE,
                    "current_year": latest_year,
                    "previous_year": previous_year,
                },
                "columns": [
                    {"key": "state", "label": _localized("UF", "State")},
                    {"key": "current_year_total", "label": _localized("Ano selecionado", "Selected year")},
                    {"key": "previous_year_total", "label": _localized("Ano anterior", "Previous year")},
                    {"key": "absolute_change", "label": _localized("Variação absoluta", "Absolute change")},
                    {"key": "pct_change", "label": _localized("Variação %", "% change")},
                ],
                "initial_comparison": {
                    "current_year": latest_year,
                    "previous_year": previous_year,
                    "rows": top_states_table,
                },
                "data": state_year_records,
            },
        ],
        "analysis_context": analysis_context,
        "yearly_file_stats": yearly_file_stats,
        "methodology": {
            "source": source_label_i18n,
            "note": _localized(
                "Este report publica agregados históricos completos para visualização e filtros, "
                "com cache incremental persistido em storage por arquivo anual. "
                "A análise textual da LLM permanece restrita à janela recente configurada.",
                "This report publishes complete historical aggregates for visualization and filtering, "
                "with incremental cache persisted in storage per annual file. "
                "The LLM text analysis remains restricted to the configured recent window.",
            ),
            "limitations": _localized(
                "O texto é descritivo e não estabelece causalidade. "
                "Filtros de bioma dependem da coluna de bioma presente nos arquivos anuais. "
                "O ano mais recente pode estar incompleto, dependendo da disponibilidade do arquivo anual corrente.",
                "The text is descriptive and does not establish causality. "
                "Biome filters depend on the biome column present in the annual files. "
                "The most recent year may be incomplete, depending on the availability of the current annual file.",
            ),
        },
    }

    generated_report = _ensure_bilingual_report(generated_report)

    live_report = _build_live_report(
        generated_report=generated_report,
        cfg=cfg,
        root=settings.root,
        logger=logger,
    )

    return {
        "report_id": cfg.id,
        "title": generated_report["title"],
        "bucket_prefix": cfg.bucket_prefix,
        "generated_report": generated_report,
        "live_report": live_report,
        "meta": {
            "schema_version": REPORT_SCHEMA_VERSION,
            "source_label": generated_report["source_label"],
            "dataset_id": cfg.dataset.dataset_id,
            "first_year": first_year,
            "latest_year": latest_year,
            "year_range": year_range,
            "latest_period": latest_period,
            "llm_enabled": cfg.llm.enabled,
            "publish_generated_as_live": cfg.editorial.publish_generated_as_live,
            "available_locales": SUPPORTED_REPORT_LOCALES,
            "default_locale": DEFAULT_REPORT_LOCALE,
            "available_biomes": available_biomes,
            "cache": incremental["cache_stats"],
        },
    }


def _merge_candidates(primary: list[str], defaults: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []

    for item in [*primary, *defaults]:
        key = item.strip()
        if not key:
            continue
        norm = _normalize(key)
        if norm in seen:
            continue
        seen.add(norm)
        merged.append(key)

    return merged


def _select_zip_files(base_dir: Path, file_glob: str, recent_years: int | None) -> list[Path]:
    candidates = sorted(base_dir.glob(file_glob))
    if not candidates:
        return []

    with_years: list[tuple[int, Path]] = []
    without_years: list[Path] = []

    for path in candidates:
        year = _extract_year_from_name(path.name)
        if year is None:
            without_years.append(path)
            continue
        with_years.append((year, path))

    with_years.sort(key=lambda x: x[0], reverse=True)

    if recent_years is not None:
        with_years = with_years[:recent_years]

    selected = [path for _, path in with_years] + without_years
    selected.sort()
    return selected


def _select_annual_reference_csv_files(
    anual_dir: Path,
    *,
    csv_glob: str = "focos_br_ref_*.csv",
    recent_years: int | None = None,
) -> list[Path]:
    """CSV extraídos em anual/ (focos_br_ref_YYYY.csv), mesma lógica de recência que os ZIPs."""
    if not anual_dir.is_dir():
        return []
    candidates = sorted(anual_dir.glob(csv_glob))
    if not candidates:
        return []

    with_years: list[tuple[int, Path]] = []
    without_years: list[Path] = []

    for path in candidates:
        year = _extract_year_from_name(path.name)
        if year is None:
            without_years.append(path)
            continue
        with_years.append((year, path))

    with_years.sort(key=lambda x: x[0], reverse=True)

    if recent_years is not None:
        with_years = with_years[:recent_years]

    selected = [path for _, path in with_years] + without_years
    selected.sort()
    return selected


def _extract_year_from_name(filename: str) -> int | None:
    match = RE_YEAR.search(filename)
    if not match:
        return None
    return int(match.group(1))


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.casefold())


def _find_previous_year(annual_series: pd.DataFrame, latest_year: int) -> int | None:
    years = sorted(int(y) for y in annual_series["year"].tolist())
    previous = [y for y in years if y < latest_year]
    return previous[-1] if previous else None


def _build_top_states_table(
    state_year_series: pd.DataFrame,
    latest_year: int,
    previous_year: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    current_df = (
        state_year_series.loc[state_year_series["year"] == latest_year, ["state", "value"]]
        .rename(columns={"value": "current_year_total"})
        .copy()
    )

    if previous_year is None:
        previous_df = pd.DataFrame(columns=["state", "previous_year_total"])
    else:
        previous_df = (
            state_year_series.loc[state_year_series["year"] == previous_year, ["state", "value"]]
            .rename(columns={"value": "previous_year_total"})
            .copy()
        )

    merged = current_df.merge(previous_df, on="state", how="outer").fillna(0)
    merged["current_year_total"] = merged["current_year_total"].astype(int)
    merged["previous_year_total"] = merged["previous_year_total"].astype(int)
    merged["absolute_change"] = merged["current_year_total"] - merged["previous_year_total"]
    merged["pct_change"] = merged.apply(
        lambda row: _safe_pct_change(row["current_year_total"], row["previous_year_total"]),
        axis=1,
    )

    merged = merged.sort_values(
        by=["current_year_total", "previous_year_total", "state"],
        ascending=[False, False, True],
    ).head(limit)

    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        rows.append(
            {
                "state": str(row["state"]),
                "current_year_total": int(row["current_year_total"]),
                "previous_year_total": int(row["previous_year_total"]),
                "absolute_change": int(row["absolute_change"]),
                "pct_change": None if row["pct_change"] is None else round(float(row["pct_change"]), 2),
            }
        )

    return rows


def _build_top_biomes_context(
    annual_by_biome_df: pd.DataFrame,
    latest_year: int,
    previous_year: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    current_df = (
        annual_by_biome_df.loc[annual_by_biome_df["year"] == latest_year, ["biome", "value"]]
        .rename(columns={"value": "current_year_total"})
        .copy()
    )

    if previous_year is None:
        previous_df = pd.DataFrame(columns=["biome", "previous_year_total"])
    else:
        previous_df = (
            annual_by_biome_df.loc[annual_by_biome_df["year"] == previous_year, ["biome", "value"]]
            .rename(columns={"value": "previous_year_total"})
            .copy()
        )

    merged = current_df.merge(previous_df, on="biome", how="outer").fillna(0)
    merged["current_year_total"] = merged["current_year_total"].astype(int)
    merged["previous_year_total"] = merged["previous_year_total"].astype(int)
    merged["absolute_change"] = merged["current_year_total"] - merged["previous_year_total"]
    merged["pct_change"] = merged.apply(
        lambda row: _safe_pct_change(row["current_year_total"], row["previous_year_total"]),
        axis=1,
    )

    merged = merged.sort_values(
        by=["current_year_total", "previous_year_total", "biome"],
        ascending=[False, False, True],
    ).head(limit)

    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        rows.append(
            {
                "biome": str(row["biome"]),
                "current_year_total": int(row["current_year_total"]),
                "previous_year_total": int(row["previous_year_total"]),
                "absolute_change": int(row["absolute_change"]),
                "pct_change": None if row["pct_change"] is None else round(float(row["pct_change"]), 2),
            }
        )

    return rows


def _build_highlights(
    latest_year: int,
    previous_year: int | None,
    current_year_total: int,
    previous_year_total: int,
    recent_12m_total: int,
    prior_12m_total: int,
    latest_period: str,
    total_rows_processed: int,
    file_count_used: int,
    year_range: str,
) -> list[dict[str, Any]]:
    return [
        {
            "id": "current_year_total",
            "label": _localized(f"Focos em {latest_year}", f"Hotspots in {latest_year}"),
            "value": current_year_total,
            "comparison_label": _localized(f"vs {previous_year}", f"vs {previous_year}") if previous_year else None,
            "comparison_value": previous_year_total if previous_year else None,
            "pct_change": _safe_pct_change(current_year_total, previous_year_total) if previous_year else None,
        },
        {
            "id": "recent_12m_total",
            "label": _localized("Últimos 12 meses", "Last 12 months"),
            "value": recent_12m_total,
            "comparison_label": _localized("12 meses anteriores", "Previous 12 months"),
            "comparison_value": prior_12m_total,
            "pct_change": _safe_pct_change(recent_12m_total, prior_12m_total),
        },
        {
            "id": "coverage_year_range",
            "label": _localized("Cobertura anual", "Annual coverage"),
            "value": year_range,
            "comparison_label": None,
            "comparison_value": None,
            "pct_change": None,
        },
        {
            "id": "total_rows_processed",
            "label": _localized("Linhas processadas", "Processed rows"),
            "value": total_rows_processed,
            "comparison_label": _localized("Arquivos usados", "Files used"),
            "comparison_value": file_count_used,
            "pct_change": None,
        },
        {
            "id": "latest_period",
            "label": _localized("Último período disponível", "Latest available period"),
            "value": latest_period,
            "comparison_label": None,
            "comparison_value": None,
            "pct_change": None,
        },
    ]


def _build_fallback_analysis(
    first_year: int,
    latest_year: int,
    previous_year: int | None,
    current_year_total: int,
    previous_year_total: int,
    recent_12m_total: int,
    prior_12m_total: int,
    latest_period: str,
    total_rows_processed: int,
    file_count_used: int,
    year_range: str,
    analysis_window_start: str,
    analysis_window_end: str,
    latest_month_total: int = 0,
    same_month_prev_year_total: int = 0,
    ytd_current_year: int = 0,
    ytd_previous_year: int = 0,
) -> dict[str, dict[str, str]]:
    yoy = _safe_pct_change(current_year_total, previous_year_total)
    recent_12m_change = _safe_pct_change(recent_12m_total, prior_12m_total)
    mom_change = _safe_pct_change(latest_month_total, same_month_prev_year_total)
    ytd_change = _safe_pct_change(ytd_current_year, ytd_previous_year)
    latest_month_label_pt = _month_label_pt(latest_period)
    latest_month_label_en = _month_label_en(latest_period)

    if previous_year is None:
        headline_pt = (
            f"A base processada cobre {year_range} e o período mais recente vai até {latest_period}, "
            f"com {_fmt_int_pt(current_year_total)} focos em {latest_year}."
        )
        comparison_pt = (
            "Ainda não há ano anterior processado no escopo atual para comparação anual direta."
        )

        headline_en = (
            f"The processed dataset covers {year_range} and the most recent period reaches {latest_period}, "
            f"with {_fmt_int_en(current_year_total)} hotspots in {latest_year}."
        )
        comparison_en = (
            "There is not yet a previous processed year within the current scope for a direct annual comparison."
        )
    else:
        headline_pt = (
            f"Em {latest_month_label_pt}, {_fmt_int_pt(latest_month_total)} focos — "
            f"{_fmt_pct_pt(mom_change)} vs {_month_label_pt(f'{previous_year}-{latest_period[-2:]}')}. "
            f"Acumulado {latest_year}: {_fmt_int_pt(ytd_current_year)} focos ({_fmt_pct_pt(ytd_change)} vs {previous_year})."
        )
        comparison_pt = (
            f"Comparação mensal ({latest_month_label_pt}): {_fmt_int_pt(latest_month_total)} focos vs "
            f"{_fmt_int_pt(same_month_prev_year_total)} no mesmo mês de {previous_year} ({_fmt_pct_pt(mom_change)}). "
            f"Acumulado jan–{latest_month_label_pt}: {_fmt_int_pt(ytd_current_year)} vs "
            f"{_fmt_int_pt(ytd_previous_year)} em {previous_year} ({_fmt_pct_pt(ytd_change)}). "
            f"No total anual: {_fmt_int_pt(current_year_total)} em {latest_year} vs "
            f"{_fmt_int_pt(previous_year_total)} em {previous_year} ({_fmt_pct_pt(yoy)})."
        )

        headline_en = (
            f"In {latest_month_label_en}, {_fmt_int_en(latest_month_total)} hotspots — "
            f"{_fmt_pct_en(mom_change)} vs {_month_label_en(f'{previous_year}-{latest_period[-2:]}')}. "
            f"{latest_year} YTD: {_fmt_int_en(ytd_current_year)} hotspots ({_fmt_pct_en(ytd_change)} vs {previous_year})."
        )
        comparison_en = (
            f"Monthly comparison ({latest_month_label_en}): {_fmt_int_en(latest_month_total)} hotspots vs "
            f"{_fmt_int_en(same_month_prev_year_total)} in the same month of {previous_year} ({_fmt_pct_en(mom_change)}). "
            f"YTD Jan–{latest_month_label_en}: {_fmt_int_en(ytd_current_year)} vs "
            f"{_fmt_int_en(ytd_previous_year)} in {previous_year} ({_fmt_pct_en(ytd_change)}). "
            f"Annual total: {_fmt_int_en(current_year_total)} in {latest_year} vs "
            f"{_fmt_int_en(previous_year_total)} in {previous_year} ({_fmt_pct_en(yoy)})."
        )

    overview_pt = (
        f"Foram processadas {_fmt_int_pt(total_rows_processed)} linhas distribuídas em {file_count_used} arquivos anuais. "
        f"Na janela editorial recente de {analysis_window_start} a {analysis_window_end}, "
        f"os 12 meses mais recentes somam {_fmt_int_pt(recent_12m_total)} focos, "
        f"contra {_fmt_int_pt(prior_12m_total)} nos 12 meses imediatamente anteriores, "
        f"o que corresponde a {_fmt_pct_pt(recent_12m_change)}."
    )

    limitations_pt = (
        "O texto é descritivo e não estabelece causalidade. "
        "A leitura editorial permanece concentrada na janela recente, embora as visualizações publiquem o histórico disponível. "
        "O ano corrente pode estar incompleto."
    )

    overview_en = (
        f"{_fmt_int_en(total_rows_processed)} rows were processed across {file_count_used} annual files. "
        f"In the recent editorial window from {analysis_window_start} to {analysis_window_end}, "
        f"the latest 12 months total {_fmt_int_en(recent_12m_total)} hotspots, "
        f"versus {_fmt_int_en(prior_12m_total)} in the immediately previous 12 months, "
        f"which corresponds to {_fmt_pct_en(recent_12m_change)}."
    )

    limitations_en = (
        "This text is descriptive and does not establish causality. "
        "The editorial reading remains focused on the recent window, although the visualizations publish the available history. "
        "The current year may be incomplete."
    )

    return {
        "headline": _localized(headline_pt, headline_en),
        "overview": _localized(overview_pt, overview_en),
        "comparison": _localized(comparison_pt, comparison_en),
        "limitations": _localized(limitations_pt, limitations_en),
    }


def _build_live_report(
    generated_report: dict[str, Any],
    cfg: ReportConfig,
    root: Path,
    logger: Any,
) -> dict[str, Any]:
    live_report = copy.deepcopy(generated_report)

    overrides_path = cfg.resolve_overrides_path(root)
    overrides = _load_overrides(overrides_path, logger)

    if overrides:
        live_report = _deep_merge(live_report, overrides)

    live_report = _ensure_bilingual_report(live_report)
    live_report["publication_status"] = "live"
    live_report["generated_from"] = {
        "report_id": generated_report["report_id"],
        "generated_at": generated_report["generated_at"],
    }

    return live_report


def _load_overrides(overrides_path: Path | None, logger: Any) -> dict[str, Any]:
    if overrides_path is None:
        return {}

    if not overrides_path.exists():
        logger.info("Nenhum arquivo de override encontrado em: %s", overrides_path)
        return {}

    suffix = overrides_path.suffix.lower()
    with open(overrides_path, "r", encoding="utf-8") as f:
        if suffix == ".json":
            data = json.load(f)
        else:
            data = yaml.safe_load(f) or {}

    logger.info("Overrides editoriais carregados: %s", overrides_path)
    return data if isinstance(data, dict) else {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)

    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value

    return merged


def _ensure_bilingual_report(report: dict[str, Any]) -> dict[str, Any]:
    report["available_locales"] = SUPPORTED_REPORT_LOCALES
    report["default_locale"] = DEFAULT_REPORT_LOCALE

    if "title" in report:
        report["title"] = _coerce_localized_value(report.get("title"))

    if "source_label" in report:
        report["source_label"] = _coerce_localized_value(report.get("source_label"))

    if "summary" in report and report.get("summary") is not None:
        report["summary"] = _coerce_localized_value(report.get("summary"))

    analysis = report.get("analysis")
    if isinstance(analysis, dict):
        report["analysis"] = {
            key: _coerce_localized_value(value)
            for key, value in analysis.items()
        }

    highlights = report.get("highlights")
    if isinstance(highlights, list):
        for item in highlights:
            if isinstance(item, dict):
                if "label" in item:
                    item["label"] = _coerce_localized_value(item.get("label"))
                if item.get("comparison_label") is not None:
                    item["comparison_label"] = _coerce_localized_value(item.get("comparison_label"))

    filters = report.get("filters")
    if isinstance(filters, dict):
        for filter_def in filters.values():
            if not isinstance(filter_def, dict):
                continue
            if "label" in filter_def:
                filter_def["label"] = _coerce_localized_value(filter_def.get("label"))
            options = filter_def.get("options")
            if isinstance(options, list):
                for option in options:
                    if isinstance(option, dict) and "label" in option:
                        option["label"] = _coerce_localized_value(option.get("label"))

    sections = report.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue

            if "title" in section:
                section["title"] = _coerce_localized_value(section.get("title"))

            columns = section.get("columns")
            if isinstance(columns, list):
                for column in columns:
                    if isinstance(column, dict) and "label" in column:
                        column["label"] = _coerce_localized_value(column.get("label"))

    methodology = report.get("methodology")
    if isinstance(methodology, dict):
        for key in ("source", "note", "limitations"):
            if key in methodology and methodology.get(key) is not None:
                methodology[key] = _coerce_localized_value(methodology.get(key))

    return report


def _coerce_localized_value(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        pt = str(value.get("pt") or value.get("en") or "").strip()
        en = str(value.get("en") or value.get("pt") or "").strip()
        return {"pt": pt, "en": en}

    if value is None:
        return {"pt": "", "en": ""}

    text = str(value).strip()
    return {"pt": text, "en": text}


def _localized(pt: str, en: str) -> dict[str, str]:
    return {
        "pt": pt.strip(),
        "en": en.strip(),
    }


def _safe_pct_change(current: int, previous: int) -> float | None:
    if previous == 0:
        return None
    return ((current - previous) / previous) * 100.0


def _fmt_int_pt(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def _fmt_int_en(value: int) -> str:
    return f"{value:,}"


def _fmt_pct_pt(value: float | None) -> str:
    if value is None:
        return "sem base comparável"
    return f"{value:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct_en(value: float | None) -> str:
    if value is None:
        return "no comparable base"
    return f"{value:,.2f}%"


def _build_top_states_month_merged(
    state_month_all_df: pd.DataFrame,
    current_period: str,
    previous_period: str | None,
) -> list[dict[str, Any]]:
    if state_month_all_df.empty:
        return []

    current_df = (
        state_month_all_df.loc[state_month_all_df["period"] == current_period, ["state", "value"]]
        .rename(columns={"value": "current_month_total"})
        .copy()
    )
    if current_df.empty:
        return []

    if previous_period is not None and not state_month_all_df.empty:
        prev_rows = state_month_all_df.loc[state_month_all_df["period"] == previous_period, ["state", "value"]]
        previous_df = prev_rows.rename(columns={"value": "previous_month_total"}).copy()
    else:
        previous_df = pd.DataFrame(columns=["state", "previous_month_total"])

    merged = current_df.merge(previous_df, on="state", how="outer").fillna(0)
    merged["current_month_total"] = merged["current_month_total"].astype(int)
    merged["previous_month_total"] = merged["previous_month_total"].astype(int)
    merged["absolute_change"] = merged["current_month_total"] - merged["previous_month_total"]
    merged["pct_change"] = merged.apply(
        lambda row: _safe_pct_change(row["current_month_total"], row["previous_month_total"]),
        axis=1,
    )

    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        rows.append({
            "state": str(row["state"]),
            "current_month_total": int(row["current_month_total"]),
            "previous_month_total": int(row["previous_month_total"]),
            "absolute_change": int(row["absolute_change"]),
            "pct_change": None if row["pct_change"] is None else round(float(row["pct_change"]), 2),
        })
    return rows


def _sort_top_states_month(
    rows: list[dict[str, Any]],
    sort_by: str,
    limit: int,
) -> list[dict[str, Any]]:
    if sort_by == "volume":
        sorted_rows = sorted(rows, key=lambda r: (r["current_month_total"], r["previous_month_total"]), reverse=True)
    else:
        def _abs_pct(r: dict[str, Any]) -> float:
            v = r.get("pct_change")
            return abs(float(v)) if v is not None else 0.0
        sorted_rows = sorted(rows, key=_abs_pct, reverse=True)
    return sorted_rows[:limit]


_PT_MONTH_ABBR = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
_EN_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _month_label_pt(period: str | None) -> str:
    if not period or "-" not in period:
        return period or ""
    year, month = period.split("-", 1)
    try:
        m = int(month)
        return f"{_PT_MONTH_ABBR[m - 1]}/{year}"
    except (ValueError, IndexError):
        return period


def _month_label_en(period: str | None) -> str:
    if not period or "-" not in period:
        return period or ""
    year, month = period.split("-", 1)
    try:
        m = int(month)
        return f"{_EN_MONTH_ABBR[m - 1]}/{year}"
    except (ValueError, IndexError):
        return period


def _now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")


def _load_mensal_counts_for_current_year(
    mensal_dir: Path,
    current_year: int,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
    satellite_candidates: list[str] | None = None,
) -> dict[str, Any]:
    """
    Reads INPE monthly CSV files (focos_mensal_br_YYYYMM.csv) for current_year and aggregates:
      - national total per month
      - per-biome total per month (BIOME_KEY uppercase, matching CSV)
      - per-state total per month (state name uppercase, matching CSV)

    Returns a dict with keys: last_closed_month, national, by_biome, by_state.
    Returns empty structure if no files are found.
    """
    month_files: dict[int, Path] = {}
    if mensal_dir.exists():
        for f in sorted(mensal_dir.iterdir()):
            m = RE_MENSAL_CSV.search(f.name)
            if not m:
                continue
            if int(m.group(1)) == current_year:
                month_files[int(m.group(2))] = f

    if not month_files:
        return {"last_closed_month": 0, "national": {}, "by_biome": {}, "by_state": {}}

    national: dict[int, int] = {}
    by_biome: dict[str, dict[int, int]] = {}
    by_state: dict[str, dict[int, int]] = {}

    for month, fpath in sorted(month_files.items()):
        df = read_focos_subset_brasil_file(
            fpath,
            datetime_candidates,
            state_candidates,
            biome_candidates,
            satellite_candidates=satellite_candidates,
            reference_satellite=INPE_REFERENCE_SATELLITE,
        )

        national[month] = len(df)

        if not df.empty:
            for biome_key, grp in df.dropna(subset=["biome"]).groupby("biome"):
                k = str(biome_key)
                if k not in by_biome:
                    by_biome[k] = {}
                by_biome[k][month] = len(grp)

            for state_key, grp in df.dropna(subset=["state"]).groupby("state"):
                k = str(state_key)
                if k not in by_state:
                    by_state[k] = {}
                by_state[k][month] = len(grp)

    return {
        "last_closed_month": max(month_files.keys()),
        "national": national,
        "by_biome": by_biome,
        "by_state": by_state,
    }


def _build_monthly_year_comparison_records(
    monthly_all_df: pd.DataFrame,
    monthly_by_biome_df: pd.DataFrame,
    state_month_all_df: pd.DataFrame,
    latest_year: int,
    previous_year: int | None,
    five_avg_candidate_years: list[int],
    last_closed_month: int,
    mensal_counts: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build flat monthly records for the year comparison chart (current/prev/5yr-avg × biome × state).

    When mensal_counts is provided, uses INPE monthly CSV data for current_year values
    (more accurate than annual ZIP aggregation for the incomplete current year).
    """
    ALL = ALL_BIOMES_VALUE
    records: list[dict[str, Any]] = []

    def _month_vals(df: pd.DataFrame, year: int, period_col: str = "period") -> dict[int, int | None]:
        vals: dict[int, int | None] = {}
        for m in range(1, 13):
            period = f"{year}-{str(m).zfill(2)}"
            rows = df.loc[df[period_col] == period, "value"]
            vals[m] = int(rows.sum()) if not rows.empty else None
        return vals

    def _avg_vals(df: pd.DataFrame, years: list[int]) -> dict[int, float | None]:
        avg: dict[int, float | None] = {}
        for m in range(1, 13):
            ys = []
            for yr in years:
                period = f"{yr}-{str(m).zfill(2)}"
                rows = df.loc[df["period"] == period, "value"]
                if not rows.empty:
                    v = float(rows.sum())
                    if v > 0:
                        ys.append(v)
            avg[m] = round(sum(ys) / len(ys), 1) if ys else None
        return avg

    def _mensal_month_vals(counts: dict[int, int]) -> dict[int, int | None]:
        return {m: counts.get(m) for m in range(1, 13)}

    def _add_records(
        scope_biome: str,
        scope_state: str,
        cur_vals: dict[int, int | None],
        prev_vals: dict[int, int | None],
        avg_5yr: dict[int, float | None],
    ) -> None:
        for m in range(1, 13):
            records.append({
                "month": m,
                "biome": scope_biome,
                "state": scope_state,
                "current_year_val": cur_vals[m] if m <= last_closed_month else None,
                "previous_year_val": prev_vals[m],
                "avg_5yr_val": avg_5yr[m],
            })

    # --- National (__all__ biome, __all__ state) ---
    if mensal_counts and mensal_counts.get("national"):
        cur_nat = _mensal_month_vals(mensal_counts["national"])
    else:
        cur_nat = _month_vals(monthly_all_df, latest_year)
    prev_nat = _month_vals(monthly_all_df, previous_year) if previous_year else {m: None for m in range(1, 13)}
    avg_nat = _avg_vals(monthly_all_df, five_avg_candidate_years)
    _add_records(ALL, ALL, cur_nat, prev_nat, avg_nat)

    # --- Per biome (__all__ state) ---
    mensal_by_biome = mensal_counts.get("by_biome", {}) if mensal_counts else {}
    if not monthly_by_biome_df.empty:
        for biome in monthly_by_biome_df["biome"].unique():
            biome_df = monthly_by_biome_df[monthly_by_biome_df["biome"] == biome]
            biome_key = str(biome).upper()
            if biome_key in mensal_by_biome:
                cur_b = _mensal_month_vals(mensal_by_biome[biome_key])
            else:
                cur_b = _month_vals(biome_df, latest_year)
            prev_b = _month_vals(biome_df, previous_year) if previous_year else {m: None for m in range(1, 13)}
            avg_b = _avg_vals(biome_df, five_avg_candidate_years)
            _add_records(str(biome), ALL, cur_b, prev_b, avg_b)

    # --- Per state (__all__ biome) ---
    mensal_by_state = mensal_counts.get("by_state", {}) if mensal_counts else {}
    if not state_month_all_df.empty:
        for state in state_month_all_df["state"].unique():
            state_df = state_month_all_df[state_month_all_df["state"] == state]
            state_key = str(state).upper()
            if state_key in mensal_by_state:
                cur_s = _mensal_month_vals(mensal_by_state[state_key])
            else:
                cur_s = _month_vals(state_df, latest_year)
            prev_s = _month_vals(state_df, previous_year) if previous_year else {m: None for m in range(1, 13)}
            avg_s = _avg_vals(state_df, five_avg_candidate_years)
            _add_records(ALL, str(state), cur_s, prev_s, avg_s)

    return records