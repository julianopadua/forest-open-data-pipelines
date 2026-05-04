# src/forest_pipelines/reports/builders/bdqueimadas_overview.py
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import yaml

from forest_pipelines.datasets.inpe.bdqueimadas_mensal_listing import (
    DEFAULT_MENSAL_BASE_URL,
    ensure_mensal_files_for_year,
)
from forest_pipelines.reports.builders.bdqueimadas_incremental import (
    ALL_BIOMES_VALUE,
    INPE_REFERENCE_SATELLITE,
    biome_label_i18n,
    build_incremental_year_caches,
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

# Fonte operacional dos agregados de focos (COIDS / INPE).
FOCOS_DATASERVER_CSV_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/"


def build_package(
    settings: Any,
    storage: Any,
    logger: Any,
    current_year_only: bool = False,
    skip_llm: bool = False,
    skip_mensal_download: bool = False,
    refresh_mensal: bool = False,
    reference_month_mode: Literal["previous", "current"] | str = "previous",
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
    state_month_by_biome_df = consolidated["state_month_by_biome_df"]
    available_biomes = consolidated["available_biomes"]
    yearly_file_stats = consolidated["yearly_file_stats"]
    total_rows_processed = consolidated["total_rows_processed"]

    if monthly_all_df.empty or annual_all_df.empty:
        raise RuntimeError("Não foi possível montar séries agregadas para o report BDQueimadas.")

    first_year = int(annual_all_df["year"].min())
    zip_latest_year = int(annual_all_df["year"].max())
    zip_previous_year = _find_previous_year(annual_all_df, zip_latest_year)

    first_period = str(monthly_all_df["period"].iloc[0])
    zip_latest_period = str(monthly_all_df["period"].iloc[-1])

    available_years = [int(y) for y in annual_all_df["year"].tolist()]
    available_periods = [str(period) for period in monthly_all_df["period"].tolist()]

    reference_month_mode = _normalize_reference_month_mode(reference_month_mode)
    mensal_dir = settings.data_dir / cfg.dataset.local_relative_dir / "mensal"
    calendar_year = pd.Timestamp.now().year
    reference_year, reference_month = _resolve_reference_month(calendar_year, reference_month_mode)

    if refresh_mensal and mensal_dir.exists():
        for f in mensal_dir.iterdir():
            if not f.is_file():
                continue
            match = RE_MENSAL_CSV.search(f.name)
            if not match:
                continue
            if int(match.group(1)) == calendar_year:
                f.unlink()

    if not skip_mensal_download:
        try:
            ensure_mensal_files_for_year(
                base_url=DEFAULT_MENSAL_BASE_URL,
                year=calendar_year,
                cache_dir=mensal_dir,
                skip_download=False,
            )
        except FileNotFoundError as e:
            logger.warning(
                "Nenhum CSV mensal disponível para %d em %s (%s).",
                calendar_year,
                DEFAULT_MENSAL_BASE_URL,
                e,
            )

    mensal_counts = _load_mensal_counts_for_current_year(
        mensal_dir=mensal_dir,
        current_year=calendar_year,
        datetime_candidates=datetime_candidates,
        state_candidates=state_candidates,
        biome_candidates=biome_candidates,
        satellite_candidates=DEFAULT_SATELLITE_CANDIDATES,
    )

    mensal_available_months = sorted(int(m) for m in mensal_counts["national"].keys())
    _mensal_is_current = (
        mensal_counts["last_closed_month"] > 0
        and calendar_year > zip_latest_year
        and reference_year == calendar_year
    )
    if _mensal_is_current:
        if reference_month not in mensal_available_months:
            if reference_month_mode == "current":
                raise RuntimeError(
                    f"Modo mês vigente selecionado ({calendar_year}-{reference_month:02d}), "
                    "mas o CSV mensal ainda não está disponível no cache/listagem do INPE."
                )
            raise RuntimeError(
                f"Modo mês anterior selecionado ({calendar_year}-{reference_month:02d}), "
                "mas o CSV mensal não foi encontrado no cache local."
            )

        mensal_counts = _truncate_mensal_counts(mensal_counts, reference_month)
        latest_year = calendar_year
        previous_year = zip_latest_year
        last_closed_month = reference_month
        latest_period = f"{latest_year}-{str(last_closed_month).zfill(2)}"
        latest_month_num = last_closed_month
        logger.info(
            "Dados mensais INPE para %d: %d meses (último: mês %d). ZIPs cobrem até %d.",
            latest_year, len(mensal_counts["national"]), last_closed_month, zip_latest_year,
        )
    else:
        latest_year = zip_latest_year
        previous_year = zip_previous_year
        latest_period = zip_latest_period
        last_closed_month = int(latest_period.split("-")[1]) if "-" in latest_period else 12
        latest_month_num = last_closed_month

    if latest_period not in available_periods:
        available_periods = sorted({*available_periods, latest_period})
    if latest_year not in available_years:
        available_years = sorted({*available_years, latest_year})

    default_start_year = min(latest_year, max(2019, first_year))
    default_monthly_start = _resolve_monthly_start_period(
        available_periods=available_periods,
        start_year=default_start_year,
        fallback_start=first_period,
        fallback_end=latest_period,
    )
    default_monthly_end = latest_period
    default_annual_start = default_start_year
    default_annual_end = latest_year

    year_range = f"{first_year}-{latest_year}"

    if _mensal_is_current:
        current_year_total = _sum_mensal_until(mensal_counts["national"], last_closed_month)
    else:
        current_year_total = int(annual_all_df.loc[annual_all_df["year"] == latest_year, "value"].iloc[0])
    previous_year_total = int(
        annual_all_df.loc[annual_all_df["year"] == previous_year, "value"].iloc[0]
    ) if previous_year is not None else 0

    effective_national_series = _build_effective_national_monthly_series(
        monthly_all_df=monthly_all_df,
        mensal_counts=mensal_counts,
        mensal_is_current=_mensal_is_current,
        calendar_year=calendar_year,
    )
    rolling_12m = _compute_rolling_12m_metrics(
        monthly_series=effective_national_series,
        latest_period=latest_period,
    )
    recent_12m_total = int(rolling_12m["recent_total"])
    prior_12m_total = int(rolling_12m["prior_total"] or 0)

    top_states_table = _build_top_states_table(
        state_year_series=state_year_all_df,
        latest_year=zip_latest_year,
        previous_year=zip_previous_year,
        limit=cfg.display.top_states_limit,
    )

    analysis_window_periods = [period for period, _ in effective_national_series][-cfg.analysis.recent_months:]
    analysis_window_start = analysis_window_periods[0] if analysis_window_periods else first_period
    analysis_window_end = analysis_window_periods[-1] if analysis_window_periods else latest_period

    top_biomes_context = _build_top_biomes_context(
        annual_by_biome_df=annual_by_biome_df,
        latest_year=zip_latest_year,
        previous_year=zip_previous_year,
        limit=cfg.analysis.top_biomes_context_limit,
    )

    if _mensal_is_current:
        latest_month_total = mensal_counts["national"].get(last_closed_month, 0)
        ytd_current_year = _sum_mensal_until(mensal_counts["national"], last_closed_month)
    else:
        latest_month_total = int(monthly_all_df.loc[monthly_all_df["period"] == latest_period, "value"].sum())
        current_year_month_periods = [f"{latest_year}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)]
        ytd_current_year = int(monthly_all_df.loc[monthly_all_df["period"].isin(current_year_month_periods), "value"].sum())

    same_period_prev_year: str | None = None
    same_month_prev_year_total = 0
    if previous_year is not None:
        same_period_prev_year = f"{previous_year}-{str(last_closed_month).zfill(2)}"
        prev_vals = monthly_all_df.loc[monthly_all_df["period"] == same_period_prev_year, "value"]
        same_month_prev_year_total = int(prev_vals.sum()) if not prev_vals.empty else 0

    ytd_previous_year = 0
    if previous_year is not None:
        prev_year_month_periods = [f"{previous_year}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)]
        ytd_previous_year = int(monthly_all_df.loc[monthly_all_df["period"].isin(prev_year_month_periods), "value"].sum())

    five_avg_candidate_years = [y for y in available_years if latest_year - 5 <= y < latest_year]
    ytd_per_year: list[float] = []
    for yr in five_avg_candidate_years:
        yr_periods = [f"{yr}-{str(m).zfill(2)}" for m in range(1, latest_month_num + 1)]
        yr_ytd = float(monthly_all_df.loc[monthly_all_df["period"].isin(yr_periods), "value"].sum())
        if yr_ytd > 0:
            ytd_per_year.append(yr_ytd)
    ytd_5yr_avg = round(sum(ytd_per_year) / len(ytd_per_year), 0) if ytd_per_year else None

    # Top UF / bioma: mês de latest_period vs mesmo mês do ano civil anterior (alinhado ao comparativo mensal)
    month_compare_current, month_compare_previous = _month_same_month_prev_year_periods(latest_period)
    top_states_month_data = _build_top_states_month_comparison(
        state_month_all_df=state_month_all_df,
        current_period=month_compare_current,
        previous_period=month_compare_previous,
        mensal_counts=mensal_counts,
        mensal_is_current=_mensal_is_current,
        calendar_year=calendar_year,
    )
    top_states_by_volume_month = _sort_top_states_month(
        top_states_month_data, sort_by="volume", limit=cfg.display.top_states_limit
    )
    top_biomes_month_data = _build_top_biomes_month_comparison(
        monthly_by_biome_df=monthly_by_biome_df,
        current_period=month_compare_current,
        previous_period=month_compare_previous,
        mensal_counts=mensal_counts,
        mensal_is_current=_mensal_is_current,
        calendar_year=calendar_year,
    )
    top_biomes_by_volume_month = _sort_top_biomes_month(
        top_biomes_month_data, sort_by="volume", limit=cfg.display.top_states_limit
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

    # Historical average: all years before the current one
    hist_avg_candidate_years = [y for y in available_years if y < latest_year]
    avg_window_start = min(hist_avg_candidate_years) if hist_avg_candidate_years else first_year
    avg_window_end = max(hist_avg_candidate_years) if hist_avg_candidate_years else latest_year - 1

    # Latest month 5yr average for LLM context
    latest_month_5yr_avg_vals = []
    for yr in five_avg_candidate_years:
        yr_period = f"{yr}-{str(last_closed_month).zfill(2)}"
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
            "reference_month_mode": reference_month_mode,
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
            "rolling_12_months": {
                "window_end_period": latest_period,
                "recent_window_start_period": rolling_12m["recent_window_start_period"],
                "prior_window_start_period": rolling_12m["prior_window_start_period"],
                "recent_total": rolling_12m["recent_total"],
                "prior_total": rolling_12m["prior_total"],
                "pct_change": rolling_12m["pct_change"],
                "has_full_prior_window": rolling_12m["has_full_prior_window"],
            },
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
        skip_llm=skip_llm,
    )

    title_i18n = localized_text_dict(cfg.title) or _localized("", "")
    source_label_i18n = localized_text_dict(cfg.source_label) or _localized("", "")
    summary_i18n = localized_text_dict(cfg.summary) if cfg.summary is not None else None

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
        five_avg_candidate_years=hist_avg_candidate_years,
        last_closed_month=last_closed_month,
        mensal_counts=mensal_counts,
    )

    # Month labels for top-states / top-biomes tables (alinhados a latest_period)
    latest_month_label_pt = _month_label_pt(month_compare_current)
    latest_month_label_en = _month_label_en(month_compare_current)
    prev_month_label_pt = _month_label_pt(month_compare_previous) if month_compare_previous else ""
    prev_month_label_en = _month_label_en(month_compare_previous) if month_compare_previous else ""

    monthly_series_records = _build_state_biome_monthly_series_records(
        monthly_all_df=monthly_all_df,
        monthly_by_biome_df=monthly_by_biome_df,
        state_month_all_df=state_month_all_df,
        state_month_by_biome_df=state_month_by_biome_df,
        mensal_counts=mensal_counts,
        mensal_is_current=_mensal_is_current,
        calendar_year=calendar_year,
        max_month_in_current_year=last_closed_month if _mensal_is_current else None,
    )
    annual_totals_records = _build_annual_totals_from_monthly_series(monthly_series_records)

    data_attribution = {
        "source_url": FOCOS_DATASERVER_CSV_URL,
        "source_label": _localized(
            "INPE - COIDS (transferência de dados de focos)",
            "INPE - COIDS (hotspot data transfer)",
        ),
        "charts_legend": _localized(
            "Série filtrada por bioma, UF e período ativo; valores são contagens agregadas de focos.",
            "Series filtered by biome, state and active period; values are aggregated hotspot counts.",
        ),
        "tables_legend": _localized(
            "Totais do mês de referência comparados ao mesmo mês do ano civil anterior.",
            "Reference month totals compared to the same month in the previous calendar year.",
        ),
    }

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
        "data_attribution": data_attribution,
        "sections": [
            {
                "id": "monthly_year_comparison",
                "kind": "monthly_year_comparison",
                "is_static": True,
                "title": _localized(
                    f"Focos mensais por ano - comparativo {latest_year} vs {previous_year} vs média histórica",
                    f"Monthly hotspots by year - {latest_year} vs {previous_year} vs historical average",
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
                "id": "top_states_latest_month",
                "kind": "table",
                "is_static": True,
                "title": _localized(
                    f"Top UFs em focos - {latest_month_label_pt} vs {prev_month_label_pt}",
                    f"Top states by hotspots - {latest_month_label_en} vs {prev_month_label_en}",
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
                "id": "top_biomes_latest_month",
                "kind": "table",
                "is_static": True,
                "title": _localized(
                    f"Top biomas em focos - {latest_month_label_pt} vs {prev_month_label_pt}",
                    f"Top biomes by hotspots - {latest_month_label_en} vs {prev_month_label_en}",
                ),
                "columns": [
                    {"key": "biome", "label": _localized("Bioma", "Biome")},
                    {"key": "current_month_total", "label": _localized(latest_month_label_pt, latest_month_label_en)},
                    {"key": "previous_month_total", "label": _localized(prev_month_label_pt, prev_month_label_en)},
                    {"key": "absolute_change", "label": _localized("Variação absoluta", "Absolute change")},
                    {"key": "pct_change", "label": _localized("Variação %", "% change")},
                ],
                "filterable_by": [],
                "data": top_biomes_by_volume_month,
            },
            {
                "id": "monthly_series",
                "kind": "timeseries",
                "is_static": False,
                "inline_biome_state_filter": True,
                "title": _localized(
                    "Série mensal de focos",
                    "Monthly hotspot series",
                ),
                "x_key": "period",
                "y_key": "value",
                "biome_key": "biome",
                "state_key": "state",
                "filterable_by": ["period", "biome", "state"],
                "period_filter_granularity": "month",
                "available_states": available_states,
                "default_view": {
                    "biome": ALL_BIOMES_VALUE,
                    "state": ALL_BIOMES_VALUE,
                    "start_period": default_monthly_start,
                    "end_period": default_monthly_end,
                },
                "data": monthly_series_records,
            },
            {
                "id": "annual_totals",
                "kind": "bar",
                "is_static": False,
                "inline_biome_state_filter": True,
                "title": _localized(
                    "Totais anuais de focos",
                    "Annual hotspot totals",
                ),
                "x_key": "year",
                "y_key": "value",
                "biome_key": "biome",
                "state_key": "state",
                "filterable_by": ["period", "biome", "state"],
                "period_filter_granularity": "year",
                "available_states": available_states,
                "default_view": {
                    "biome": ALL_BIOMES_VALUE,
                    "state": ALL_BIOMES_VALUE,
                    "start_year": default_annual_start,
                    "end_year": default_annual_end,
                },
                "data": annual_totals_records,
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
            f"{latest_month_label_pt}: {_fmt_int_pt(latest_month_total)} focos "
            f"({_fmt_pct_pt(mom_change)} vs {_month_label_pt(f'{previous_year}-{latest_period[-2:]}')})."
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
            f"{latest_month_label_en}: {_fmt_int_en(latest_month_total)} hotspots "
            f"({_fmt_pct_en(mom_change)} vs {_month_label_en(f'{previous_year}-{latest_period[-2:]}')})."
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

    da = report.get("data_attribution")
    if isinstance(da, dict):
        for dk in ("source_label", "charts_legend", "tables_legend"):
            if dk in da and da.get(dk) is not None:
                da[dk] = _coerce_localized_value(da.get(dk))

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


def _month_same_month_prev_year_periods(latest_period: str) -> tuple[str, str | None]:
    if "-" not in latest_period:
        return latest_period, None
    y_str, mo = latest_period.split("-", 1)
    y = int(y_str)
    return latest_period, f"{y - 1}-{mo}"


def _augment_state_month_with_mensal(
    state_month_all_df: pd.DataFrame,
    current_period: str,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
) -> pd.DataFrame:
    if not mensal_is_current or "-" not in current_period:
        return state_month_all_df
    year_cur = int(current_period.split("-")[0])
    month_num = int(current_period.split("-")[1])
    if year_cur != calendar_year:
        return state_month_all_df
    by_state = mensal_counts.get("by_state") or {}
    if not by_state:
        return state_month_all_df
    extra_rows: list[dict[str, Any]] = []
    for st_key, per_m in by_state.items():
        v = int(per_m.get(month_num, 0))
        extra_rows.append({
            "period": current_period,
            "year": year_cur,
            "state": str(st_key),
            "value": v,
        })
    if not extra_rows:
        return state_month_all_df
    extra_df = pd.DataFrame(extra_rows)
    base = state_month_all_df.loc[state_month_all_df["period"] != current_period].copy()
    return pd.concat([base, extra_df], ignore_index=True)


def _augment_monthly_by_biome_with_mensal(
    monthly_by_biome_df: pd.DataFrame,
    current_period: str,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
) -> pd.DataFrame:
    if not mensal_is_current or "-" not in current_period:
        return monthly_by_biome_df
    year_cur = int(current_period.split("-")[0])
    month_num = int(current_period.split("-")[1])
    if year_cur != calendar_year:
        return monthly_by_biome_df
    by_bio = mensal_counts.get("by_biome") or {}
    if not by_bio:
        return monthly_by_biome_df
    extra_rows: list[dict[str, Any]] = []
    for bio_key, per_m in by_bio.items():
        v = int(per_m.get(month_num, 0))
        extra_rows.append({
            "period": current_period,
            "year": year_cur,
            "biome": str(bio_key),
            "value": v,
        })
    if not extra_rows:
        return monthly_by_biome_df
    extra_df = pd.DataFrame(extra_rows)
    base = monthly_by_biome_df.loc[monthly_by_biome_df["period"] != current_period].copy()
    return pd.concat([base, extra_df], ignore_index=True)


def _build_top_states_month_comparison(
    state_month_all_df: pd.DataFrame,
    current_period: str,
    previous_period: str | None,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
) -> list[dict[str, Any]]:
    eff = _augment_state_month_with_mensal(
        state_month_all_df=state_month_all_df,
        current_period=current_period,
        mensal_counts=mensal_counts,
        mensal_is_current=mensal_is_current,
        calendar_year=calendar_year,
    )
    return _build_top_states_month_merged(
        state_month_all_df=eff,
        current_period=current_period,
        previous_period=previous_period,
    )


def _build_top_biomes_month_comparison(
    monthly_by_biome_df: pd.DataFrame,
    current_period: str,
    previous_period: str | None,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
) -> list[dict[str, Any]]:
    eff = _augment_monthly_by_biome_with_mensal(
        monthly_by_biome_df=monthly_by_biome_df,
        current_period=current_period,
        mensal_counts=mensal_counts,
        mensal_is_current=mensal_is_current,
        calendar_year=calendar_year,
    )
    return _build_top_biomes_month_merged(
        monthly_by_biome_df=eff,
        current_period=current_period,
        previous_period=previous_period,
    )


def _build_top_biomes_month_merged(
    monthly_by_biome_df: pd.DataFrame,
    current_period: str,
    previous_period: str | None,
) -> list[dict[str, Any]]:
    if monthly_by_biome_df.empty:
        return []

    current_df = (
        monthly_by_biome_df.loc[monthly_by_biome_df["period"] == current_period, ["biome", "value"]]
        .rename(columns={"value": "current_month_total"})
        .copy()
    )
    if current_df.empty:
        return []

    if previous_period is not None and not monthly_by_biome_df.empty:
        prev_rows = monthly_by_biome_df.loc[
            monthly_by_biome_df["period"] == previous_period, ["biome", "value"]
        ]
        previous_df = prev_rows.rename(columns={"value": "previous_month_total"}).copy()
    else:
        previous_df = pd.DataFrame(columns=["biome", "previous_month_total"])

    merged = current_df.merge(previous_df, on="biome", how="outer").fillna(0)
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
            "biome": str(row["biome"]),
            "current_month_total": int(row["current_month_total"]),
            "previous_month_total": int(row["previous_month_total"]),
            "absolute_change": int(row["absolute_change"]),
            "pct_change": None if row["pct_change"] is None else round(float(row["pct_change"]), 2),
        })
    return rows


def _sort_top_biomes_month(
    rows: list[dict[str, Any]],
    sort_by: str,
    limit: int,
) -> list[dict[str, Any]]:
    if sort_by == "volume":
        sorted_rows = sorted(
            rows, key=lambda r: (r["current_month_total"], r["previous_month_total"]), reverse=True
        )
    else:
        def _abs_pct(r: dict[str, Any]) -> float:
            v = r.get("pct_change")
            return abs(float(v)) if v is not None else 0.0
        sorted_rows = sorted(rows, key=_abs_pct, reverse=True)
    return sorted_rows[:limit]


def _build_state_biome_monthly_series_records(
    monthly_all_df: pd.DataFrame,
    monthly_by_biome_df: pd.DataFrame,
    state_month_all_df: pd.DataFrame,
    state_month_by_biome_df: pd.DataFrame,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
    max_month_in_current_year: int | None = None,
) -> list[dict[str, Any]]:
    ALL = ALL_BIOMES_VALUE
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    def upsert(period: str, year: int, value: int, biome: str, state: str) -> None:
        k = (period, biome, state)
        by_key[k] = {
            "period": period,
            "year": year,
            "value": int(value),
            "biome": biome,
            "state": state,
        }

    if not monthly_all_df.empty:
        for _, r in monthly_all_df.iterrows():
            upsert(str(r["period"]), int(r["year"]), int(r["value"]), ALL, ALL)
    if not monthly_by_biome_df.empty:
        for _, r in monthly_by_biome_df.iterrows():
            upsert(str(r["period"]), int(r["year"]), int(r["value"]), str(r["biome"]), ALL)
    if not state_month_all_df.empty:
        for _, r in state_month_all_df.iterrows():
            upsert(str(r["period"]), int(r["year"]), int(r["value"]), ALL, str(r["state"]))
    if not state_month_by_biome_df.empty:
        for _, r in state_month_by_biome_df.iterrows():
            upsert(str(r["period"]), int(r["year"]), int(r["value"]), str(r["biome"]), str(r["state"]))

    if mensal_is_current and mensal_counts.get("national"):
        cy = calendar_year
        for m, v in mensal_counts["national"].items():
            if max_month_in_current_year is not None and int(m) > max_month_in_current_year:
                continue
            p = f"{cy}-{int(m):02d}"
            upsert(p, cy, int(v), ALL, ALL)
        for bio_key, per_m in (mensal_counts.get("by_biome") or {}).items():
            for m, v in per_m.items():
                if max_month_in_current_year is not None and int(m) > max_month_in_current_year:
                    continue
                p = f"{cy}-{int(m):02d}"
                upsert(p, cy, int(v), str(bio_key), ALL)
        for st_key, per_m in (mensal_counts.get("by_state") or {}).items():
            for m, v in per_m.items():
                if max_month_in_current_year is not None and int(m) > max_month_in_current_year:
                    continue
                p = f"{cy}-{int(m):02d}"
                upsert(p, cy, int(v), ALL, str(st_key))
        for pair, per_m in (mensal_counts.get("by_state_biome") or {}).items():
            if isinstance(pair, tuple) and len(pair) == 2:
                st_k, bio_k = str(pair[0]), str(pair[1])
            else:
                continue
            for m, v in per_m.items():
                if max_month_in_current_year is not None and int(m) > max_month_in_current_year:
                    continue
                p = f"{cy}-{int(m):02d}"
                upsert(p, cy, int(v), bio_k, st_k)

    rows = list(by_key.values())
    rows.sort(key=lambda r: (r["period"], r["state"], r["biome"]))
    return rows


def _build_annual_totals_from_monthly_series(
    monthly_series_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[tuple[int, str, str], int] = {}
    for row in monthly_series_records:
        year = int(row.get("year") or 0)
        if year <= 0:
            continue
        biome = str(row.get("biome") or ALL_BIOMES_VALUE)
        state = str(row.get("state") or ALL_BIOMES_VALUE)
        value = int(row.get("value") or 0)
        key = (year, biome, state)
        by_key[key] = by_key.get(key, 0) + value

    rows = [
        {"year": year, "biome": biome, "state": state, "value": int(value)}
        for (year, biome, state), value in by_key.items()
    ]
    rows.sort(key=lambda r: (r["year"], r["state"], r["biome"]))
    return rows


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


def _normalize_reference_month_mode(mode: str) -> Literal["previous", "current"]:
    norm = str(mode).strip().lower()
    if norm in {"current", "vigente", "mes_vigente"}:
        return "current"
    if norm in {"previous", "anterior", "mes_anterior", ""}:
        return "previous"
    raise ValueError("reference_month_mode deve ser 'previous' ou 'current'.")


def _resolve_reference_month(
    calendar_year: int,
    mode: Literal["previous", "current"],
) -> tuple[int, int]:
    now = pd.Timestamp.now()
    if mode == "current":
        return calendar_year, int(now.month)

    if int(now.month) == 1:
        return calendar_year - 1, 12
    return calendar_year, int(now.month) - 1


def _sum_mensal_until(counts: dict[int, int], max_month: int) -> int:
    total = 0
    for month, value in counts.items():
        if int(month) <= max_month:
            total += int(value)
    return total


def _truncate_mensal_counts(mensal_counts: dict[str, Any], max_month: int) -> dict[str, Any]:
    def _truncate(per_month: dict[int, int]) -> dict[int, int]:
        return {
            int(month): int(value)
            for month, value in per_month.items()
            if int(month) <= max_month
        }

    truncated_by_biome: dict[str, dict[int, int]] = {}
    for biome, per_month in (mensal_counts.get("by_biome") or {}).items():
        values = _truncate(per_month)
        if values:
            truncated_by_biome[str(biome)] = values

    truncated_by_state: dict[str, dict[int, int]] = {}
    for state, per_month in (mensal_counts.get("by_state") or {}).items():
        values = _truncate(per_month)
        if values:
            truncated_by_state[str(state)] = values

    truncated_by_state_biome: dict[tuple[str, str], dict[int, int]] = {}
    for pair, per_month in (mensal_counts.get("by_state_biome") or {}).items():
        values = _truncate(per_month)
        if values:
            if isinstance(pair, tuple) and len(pair) == 2:
                truncated_by_state_biome[(str(pair[0]), str(pair[1]))] = values

    truncated_national = _truncate(mensal_counts.get("national") or {})
    last_closed_month = max(truncated_national.keys()) if truncated_national else 0
    return {
        "last_closed_month": last_closed_month,
        "national": truncated_national,
        "by_biome": truncated_by_biome,
        "by_state": truncated_by_state,
        "by_state_biome": truncated_by_state_biome,
    }


def _resolve_monthly_start_period(
    available_periods: list[str],
    start_year: int,
    fallback_start: str,
    fallback_end: str,
) -> str:
    min_period = f"{start_year}-01"
    for period in sorted(available_periods):
        if period >= min_period and period <= fallback_end:
            return period
    return fallback_start


def _build_effective_national_monthly_series(
    monthly_all_df: pd.DataFrame,
    mensal_counts: dict[str, Any],
    mensal_is_current: bool,
    calendar_year: int,
) -> list[tuple[str, int]]:
    merged: dict[str, int] = {}
    if not monthly_all_df.empty:
        for _, row in monthly_all_df.iterrows():
            period = str(row["period"])
            merged[period] = int(row["value"])

    if mensal_is_current:
        for month, value in (mensal_counts.get("national") or {}).items():
            period = f"{calendar_year}-{int(month):02d}"
            merged[period] = int(value)

    sorted_items = sorted(merged.items(), key=lambda item: item[0])
    return [(period, int(value)) for period, value in sorted_items]


def _compute_rolling_12m_metrics(
    monthly_series: list[tuple[str, int]],
    latest_period: str,
) -> dict[str, Any]:
    if not monthly_series:
        return {
            "recent_window_start_period": latest_period,
            "prior_window_start_period": None,
            "recent_total": 0,
            "prior_total": None,
            "pct_change": None,
            "has_full_prior_window": False,
        }

    periods = [period for period, _ in monthly_series]
    values = [int(value) for _, value in monthly_series]
    try:
        end_idx = periods.index(latest_period)
    except ValueError:
        end_idx = len(periods) - 1

    series_periods = periods[: end_idx + 1]
    series_values = values[: end_idx + 1]
    recent_values = series_values[-12:]
    prior_values = series_values[-24:-12] if len(series_values) >= 24 else []

    recent_start = series_periods[-len(recent_values)] if recent_values else latest_period
    prior_start = series_periods[-24] if len(series_values) >= 24 else None
    recent_total = int(sum(recent_values))
    prior_total = int(sum(prior_values)) if len(prior_values) == 12 else None
    return {
        "recent_window_start_period": recent_start,
        "prior_window_start_period": prior_start,
        "recent_total": recent_total,
        "prior_total": prior_total,
        "pct_change": _safe_pct_change(recent_total, prior_total) if prior_total is not None else None,
        "has_full_prior_window": prior_total is not None,
    }


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
        return {
            "last_closed_month": 0,
            "national": {},
            "by_biome": {},
            "by_state": {},
            "by_state_biome": {},
        }

    national: dict[int, int] = {}
    by_biome: dict[str, dict[int, int]] = {}
    by_state: dict[str, dict[int, int]] = {}
    by_state_biome: dict[tuple[str, str], dict[int, int]] = {}

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

            pair_df = df.dropna(subset=["state", "biome"])
            if not pair_df.empty:
                for (sk, bk), grp in pair_df.groupby(["state", "biome"]):
                    key = (str(sk).upper(), str(bk).upper())
                    if key not in by_state_biome:
                        by_state_biome[key] = {}
                    by_state_biome[key][month] = len(grp)

    return {
        "last_closed_month": max(month_files.keys()),
        "national": national,
        "by_biome": by_biome,
        "by_state": by_state,
        "by_state_biome": by_state_biome,
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