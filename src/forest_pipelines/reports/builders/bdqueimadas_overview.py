# src/forest_pipelines/reports/builders/bdqueimadas_overview.py
from __future__ import annotations

import copy
import csv
import json
import re
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from forest_pipelines.reports.definitions.base import (
    ReportConfig,
    load_report_cfg,
    localized_text_dict,
)
from forest_pipelines.reports.llm.base import maybe_generate_analysis_blocks

RE_YEAR = re.compile(r"(\d{4})")

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

SUPPORTED_REPORT_LOCALES = ["pt", "en"]
DEFAULT_REPORT_LOCALE = "pt"


def build_package(
    settings: Any,
    storage: Any,
    logger: Any,
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

    zip_files = _select_zip_files(
        base_dir=settings.data_dir / cfg.dataset.local_relative_dir,
        file_glob=cfg.dataset.file_glob,
        recent_years=cfg.dataset.recent_years,
    )

    if not zip_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado para o report em: "
            f"{(settings.data_dir / cfg.dataset.local_relative_dir).resolve()}"
        )

    monthly_frames: list[pd.DataFrame] = []
    annual_frames: list[pd.DataFrame] = []
    state_year_frames: list[pd.DataFrame] = []
    yearly_file_stats: list[dict[str, Any]] = []

    for zip_path in zip_files:
        logger.info("Processando ZIP do report: %s", zip_path.name)
        subset, detected_columns = _read_zip_subset(
            zip_path=zip_path,
            datetime_candidates=datetime_candidates,
            state_candidates=state_candidates,
        )

        logger.info(
            "Colunas detectadas em %s -> datetime=%s | state=%s",
            zip_path.name,
            detected_columns["datetime"],
            detected_columns["state"],
        )

        if subset.empty:
            logger.warning("Arquivo sem linhas úteis para análise: %s", zip_path.name)
            continue

        inferred_year = _extract_year_from_name(zip_path.name)
        yearly_file_stats.append(
            {
                "file_name": zip_path.name,
                "file_size_bytes": int(zip_path.stat().st_size),
                "inferred_year": inferred_year,
                "row_count": int(len(subset)),
                "month_span_min": str(subset["period_month"].min()),
                "month_span_max": str(subset["period_month"].max()),
                "detected_datetime_column": detected_columns["datetime"],
                "detected_state_column": detected_columns["state"],
            }
        )

        month_df = (
            subset.groupby("period_month")
            .size()
            .rename("value")
            .reset_index()
            .rename(columns={"period_month": "period"})
        )
        month_df["period"] = month_df["period"].astype(str)
        monthly_frames.append(month_df)

        year_df = (
            subset.groupby("year")
            .size()
            .rename("value")
            .reset_index()
        )
        annual_frames.append(year_df)

        state_year_df = (
            subset.dropna(subset=["state"])
            .groupby(["year", "state"])
            .size()
            .rename("value")
            .reset_index()
        )
        state_year_frames.append(state_year_df)

    monthly_series = _merge_sum_frames(monthly_frames, key_cols=["period"])
    annual_series = _merge_sum_frames(annual_frames, key_cols=["year"])
    state_year_series = _merge_sum_frames(state_year_frames, key_cols=["year", "state"])

    if monthly_series.empty or annual_series.empty:
        raise RuntimeError("Não foi possível montar séries agregadas para o report BDQueimadas.")

    monthly_series = monthly_series.sort_values("period").reset_index(drop=True)
    annual_series = annual_series.sort_values("year").reset_index(drop=True)
    state_year_series = state_year_series.sort_values(["year", "state"]).reset_index(drop=True)

    latest_year = int(annual_series["year"].max())
    previous_year = _find_previous_year(annual_series, latest_year)

    latest_period = str(monthly_series["period"].iloc[-1])
    recent_monthly = monthly_series.tail(cfg.display.monthly_points).copy()
    recent_annual = annual_series.tail(cfg.display.annual_years).copy()

    current_year_total = int(
        annual_series.loc[annual_series["year"] == latest_year, "value"].iloc[0]
    )
    previous_year_total = int(
        annual_series.loc[annual_series["year"] == previous_year, "value"].iloc[0]
    ) if previous_year is not None else 0

    recent_12m_total = int(monthly_series["value"].tail(12).sum())
    prior_12m_total = int(monthly_series["value"].iloc[-24:-12].sum()) if len(monthly_series) >= 24 else 0
    total_rows_processed = int(sum(item["row_count"] for item in yearly_file_stats))

    top_states_table = _build_top_states_table(
        state_year_series=state_year_series,
        latest_year=latest_year,
        previous_year=previous_year,
        limit=cfg.display.top_states_limit,
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
    )

    analysis_context = {
        "latest_year": latest_year,
        "previous_year": previous_year,
        "latest_period": latest_period,
        "current_year_total": current_year_total,
        "previous_year_total": previous_year_total,
        "recent_12m_total": recent_12m_total,
        "prior_12m_total": prior_12m_total,
        "total_rows_processed": total_rows_processed,
        "file_count_used": len(zip_files),
        "top_states_current_year": [
            {
                "state": row["state"],
                "current_year_total": row["current_year_total"],
                "previous_year_total": row["previous_year_total"],
            }
            for row in top_states_table[: min(5, len(top_states_table))]
        ],
        "yearly_file_stats": yearly_file_stats[: min(6, len(yearly_file_stats))],
    }

    fallback_analysis = _build_fallback_analysis(
        latest_year=latest_year,
        previous_year=previous_year,
        current_year_total=current_year_total,
        previous_year_total=previous_year_total,
        recent_12m_total=recent_12m_total,
        prior_12m_total=prior_12m_total,
        latest_period=latest_period,
        total_rows_processed=total_rows_processed,
        file_count_used=len(zip_files),
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

    generated_report = {
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
        },
        "coverage": {
            "latest_year": latest_year,
            "previous_year": previous_year,
            "latest_period": latest_period,
            "recent_years_loaded": cfg.dataset.recent_years,
        },
        "highlights": highlights,
        "analysis": analysis_blocks,
        "sections": [
            {
                "id": "monthly_series",
                "kind": "timeseries",
                "title": _localized(
                    f"Série mensal de focos (últimos {cfg.display.monthly_points} pontos)",
                    f"Monthly hotspot series (last {cfg.display.monthly_points} points)",
                ),
                "x_key": "period",
                "y_key": "value",
                "data": _df_to_records(recent_monthly),
            },
            {
                "id": "annual_totals",
                "kind": "bar",
                "title": _localized(
                    f"Totais anuais de focos (últimos {cfg.display.annual_years} anos)",
                    f"Annual hotspot totals (last {cfg.display.annual_years} years)",
                ),
                "x_key": "year",
                "y_key": "value",
                "data": _df_to_records(recent_annual),
            },
            {
                "id": "top_states_current_vs_previous",
                "kind": "table",
                "title": _localized(
                    "Comparação por UF: ano mais recente vs ano anterior",
                    "State comparison: most recent year vs previous year",
                ),
                "columns": [
                    {"key": "state", "label": _localized("UF", "State")},
                    {"key": "current_year_total", "label": _localized(f"Focos em {latest_year}", f"Hotspots in {latest_year}")},
                    {
                        "key": "previous_year_total",
                        "label": _localized(
                            f"Focos em {previous_year}" if previous_year else "Ano anterior",
                            f"Hotspots in {previous_year}" if previous_year else "Previous year",
                        ),
                    },
                    {"key": "absolute_change", "label": _localized("Variação absoluta", "Absolute change")},
                    {"key": "pct_change", "label": _localized("Variação %", "% change")},
                ],
                "rows": top_states_table,
            },
        ],
        "analysis_context": analysis_context,
        "methodology": {
            "source": source_label_i18n,
            "note": _localized(
                "Este report usa artefatos agregados e leves para publicação. "
                "Os dados são processados localmente a partir dos ZIPs anuais do BDQueimadas "
                "e a página pública consome somente o JSON final do report.",
                "This report uses lightweight aggregated artifacts for publication. "
                "Data is processed locally from the annual BDQueimadas ZIP files, "
                "and the public page consumes only the final report JSON.",
            ),
            "limitations": _localized(
                "O ano mais recente pode estar incompleto, dependendo da disponibilidade "
                "do arquivo anual corrente no momento da atualização.",
                "The most recent year may be incomplete, depending on the availability "
                "of the current annual file at the time of the update.",
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
            "source_label": generated_report["source_label"],
            "dataset_id": cfg.dataset.dataset_id,
            "latest_year": latest_year,
            "latest_period": latest_period,
            "llm_enabled": cfg.llm.enabled,
            "publish_generated_as_live": cfg.editorial.publish_generated_as_live,
            "available_locales": SUPPORTED_REPORT_LOCALES,
            "default_locale": DEFAULT_REPORT_LOCALE,
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


def _extract_year_from_name(filename: str) -> int | None:
    match = RE_YEAR.search(filename)
    if not match:
        return None
    return int(match.group(1))


def _read_zip_subset(
    zip_path: Path,
    datetime_candidates: list[str],
    state_candidates: list[str],
) -> tuple[pd.DataFrame, dict[str, str]]:
    with zipfile.ZipFile(zip_path) as zf:
        member = _pick_member(zf)
        delimiter = _detect_delimiter(zf, member)
        columns = _detect_columns(
            zf=zf,
            member=member,
            datetime_candidates=datetime_candidates,
            state_candidates=state_candidates,
            delimiter=delimiter,
        )

        dt_col = columns["datetime"]
        state_col = columns["state"]

        df = _read_member_csv(
            zf=zf,
            member=member,
            delimiter=delimiter,
            usecols=[dt_col, state_col],
        )

    df = df.rename(columns={dt_col: "raw_datetime", state_col: "raw_state"}).copy()

    dt = pd.to_datetime(
        df["raw_datetime"].astype("string").str.strip(),
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )

    state = (
        df["raw_state"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    )

    out = pd.DataFrame(
        {
            "datetime": dt,
            "state": state,
        }
    ).dropna(subset=["datetime"])

    out["year"] = out["datetime"].dt.year.astype(int)
    out["period_month"] = out["datetime"].dt.to_period("M").astype(str)

    return out[["datetime", "year", "period_month", "state"]], {
        "datetime": dt_col,
        "state": state_col,
    }


def _pick_member(zf: zipfile.ZipFile) -> str:
    members = [
        name
        for name in zf.namelist()
        if not name.endswith("/") and Path(name).suffix.lower() in {".csv", ".txt"}
    ]
    if not members:
        raise FileNotFoundError("ZIP sem arquivo CSV/TXT legível.")
    members.sort()
    return members[0]


def _detect_delimiter(zf: zipfile.ZipFile, member: str) -> str:
    with zf.open(member) as f:
        sample = f.read(4096).decode("utf-8", errors="ignore")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def _detect_columns(
    zf: zipfile.ZipFile,
    member: str,
    datetime_candidates: list[str],
    state_candidates: list[str],
    delimiter: str,
) -> dict[str, str]:
    header_df = _read_member_csv(
        zf=zf,
        member=member,
        delimiter=delimiter,
        nrows=0,
    )
    available = list(header_df.columns)

    dt_col = _pick_column(available, datetime_candidates)
    state_col = _pick_column(available, state_candidates)

    if dt_col is None:
        raise KeyError(
            f"Não foi possível identificar a coluna temporal. "
            f"Candidatas testadas: {datetime_candidates}. "
            f"Colunas disponíveis: {available}"
        )
    if state_col is None:
        raise KeyError(
            f"Não foi possível identificar a coluna de UF/estado. "
            f"Candidatas testadas: {state_candidates}. "
            f"Colunas disponíveis: {available}"
        )

    return {
        "datetime": dt_col,
        "state": state_col,
    }


def _pick_column(available: list[str], candidates: list[str]) -> str | None:
    normalized_map = {_normalize(col): col for col in available}
    for candidate in candidates:
        norm = _normalize(candidate)
        if norm in normalized_map:
            return normalized_map[norm]
    return None


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.casefold())


def _read_member_csv(
    zf: zipfile.ZipFile,
    member: str,
    delimiter: str,
    usecols: list[str] | None = None,
    nrows: int | None = None,
) -> pd.DataFrame:
    encodings = ["utf-8", "latin-1", "cp1252"]

    last_error: Exception | None = None
    for encoding in encodings:
        try:
            with zf.open(member) as f:
                return pd.read_csv(
                    f,
                    sep=delimiter,
                    encoding=encoding,
                    usecols=usecols,
                    nrows=nrows,
                    dtype="string",
                    low_memory=False,
                    on_bad_lines="skip",
                )
        except Exception as e:  # noqa: BLE001
            last_error = e
            continue

    raise RuntimeError(f"Falha ao ler {member} com encodings suportados.") from last_error


def _merge_sum_frames(frames: list[pd.DataFrame], key_cols: list[str]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=[*key_cols, "value"])

    merged = pd.concat(frames, ignore_index=True)
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce").fillna(0).astype(int)

    return (
        merged.groupby(key_cols, as_index=False)["value"]
        .sum()
        .sort_values(key_cols)
        .reset_index(drop=True)
    )


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
    latest_year: int,
    previous_year: int | None,
    current_year_total: int,
    previous_year_total: int,
    recent_12m_total: int,
    prior_12m_total: int,
    latest_period: str,
    total_rows_processed: int,
    file_count_used: int,
) -> dict[str, dict[str, str]]:
    yoy = _safe_pct_change(current_year_total, previous_year_total)
    recent_12m_change = _safe_pct_change(recent_12m_total, prior_12m_total)

    if previous_year is None:
        headline_pt = (
            f"O processamento mais recente vai até {latest_period} e agrega "
            f"{_fmt_int_pt(current_year_total)} focos em {latest_year}."
        )
        comparison_pt = (
            "Ainda não há ano anterior processado no escopo atual para comparação anual direta."
        )

        headline_en = (
            f"The latest processed coverage reaches {latest_period} and aggregates "
            f"{_fmt_int_en(current_year_total)} hotspots in {latest_year}."
        )
        comparison_en = (
            "There is not yet a previous processed year within the current scope for a direct annual comparison."
        )
    else:
        headline_pt = (
            f"Em {latest_year}, o conjunto processado registra {_fmt_int_pt(current_year_total)} focos, "
            f"contra {_fmt_int_pt(previous_year_total)} em {previous_year}."
        )
        comparison_pt = (
            f"A comparação anual indica {_fmt_pct_pt(yoy)} entre {previous_year} e {latest_year}. "
            f"O último mês disponível no recorte processado é {latest_period}."
        )

        headline_en = (
            f"In {latest_year}, the processed set records {_fmt_int_en(current_year_total)} hotspots, "
            f"versus {_fmt_int_en(previous_year_total)} in {previous_year}."
        )
        comparison_en = (
            f"The annual comparison indicates {_fmt_pct_en(yoy)} between {previous_year} and {latest_year}. "
            f"The latest available month in the processed scope is {latest_period}."
        )

    overview_pt = (
        f"Foram processadas {_fmt_int_pt(total_rows_processed)} linhas distribuídas em {file_count_used} arquivos anuais. "
        f"Na janela móvel mais recente de 12 meses, o total agregado soma {_fmt_int_pt(recent_12m_total)} focos, "
        f"contra {_fmt_int_pt(prior_12m_total)} nos 12 meses imediatamente anteriores, "
        f"o que corresponde a {_fmt_pct_pt(recent_12m_change)}."
    )

    limitations_pt = (
        "O texto é descritivo e não estabelece causalidade. "
        "O ano corrente pode estar incompleto, pois depende do arquivo anual mais recente disponível no BDQueimadas."
    )

    overview_en = (
        f"{_fmt_int_en(total_rows_processed)} rows were processed across {file_count_used} annual files. "
        f"In the most recent rolling 12-month window, the aggregate total reaches {_fmt_int_en(recent_12m_total)} hotspots, "
        f"versus {_fmt_int_en(prior_12m_total)} in the immediately previous 12 months, "
        f"which corresponds to {_fmt_pct_en(recent_12m_change)}."
    )

    limitations_en = (
        "This text is descriptive and does not establish causality. "
        "The current year may be incomplete, as it depends on the latest annual file available in BDQueimadas."
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


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        clean: dict[str, Any] = {}
        for key, value in row.items():
            if pd.isna(value):
                clean[key] = None
            elif isinstance(value, (pd.Timestamp,)):
                clean[key] = value.isoformat()
            elif hasattr(value, "item"):
                clean[key] = value.item()
            else:
                clean[key] = value
        out.append(clean)
    return out


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


def _now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")