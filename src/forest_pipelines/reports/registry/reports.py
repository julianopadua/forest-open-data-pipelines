# src/forest_pipelines/reports/registry/reports.py
from __future__ import annotations

from typing import Any, Callable

from forest_pipelines.reports.builders import bdqueimadas_overview

ReportRunner = Callable[..., dict[str, Any]]

RUNNERS: dict[str, ReportRunner] = {
    "bdqueimadas_overview": bdqueimadas_overview.build_package,
}


def get_report_runner(report_id: str) -> ReportRunner:
    try:
        return RUNNERS[report_id]
    except KeyError as e:
        raise KeyError(f"Report não registrado: {report_id}") from e