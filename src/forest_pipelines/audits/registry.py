# src/forest_pipelines/audits/registry.py
from __future__ import annotations

from typing import Any, Callable

from forest_pipelines.audits.inpe import bdqueimadas_focos

AuditRunner = Callable[..., dict[str, Any]]

RUNNERS: dict[str, AuditRunner] = {
    "inpe_bdqueimadas_focos": bdqueimadas_focos.run_audit,
}


def get_audit_runner(dataset_id: str) -> AuditRunner:
    try:
        return RUNNERS[dataset_id]
    except KeyError as e:
        raise KeyError(f"Auditoria não registrada para dataset: {dataset_id}") from e
