# src/forest_pipelines/registry/datasets.py
from __future__ import annotations

from typing import Any, Callable

from forest_pipelines.datasets.cvm import fi_inf_diario


DatasetRunner = Callable[..., dict[str, Any]]


def get_dataset_runner(dataset_id: str) -> DatasetRunner:
    """
    Mapeia dataset_id -> função runner(settings, storage, logger, latest_months?)
    """
    if dataset_id == "cvm_fi_inf_diario":
        return fi_inf_diario.sync

    raise KeyError(f"Dataset não registrado: {dataset_id}")
