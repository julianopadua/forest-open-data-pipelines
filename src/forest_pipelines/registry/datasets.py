# src/forest_pipelines/registry/datasets.py
from __future__ import annotations

from typing import Any, Callable

from forest_pipelines.datasets.cvm import (
    fi_inf_diario,
    fi_doc_extrato,
    fi_cad_registro_fundo_classe,
    fi_cad_nao_adaptados_rcvm175,
    fi_cad_icvm555_hist,
    fii_doc_inf_trimestral,
)

DatasetRunner = Callable[..., dict[str, Any]]

RUNNERS: dict[str, DatasetRunner] = {
    "cvm_fi_inf_diario": fi_inf_diario.sync,
    "cvm_fi_doc_extrato": fi_doc_extrato.sync,
    "cvm_fi_cad_registro_fundo_classe": fi_cad_registro_fundo_classe.sync,
    "cvm_fi_cad_nao_adaptados_rcvm175": fi_cad_nao_adaptados_rcvm175.sync,
    "cvm_fi_cad_icvm555_hist": fi_cad_icvm555_hist.sync,
    "cvm_fii_doc_inf_trimestral": fii_doc_inf_trimestral.sync,
}


def get_dataset_runner(dataset_id: str) -> DatasetRunner:
    """
    Mapeia dataset_id -> função runner(settings, storage, logger, latest_months?)
    Observação: para datasets anuais, `latest_months` é usado como override de `latest_years`.
    """
    try:
        return RUNNERS[dataset_id]
    except KeyError as e:
        raise KeyError(f"Dataset não registrado: {dataset_id}") from e
