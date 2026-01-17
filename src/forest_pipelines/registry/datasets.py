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
    fi_doc_entrega,
    fii_doc_inf_mensal,
    fii_doc_inf_anual
)
from forest_pipelines.datasets.eia import petroleum_weekly, heating_oil_propane
from forest_pipelines.datasets.inpe import bdqueimadas_focos, area_queimada_focos1km
from forest_pipelines.datasets.inmet import dados_historicos

DatasetRunner = Callable[..., dict[str, Any]]

RUNNERS: dict[str, DatasetRunner] = {
    # CVM Datasets
    "cvm_fi_inf_diario": fi_inf_diario.sync,
    "cvm_fi_doc_extrato": fi_doc_extrato.sync,
    "cvm_fi_cad_registro_fundo_classe": fi_cad_registro_fundo_classe.sync,
    "cvm_fi_cad_nao_adaptados_rcvm175": fi_cad_nao_adaptados_rcvm175.sync,
    "cvm_fi_cad_icvm555_hist": fi_cad_icvm555_hist.sync,
    "cvm_fii_doc_inf_trimestral": fii_doc_inf_trimestral.sync,
    "cvm_fi_doc_entrega": fi_doc_entrega.sync,
    "cvm_fii_doc_inf_mensal": fii_doc_inf_mensal.sync, 
    "cvm_fii_doc_inf_anual": fii_doc_inf_anual.sync,

    # EIA Datasets
    "eia_petroleum_weekly": petroleum_weekly.sync,
    "eia_heating_oil_propane": heating_oil_propane.sync,

    # INPE Datasets
    "inpe_bdqueimadas_focos": bdqueimadas_focos.sync,
    "inpe_area_queimada_focos1km": area_queimada_focos1km.sync,

    # INMET Datasets
    "inmet_dados_historicos": dados_historicos.sync,
}

def get_dataset_runner(dataset_id: str) -> DatasetRunner:
    try:
        return RUNNERS[dataset_id]
    except KeyError as e:
        raise KeyError(f"Dataset não registrado: {dataset_id}") from e