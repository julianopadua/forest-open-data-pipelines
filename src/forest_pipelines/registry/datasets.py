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
from forest_pipelines.datasets.eia import petroleum_weekly, heating_oil_propane, petroleum_monthly
from forest_pipelines.datasets.inpe import (
    area_queimada_focos1km,
    bdqueimadas_boletins_integrados,
    bdqueimadas_focos,
    bdqueimadas_painel_fogo,
)
from forest_pipelines.datasets.inmet import dados_historicos
from forest_pipelines.datasets.mma import cnuc_unidades_conservacao
from forest_pipelines.datasets.noticias_agricolas.sync import sync as noticias_agricolas_news_sync

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
    "eia_petroleum_monthly": petroleum_monthly.sync,

    # INPE Datasets
    "inpe_bdqueimadas_focos": bdqueimadas_focos.sync,
    "inpe_bdqueimadas_boletins_integrados": bdqueimadas_boletins_integrados.sync,
    "inpe_bdqueimadas_painel_fogo": bdqueimadas_painel_fogo.sync,
    "inpe_area_queimada_focos1km": area_queimada_focos1km.sync,

    # INMET Datasets
    "inmet_dados_historicos": dados_historicos.sync,

    # MMA Datasets
    "mma_cnuc_unidades_conservacao": cnuc_unidades_conservacao.sync,

    # News feeds
    "noticias_agricolas_news": noticias_agricolas_news_sync,
}

def get_dataset_runner(dataset_id: str) -> DatasetRunner:
    try:
        return RUNNERS[dataset_id]
    except KeyError as e:
        raise KeyError(f"Dataset não registrado: {dataset_id}") from e