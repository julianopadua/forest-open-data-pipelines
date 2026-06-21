#src/forest_pipelines/registry/datasets.py
from __future__ import annotations
from typing import Any, Callable

from forest_pipelines.datasets.anp.govbr import ANP_DATASET_IDS
from forest_pipelines.datasets.anp import govbr as anp_govbr
from forest_pipelines.datasets.cvm import ckan_dataset
from forest_pipelines.datasets.eia import petroleum_weekly, heating_oil_propane, petroleum_monthly
from forest_pipelines.datasets.inpe import (
    area_queimada_focos1km,
    bdqueimadas_boletins_integrados,
    bdqueimadas_focos,
    bdqueimadas_focos_coids,
    bdqueimadas_painel_fogo,
)
from forest_pipelines.datasets.inmet import dados_historicos
from forest_pipelines.datasets.mma import cnuc_unidades_conservacao
from forest_pipelines.datasets.noticias_agricolas.sync import sync as noticias_agricolas_news_sync
from forest_pipelines.datasets.supranational import SUPRANATIONAL_DATASET_IDS
from forest_pipelines.datasets.supranational import make_sync as make_supranational_sync

DatasetRunner = Callable[..., dict[str, Any]]

CVM_DATASET_IDS: tuple[str, ...] = (
    "cvm_processo_sancionador",
    "cvm_crowdfunding_cad",
    "cvm_agente_fiduc_cad",
    "cvm_oferta_distrib",
    "cvm_emissor_cepac_cad",
    "cvm_coord_oferta_cad",
    "cvm_auditor_cad",
    "cvm_intermed_cad",
    "cvm_agente_auton_cad",
    "cvm_ato_declr_intermed",
    "cvm_cia_aberta_eventos_recompra_acoes",
    "cvm_cia_incent_cad",
    "cvm_cia_estrang_cad",
    "cvm_cia_aberta_cad",
    "cvm_fi_inf_diario",
    "cvm_fi_doc_extrato",
    "cvm_invnr_cad",
    "cvm_fi_cad",
    "cvm_consultor_vlmob_cad",
    "cvm_adm_fii_cad",
    "cvm_adm_cart_cad",
    "cvm_fii_doc_inf_trimestral",
    "cvm_securit_doc_inf_mensal_ots",
    "cvm_fii_doc_inf_mensal",
    "cvm_securit_doc_inf_mensal_cri",
    "cvm_securit_doc_inf_mensal_cra",
    "cvm_fii_doc_inf_anual",
    "cvm_fiagro_doc_inf_mensal",
    "cvm_fi_doc_entrega",
    "cvm_fii_doc_dfin",
    "cvm_securit_doc_dfin_cri",
    "cvm_securit_doc_dfin_cra",
    "cvm_cia_aberta_doc_vlmo",
    "cvm_cia_aberta_doc_itr",
    "cvm_cia_aberta_doc_ipe",
    "cvm_cia_aberta_doc_fre",
    "cvm_cia_aberta_doc_fca",
    "cvm_cia_aberta_doc_dfp",
    "cvm_cia_aberta_doc_cgvn",
    "cvm_fi_doc_perfil_mensal",
    "cvm_fie_medidas",
    "cvm_fi_doc_lamina",
    "cvm_fip_doc_inf_trimestral",
    "cvm_fip_doc_inf_quadrimestral",
    "cvm_fidc_doc_inf_mensal",
    "cvm_fi_doc_eventual",
    "cvm_fi_doc_compl",
    "cvm_fi_doc_cda",
    "cvm_fie_doc_balancete",
    "cvm_fi_doc_balancete",
    "cvm_fie_doc_balanco",
    "cvm_distrpubl",
    "cvm_emissores",
    "cvm_arrecadacao_receita_publica",
)

RUNNERS: dict[str, DatasetRunner] = {
    **{dataset_id: anp_govbr.make_sync(dataset_id) for dataset_id in ANP_DATASET_IDS},
    **{dataset_id: ckan_dataset.make_sync(dataset_id) for dataset_id in CVM_DATASET_IDS},
    **{dataset_id: make_supranational_sync(dataset_id) for dataset_id in SUPRANATIONAL_DATASET_IDS},

    #eia datasets
    "eia_petroleum_weekly": petroleum_weekly.sync,
    "eia_heating_oil_propane": heating_oil_propane.sync,
    "eia_petroleum_monthly": petroleum_monthly.sync,

    #inpe datasets
    "inpe_bdqueimadas_focos": bdqueimadas_focos.sync,
    "inpe_bdqueimadas_focos_anual_ams_sat_ref": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_anual_ams_sat_ref"),
    "inpe_bdqueimadas_focos_anual_brasil_todos_sats": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_anual_brasil_todos_sats"),
    "inpe_bdqueimadas_focos_anual_estados_sat_ref": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_anual_estados_sat_ref"),
    "inpe_bdqueimadas_focos_mensal_brasil": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_mensal_brasil"),
    "inpe_bdqueimadas_focos_mensal_america_sul": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_mensal_america_sul"),
    "inpe_bdqueimadas_focos_diario_brasil": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_diario_brasil"),
    "inpe_bdqueimadas_focos_diario_america_sul": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_diario_america_sul"),
    "inpe_bdqueimadas_focos_10min": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_10min"),
    "inpe_bdqueimadas_focos_documentos": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_documentos"),
    "inpe_bdqueimadas_focos_kml": bdqueimadas_focos_coids.make_sync("bdqueimadas_focos_kml"),
    "inpe_bdqueimadas_boletins_integrados": bdqueimadas_boletins_integrados.sync,
    "inpe_bdqueimadas_painel_fogo": bdqueimadas_painel_fogo.sync,
    "inpe_area_queimada_focos1km": area_queimada_focos1km.sync,

    #inmet datasets
    "inmet_dados_historicos": dados_historicos.sync,

    #mma datasets
    "mma_cnuc_unidades_conservacao": cnuc_unidades_conservacao.sync,

    #news feeds
    "noticias_agricolas_news": noticias_agricolas_news_sync,
}

def get_dataset_runner(dataset_id: str) -> DatasetRunner:
    try:
        return RUNNERS[dataset_id]
    except KeyError as e:
        raise KeyError(f"Dataset não registrado: {dataset_id}") from e
