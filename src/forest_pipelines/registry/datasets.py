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
    bdqueimadas_painel_fogo,
)
from forest_pipelines.datasets.inmet import dados_historicos
from forest_pipelines.datasets.mma import cnuc_unidades_conservacao
from forest_pipelines.datasets.noticias_agricolas.sync import sync as noticias_agricolas_news_sync

DatasetRunner = Callable[..., dict[str, Any]]

ANP_DATASET_IDS: tuple[str, ...] = (
    "anp_acervo_de_dados_tecnicos",
    "anp_acoes_de_fiscalizacao_do_abastecimento",
    "anp_aditamento_de_conteudo_local",
    "anp_amostras_de_rochas_e_fluidos",
    "anp_anuario_estatistico_brasileiro_do_petroleo_gas_natural_e_biocombustiveis",
    "anp_aquisicao_processamento_e_estudo_de_dados",
    "anp_autorizacoes_de_gas_natural",
    "anp_blocos_com_fase_exploratoria_encerrada",
    "anp_capacidade_de_armazenagem_de_terminais",
    "anp_comercializacao_de_gas_natural",
    "anp_dados_cadastrais_das_revendas_de_gas_liquefeito_de_petroleo_glp",
    "anp_dados_cadastrais_dos_revendedores_varejistas_de_combustiveis_automotivos",
    "anp_gestao_de_contratos_de_exploracao_e_producao__dados_de_ep",
    "anp_dados_georreferenciados_das_bacias_sedimentares_brasileiras",
    "anp_distribuidores_de_combustiveis_liquidos",
    "anp_fase_de_exploracao",
    "anp_fase_de_desenvolvimento_e_producao",
    "anp_fiscalizacao_de_conteudo_local",
    "anp_importacoes_e_exportacoes",
    "anp_dados_de_incidentes_de_exploracao_e_producao_de_petroleo_e_gas_natural",
    "anp_movimentacao_de_derivados_de_petroleo_e_biocombustiveis",
    "anp_movimentacao_dos_terminais_aquaviarios",
    "anp_dados_consolidados_de_movimentacao_de_gas_natural_em_gasodutos_de_transporte",
    "anp_multas_aplicadas___vencimento_a_partir_de_2016",
    "anp_participacoes_governamentais",
    "anp_pesquisa_e_desenvolvimento_e_inovacao_pdi",
    "anp_pontos_de_abastecimento_autorizados",
    "anp_pmqc___programa_de_monitoramento_da_qualidade_dos_combustiveis",
    "anp_programa_de_monitoramento_dos_lubrificantes_pml",
    "anp_prestadores_de_servicos_de_apoio_administrativo",
    "anp_previso_de_investimentos_exploratrios",
    "anp_processamento_de_petroleo_e_producao_de_derivados",
    "anp_producao_de_biocombustiveis",
    "anp_producao_de_petroleo_e_gas_natural_por_estado_e_localizacao",
    "anp_producao_de_petroleo_e_gas_natural_por_poco",
    "anp_relacao_de_concessionarios",
    "anp_registro_de_leos_e_graxas_lubrificantes",
    "anp_resultado_de_poco",
    "anp_rodadas_de_licitacoes_de_petroleo_e_gas_natural",
    "anp_serie_historica_de_precos_de_combustiveis_e_de_glp",
    "anp_tancagem_do_abastecimento_nacional_de_combustiveis",
    "anp_vendas_de_derivados_de_petroleo_e_biocombustiveis",
)


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

    #eia datasets
    "eia_petroleum_weekly": petroleum_weekly.sync,
    "eia_heating_oil_propane": heating_oil_propane.sync,
    "eia_petroleum_monthly": petroleum_monthly.sync,

    #inpe datasets
    "inpe_bdqueimadas_focos": bdqueimadas_focos.sync,
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
