# Frontend (superfície de uso: CLI)

## Escopo

**Não há aplicação frontend neste repositório.** A “interface” do sistema é o **CLI Typer** (`forest-pipelines`) e os **arquivos de configuração**. O consumo web é feito por outro projeto (`forest-portal`), que lê JSON públicos no Storage.

## Entrypoint

- Comando instalado: `forest-pipelines` → `forest_pipelines.cli:app` ([`pyproject.toml`](../../../pyproject.toml)).
- Execução direta do módulo: `python -m forest_pipelines.cli` (com pacote instalado).

## Comandos e parâmetros (matriz)

| Comando Typer | Args posicionais | Opções | Precisa Supabase env? |
| --- | --- | --- | --- |
| `sync` | `dataset_id` | `--config-path` (default `configs/app.yml`), `--latest-months` | Sim |
| `build-report` | `report_id` | `--config-path` | Sim |
| `audit-dataset` | `dataset_id` (registry de **auditoria**) | `--config-path` | Não |

## Matriz: registry ID → comando → YAML

O **ID** na primeira coluna é o que o usuário passa ao CLI. A coluna **Arquivo YAML** é o que o código passa a `load_dataset_cfg` (caminho relativo a `configs/datasets`, extensão `.yml`).

| ID (CLI/registry) | Comando | Arquivo YAML (`configs/datasets/`) |
| --- | --- | --- |
| `cvm_fi_inf_diario` | `sync` | `cvm/fi_inf_diario.yml` |
| `cvm_fi_doc_extrato` | `sync` | `cvm/fi_doc_extrato.yml` |
| `cvm_fi_cad_registro_fundo_classe` | `sync` | `cvm/fi_cad_registro_fundo_classe.yml` |
| `cvm_fi_cad_nao_adaptados_rcvm175` | `sync` | `cvm/fi_cad_nao_adaptados_rcvm175.yml` |
| `cvm_fi_cad_icvm555_hist` | `sync` | `cvm/fi_cad_icvm555_hist.yml` |
| `cvm_fii_doc_inf_trimestral` | `sync` | `cvm/fii_doc_inf_trimestral.yml` |
| `cvm_fi_doc_entrega` | `sync` | `cvm/fi_doc_entrega.yml` |
| `cvm_fii_doc_inf_mensal` | `sync` | `cvm/fii_doc_inf_mensal.yml` |
| `cvm_fii_doc_inf_anual` | `sync` | `cvm/fii_doc_inf_anual.yml` |
| `eia_petroleum_weekly` | `sync` | `eia/petroleum_weekly.yml` |
| `eia_heating_oil_propane` | `sync` | `eia/heating_oil_propane.yml` |
| `eia_petroleum_monthly` | `sync` | `eia/petroleum_monthly.yml` |
| `inpe_bdqueimadas_focos` | `sync` | `inpe/bdqueimadas_focos.yml` |
| `inpe_area_queimada_focos1km` | `sync` | `inpe/area_queimada_focos1km.yml` |
| `inmet_dados_historicos` | `sync` | `inmet/dados_historicos.yml` |
| `noticias_agricolas_news` | `sync` | `noticias_agricolas_news.yml` (na raiz de `datasets/`) |

Fonte: [`src/forest_pipelines/registry/datasets.py`](../../../src/forest_pipelines/registry/datasets.py) e `load_dataset_cfg(..., "<path>")` em cada módulo `sync`.

## Reports

| ID | Comando | Config |
| --- | --- | --- |
| `bdqueimadas_overview` | `forest-pipelines build-report bdqueimadas_overview` | `configs/reports/bdqueimadas_overview.yml` |

## Auditorias

| ID | Comando | Pré-requisito local |
| --- | --- | --- |
| `inpe_bdqueimadas_focos` | `forest-pipelines audit-dataset inpe_bdqueimadas_focos` | ZIPs `focos_br_ref_*.zip` em `data/inpe_bdqueimadas/` |

## Scripts auxiliares

| Script | Quando usar | Observação |
| --- | --- | --- |
| `scripts/backfill_cvm_inf_diario.py` | Backfill longo (60 meses no código) | Chama runner diretamente; **sempre** faz upload do manifest (não usa flag `_cli_skip_manifest_upload`). |
| `scripts/run_local.sh` | Bootstrap venv + exemplo sync | Executa `forest-pipelines sync cvm_fi_inf_diario --latest-months 12`. |

## Estado / hooks

Não há React/Vue; não há providers nem routers web. Typer não adiciona estado global além do processo atual.

## Acoplamento

- Forte acoplamento ao **Supabase** como backend de arquivos para `sync` e `build-report`.
- **Desacoplamento** do frontend Next.js — correto para deploy independente.

## Duplicação

- Lógica de publicação de manifest aparece no CLI e no `scripts/backfill_cvm_inf_diario.py` — manter em sincronia manualmente.
