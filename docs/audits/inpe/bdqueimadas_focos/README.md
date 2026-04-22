# Auditoria de esquema do BDQueimadas

## Escopo

Esta auditoria inspeciona localmente os arquivos ZIP anuais do BDQueimadas, identificando estrutura de colunas, colunas temporais e geográficas detectáveis, tipos inferidos a partir de amostras e diferenças de esquema entre anos.

## Metadados

| dataset_id | generated_at | files_total | base_dir | output_dir |
| --- | --- | --- | --- | --- |
| inpe_bdqueimadas_focos | 2026-04-22T02:48:50.109798Z | 7 | /Users/julianopadua/Projects/forest-open-data-pipelines/data/inpe_bdqueimadas | /Users/julianopadua/Projects/forest-open-data-pipelines/docs/audits/inpe/bdqueimadas_focos |

## Achados principais

- Foram auditados 7 arquivos ZIP do BDQueimadas localizados em `/Users/julianopadua/Projects/forest-open-data-pipelines/data/inpe_bdqueimadas`.
- O esquema modal apareceu em 7 arquivo(s).
- A interseção de colunas entre todos os arquivos contém 9 coluna(s).
- A união de colunas observadas contém 9 coluna(s).
- Detecção de coluna temporal: data_pas (7)
- Detecção de coluna de UF/estado: estado (7)
- Nenhum arquivo divergiu do esquema modal observado.

## Inventário por arquivo

| year | zip_name | row_count | column_count | datetime_column_detected | state_column_detected |
| --- | --- | --- | --- | --- | --- |
| 2019 | focos_br_ref_2019.zip | 197.632 | 9 | data_pas | estado |
| 2020 | focos_br_ref_2020.zip | 222.797 | 9 | data_pas | estado |
| 2021 | focos_br_ref_2021.zip | 184.081 | 9 | data_pas | estado |
| 2022 | focos_br_ref_2022.zip | 200.763 | 9 | data_pas | estado |
| 2023 | focos_br_ref_2023.zip | 189.901 | 9 | data_pas | estado |
| 2024 | focos_br_ref_2024.zip | 278.299 | 9 | data_pas | estado |
| 2025 | focos_br_ref_2025.zip | 136.393 | 9 | data_pas | estado |

## Esquema modal

O esquema modal apareceu em **7** arquivo(s) e possui **9** coluna(s).

- `id_bdq`
- `foco_id`
- `lat`
- `lon`
- `data_pas`
- `pais`
- `estado`
- `municipio`
- `bioma`

## Colunas presentes em todos os arquivos

- `bioma`
- `data_pas`
- `estado`
- `foco_id`
- `id_bdq`
- `lat`
- `lon`
- `municipio`
- `pais`

## Resumo por coluna

| column | present_in_files | presence_pct | dominant_types | sample_values |
| --- | --- | --- | --- | --- |
| bioma | 7 | 100,00% | string (7) | Amazônia, Pantanal, Cerrado, Caatinga |
| data_pas | 7 | 100,00% | datetime (7) | 2019-10-27 17:05:00, 2019-10-28 17:45:00, 2019-10-30 17:30:00, 2019-11-01 15:45:00 |
| estado | 7 | 100,00% | string (7) | PARÁ, AMAZONAS, MATO GROSSO, MATO GROSSO DO SUL |
| foco_id | 7 | 100,00% | string (7) | 163f85ea-2a1e-3c9b-b2ab-6d8cb5918061, 89b4df9b-89a9-3ec8-8d34-1b005ee91c33, 30f9a863-0b18-39be-ba1f-b0a6ad6903ca, 70436548-1295-39e7-aa70-39b69c445394 |
| id_bdq | 7 | 100,00% | int (7) | 1407449197, 1407449196, 1407449195, 1407449194 |
| lat | 7 | 100,00% | float (7) | -3.602000, -3.630000, -3.631000, -3.642000 |
| lon | 7 | 100,00% | float (7) | -49.963000, -52.255000, -52.264000, -52.265000 |
| municipio | 7 | 100,00% | string (7) | PACAJÁ, ALTAMIRA, NOVA OLINDA DO NORTE, PORTO DE MOZ |
| pais | 7 | 100,00% | categorical (7) | Brasil |

## Arquivos divergentes em relação ao esquema modal

Nenhum arquivo divergente.

## Observações para construção de reports

- A coluna temporal detectada deve ser priorizada na configuração do report quando houver estabilidade suficiente entre os anos.
- A coluna geográfica detectada para UF/estado pode ser usada para tabelas comparativas e agregações regionais.
- O arquivo `summary.json` gerado junto deste Markdown pode ser consumido por utilitários internos para preparar builders de reports.
- Esta auditoria usa leitura de cabeçalho, contagem de linhas e amostragem de dados; ela não substitui validação semântica integral da base.
