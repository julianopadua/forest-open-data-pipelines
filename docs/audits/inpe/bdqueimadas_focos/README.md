# Auditoria de esquema do BDQueimadas

## Escopo

Esta auditoria inspeciona localmente os arquivos ZIP anuais do BDQueimadas, identificando estrutura de colunas, colunas temporais e geográficas detectáveis, tipos inferidos a partir de amostras e diferenças de esquema entre anos.

## Metadados

| dataset_id | generated_at | files_total | base_dir | output_dir |
| --- | --- | --- | --- | --- |
| inpe_bdqueimadas_focos | 2026-03-14T21:24:46.360092Z | 23 | D:\Projetos\forest-open-data-pipelines\data\inpe_bdqueimadas | D:\Projetos\forest-open-data-pipelines\docs\audits\inpe\bdqueimadas_focos |

## Achados principais

- Foram auditados 23 arquivos ZIP do BDQueimadas localizados em `D:\Projetos\forest-open-data-pipelines\data\inpe_bdqueimadas`.
- O esquema modal apareceu em 23 arquivo(s).
- A interseção de colunas entre todos os arquivos contém 9 coluna(s).
- A união de colunas observadas contém 9 coluna(s).
- Detecção de coluna temporal: data_pas (23)
- Detecção de coluna de UF/estado: estado (23)
- Nenhum arquivo divergiu do esquema modal observado.

## Inventário por arquivo

| year | zip_name | row_count | column_count | datetime_column_detected | state_column_detected |
| --- | --- | --- | --- | --- | --- |
| 2003 | focos_br_ref_2003.zip | 341.237 | 9 | data_pas | estado |
| 2004 | focos_br_ref_2004.zip | 380.445 | 9 | data_pas | estado |
| 2005 | focos_br_ref_2005.zip | 362.563 | 9 | data_pas | estado |
| 2006 | focos_br_ref_2006.zip | 249.179 | 9 | data_pas | estado |
| 2007 | focos_br_ref_2007.zip | 393.915 | 9 | data_pas | estado |
| 2008 | focos_br_ref_2008.zip | 211.933 | 9 | data_pas | estado |
| 2009 | focos_br_ref_2009.zip | 155.102 | 9 | data_pas | estado |
| 2010 | focos_br_ref_2010.zip | 319.383 | 9 | data_pas | estado |
| 2011 | focos_br_ref_2011.zip | 158.099 | 9 | data_pas | estado |
| 2012 | focos_br_ref_2012.zip | 217.234 | 9 | data_pas | estado |
| 2013 | focos_br_ref_2013.zip | 128.145 | 9 | data_pas | estado |
| 2014 | focos_br_ref_2014.zip | 175.892 | 9 | data_pas | estado |
| 2015 | focos_br_ref_2015.zip | 216.778 | 9 | data_pas | estado |
| 2016 | focos_br_ref_2016.zip | 184.217 | 9 | data_pas | estado |
| 2017 | focos_br_ref_2017.zip | 207.508 | 9 | data_pas | estado |
| 2018 | focos_br_ref_2018.zip | 132.870 | 9 | data_pas | estado |
| 2019 | focos_br_ref_2019.zip | 197.632 | 9 | data_pas | estado |
| 2020 | focos_br_ref_2020.zip | 222.797 | 9 | data_pas | estado |
| 2021 | focos_br_ref_2021.zip | 184.081 | 9 | data_pas | estado |
| 2022 | focos_br_ref_2022.zip | 200.763 | 9 | data_pas | estado |
| 2023 | focos_br_ref_2023.zip | 189.901 | 9 | data_pas | estado |
| 2024 | focos_br_ref_2024.zip | 278.299 | 9 | data_pas | estado |
| 2025 | focos_br_ref_2025.zip | 136.393 | 9 | data_pas | estado |

## Esquema modal

O esquema modal apareceu em **23** arquivo(s) e possui **9** coluna(s).

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
| bioma | 23 | 100,00% | string (23) | Cerrado, Pantanal, Mata Atlântica, Pampa |
| data_pas | 23 | 100,00% | datetime (23) | 2003-05-15 17:05:00, 2003-05-15 17:04:00, 2003-05-15 17:03:00, 2003-05-15 17:02:00 |
| estado | 23 | 100,00% | string (23) | MATO GROSSO DO SUL, GOIÁS, MINAS GERAIS, PARANÁ |
| foco_id | 23 | 100,00% | string (23) | 5e864494-5abb-11e8-911c-28924ad12c5c, 5e864493-5abb-11e8-911c-28924ad12c5c, 5e864492-5abb-11e8-911c-28924ad12c5c, 5e864491-5abb-11e8-911c-28924ad12c5c |
| id_bdq | 23 | 100,00% | int (23) | 9988885, 9988884, 9988882, 9988881 |
| lat | 23 | 100,00% | float (23) | -18.518000, -17.542000, -17.612000, -18.386000 |
| lon | 23 | 100,00% | float (23) | -55.028000, -48.815000, -47.160000, -51.919000 |
| municipio | 23 | 100,00% | string (23) | RIO VERDE DE MATO GROSSO, PIRACANJUBA, GUARDA-MOR, SERRANÓPOLIS |
| pais | 23 | 100,00% | categorical (23) | Brasil |

## Arquivos divergentes em relação ao esquema modal

Nenhum arquivo divergente.

## Observações para construção de reports

- A coluna temporal detectada deve ser priorizada na configuração do report quando houver estabilidade suficiente entre os anos.
- A coluna geográfica detectada para UF/estado pode ser usada para tabelas comparativas e agregações regionais.
- O arquivo `summary.json` gerado junto deste Markdown pode ser consumido por utilitários internos para preparar builders de reports.
- Esta auditoria usa leitura de cabeçalho, contagem de linhas e amostragem de dados; ela não substitui validação semântica integral da base.
