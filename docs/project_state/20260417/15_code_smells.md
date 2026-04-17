# Code smells

Evidências com caminhos relativos ao repositório.

## Arquivos grandes / funções longas

| Arquivo | Observação |
| --- | --- |
| `src/forest_pipelines/reports/builders/bdqueimadas_overview.py` | Arquivo muito longo (~900+ linhas); múltiplas responsabilidades (I/O, agregação, LLM). |
| `src/forest_pipelines/datasets/noticias_agricolas/sync.py` | Orquestra scraping, merge, validação, upload — complexidade elevada. |

## Duplicação

- Lógica de upload de manifest duplicada entre `cli.sync` e `scripts/backfill_cvm_inf_diario.py`.
- Padrões `load_dataset_cfg` repetidos em vários módulos CVM/EIA (aceitável, porém boilerplate).

## Nomes

- `logging_.py`: sufixo underscore é idiomático para evitar sombra, mas incomum para novos devs.

## Acoplamento

- Forte dependência de **Supabase** concreta em CLI e relatórios — dificulta testes sem monkeypatch.

## Baixa coesão

- `bdqueimadas_overview.py` mistura configuração de colunas, seleção de arquivos, agregação e publicação conceitual.

## Legibilidade

- Uso de `except Exception` amplo em storage — facilita robustez, mascara bugs; comentário `noqa: BLE001` indica decisão consciente.

## Comentários úteis

- `cli.py` e `fi_inf_diario.py` contêm comentários sobre não vazar secrets — positivo.
