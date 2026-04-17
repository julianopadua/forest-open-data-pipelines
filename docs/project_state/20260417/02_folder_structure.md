# Estrutura de pastas

## Árvore resumida

```
forest-open-data-pipelines/
├── .github/workflows/       # CI (ex.: weekly_sync.yml)
├── configs/
│   ├── app.yml              # paths, supabase env key name, LLM defaults
│   ├── datasets/            # YAML por dataset (subpastas cvm/, eia/, inpe/, inmet/ + raiz)
│   └── reports/             # YAML por report
├── docs/                    # auditorias geradas, INDEX, docs/src/, project_state/…
├── scripts/                 # backfill e exemplos shell
├── src/forest_pipelines/    # pacote Python
├── tests/                   # pytest
├── data/                    # cache local (tipicamente gitignored)
├── logs/                    # logs (tipicamente gitignored)
├── pyproject.toml
├── README.md
└── .env.example
```

## Responsabilidades

| Pasta / arquivo | Função |
| --- | --- |
| `src/forest_pipelines/` | Implementação: CLI, datasets, reports, storage, LLM, audits, utils. |
| `configs/datasets/` | Parâmetros por fonte (`bucket_prefix`, `latest_months`, URLs, etc.). Nomes refletem **caminho relativo** ao carregar (ex.: `cvm/fi_inf_diario.yml`). |
| `configs/reports/` | Metadados e colunas esperadas para relatórios. |
| `configs/app.yml` | Diretórios globais e referência ao nome da env do bucket. |
| `docs/` | Saída humana (auditorias), notas `docs/src/`, e esta documentação de estado. |
| `scripts/` | Automação ad hoc que **duplica** parcialmente a lógica do CLI (ex.: backfill). |
| `tests/` | Testes unitários pontuais (parsers notícias). |

## Observações de organização

- **Boa separação** entre código (`src/`) e config (`configs/`).
- **`docs/src/`** espelha módulos, mas o README admite que subpacotes (EIA, INPE, reports, LLM) podem não ter notas — lacuna documentada no próprio README.

## Pastas confusas ou nomes

- **`logging_.py`** (underscore) evita colisão com stdlib `logging` — correto, porém fácil de esquecer em imports.
- **Duplo esquema de ID:** registry `cvm_fi_inf_diario` vs arquivo `configs/datasets/cvm/fi_inf_diario.yml` — exige tabela de mapeamento (ver `04_frontend.md`).

## Pastas desnecessárias

Nenhuma removida nesta análise; `.venv/` se versionado acidentalmente polui o git (fora do escopo de limpeza neste entregável).
