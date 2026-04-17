# Configurações

## `configs/app.yml`

- **`app`:** `data_dir`, `logs_dir`, `docs_dir` (relativos ao `root` derivado do config).
- **`supabase.bucket_open_data_env`:** nome da **variável de ambiente** cujo valor é o bucket (não o nome do bucket em si).
- **`datasets_dir`:** default `configs/datasets`.
- **`reports_dir`:** default `configs/reports`.
- **`llm`:** defaults para Groq (temperatura, tokens, `preferred_models`, `reasoning_effort`, etc.).

## YAMLs de dataset (`configs/datasets/`)

- Organização por subpastas (`cvm/`, `eia/`, …) mais `noticias_agricolas_news.yml` na raiz.
- Campos variam por fonte; comum: `id`, `title`, `bucket_prefix`, parâmetros de janela temporal.

## YAMLs de report (`configs/reports/`)

- Ex.: `bdqueimadas_overview.yml` define `bucket_prefix`, colunas candidatas, limites de UI, bloco `llm.enabled`.

## Build / empacotamento

- **`pyproject.toml`:** `setuptools`, `package-dir = src`.
- Sem **Dockerfile** no repositório (análise por glob anterior).

## Lint / format

- **Não há** `ruff`, `flake8`, `black`, `mypy` configurados no `pyproject.toml` observado.

## Conflitos ou inconsistências

- Primeira linha de alguns YAMLs pode conter comentário com caminho “antigo” ou alternativo — confiar no path real do arquivo no disco.
- `FP_PUBLIC_BASE_URL` em [`.env.example`](../../../.env.example) **não** aparece no código-fonte (`src/`) — inconsistência documental.
