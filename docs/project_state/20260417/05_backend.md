# Backend

## Escopo

Não há API HTTP neste repositório. “Backend” aqui significa **lógica de ingestão**, **cliente Storage** e **publicação de artefatos**.

## Componentes principais

### Supabase Storage (`storage/supabase_storage.py`)

- `from_env(logger, bucket_open_data)`: lê `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` do ambiente; bucket nomeado pela env configurada em `app.yml` (valor default string `"open-data"` em `settings` se ausente).
- `create_client` com service role — operações com privilégio elevado ([Supabase API keys](https://supabase.com/docs/guides/api/api-keys)).
- Uploads: `upload_file`, `upload_bytes` com até 3 tentativas e sleep linear.
- `public_url`: monta URL pública `.../storage/v1/object/public/<bucket>/<path>`.

### Dataset runners

- Um módulo por família (CVM, EIA, …); função `sync(settings, storage, logger, latest_months=None)` retorna dict compatível com manifest.
- Downloads: [`http.py`](../../../src/forest_pipelines/http.py) (`stream_download`) onde aplicável.

### Relatórios (`reports/builders/bdqueimadas_overview.py`)

- Carrega ZIPs locais conforme `configs/reports/bdqueimadas_overview.yml`.
- `build_incremental_year_caches` interage com storage para cache por ano.
- Opcionalmente chama LLM via `maybe_generate_analysis_blocks` (`reports/llm/`).

### LLM (`llm/router.py`, `llm/groq_client.py`)

- Provider suportado na prática: **Groq** (`llm_settings.provider != "groq"` → `NotImplementedError`).
- Chave: `os.getenv(llm_settings.api_key_env)` — default `GROQ_API_KEY` via `configs/app.yml`.

### Auditorias

- `audits/inpe/bdqueimadas_focos.py`: lê ZIPs locais, não chama Supabase.

## Autenticação / autorização

- Não há JWT de usuário; autenticação é **chave de serviço** para Storage.
- Autorização na origem (CKAN, sites públicos) é “acesso anônimo” conforme cada fonte.

## Fluxo de dados (resumo)

1. **Sync:** download → hash/size → upload objeto → lista `items` no manifest.
2. **Report:** ZIP → agregação pandas → JSON → upload múltiplos paths + manifest.
3. **Audit:** ZIP → estatísticas → arquivos em `docs/`.

## Gargalos e limites

| Área | Problema | Impacto | Alternativas (resumo) |
| --- | --- | --- | --- |
| pandas em relatório | Leitura de múltiplos ZIPs grandes | Memória alta em runner pequeno | Processar por chunk; DuckDB local; VM maior |
| Upload sequencial | Muitos arquivos | Tempo total de sync longo | Paralelizar uploads (cuidado com rate limit); multipart se API suportar |
| Groq | Custo e rate limit por API key | Falha intermitente ou custo | Cache de blocos LLM; desabilitar `llm.enabled`; modelo menor |
| Service role | Superfície de abuso se vazada | Comprometimento do projeto Supabase | Secrets manager; runner isolado; signed URLs com backend mínimo |

## Inconsistências

- Comentário em `fi_inf_diario.sync` menciona não vazar service role via repr — `SupabaseStorage` usa `repr=False` em `service_role_key` (bom).
