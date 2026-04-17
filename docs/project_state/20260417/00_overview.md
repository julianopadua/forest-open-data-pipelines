# Visão geral do estado do projeto (2026-04-17)

## Resumo executivo

**forest-open-data-pipelines** é um pacote Python (CLI Typer) que ingere datasets abertos (CVM, EIA, INPE, INMET, feed de notícias), espelha artefatos no **Supabase Storage** (bucket configurável, tipicamente público para leitura) e publica **manifests JSON** consumíveis por um frontend estático (ex.: `forest-portal`). O repositório isola dependências Python, caches locais e automação (cron / GitHub Actions) do deploy do site.

## Propósito

- Sincronizar datasets registrados, fazer upload de arquivos e `manifest.json` por prefixo no bucket.
- Construir **relatórios** agregados (hoje: `bdqueimadas_overview`) com opcional **LLM (Groq)** para blocos de análise.
- Executar **auditorias** offline sobre dados locais e gerar Markdown/JSON em `docs/`.

## Maturidade

- **Pontos fortes:** README detalhado, CLI clara, registries explícitos (`registry/datasets.py`, `reports/registry/reports.py`, `audits/registry.py`), retries em uploads Storage, documentação modular em `docs/src/` para parte do código.
- **Pontos fracos:** cobertura de testes muito restrita; sem lockfile de dependências; CI com um único workflow semanal; uso de **service role** no pipeline (adequado ao servidor, porém privilegiado); variável `FP_PUBLIC_BASE_URL` em `.env.example` sem uso no código (candidata a remoção ou implementação).

## Módulos principais

| Área | Caminho |
| --- | --- |
| CLI | `src/forest_pipelines/cli.py` |
| Config | `configs/app.yml`, `src/forest_pipelines/settings.py` |
| Datasets | `src/forest_pipelines/datasets/` + `configs/datasets/**/*.yml` |
| Storage | `src/forest_pipelines/storage/supabase_storage.py` |
| Relatórios | `src/forest_pipelines/reports/` |
| LLM | `src/forest_pipelines/llm/` |
| Auditorias | `src/forest_pipelines/audits/` |

## Principais riscos

1. **Chave service role** em CI e ambientes locais: bypass de RLS se usada fora de contexto confiável ([documentação Supabase sobre API keys](https://supabase.com/docs/guides/api/api-keys)).
2. **Bucket público:** URLs previsíveis; dados já são “open data”, mas manifests podem expor metadados e URLs de origem.
3. **Escalabilidade:** jobs únicos, processamento majoritariamente em memória (pandas) em relatórios; sem fila de trabalhos nem paralelismo entre datasets no CLI.
4. **Operação:** registry ID do dataset (ex.: `cvm_fi_inf_diario`) **não** é o mesmo caminho do YAML (ex.: `cvm/fi_inf_diario.yml`); a correspondência está **hardcoded** em cada runner — risco de confusão operacional.

## Prioridades futuras (alto nível)

1. Documentação operacional e testes para fluxos críticos (sync + report).
2. Reduzir superfície da service role (signed upload, Edge Function, ou runner dedicado) onde fizer sentido.
3. Lockfile (`uv.lock` / `pip-tools`) e pin de versões para builds reproduzíveis.

## Como executar (resumo)

Instalação: `pip install -e .` na raiz; entrypoint: `forest-pipelines`.

Comandos principais (detalhes em `01_architecture.md` e `04_frontend.md`):

```bash
forest-pipelines sync <dataset_id> [--config-path PATH] [--latest-months N]
forest-pipelines build-report <report_id> [--config-path PATH]
forest-pipelines audit-dataset <dataset_id> [--config-path PATH]
```

Pré-requisitos de ambiente: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, bucket via `SUPABASE_BUCKET_OPEN_DATA` (nome da env em `configs/app.yml`); relatórios com LLM: `GROQ_API_KEY`.
