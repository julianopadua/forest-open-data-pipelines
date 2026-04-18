# Fluxo LLM e carrossel BDQueimadas (social)

## Comando fim a fim (gráficos + LLM)

Na raiz do repositório, com `pip install -e .`, dados em `data/inpe_bdqueimadas` e `GROQ_API_KEY` no `.env`:

```bash
make bdqueimadas-social-full
```

Equivale a `python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm`. Alternativa: `scripts/bdqueimadas-social-full.sh` (valida `GROQ_API_KEY` antes de rodar).

Para **só** gráficos e manifest sem chamadas à API (sem key): `make bdqueimadas-social-assets`.

Para ver o compositor no browser: `cd apps/social-post-templates && npm run dev` e abrir o preset BDQueimadas (`/green/composer.html?preset=bdqueimadas`). O pipeline só grava arquivos em `public/`; não sobe servidor HTTP.

## Visão geral

O comando `python -m forest_pipelines.social` gera **quatro recortes** fixos de série temporal, na ordem:

1. **Nacional** (território Brasil, agregação `monthly_all` dos CSVs anuais em `anual/`).
2. **Amazônia**, **Cerrado**, **Pantanal** — séries obtidas da agregação `monthly_by_biome` dos mesmos CSVs, filtrando pela chave de bioma INPE (`AMAZÔNIA`, `CERRADO`, `PANTANAL`).

Para cada recorte, o ano civil corrente usa os **arquivos mensais** (`focos_mensal_br_YYYYMM.*`) em cache, com contagem de linhas filtrada por bioma quando aplicável (mesma lógica de colunas datetime/estado/bioma/satélite que o agregado nacional).

O **último mês civil fechado** segue a regra `last_closed_month` (ex.: em 18 de abril → até março).

## Saídas de gráfico

Para cada recorte, são gravados:

- `bdqueimadas-chart-<slug>.png` e `chart_spec-<slug>.json` em `apps/social-post-templates/public/generated/`, com `<slug>` ∈ `nacional`, `amazonia`, `cerrado`, `pantanal`.
- Cópias **nacionais** canônicas: `bdqueimadas-chart.png` e `chart_spec.json` (equivalentes ao slide Nacional).

## Manifest (6 slides)

Com `--emit-manifest`, o JSON do compositor tem **6 entradas** em `slides`:

| Índice | Tipo        | Conteúdo |
|--------|------------|----------|
| 0      | `cover`    | Capa estática (título/sumário do carrossel). |
| 1–4    | `body_chart` | Nacional + três biomas; `image_url` aponta para o PNG correspondente; `body_text` = texto LLM do slide ou mensagem de fallback se falha. |
| 5      | `cta`      | Encerramento estático. |

Cada `body_chart` pode incluir `generation: { ok, error }` em falhas de dados ou de LLM. O campo opcional `instagram_caption_draft` guarda a legenda única gerada quando `--llm` está ativo.

## LLM

- **Uma legenda Instagram** (`carousel_post_description`): chamada única, prompt curto, sem números por slide; prefixo `[YYYY-MM-DD]` obrigatório na primeira linha.
- **Quatro textos de slide** (`graphic_text`): um por recorte, com payload analítico `focos_incendio_br_v3` (inclui `metadata.bioma`) e tom **País** (Nacional) vs **regional** (biomas).

Arquivo agregado: `public/generated/social_llm.json` (`schema_version: 2`) com `post_description`, `post_description_model` e lista `scopes` (paths, textos, modelos, erros).

## Logs

Em `logs/social/bdqueimadas/<ano>/<mês>/<dia>.log`: estágios `carousel_scope_start`, `carousel_scope_ok`, `carousel_scope_failed`, e `llm_roundtrip` com campo `scope` (ex.: `carousel`, `nacional`, `amazonia`).

## Dependência de dados

Se `monthly_by_biome` vier vazio para um bioma após o filtro, o recorte falha de forma **não fatal**: o pipeline continua, o erro é logado e o manifest/sidecar registram a falha.
