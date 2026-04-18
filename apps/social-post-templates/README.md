# social-post-templates

Mini front para design e preview de templates de post (Instagram 1080 × 1350 px).  
Stack: **Vite + Tailwind CSS v4**.

## Rodar localmente

```bash
cd apps/social-post-templates
npm install
npm run dev
```

Abrir no browser:

| URL | Descrição |
|-----|-----------|
| `http://localhost:5173/` | Seletor de temas |
| `http://localhost:5173/green/index.html` | Hub do tema verde (links para slides + compositor) |
| `http://localhost:5173/green/composer.html` | Compositor: montar sequência e gerar `manifest.json` |
| `http://localhost:5173/green/composer.html?preset=bdqueimadas` | Compositor com deck de exemplo (BDQueimadas) já carregado |
| `http://localhost:5173/navy/index.html` | Tema Azul Marinho (Ocean) |
| `http://localhost:5173/white/index.html` | Tema Branco (Clean) |

O canvas (1080 × 1350) é escalado automaticamente para caber na janela — o layout interno permanece em pixels reais.

## Tema verde modular

Slides atômicos em `green/slides/`:

| Arquivo | `type` no manifest | Conteúdo |
|---------|-------------------|----------|
| `cover.html` | `cover` | Capa (título + resumo) |
| `body-image-text.html` | `body_image_text` | Imagem + texto; `imageSide`: `left` ou `right` |
| `body-chart.html` | `body_chart` | Gráfico grande + texto (legenda só no PNG do gráfico; `image_url` em `/public/generated/…`) |
| `body-text.html` | `body_text` | Só texto; `columns`: `1` ou `2` |
| `cta.html` | `cta` | Encerramento com CTA e URL |

Tokens compartilhados: [`src/green/theme.css`](src/green/theme.css) (importa [`src/chrome/chrome-ui.css`](src/chrome/chrome-ui.css)). Preenchimento por query string ou por `window.applySlots` (export).

**Tamanhos do canto (tópico, data, número da página, altura do logo):** editáveis no [compositor](green/composer.html) (painel “Metadados · tamanhos”), refletidos no preview e no export via objeto `sizes` no manifest ou query `?chrome_topic=&chrome_date=&chrome_page=&chrome_logo=` (valores em px). Implementação: [`src/chrome/sizes.js`](src/chrome/sizes.js).

Exemplos de manifest: [`examples/green-manifest.example.json`](examples/green-manifest.example.json), [`examples/bdqueimadas-social.manifest.json`](examples/bdqueimadas-social.manifest.json) (pipeline BDQueimadas; cópia servida ao preset em [`public/examples/bdqueimadas-social.manifest.json`](public/examples/bdqueimadas-social.manifest.json)).

### Schema do manifest

- `theme`: `"green"`
- `runId`: nome da pasta de saída em `dist-exports/green/<runId>/`
- `sizes` (opcional): `{ "topicTagPx", "datePx", "pageNumberPx", "logoHeightPx" }` — números em pixels
- `slides`: array ordenado; cada item tem:
  - `type`: `cover` | `body_image_text` | `body_chart` | `body_text` | `cta`
  - `slots`: objeto string → string (conteúdo dos `data-slot`)
  - `imageSide` (opcional, para `body_image_text`): `"left"` | `"right"`
  - `columns` (opcional, para `body_text`): `1` | `2`

O campo `card_number` é preenchido automaticamente na exportação (`01 / N`, …).

## Logo (rodapé)

Os templates usam imagens em [`public/images/logos/`](public/images/logos/):

- Fundos escuros (verde, azul): `002-wbig-logo.png`
- Tema branco: `002-big-logo.png`

O canto inferior esquerdo mostra só o logo (sem texto “ForestLab” ao lado).

## Estrutura geral

```
green/index.html          ← hub do verde
green/composer.html       ← UI do deck + JSON
green/slides/*.html       ← slides atômicos
src/green/theme.css       ← tokens do verde
src/green/slots.js        ← applySlots + query string
src/chrome/sizes.js       ← tamanhos do chrome (preview + PNG)
navy/index.html
white/index.html
src/style.css             ← Tailwind + canvas / grid
```

Comentários `<!-- slot: name -->` marcam áreas lógicas; elementos editáveis usam `data-slot="name"`.

## Slots por slide

**Capa (`cover`):** `topic_tag`, `published_at`, `series_label`, `title`, `summary`, `card_number`

**Imagem + texto:** `topic_tag`, `published_at`, `caption`, `body_text`, `image_url`, `card_number`

**Gráfico + texto (`body_chart`):** `topic_tag`, `published_at`, `caption`, `image_url`, `body_text`, `card_number`

**Só texto:** `topic_tag`, `published_at`, `text_col_1`, `text_col_2`, `card_number` (com uma coluna, `text_col_2` fica oculto)

**CTA:** `topic_tag`, `published_at`, `cta_kicker`, `cta_headline`, `cta_subline`, `cta_url`, `card_number`

## Exportar PNGs (Playwright)

Instalação única do Chromium para o Playwright:

```bash
npx playwright install chromium
```

Com o servidor de desenvolvimento em execução (`npm run dev` em outro terminal):

**Legado** — um PNG por tema (`green` usa a capa verde; navy/white usam as páginas principais):

```bash
npm run export
```

Saída: `dist-exports/green/card.png`, `dist-exports/navy/card.png`, `dist-exports/white/card.png`.

**Manifest** — sequência de slides verdes:

```bash
npm run export:manifest -- examples/green-manifest.example.json
```

Ou:

```bash
MANIFEST=examples/green-manifest.example.json npm run export:manifest
```

Saída: `dist-exports/green/<runId>/01-cover.png`, `02-body_image_text.png`, … (pasta `dist-exports/` está no `.gitignore`).

## Pipeline BDQueimadas (carrossel 6 slides + biomas)

Na **raiz do repositório** `forest-open-data-pipelines`, com venv ativo e `pip install -e .`:

- **ZIPs anuais** `focos_br_ref_*.zip` em `data/inpe_bdqueimadas/` (para o ano anterior e a média de 5 anos por mês, ex. 2021–2025).
- **Arquivos mensais** do ano civil em curso: baixados automaticamente do [listagem INPE mensal Brasil](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/) (`focos_mensal_br_YYYYMM.csv` ou `.zip`) para `data/inpe_bdqueimadas/mensal/` (cache reutilizável).

A **linha do ano atual** no gráfico e nas estatísticas usa **apenas meses civis já encerrados**: o último ponto é o mês anterior ao mês civil corrente (ex.: em 18 de abril, até março; em 1º de maio, até abril). Não entra o mês em curso. A data de referência do comando (`--as-of`, default: hoje) define esse recorte.

O pipeline gera **quatro recortes** na ordem fixa: **Nacional**, **Amazônia**, **Cerrado**, **Pantanal**. Para cada um há um PNG e um `chart_spec` com sufixo estável (`bdqueimadas-chart-nacional.png`, `…-amazonia.png`, etc.). Os arquivos **sem sufixo** (`bdqueimadas-chart.png`, `chart_spec.json`) são cópias do recorte **nacional**.

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest
```

Opções úteis: `--current-year 2026` (default: ano do sistema), `--skip-mensal-download` (só cache já baixado), `--mensal-base-url` e `--mensal-cache-dir`.

Com `--emit-manifest`, o JSON inclui **6 slides**: capa, quatro `body_chart` (um por recorte), CTA. Cada `body_chart` pode trazer `generation: { ok, error }` se dados ou LLM falharem para aquele escopo (o pipeline continua). Opcionalmente `instagram_caption_draft` e `generation_errors` no topo.

Descrição do fluxo (escopos, LLM, manifest): [`docs/social_llm_flow.md`](../../docs/social_llm_flow.md) na raiz do repositório.

### Logs (`python -m forest_pipelines.social`)

Cada execução grava em disco (pasta **`logs/` na raiz do repositório** por padrão, alinhada a `configs/app.yml` → `app.logs_dir`) um arquivo por dia:

- Caminho: `logs/social/bdqueimadas/<ano>/<mês>/<YYYY-MM-DD>.log`
- O mesmo fluxo também aparece no **stdout** (console).

Formato: linhas de texto com um **objeto JSON** por evento:

| `event` | Conteúdo |
|--------|----------|
| `stage` | Etapas do pipeline (`pipeline_start`, `extract_anual_*`, `load_monthly_all_df_done`, `mensal_files_ready`, iteração por escopo com `carousel_scope_start` / `carousel_scope_ok` / `carousel_scope_failed`, `chart_spec_computed`, `render_chart_png_done`, `chart_spec_json_written`, `llm_run_start` e estágios LLM, `social_llm_json_written`, `manifest_written` / `manifest_skipped`, `plot_sources_metadata_written`, `pipeline_done`). |
| `llm_roundtrip` | Só com `--llm`: prompt/resposta; a legenda única do carrossel usa `component` `carousel_post_description` e `scope` `carousel`; cada texto de slide usa `graphic_text` e `scope` com o slug do recorte (`nacional`, `amazonia`, …). |

Opção de CLI: `--logs-dir <pasta>` altera só o diretório base (o subcaminho `social/bdqueimadas/<ano>/<mês>/` continua igual).

### Textos com LLM (Groq)

Na **raiz do repositório**, configure `GROQ_API_KEY` no arquivo **`.env`** (variável lida via `configs/app.yml` → `llm.api_key_env`). O comando `python -m forest_pipelines.social` carrega esse `.env` automaticamente.

Com rede e key válida:

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm
```

- **`post_description`** — uma única legenda Instagram para o carrossel inteiro (prompt dedicado; não detalha números por slide).
- **`graphic_text`** — quatro chamadas, uma por recorte, com payload analítico já filtrado (`focos_incendio_br_v3`).

Opções: `--as-of YYYY-MM-DD` (define o último mês fechado no gráfico — ex. abril → até março — e o prefixo `[YYYY-MM-DD]` na legenda LLM), `--app-config` (default `<repo>/configs/app.yml`), `--out-social-llm` (default `public/generated/social_llm.json`), `--llm-components post_description,graphic_text` (default: ambos).

Para rodar **só a legenda do carrossel**:

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --llm --llm-components post_description
```

Para rodar **só os textos dos slides** (quatro escopos):

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm --llm-components graphic_text
```

O arquivo `social_llm.json` usa **`schema_version: 2`**: `post_description`, `post_description_model`, lista `scopes` (paths, `graphic_text`, modelos, erros por slug). Versões antigas podem ter `schema_version: 1` com um único par legenda + texto.

Saídas extras:

- `public/generated/social_llm.json` — legenda + entradas por escopo (pt-BR).
- Com `--emit-manifest`, cada `body_chart` recebe o texto correspondente em `slots.body_text` (ou mensagem de fallback).

Ou:

```bash
make bdqueimadas-social-assets
```

Isso gera:

- `public/generated/bdqueimadas-chart-<slug>.png` e `chart_spec-<slug>.json` para cada recorte; cópias nacionais `bdqueimadas-chart.png` e `chart_spec.json`.
- `examples/bdqueimadas-social.manifest.json` e cópia em `public/examples/` para o preset `?preset=bdqueimadas` no compositor.

Depois: `npm run dev` neste app e, em outro terminal, `npm run export:manifest -- examples/bdqueimadas-social.manifest.json`.

A legenda para colar no Instagram está em `social_llm.json` → `post_description` quando você roda o pipeline com `--llm` e inclui `post_description` em `--llm-components`.
