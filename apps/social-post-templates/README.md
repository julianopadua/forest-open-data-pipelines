# social-post-templates

Mini front para design e preview de templates de post (Instagram 1080 × 1350 px).  
Stack: **Vite + Tailwind CSS v4**.

## Rodar localmente

```bash
cd apps/social-post-templates
npm install
npm run dev
```

openAbrir no browser:


| URL                                                                | Descrição                                                              |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `http://localhost:5173/`                                           | Seletor com os quatro temas (Verde · Vermelho · Branco · Azul Marinho) |
| `http://localhost:5173/green/index.html`                           | Hub do tema verde (links para slides + compositor)                     |
| `http://localhost:5173/green/composer.html`                        | Compositor do tema verde                                               |
| `http://localhost:5173/red/index.html`                             | Hub do tema vermelho — host atual do deck BDQueimadas                  |
| `http://localhost:5173/red/composer.html?preset=bdqueimadas`       | Compositor com deck BDQueimadas pré-carregado                          |
| `http://localhost:5173/white/index.html`                           | Hub do tema branco — host do deck de pesquisa                          |
| `http://localhost:5173/white/composer.html?preset=research-trends` | Compositor com deck Research Trends pré-carregado                      |
| `http://localhost:5173/navy/index.html`                            | Hub do tema azul marinho                                               |
| `http://localhost:5173/navy/composer.html?preset=anp-producao-petroleo-gas` | Compositor com deck ANP produção pré-carregado                |


### Temas


| Tema                 | Cor de destaque | Caso de uso                       | Preset automático                                                                      |
| -------------------- | --------------- | --------------------------------- | -------------------------------------------------------------------------------------- |
| Verde (Forest)       | `#2ECC9A`       | Conteúdo geral, séries de análise | —                                                                                      |
| Vermelho (Wildfire)  | `#E53E3E`       | Queimadas e focos de calor        | `preset=bdqueimadas` (Python: `python -m forest_pipelines.social`)                     |
| Branco (Research)    | `#0B7B56`       | Indicadores de pesquisa           | `preset=research-trends` (Python: `python -m forest_pipelines.social.research_trends`) |
| Azul Marinho (Ocean) | `#4A9EFF`       | Monitoramento ambiental e energia | `preset=anp-producao-petroleo-gas` (Python: `python -m forest_pipelines.social.anp_producao`) |


Todos os temas usam a mesma estrutura modular: `<theme>/{index,composer}.html` + `<theme>/slides/{cover,body-image-text,body-chart,body-text,cta}.html`. Tokens de cor em `src/<theme>/theme.css`. Scripts compartilhados em `[src/shared/](src/shared/)`.

O canvas (1080 × 1350) é escalado automaticamente para caber na janela - o layout interno permanece em pixels reais.

## Tema verde modular

Slides atômicos em `green/slides/`:


| Arquivo                | `type` no manifest | Conteúdo                                                                                    |
| ---------------------- | ------------------ | ------------------------------------------------------------------------------------------- |
| `cover.html`           | `cover`            | Capa (título + resumo)                                                                      |
| `body-image-text.html` | `body_image_text`  | Imagem + texto; `imageSide`: `left` ou `right`                                              |
| `body-chart.html`      | `body_chart`       | Gráfico grande + texto (legenda só no PNG do gráfico; `image_url` em `/public/generated/…`) |
| `body-text.html`       | `body_text`        | Só texto; `columns`: `1` ou `2`                                                             |
| `cta.html`             | `cta`              | Encerramento com CTA e URL                                                                  |


Tokens compartilhados: `[src/green/theme.css](src/green/theme.css)` (importa `[src/chrome/chrome-ui.css](src/chrome/chrome-ui.css)`). Preenchimento por query string ou por `window.applySlots` (export).

**Tamanhos do canto (tópico, data, número da página, altura do logo):** editáveis no [compositor](green/composer.html) (painel “Metadados · tamanhos”), refletidos no preview e no export via objeto `sizes` no manifest ou query `?chrome_topic=&chrome_date=&chrome_page=&chrome_logo=` (valores em px). Implementação: `[src/chrome/sizes.js](src/chrome/sizes.js)`.

Exemplos de manifest: `[examples/green-manifest.example.json](examples/green-manifest.example.json)`, `[examples/bdqueimadas-social.manifest.json](examples/bdqueimadas-social.manifest.json)` (pipeline BDQueimadas; cópia servida ao preset em `[public/examples/bdqueimadas-social.manifest.json](public/examples/bdqueimadas-social.manifest.json)`).

### Schema do manifest

- `theme`: `"green"`
- `runId`: nome da pasta de saída em `dist-exports/green/<runId>/`
- `sizes` (opcional): `{ "topicTagPx", "datePx", "pageNumberPx", "logoHeightPx" }` - números em pixels
- `slides`: array ordenado; cada item tem:
  - `type`: `cover` | `body_image_text` | `body_chart` | `body_text` | `cta`
  - `slots`: objeto string → string (conteúdo dos `data-slot`)
  - `imageSide` (opcional, para `body_image_text`): `"left"` | `"right"`
  - `columns` (opcional, para `body_text`): `1` | `2`

O campo `card_number` é preenchido automaticamente na exportação (`01 / N`, …).

## Logo (rodapé)

Os templates usam imagens em `[public/images/logos/](public/images/logos/)`:

- Fundos escuros (verde, azul): `002-wbig-logo.png`
- Tema branco: `002-big-logo.png`

O canto inferior esquerdo mostra só o logo (sem texto “ForestLab” ao lado).

## Estrutura geral

```
index.html                  ← landing page com os 4 temas
green/{index,composer}.html ← hub + compositor verde
green/slides/*.html         ← slides atômicos
red/{index,composer}.html   ← hub + compositor vermelho (host BDQueimadas)
red/slides/*.html
white/{index,composer}.html ← hub + compositor branco (host research-trends)
white/slides/*.html
navy/{index,composer}.html  ← hub + compositor azul marinho
navy/slides/*.html
src/<theme>/theme.css       ← tokens do tema (4 arquivos)
src/shared/slots.js         ← applySlots + query string (compartilhado)
src/shared/fit-canvas.js    ← scaling responsivo (compartilhado)
src/shared/composerZipExport.js ← ZIP export client-side (compartilhado)
src/chrome/sizes.js         ← tamanhos do chrome (preview + PNG)
src/style.css               ← Tailwind + canvas / grid
```

Comentários `<!-- slot: name -->` marcam áreas lógicas; elementos editáveis usam `data-slot="name"`.

## Slots por slide

**Capa (`cover`):** `topic_tag`, `published_at`, `series_label`, `title`, `summary`, `card_number`

**Imagem + texto:** `topic_tag`, `published_at`, `caption`, `body_text`, `image_url`, `card_number`

**Gráfico + texto (`body_chart`):** `topic_tag`, `published_at`, `caption`, `image_url`, `body_text`, `card_number`

**Só texto:** `topic_tag`, `published_at`, `text_col_1`, `text_col_2`, `card_number` (com uma coluna, `text_col_2` fica oculto)

**CTA:** `topic_tag`, `published_at`, `cta_kicker`, `cta_headline`, `cta_subline`, `cta_url`, `card_number`

## Controles padronizados do compositor

Todos os compositores de tema (`green`, `red`, `white` e `navy`) devem expor a mesma base de edição para qualquer preset automático:

- Campo de tamanho do chrome via `sizes`: `topicTagPx`, `datePx`, `pageNumberPx` e `logoHeightPx`.
- Checkbox de visibilidade por slot, serializado como `hiddenSlots` no slide e propagado ao preview como `hide=...`.
- Painel `Ajustar estilo` por slot, serializado como `slotStyles` no slide e propagado ao preview como `style=<JSON>`.
- Exportação normal em ZIP e exportação sem texto para Canva via `blank=1`.

Pipelines sociais devem preservar esses campos quando carregam, editam ou regeneram manifests. Um preset novo só deve entrar em `index.html` quando seu compositor de tema conseguir carregar `slotStyles` e `hiddenSlots` sem perder informação.

## Tamanhos default de texto e logo

Dois níveis de configuração de tamanho convivem nos slides:

**1. Chrome metadata (tópico, data, número da página, altura do logo).** Ajuste por slide no compositor (painel "Metadados · tamanhos") ou via objeto `sizes` no manifest: `{ "topicTagPx", "datePx", "pageNumberPx", "logoHeightPx" }`. Implementação em `[src/chrome/sizes.js](src/chrome/sizes.js)`; aplicado em runtime via CSS variables. Default em `DEFAULT_CHROME` (mesmo arquivo). Querystring equivalente: `?chrome_topic=24&chrome_date=26&chrome_page=24&chrome_logo=54`.

**2. Fonte e tamanho do corpo do slide (título, CTA, body_text, etc.).** Estão inline no HTML de cada slide (`<theme>/slides/*.html`), em `style="font-size: NNpx"`. Para mudar global, edite o slide HTML do tema; o mesmo `font-size` propaga via Tailwind/inline para todas as ocorrências do `data-slot`. Locais relevantes:


| Slot                          | Onde está                             | Default            |
| ----------------------------- | ------------------------------------- | ------------------ |
| `title` (cover)               | `<theme>/slides/cover.html`           | `font-size: 144px` |
| `summary` (cover)             | `<theme>/slides/cover.html`           | `font-size: 34px`  |
| `body_text` (body_chart)      | `<theme>/slides/body-chart.html`      | `font-size: 30px`  |
| `body_text` (body_image_text) | `<theme>/slides/body-image-text.html` | `font-size: 32px`  |
| `cta_kicker`                  | `<theme>/slides/cta.html`             | `font-size: 30px`  |
| `cta_headline`                | `<theme>/slides/cta.html`             | `font-size: 64px`  |
| `cta_subline`                 | `<theme>/slides/cta.html`             | `font-size: 28px`  |
| `cta_url`                     | `<theme>/slides/cta.html`             | `font-size: 36px`  |


Mudar um default num único tema: edite o `font-size` direto no HTML correspondente. Para mudar global, faça o mesmo nos quatro temas (`green/`, `red/`, `white/`, `navy/`).

**3. Paleta do tema** (background, accent, text-primary/secondary/muted): tokens em `src/<theme>/theme.css`. Trocar o accent muda automaticamente a cor do CTA kicker, URL, gráficos placeholder, bordas.

## Exportar PNGs (Playwright)

Instalação única do Chromium para o Playwright:

```bash
npx playwright install chromium
```

Com o servidor de desenvolvimento em execução (`npm run dev` em outro terminal):

**Legado** - um PNG por tema (`green` usa a capa verde; navy/white usam as páginas principais):

```bash
npm run export
```

Saída: `dist-exports/green/card.png`, `dist-exports/navy/card.png`, `dist-exports/white/card.png`.

**Manifest** - sequência de slides verdes:

```bash
npm run export:manifest -- examples/green-manifest.example.json
```

Ou:

```bash
MANIFEST=examples/green-manifest.example.json npm run export:manifest
```

Saída: `dist-exports/green/<runId>/01-cover.png`, `02-body_image_text.png`, … (pasta `dist-exports/` está no `.gitignore`).

## Pipeline ANP produção (carrossel 5 slides, tema azul)

Deck automático para `Produção de petróleo e gás natural por estado e localização`, a partir dos CSV oficiais da ANP. O pipeline baixa e cacheia os sete arquivos do pacote em `data/anp_producao_petroleo_gas/`, mas plota no MVP apenas os dois indicadores principais: produção de petróleo e produção de gás natural.

Quick start sem LLM:

```bash
make anp-producao-social-assets
```

Com textos Groq por slide:

```bash
make anp-producao-social-full
```

Saídas principais:

- `public/generated/anp-producao-national.png`
- `public/generated/anp-producao-petroleo-ufs.png`
- `public/generated/anp-producao-gas-ufs.png`
- `examples/anp-producao-petroleo-gas.manifest.json`
- `public/examples/anp-producao-petroleo-gas.manifest.json`

Preview: `cd apps/social-post-templates && npm run dev`, depois abrir `http://localhost:5173/navy/composer.html?preset=anp-producao-petroleo-gas`.

## Pipeline BDQueimadas (carrossel 6 slides + biomas, tema vermelho)

> Desde a virada para o tema vermelho, o pipeline emite `theme: "red"` no manifest e o preset é servido em `red/composer.html?preset=bdqueimadas`. Tudo abaixo continua funcionando como antes; só o tema do preview mudou.

### Quick start (gera o deck completo com textos LLM)

Pré-requisitos: `GROQ_API_KEY` no `.env` na raiz do `forest-open-data-pipelines/`. Depois, na mesma raiz:

```bash
make bdqueimadas-social-full
```

Equivale a `python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm`. Produz:

- 4 charts PNG por bioma em `apps/social-post-templates/public/generated/`
- `examples/bdqueimadas-social.manifest.json` (e cópia em `public/examples/`) com 6 slides
- `public/generated/social_llm.json` com a legenda do carrossel (`post_description`) e os quatro textos de slide (`graphic_text`)

Para gerar sem LLM (só os charts + manifest com texto placeholder):

```bash
make bdqueimadas-social-assets
```

Variantes mais granulares (só legenda, só textos por slide, recorte de data) estão documentadas em [Textos com LLM (Groq)](#textos-com-llm-groq) abaixo.

Na **raiz do repositório** `forest-open-data-pipelines`, com venv ativo e `pip install -e .`:

- **ZIPs anuais** `focos_br_ref_*.zip` em `data/inpe_bdqueimadas/` (para o ano anterior e a média de 5 anos por mês, ex. 2021–2025).
- **Arquivos mensais** do ano civil em curso: baixados automaticamente do [listagem INPE mensal Brasil](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/) (`focos_mensal_br_YYYYMM.csv` ou `.zip`) para `data/inpe_bdqueimadas/mensal/` (cache reutilizável).

A **linha do ano atual** no gráfico e nas estatísticas usa **apenas meses civis já encerrados**: o último ponto é o mês anterior ao mês civil corrente (ex.: em 18 de abril, até março; em 1º de maio, até abril). Não entra o mês em curso. A data de referência do comando (`--as-of`, default: hoje) define esse recorte.

O pipeline gera **quatro recortes** na ordem fixa: **Nacional**, **Amazônia**, **Cerrado**, **Pantanal**. Para cada um há um PNG e um `chart_spec` com sufixo estável (`bdqueimadas-chart-nacional.png`, `…-amazonia.png`, etc.). Os arquivos **sem sufixo** (`bdqueimadas-chart.png`, `chart_spec.json`) são cópias do recorte **nacional**.

**Fluxo completo** (gráficos + manifest + LLM em um passo, a partir da raiz do monorepo):

```bash
make bdqueimadas-social-full
```

Depois, para o preview no compositor: `cd apps/social-post-templates && npm run dev` e abrir `http://localhost:5173/red/composer.html?preset=bdqueimadas`. Equivale a `python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm`; também existe `[scripts/bdqueimadas-social-full.sh](../../scripts/bdqueimadas-social-full.sh)` com verificação de `GROQ_API_KEY`.

Só dados e artefatos estáticos (sem LLM / sem rede para Groq):

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest
```

Opções úteis: `--current-year 2026` (default: ano do sistema), `--skip-mensal-download` (só cache já baixado), `--mensal-base-url` e `--mensal-cache-dir`.

Com `--emit-manifest`, o JSON inclui **6 slides**: capa, quatro `body_chart` (um por recorte), CTA. Cada `body_chart` pode trazer `generation: { ok, error }` se dados ou LLM falharem para aquele escopo (o pipeline continua). Opcionalmente `instagram_caption_draft` e `generation_errors` no topo.

Descrição do fluxo (escopos, LLM, manifest): `[docs/social_llm_flow.md](../../docs/social_llm_flow.md)` na raiz do repositório.

### Logs (`python -m forest_pipelines.social`)

Cada execução grava em disco (pasta `**logs/` na raiz do repositório** por padrão, alinhada a `configs/app.yml` → `app.logs_dir`) um arquivo por dia:

- Caminho: `logs/social/bdqueimadas/<ano>/<mês>/<YYYY-MM-DD>.log`
- O mesmo fluxo também aparece no **stdout** (console).

Formato: linhas de texto com um **objeto JSON** por evento:


| `event`         | Conteúdo                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stage`         | Etapas do pipeline (`pipeline_start`, `extract_anual_*`, `load_monthly_all_df_done`, `mensal_files_ready`, iteração por escopo com `carousel_scope_start` / `carousel_scope_ok` / `carousel_scope_failed`, `chart_spec_computed`, `render_chart_png_done`, `chart_spec_json_written`, `llm_run_start` e estágios LLM, `social_llm_json_written`, `manifest_written` / `manifest_skipped`, `plot_sources_metadata_written`, `pipeline_done`). |
| `llm_roundtrip` | Só com `--llm`: prompt/resposta; a legenda única do carrossel usa `component` `carousel_post_description` e `scope` `carousel`; cada texto de slide usa `graphic_text` e `scope` com o slug do recorte (`nacional`, `amazonia`, …).                                                                                                                                                                                                          |


Opção de CLI: `--logs-dir <pasta>` altera só o diretório base (o subcaminho `social/bdqueimadas/<ano>/<mês>/` continua igual).

### Textos com LLM (Groq)

Na **raiz do repositório**, configure `GROQ_API_KEY` no arquivo `**.env`** (variável lida via `configs/app.yml` → `llm.api_key_env`). O comando `python -m forest_pipelines.social` carrega esse `.env` automaticamente.

Com rede e key válida:

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm
```

- `**post_description**` - uma única legenda Instagram para o carrossel inteiro (prompt dedicado; não detalha números por slide).
- `**graphic_text**` - quatro chamadas, uma por recorte, com payload analítico já filtrado (`focos_incendio_br_v3`).

Opções: `--as-of YYYY-MM-DD` (define o último mês fechado no gráfico - ex. abril → até março - e o prefixo `[YYYY-MM-DD]` na legenda LLM), `--app-config` (default `<repo>/configs/app.yml`), `--out-social-llm` (default `public/generated/social_llm.json`), `--llm-components post_description,graphic_text` (default: ambos).

Para rodar **só a legenda do carrossel**:

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --llm --llm-components post_description
```

Para rodar **só os textos dos slides** (quatro escopos):

```bash
python -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm --llm-components graphic_text
```

O arquivo `social_llm.json` usa `**schema_version: 2**`: `post_description`, `post_description_model`, lista `scopes` (paths, `graphic_text`, modelos, erros por slug). Versões antigas podem ter `schema_version: 1` com um único par legenda + texto.

Saídas extras:

- `public/generated/social_llm.json` - legenda + entradas por escopo (pt-BR).
- Com `--emit-manifest`, cada `body_chart` recebe o texto correspondente em `slots.body_text` (ou mensagem de fallback).

Alvos Make na raiz do repositório:

- `make bdqueimadas-social-assets` - gráficos + manifest **sem** `--llm` (útil sem `GROQ_API_KEY`).
- `make bdqueimadas-social-full` - inclui `--llm` (legenda única + quatro textos de slide).

Isso gera:

- `public/generated/bdqueimadas-chart-<slug>.png` e `chart_spec-<slug>.json` para cada recorte; cópias nacionais `bdqueimadas-chart.png` e `chart_spec.json`.
- `examples/bdqueimadas-social.manifest.json` e cópia em `public/examples/` para o preset `?preset=bdqueimadas` no compositor.

Depois: `npm run dev` neste app; export Playwright opcional: `npm run export:manifest -- examples/bdqueimadas-social.manifest.json`.

A legenda para colar no Instagram está em `social_llm.json` → `post_description` quando você roda o pipeline com `--llm` e inclui `post_description` em `--llm-components`.

## Pipeline Research Trends (carrossel sobre pesquisa, tema branco)

Deck automático com base em **OpenAlex** (fonte analítica primária, busca de trabalhos sobre queimadas com afiliação institucional no Brasil), **Crossref** (validação dos DOIs mais citados) e **Google Trends** (interesse público comparado à produção científica). Saída na **white** theme; preset em `white/composer.html?preset=research-trends`.

Estrutura paralela ao pipeline BDQueimadas:

- Cliente HTTP: `[src/forest_pipelines/social/research_trends/openalex_client.py](../../src/forest_pipelines/social/research_trends/openalex_client.py)`, `[crossref_client.py](../../src/forest_pipelines/social/research_trends/crossref_client.py)`, `[google_trends_client.py](../../src/forest_pipelines/social/research_trends/google_trends_client.py)`.
- Renderização: `[charts.py](../../src/forest_pipelines/social/research_trends/charts.py)` (matplotlib, PNGs 1080×620 alinhados ao slot `body-chart-frame`).
- Orquestrador: `[pipeline.py](../../src/forest_pipelines/social/research_trends/pipeline.py)`.
- Configuração documentada: `[configs/social/research_trends.yml](../../configs/social/research_trends.yml)`.
- Cache: `data/research_trends/cache/` (respostas brutas de cada API).

**Como rodar** (com venv ativo e `pip install -e .`):

```bash
make research-social-assets
# ou: python -m forest_pipelines.social.research_trends --verbose
```

Forçar atualização de cache:

```bash
make research-social-refresh
```

**O que sai:**

- `public/generated/research-<key>.png` (6 charts: `publications-per-year`, `google-trends`, `top-institutions`, `top-concepts`, `top-venues`, `open-access-share`).
- `public/generated/chart_spec-research-<key>.json` (séries agregadas, para reprodutibilidade).
- `examples/research-trends.manifest.json` + cópia em `public/examples/` para o preset.
- `data/research_trends/cache/crossref_validation.json` (auditoria DOI-a-DOI dos top citados).

**Sem chave de API.** OpenAlex e Crossref usam o "polite pool" só com header `mailto`. Defina `FOREST_POLITE_EMAIL` no `.env` ou passe `--mailto`. Default: `julianofpadua@gmail.com`.

**Google Trends** usa a biblioteca não-oficial `pytrends`. Se o Google bloquear ou estiver fora do ar, o pipeline registra um warning e **pula só o slide `google-trends`**. Para desabilitar explicitamente: `--skip-google-trends`.

**Tópico atual:** queimadas/incêndios florestais com afiliação no Brasil (`concepts.id:C2776775217|C84111414` + `authorships.institutions.country_code:BR`, desde 2000). Trocar o tópico significa editar o filtro em `pipeline.DEFAULT_OPENALEX_FILTER` ou parametrizar.
