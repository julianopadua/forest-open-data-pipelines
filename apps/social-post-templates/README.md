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
| `http://localhost:5173/navy/index.html` | Tema Azul Marinho (Ocean) |
| `http://localhost:5173/white/index.html` | Tema Branco (Clean) |

O canvas (1080 × 1350) é escalado automaticamente para caber na janela — o layout interno permanece em pixels reais.

## Tema verde modular

Slides atômicos em `green/slides/`:

| Arquivo | `type` no manifest | Conteúdo |
|---------|-------------------|----------|
| `cover.html` | `cover` | Capa (título + resumo) |
| `body-image-text.html` | `body_image_text` | Imagem + texto; `imageSide`: `left` ou `right` |
| `body-text.html` | `body_text` | Só texto; `columns`: `1` ou `2` |
| `cta.html` | `cta` | Encerramento com CTA e URL |

Tokens compartilhados: [`src/green/theme.css`](src/green/theme.css). Preenchimento por query string ou por `window.applySlots` (export).

Exemplo de manifest: [`examples/green-manifest.example.json`](examples/green-manifest.example.json).

### Schema do manifest

- `theme`: `"green"`
- `runId`: nome da pasta de saída em `dist-exports/green/<runId>/`
- `slides`: array ordenado; cada item tem:
  - `type`: `cover` | `body_image_text` | `body_text` | `cta`
  - `slots`: objeto string → string (conteúdo dos `data-slot`)
  - `imageSide` (opcional, para `body_image_text`): `"left"` | `"right"`
  - `columns` (opcional, para `body_text`): `1` | `2`

O campo `card_number` é preenchido automaticamente na exportação (`01 / N`, …).

## Logo

Coloque o arquivo do logo em `public/logos/logo.png` para substituir o placeholder de texto.  
O arquivo é ignorado pelo git; copie manualmente do diretório `images/logos/` na raiz do repositório.

```bash
cp ../../images/logos/001-wlogo.png public/logos/logo.png
```

## Estrutura geral

```
green/index.html          ← hub do verde
green/composer.html       ← UI do deck + JSON
green/slides/*.html       ← slides atômicos
src/green/theme.css       ← tokens do verde
src/green/slots.js        ← applySlots + query string
navy/index.html
white/index.html
src/style.css             ← Tailwind + canvas / grid
```

Comentários `<!-- slot: name -->` marcam áreas lógicas; elementos editáveis usam `data-slot="name"`.

## Slots por slide

**Capa (`cover`):** `topic_tag`, `published_at`, `series_label`, `title`, `summary`, `brand_name`, `card_number`

**Imagem + texto:** `topic_tag`, `published_at`, `caption`, `body_text`, `image_url`, `brand_name`, `card_number`

**Só texto:** `topic_tag`, `published_at`, `text_col_1`, `text_col_2`, `brand_name`, `card_number` (com uma coluna, `text_col_2` fica oculto)

**CTA:** `topic_tag`, `published_at`, `cta_kicker`, `cta_headline`, `cta_subline`, `cta_url`, `brand_name`, `card_number`

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
