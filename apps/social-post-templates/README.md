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

| URL | Template |
|-----|----------|
| `http://localhost:5173/` | Seletor de temas |
| `http://localhost:5173/green/index.html` | Tema Verde (Forest) |
| `http://localhost:5173/navy/index.html` | Tema Azul Marinho (Ocean) |
| `http://localhost:5173/white/index.html` | Tema Branco (Clean) |

O canvas (1080 × 1350) é escalado automaticamente para caber na janela — mas o layout interno está sempre em pixels reais.

## Logo

Coloque o arquivo do logo em `public/logos/logo.png` para substituir o placeholder de texto.  
O arquivo é ignorado pelo git; copie manualmente do diretório `images/logos/` na raiz do repositório.

```bash
cp ../../images/logos/001-wlogo.png public/logos/logo.png
```

## Estrutura dos templates

```
green/index.html   ← tema verde escuro com acento #2ECC9A
navy/index.html    ← tema azul marinho com acento #4A9EFF
white/index.html   ← tema branco com acento #0B7B56
src/style.css      ← estilos compartilhados (canvas, grid, scaler)
```

Cada arquivo HTML tem comentários `<!-- slot: variable_name -->` indicando onde o conteúdo real virá no futuro (preenchido por JSON / LLM).

## Slots disponíveis

| Slot | Descrição |
|------|-----------|
| `topic_tag` | Categoria da notícia |
| `published_at` | Data de publicação |
| `series_label` | Rótulo da série editorial |
| `title` | Título principal do card |
| `summary` | Resumo ou parágrafo de apoio |
| `key_point_1` | Bullet/destaque 1 |
| `key_point_2` | Bullet/destaque 2 |
| `brand_name` | Nome da marca no rodapé |
| `card_number` | Numeração do card (ex: `01 / 05`) |

## Exportar PNGs (futura fase)

O script `scripts/export.js` está preparado para usar **Playwright**.  
Para ativar quando quiser:

```bash
npm install --save-dev playwright
npx playwright install chromium
```

Remova o bloco `TODO` no início do script e rode:

```bash
npm run dev &          # servidor deve estar rodando
npm run export
```

Os PNGs aparecem em `dist-exports/{green,navy,white}/card.png` (gitignored).
