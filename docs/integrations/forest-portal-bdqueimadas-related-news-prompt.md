# Prompt para o projeto forest-portal 

Envie o bloco abaixo como mensagem de tarefa no repositório **forest-portal**. O conteúdo descreve como consumir o feed JSON publicado por este repositório (`forest-open-data-pipelines`) e onde exibir no front.

---

## Texto do prompt 

**Contexto**

O repositório `forest-open-data-pipelines` publica um feed de notícias do portal Notícias Agrícolas no **Supabase Storage** (bucket público de dados abertos), no caminho estável:

`news/noticias-agricolas/manifest.json`

O JSON é servido por URL pública no mesmo padrão dos outros manifests de open data:

`<NEXT_PUBLIC_SUPABASE_URL>/storage/v1/object/public/<bucket>/news/noticias-agricolas/manifest.json`

(use a variável de ambiente e o nome de bucket que o projeto já utiliza para manifests ou arquivos públicos do Storage; o padrão costuma ser análogo ao consumo de `manifest.json` de outros datasets).

**Contrato útil no front**

O corpo do manifest inclui, entre outros campos: `generated_at`, `item_count`, `categories_monitored`, e `items` (array).

Cada elemento de `items` traz, para exibição na UI:

- `title` (string)
- `lead` (pode ser string vazia)
- `excerpt` (redação curta; quando existe lead, o pipeline replica o lead em `excerpt`; caso contrário é derivado do corpo)
- `url` (link absoluto para a matéria no Notícias Agrícolas)
- `published_at` (ISO 8601 UTC, sufixo `Z`)
- `category_label` / `categories` (metadado opcional de categoria)
- `image_url` (pode ser `null`)

Os itens já vêm **ordenados por `published_at` decrescente** no array `items`. O pipeline pode publicar até cerca de 15 itens (deduplicação entre categorias); para a sidebar, **mostrar no máximo as 5 notícias mais recentes** (por exemplo `items.slice(0, 5)` após validar que `items` existe).

**O que preciso que você implemente**

1. Reutilizar o **mesmo padrão** que o forest-portal já usa para **buscar JSON público do Supabase Storage** (ou o helper existente para open data / manifests), em vez de inventar um fluxo paralelo.
2. Na página do **report de queimadas** associado ao relatório `bdqueimadas_overview` (ou rota equivalente já existente), inserir um bloco **no menu lateral direito** da área de reports, com título do tipo **Notícias relacionadas** (ou similar, alinhado ao design system).
3. Para cada item (até 5): link com o **título** em destaque; abaixo ou como subtítulo, **lead** quando `lead` não estiver vazio; se `lead` estiver vazio, usar **`excerpt`** como texto de apoio (evitar exibir `content_text` completo na sidebar).
4. Links devem apontar para `url` e abrir em nova aba (`target="_blank"` + `rel="noopener noreferrer"`) quando for política do projeto.
5. Tratar estados de **carregamento**, **erro** (fetch falhou ou JSON inválido) e **lista vazia** sem quebrar o layout do relatório.
6. Não acoplar lógica a Postgres para este feed: a fonte da verdade é o **arquivo JSON no Storage**, como nos demais open data.

**Fora de escopo**

- Não espelhar imagens no Storage nesta etapa; `image_url` é opcional e pode ser ignorado na primeira versão da sidebar ou usada como thumbnail se couber no design.

**Referência técnica no repositório de pipelines** (somente leitura para o implementador): documentação resumida em `docs/datasets/noticias_agricolas_news.md` no repo `forest-open-data-pipelines`, descrevendo o prefixo `news/noticias-agricolas` e o contrato.

Por favor, indique quais arquivos você alterou e como testar localmente (env vars necessárias).

---

## Notas (mantidas neste arquivo)

- A URL exata do manifest depende de `NEXT_PUBLIC_SUPABASE_URL` (ou equivalente) e do bucket; o prompt deixa isso explícito para o implementador seguir o projeto.
- A ordenação global já vem do pipeline; limitar a 5 no front atende "as 5 mais recentes" de forma determinística.
