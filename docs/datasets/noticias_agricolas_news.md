# Dataset `noticias_agricolas_news`

## Finalidade

Ingerir as cinco notícias mais recentes de cada uma das três categorias configuradas no portal Notícias Agrícolas (somente a primeira página de listagem), enriquecer cada URL com o HTML da matéria (título, data, lead, corpo textual, tags e imagem opcional) e publicar um feed JSON no Supabase Storage para consumo pelo `forest-portal`, sem uso de Postgres nesta etapa.

## Identificadores e caminhos

| Campo | Valor |
| --- | --- |
| `dataset_id` (CLI) | `noticias_agricolas_news` |
| Configuração | `configs/datasets/noticias_agricolas_news.yml` |
| Prefixo no bucket | `news/noticias-agricolas` |
| Manifest estável | `news/noticias-agricolas/manifest.json` |
| Snapshots versionados | `news/noticias-agricolas/snapshots/YYYY/MM/DD/<timestamp>.json` |

## Execução local

Com variáveis `SUPABASE_*` definidas (ver `README.md` na raiz):

```bash
forest-pipelines sync noticias_agricolas_news
```

O parâmetro `--latest-months` não se aplica a este dataset e é ignorado.

## Contrato JSON (resumo)

O manifest inclui `source`, `source_name`, `generated_at`, `item_count`, `categories_monitored`, `items[]`, além de `dataset_id`, `title`, `source_dataset_url` e `bucket_prefix` para alinhamento com o restante do repositório. Cada item contém `url`, `title`, `category_slug`, `category_label`, `categories`, `published_at` (ISO 8601 em UTC com sufixo `Z`), `lead`, `excerpt` (igual ao lead quando existente; senão primeiro parágrafo útil do corpo), `content_text`, `image_url` (pode ser `null`), `tags`, `source`, `source_name`, `source_article_id`, `scraped_at` e `rank_within_category`.

## Publicação segura (rollback)

1. Toda a coleta e montagem ocorrem em memória.
2. Validação mínima (quantidade de itens e campos obrigatórios por item).
3. Só então são enviados o snapshot datado e, em seguida, o `manifest.json` estável.
4. Se a validação falhar, nenhum upload é feito e o manifest anterior permanece inalterado no Storage.

## Limitações (versão atual)

- Sem automação de navegador: apenas `requests` e BeautifulSoup; mudanças fortes no HTML podem exigir ajuste de seletores.
- Sem paginação além da primeira página por categoria.
- Imagens não são baixadas nem espelhadas; apenas URLs quando detectáveis.
- Taxa de requisições conservadora (atraso entre chamadas, retries em códigos transitórios, poucos workers).

## Próximos passos sugeridos (fora do escopo atual)

- Espelhar imagens no Storage.
- Incluir mais categorias ou filtros temáticos (incêndios, desmatamento, clima extremo).
- Integração explícita na sidebar do relatório `bdqueimadas_overview` no `forest-portal` ([forest-portal-bdqueimadas-related-news-prompt.md](../integrations/forest-portal-bdqueimadas-related-news-prompt.md)).
