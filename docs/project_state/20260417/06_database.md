# Banco de dados

## Escopo neste repositório

**Não há schema SQL, ORM de banco relacional nem migrations** no projeto. Persistência observável no código é:

1. **Supabase Storage** (objetos/arquivos + manifests JSON).
2. **Arquivos locais** em `data/` (cache) e `docs/` (auditorias).
3. Opcionalmente, o projeto Supabase na nuvem pode ter **Postgres** para outros apps, mas **não é usado diretamente** por este pacote (apenas API de Storage via cliente Python).

## Entidades lógicas (Storage)

| Conceito | Representação | Onde é definido |
| --- | --- | --- |
| Objeto de dataset | Arquivos sob `bucket_prefix` + `manifest.json` | Runners + `cli.sync` |
| Manifest de dataset | JSON com `items`, `meta`, `generated_at`, etc. | `manifests/build_manifest.py` e extensões |
| Snapshot de notícias | `…/snapshots/YYYY/MM/DD/*.json` | `datasets/noticias_agricolas/sync.py` |
| Report package | `generated/report.json`, `live/report.json`, `report.json`, `manifest.json` | `reports/publish/supabase.py` |

## Relacionamentos

- **1 dataset runner → N objetos** no prefixo do bucket.
- **1 report → vários JSON** derivados do mesmo conjunto agregado.

## Riscos de modelagem

- **Sem transação:** uploads parciais podem deixar prefixo inconsistente se o processo falhar a meio — mitigado parcialmente por retries, não por atomicidade multi-objeto.
- **Manifest como fonte da verdade:** frontend depende de JSON; validação de schema no repo é limitada (alguma validação em notícias).

## Índices

Não aplicável a Storage de arquivos; **listagem** depende de caminhos conhecidos ou manifest.

## Alternativas se evoluir para dados tabulares

1. **Postgres (Supabase) + tabela de inventário:** esforço médio; custo no tier Supabase; consultas e índices reais.
2. **DuckDB local + parquet no bucket:** esforço médio; bom para analytics; storage como arquivo.
3. **Manter apenas Storage:** esforço zero adicional; limita consultas ad hoc.
