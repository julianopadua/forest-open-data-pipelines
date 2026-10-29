# Limpeza de payloads legados no Supabase Storage

O bucket `open-data` deve conter catálogos, manifests, relatórios e metadados compactos. Implementações antigas também publicaram cópias de ZIP, PDF, CSV e planilhas em caminhos `.../data/...`. O portal, a API e o SDK atuais usam os `source_url` oficiais dos manifests para downloads.

## Pré-requisito

A API do projeto precisa estar liberada. Durante a restrição `exceed_storage_size_quota`, até a listagem autenticada de objetos retorna HTTP 402 e a limpeza pela API não pode ser executada. No plano Free, a Supabase informa que o upgrade libera o serviço imediatamente; a outra opção é aguardar o próximo ciclo de cobrança. Consulte [Billing FAQ](https://supabase.com/docs/guides/platform/billing-faq).

## Procedimento

1. Instale as dependências com `make dev` e defina `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` e `SUPABASE_BUCKET_OPEN_DATA` em `.env`.
2. Execute `python scripts/cleanup_legacy_storage.py` para listar os candidatos e o total. O modo padrão é somente de leitura.
3. Revise os caminhos. O script seleciona objetos de datasets sob `data/`, arquivos brutos antigos sob `meta/` e snapshots antigos de Notícias Agrícolas. Ele preserva manifests, catálogos, relatórios e caches de agregação.
4. O script lê os JSONs publicados e bloqueia a limpeza se algum deles ainda aponta para um candidato. Nesse caso, sincronize o dataset afetado com o runner atual, por exemplo `forest-pipelines sync inpe_bdqueimadas_focos --force`, e repita a auditoria. O runner atual descobre a URL oficial e não envia o payload ao Storage.
5. Após revisar a lista, execute `python scripts/cleanup_legacy_storage.py --apply --expected-count N`, substituindo `N` pela contagem do passo 2. O script usa a API oficial de Storage, em lotes, e verifica o resultado por uma nova listagem.

A exclusão pela API é permanente. Nunca use `DELETE FROM storage.objects`: apagar somente a linha de metadados pode deixar o arquivo físico órfão e ainda sujeito à cobrança. Consulte [Delete Objects](https://supabase.com/docs/guides/storage/management/delete-objects) e [Storage Schema](https://supabase.com/docs/guides/storage/schema/design).

A métrica de cota é calculada como média de GB-horas no ciclo. Mesmo após a limpeza, a restrição pode persistir até o novo ciclo. Consulte [Manage Storage size usage](https://supabase.com/docs/guides/platform/manage-your-usage/storage-size).
