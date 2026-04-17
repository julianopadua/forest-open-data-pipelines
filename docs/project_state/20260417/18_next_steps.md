# Próximos passos

## Imediato (1–2 semanas)

1. **Rodar `pytest`** localmente e adicionar job de teste no CI (mesmo que só unitários rápidos).
2. **Congelar dependências** com lockfile ou `pip-compile`.
3. **Alinhar `.env.example`** ao código (remover ou explicar `FP_PUBLIC_BASE_URL`).

## Curto prazo (1 mês)

1. **Smoke tests** com mock de `SupabaseStorage` para `sync` e `publish_report_package`.
2. **`workflow_dispatch` com input** `dataset_id` para operação manual sem editar YAML de workflow.
3. Completar **docs/src** para pacotes sem nota (EIA, INPE, LLM) — ou marcar explicitamente como “não documentado”.

## Médio prazo (1–3 meses)

1. Refatorar **`bdqueimadas_overview.py`** em módulos testáveis.
2. Avaliar **menor privilégio** para uploads (ver `11_security.md`).
3. **Performance:** perfilar relatório com muitos anos de ZIP; planejar cache incremental já usado — validar limites de memória.

## Longo prazo (3+ meses)

1. **Abstração de storage** para facilitar troca de backend ou testes.
2. **Orquestração** (Airflow/Prefect/Cloud Scheduler) se o número de datasets schedulados crescer além do confortável no GitHub Actions.

## Quick wins (checklist)

- [ ] Pytest no CI  
- [ ] Lockfile  
- [ ] `.env.example` consistente  
- [ ] Link para matriz CLI→YAML no README  

## O que pode esperar

- Mudanças de **APIs externas** (CKAN, HTML) podem quebrar scrapers — monitorar com testes de fixture quando possível.
- **Preços e limites** de Groq e GitHub Actions mudam — revisar anualmente.
