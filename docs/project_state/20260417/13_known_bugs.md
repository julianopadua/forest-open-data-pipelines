# Bugs conhecidos

## Metodologia

Análise **estática** do repositório sem execução dos pipelines contra APIs reais. Itens abaixo são **problemas potenciais** ou **inconsistências** com evidência no código; não foram reproduzidos em runtime nesta auditoria.

## Potenciais problemas

| ID | Sintoma provável | Possível causa | Evidência | Criticidade | Recomendação |
| --- | --- | --- | --- | --- | --- |
| B1 | Confusão ao localizar YAML | ID do CLI ≠ nome do arquivo | Registry usa `cvm_fi_inf_diario`, YAML em `cvm/fi_inf_diario.yml` | Baixa (operacional) | Usar matriz em `04_frontend.md`; opcionalmente unificar convenção |
| B2 | Falha LLM em report | `GROQ_API_KEY` ausente | `router.py` lança se env vazia | Média | Documentar pré-requisito por report; fail fast já ocorre |
| B3 | Upload parcial no bucket | Falha após alguns uploads | Sem transação multi-objeto | Média | Re-run idempotente com upsert; monitorar logs |

## Bugs de runtime não confirmados

- **Nenhum bug reproduzido** nesta sessão. Recomenda-se teste de integração com bucket de staging.
