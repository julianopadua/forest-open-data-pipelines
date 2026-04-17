# Funcionalidades incompletas

## TODOs no código

- Busca por `TODO`/`FIXME`/`XXX` em `*.py` e `*.yml`: **nenhuma ocorrência** encontrada nesta auditoria.

## Integrações parciais

| Item | Estado | Detalhe |
| --- | --- | --- |
| LLM providers | Parcial | `llm/router.py` só implementa `groq`; outros providers levantam `NotImplementedError`. |
| Documentação `docs/src/` | Parcial | README lista módulos sem arquivo espelhado (EIA, INPE completo, LLM, etc.). |

## Mocks temporários

- Não identificados no código de produção.

## Endpoints

- Não há servidor HTTP; n/a.

## Componentes vazios

- Não identificado componente “stub” além da extensão futura de providers LLM.

## Observação

- `noticias_agricolas` possui validação explícita antes de publicar manifest estável — “incompleto” pode significar **falha intencional** se validação não passar.
