# Recomendações priorizadas

## Quick wins (baixo esforço, alto retorno)

| # | Recomendação | Por quê | Facilidade | Custo |
| --- | --- | --- | --- | --- |
| R1 | Adicionar step `pytest` no workflow semanal ou em PR | Evita regressão básica | Alta | Grátis (minutos CI) |
| R2 | Introduzir `uv.lock` ou `requirements.txt` pinado via `pip-compile` | Reprodutibilidade | Alta | Grátis |
| R3 | Remover ou documentar `FP_PUBLIC_BASE_URL` em `.env.example` | Menos confusão | Alta | Grátis |
| R4 | Tabela registry→YAML já em `04_frontend.md` — link no README | Menos erro operacional | Alta | Grátis |

## Médio prazo

| # | Recomendação | Alternativas comparadas |
| --- | --- | --- |
| R5 | Testes com mock de Storage | **A)** `pytest` + `unittest.mock` — fácil, grátis. **B)** interface `StoragePort` — médio, melhor arquitetura. **C)** testes de integração em bucket staging — médio, pode ter custo Storage. |
| R6 | Quebrar `bdqueimadas_overview.py` em módulos | Melhora manutenção; esforço médio; sem custo direto. |
| R7 | CI matrix para N datasets críticos | **A)** inputs em `workflow_dispatch` — fácil. **B)** matrix — médio; multiplica minutos ([billing Actions](https://docs.github.com/billing/managing-billing-for-github-actions/about-billing-for-github-actions)). |

## Estruturais / segurança

| # | Recomendação | Detalhe |
| --- | --- | --- |
| R8 | Reduzir superfície da service role | Ver análise em `11_security.md`: secrets manager, rotação, ou camada de upload intermediária. |
| R9 | Escalabilidade de relatório | **A)** Processar anos em subprocessos — médio. **B)** Spark/Dask — alto. **C)** pré-agregar offline — médio, reduz custo runtime. |

## Refactors importantes (não implementar sem RFC)

- Unificar **convenção de paths** de YAML com IDs de registry (geração automática ou validação na importação).

## Roadmap técnico sugerido (síntese)

1. **Estabilizar qualidade:** pytest no CI + lockfile.
2. **Operação:** workflows manuais para datasets não cobertos pelo cron.
3. **Segurança:** política de chaves e rotação.
4. **Escala:** perfilar relatório em dataset grande; otimizar I/O.
