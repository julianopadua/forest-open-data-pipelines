# CI/CD

## Pipelines existentes

Arquivo: [`.github/workflows/weekly_sync.yml`](../../../.github/workflows/weekly_sync.yml).

| Campo | Valor |
| --- | --- |
| Nome | Weekly Open Data Sync |
| Gatilhos | `workflow_dispatch`, `schedule` cron `0 12 * * 1` (segundas 12:00 UTC) |
| Runner | `ubuntu-latest` |
| Python | 3.11 |
| Instalação | `pip install -e .` |

### Comando equivalente ao job

Com as envs injetadas:

```bash
export SUPABASE_URL="***"
export SUPABASE_SERVICE_ROLE_KEY="***"
export SUPABASE_BUCKET_OPEN_DATA=open-data
forest-pipelines sync cvm_fi_inf_diario --latest-months 12
```

## Secrets esperados

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

## Testes automatizados no CI

- **Nenhum** step de `pytest` no workflow atual.

## Verificações de qualidade

- **Nenhum** lint/typecheck no CI.

## Gaps

| Gap | Impacto |
| --- | --- |
| Sem testes no pipeline | Regressões só em dev manual |
| Um único dataset no cron | Outros datasets exigem dispatch manual ou outra automação |
| Sem cache de dependências | Instalação repetida a cada run |
| Comentário no YAML: cron não permite “a cada 10 dias” | Frequência fixa semanal pode ser subótima |

## Escalabilidade e billing (GitHub Actions)

- Repositórios **públicos** historicamente tinham uso gratuito amplo de Actions; repositórios **privados** sujeitos a minutos incluídos e cobrança extra — ver documentação oficial atual: [About billing for GitHub Actions](https://docs.github.com/billing/managing-billing-for-github-actions/about-billing-for-github-actions).
- **Alternativa 1:** manter GA e adicionar matrix/`workflow_dispatch` inputs — baixo custo, esforço baixo.
- **Alternativa 2:** **self-hosted runner** — controle de hardware; custo operacional e superfície de segurança do runner.
- **Alternativa 3:** agendamento em **VM/cloud** (cron systemd, Cloud Scheduler + Cloud Run job) — esforço médio; pode isolar segredos por KMS.
