# Dívida técnica

Classificação: **P** = prioridade sugerida (1 = mais urgente).

| ID | Descrição | Severidade | Impacto | P | Arquivos / área |
| --- | --- | --- | --- | --- | --- |
| TD1 | Ausência de lockfile de dependências | Média | Builds não reprodutíveis | 2 | `pyproject.toml` |
| TD2 | Cobertura de testes mínima | Média | Regressões silenciosas | 1 | `tests/` |
| TD3 | CI sem pytest/lint | Média | Qualidade não garantida no merge | 2 | `.github/workflows/` |
| TD4 | Duplicação lógica CLI vs `scripts/backfill_cvm_inf_diario.py` | Baixa | Drift de comportamento | 3 | `cli.py`, `scripts/` |
| TD5 | Variável `FP_PUBLIC_BASE_URL` em `.env.example` sem uso no código | Baixa | Confusão onboarding | 3 | `.env.example` |
| TD6 | Dois namespaces de ID de dataset (registry vs path YAML) | Média | Erros operacionais | 2 | Registries + `configs/datasets/` |
| TD7 | Provider LLM só Groq implementado no router | Baixa | Vendor lock-in | 4 | `llm/router.py` |

## Notas

- **TD6:** não é bug se a convenção for estável; ainda assim aumenta carga cognitiva — mitigação: tabela em README ou gerar path a partir de convenção única.
