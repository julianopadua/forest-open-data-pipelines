# Padrão de documentação reutilizável (framework)

Este documento define um **framework genérico** para auditorias de estado de repositório. Pode ser copiado para outros projetos; ajuste nomes de pastas e o escopo “sem frontend”, “sem DB”, etc.

## Convenção de pastas

```
docs/project_state/YYYYMMDD/
  00_overview.md
  01_architecture.md
  …
  18_next_steps.md
```

- **`YYYYMMDD`:** data da captura (ex.: `20260417`).
- Manter **ordem numérica** para leitura linear e diff histórico.

## Convenção de nomes de arquivos

- Prefixo `NN_` com zero à esquerda (dois dígitos).
- Nomes em **snake_case** em inglês para consistência internacional.
- Um tema por arquivo; evitar “misc”.

## Ordem ideal de leitura

1. `00_overview` — contexto e riscos.
2. `04` ou doc de **superfície de uso** (CLI/UI/API) — como operar.
3. `01_architecture` — fluxos profundos.
4. `11_security`, `10_ci_cd` — riscos operacionais.
5. `12`–`15` — qualidade.
6. `16`–`18` — decisões e plano.

## Template mínimo por arquivo

Cada `.md` deve ter:

- **Título** e **data/escopo** no topo (opcional).
- **Seções** previsíveis (abaixo).
- **Caminhos relativos** ao repo ao citar código: `` `src/foo/bar.py` ``.

### `00_overview.md`

- Resumo executivo, propósito, maturidade, módulos, riscos top, prioridades, **como executar** (3–5 linhas).

### `01_architecture.md`

- Camadas, fluxos de dados, diagrama (Mermaid), dependências entre pacotes, padrões, problemas, **fluxo por comando** (se CLI).

### `04` superfície de uso (CLI/UI)

- Tabela: comando → args → flags → pré-requisitos.
- Matriz feature → como rodar.

### `11_security.md`

- Superfície de ataque, secrets, princípio do menor privilégio, **ficha de fragilidade** (ver abaixo).

### `12_technical_debt.md`

- Tabela com severidade e prioridade.

### `16_recommendations.md` / `18_next_steps.md`

- Itens priorizados; quick wins explícitos.

## Severidade (padrão)

| Nível | Significado |
| --- | --- |
| Crítica | Comprometimento de dados/produção imediato |
| Alta | Falha frequente ou segurança séria |
| Média | Degradação, custo, ou risco moderado |
| Baixa | Limpeza, DX, documentação |

## Padrão para bugs

| Campo | Descrição |
| --- | --- |
| ID | `B1`, `B2`, … |
| Sintoma | O que o usuário vê |
| Causa provável | Hipótese |
| Evidência | Arquivo:linha ou trecho |
| Criticidade | Baixa/Média/Alta |
| Correção sugerida | Passos |

## Padrão para dívida técnica

| Campo | Descrição |
| --- | --- |
| ID | `TD1`, … |
| Descrição | O que está errado |
| Severidade | Média/… |
| Impacto | Texto curto |
| Prioridade P | 1 = primeiro |

## Padrão para riscos

- **Risco:** descrição curta  
- **Probabilidade / Impacto:** baixo-médio-alto  
- **Mitigação:** controles  
- **Residual:** o que permanece  

## Padrão para decisões arquiteturais (ADR leve)

Em `01` ou `16`, use bloco:

```
## ADR-YYYYMMDD-NN: Título
- Contexto:
- Decisão:
- Consequências:
- Alternativas rejeitadas:
```

## Padrão para análise de alternativas (fragilidade)

Quando uma escolha for **fraca ou cara**, preencher:

| Campo | Conteúdo |
| --- | --- |
| Escolha atual | O que o projeto faz |
| Por que é subótima | Acoplamento, custo, risco |
| Alternativa A/B/C | Nome |
| Facilidade implementação | Baixa/média/alta |
| Custo | Grátis / tier / pay-as-you-go |
| Eficiência | Throughput, memória, latência |
| Segurança | Notas |

## Padrão para changelog futuro

- Manter `CHANGELOG.md` no root com [Keep a Changelog](https://keepachangelog.com/) ou formato interno:
  - `## [Unreleased]`
  - `### Added` / `Changed` / `Fixed` / `Security`

## Templates específicos solicitados por este projeto

### Fluxo CLI (copiar para `01` / `04`)

```markdown
### Comando `nome`
- Typer name:
- Args:
- Options (default):
- Pré-requisitos env:
- Passos internos (1..n):
- Artefatos:
```

### Ficha de fragilidade de segurança

```markdown
### Fragilidade: <título>
- **Escolha atual:**
- **Por quê é arriscada:**
- **Alternativas (1–3):** (tabela facilidade/custo/eficiência/segurança)
```

### Escalabilidade

```markdown
### Limite: <título>
- **Sintoma esperado sob carga:**
- **Gargalo provável:**
- **Mitigação curta:**
- **Alternativas:**
```

---

**Exemplo aplicado:** este repositório usa CLI Typer + Supabase; projetos com FastAPI devem substituir `04` por rotas OpenAPI e manter o restante análogo.
