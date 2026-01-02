# 1. Visão geral e responsabilidade  

`utils/dates.py` fornece utilitários de manipulação de datas focados em duas transformações comuns no domínio de pipelines de dados:  

* Conversão de representação compacta *YYYYMM* para o formato ISO‑8601 parcial *YYYY‑MM*.  
* Geração de intervalo mensal (primeiro e último dia) no formato ISO‑8601 completo, útil para a construção de metadados temporais.

Essas funções são independentes de qualquer camada de negócio ou de acesso a dados, atuando como componentes de apoio reutilizáveis.

---

# 2. Onde este arquivo se encaixa na arquitetura  

- **Camada:** Utilitários (utility).  
- **Domínio:** Manipulação de datas/tempo.  
- **Responsabilidade:** Fornecer funções puras, sem efeitos colaterais, que podem ser consumidas por qualquer módulo que necessite normalizar ou gerar intervalos mensais.

---

# 3. Interfaces e exports  

| Export | Tipo | Descrição |
|--------|------|-----------|
| `yyyymm_to_period` | `def yyyymm_to_period(yyyymm: str) -> str` | Converte string *YYYYMM* (ex.: `"202512"`) em `"YYYY-MM"`. Levanta `ValueError` para entrada inválida. |
| `month_range_str` | `def month_range_str(year: int, month: int) -> tuple[str, str]` | Retorna tupla `(início, fim)` onde `início` = `"YYYY-MM-01"` e `fim` = último dia do mês, ambos no formato ISO‑8601 (`YYYY-MM-DD`). Levanta `ValueError` se `month` fora do intervalo 1‑12. |

---

# 4. Dependências e acoplamentos  

| Tipo | Módulo | Motivo |
|------|--------|--------|
| **Externa** | `datetime.date` (biblioteca padrão) | Necessária para cálculo de dias e formatação ISO. |
| **Interna** | Nenhuma | O módulo não importa outros componentes do projeto. |

O uso exclusivo da biblioteca padrão garante baixa acoplamento e alta portabilidade.

---

# 5. Leitura guiada do código (top‑down)  

```python
from __future__ import annotations
from datetime import date

def yyyymm_to_period(yyyymm: str) -> str:
    """\"202512\" -> \"2025-12\""""
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"yyyymm inválido: {yyyymm}")
    return f"{yyyymm[:4]}-{yyyymm[4:]}"

def month_range_str(year: int, month: int) -> tuple[str, str]:
    """Retorna (YYYY-MM-01, YYYY-MM-last_day) como string."""
    if not (1 <= month <= 12):
        raise ValueError("month deve estar entre 1 e 12")

    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)

    last = end.toordinal() - 1
    end_date = date.fromordinal(last)
    return start.isoformat(), end_date.isoformat()
```

### 5.1 `yyyymm_to_period`  

1. **Validação de entrada** – garante exatamente 6 caracteres numéricos; caso contrário, lança `ValueError`.  
2. **Construção do resultado** – concatena os quatro primeiros dígitos (ano) com um hífen e os dois últimos (mês).  
3. **Invariantes** – a função nunca modifica estado externo; seu output depende exclusivamente do parâmetro `yyyymm`.

### 5.2 `month_range_str`  

1. **Validação de mês** – aceita apenas valores entre 1 e 12.  
2. **Cálculo do primeiro dia** – `date(year, month, 1)`.  
3. **Cálculo do primeiro dia do próximo mês** – tratamento especial para dezembro (avança o ano).  
4. **Derivação do último dia** – converte o próximo mês para ordinal, subtrai 1 e reconverte para `date`.  
5. **Formato ISO** – `date.isoformat()` produz `YYYY-MM-DD`.  
6. **Invariantes** – a função é determinística e livre de efeitos colaterais; o intervalo retornado sempre cobre exatamente o mês solicitado.

---

# 6. Fluxo de dados/estado/eventos  

Ambas as funções são **puras**: recebem argumentos imutáveis, executam cálculo determinístico e retornam novos valores. Não há manutenção de estado interno nem emissão de eventos.

---

# 7. Conexões com outros arquivos do projeto  

- **Importadores:** No momento, nenhum módulo do repositório importa `utils/dates.py`.  
- **Exportados:** As duas funções são parte da API pública do pacote `utils`; podem ser referenciadas como `from utils.dates import yyyymm_to_period, month_range_str`.  

*(Caso futuros módulos precisem de normalização de períodos ou geração de intervalos mensais, recomenda‑se importar estas funções para evitar duplicação de lógica.)*

---

# 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Descrição | Ação recomendada |
|------|-----------|------------------|
| **Validação de `yyyymm`** | Apenas verifica comprimento e dígitos; aceita valores como `"000000"` que não representam data válida. | Incluir verificação de ano ≥ 1 e mês entre 01‑12, possivelmente usando `datetime.strptime`. |
| **Timezone / calendário** | Funções assumem calendário gregoriano e fuso horário UTC implícito. | Documentar explicitamente a suposição; considerar parametrização caso haja necessidade de suporte a outros calendários. |
| **Tipagem** | Anotações de retorno usam `tuple[str, str]`; compatível com Python 3.9+. | Manter coerência com o restante do código‑base; se o projeto migrar para Python 3.10+, pode usar `tuple[str, str]` sem alterações. |
| **Testes unitários** | Não há referência a cobertura de testes. | Criar testes que cubram casos válidos, limites (ex.: dezembro) e exceções (meses fora do intervalo, strings malformadas). |
| **Documentação de exceções** | Docstrings não listam explicitamente as exceções levantadas. | Atualizar docstrings para incluir `Raises: ValueError` com mensagens esperadas. |

Implementar as melhorias acima aumentará a robustez e a clareza da API de datas.
