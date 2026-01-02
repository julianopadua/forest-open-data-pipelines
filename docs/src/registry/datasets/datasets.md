## 1. Visão geral e responsabilidade
O módulo **`registry/datasets.py`** centraliza o registro de *runners* (funções de execução) associados a datasets da CVM. Ele fornece um ponto único de consulta (`get_dataset_runner`) que, a partir de um identificador de dataset, devolve a função responsável por orquestrar a extração, transformação e carregamento dos dados.

## 2. Posicionamento na arquitetura
- **Camada:** *Infraestrutura / Integração de Dados*  
- **Domínio:** *Financeiro – Dados da Comissão de Valores Mobiliários (CVM)*  
- **Tipo de componente:** *Registry* (catálogo de serviços) que desacopla a lógica de chamada dos runners da sua localização física.

## 3. Interfaces e exports
| Export | Tipo | Descrição |
|--------|------|-----------|
| `DatasetRunner` | `typing.Callable[..., dict[str, Any]]` | Alias para a assinatura esperada dos runners. |
| `RUNNERS` | `dict[str, DatasetRunner]` | Mapeamento estático `dataset_id → runner`. |
| `get_dataset_runner` | `Callable[[str], DatasetRunner]` | Função pública que recupera o runner a partir do `dataset_id`. |

## 4. Dependências e acoplamentos
- **Internas:** Nenhuma importação de módulos internos do repositório além dos runners específicos.  
- **Externas:**  
  - `typing` (padrão da biblioteca padrão).  
  - Módulos de datasets em `forest_pipelines.datasets.cvm` que expõem objetos com método `sync`.  
- **Acoplamento:** O registro depende da existência do atributo `sync` em cada módulo importado; qualquer mudança na assinatura ou remoção desse atributo quebrará o registro.

## 5. Leitura guiada do código (top‑down)

1. **Importações**  
   ```python
   from typing import Any, Callable
   from forest_pipelines.datasets.cvm import (
       fi_inf_diario,
       fi_doc_extrato,
       fi_cad_registro_fundo_classe,
       fi_cad_nao_adaptados_rcvm175,
       fi_cad_icvm555_hist,
       fii_doc_inf_trimestral,
   )
   ```
   Cada import traz um módulo que contém um runner síncrono (`sync`).

2. **Definição de alias**  
   `DatasetRunner` descreve a assinatura esperada: aceita parâmetros arbitrários (`*args, **kwargs`) e devolve um `dict[str, Any]`.

3. **Construção do dicionário `RUNNERS`**  
   ```python
   RUNNERS: dict[str, DatasetRunner] = {
       "cvm_fi_inf_diario": fi_inf_diario.sync,
       "cvm_fi_doc_extrato": fi_doc_extrato.sync,
       "cvm_fi_cad_registro_fundo_classe": fi_cad_registro_fundo_classe.sync,
       "cvm_fi_cad_nao_adaptados_rcvm175": fi_cad_nao_adaptados_rcvm175.sync,
       "cvm_fi_cad_icvm555_hist": fi_cad_icvm555_hist.sync,
       "cvm_fii_doc_inf_trimestral": fii_doc_inf_trimestral.sync,
   }
   ```
   Cada chave corresponde ao identificador usado por consumidores externos; o valor é a referência ao método `sync` do respectivo módulo.

4. **Função `get_dataset_runner`**  
   - Recebe `dataset_id` (string).  
   - Tenta localizar o runner em `RUNNERS`.  
   - Em caso de ausência, lança `KeyError` com mensagem explícita.  
   - Comentário de docstring indica que o runner tem a assinatura `(settings, storage, logger, latest_months?)`, e que para datasets anuais o parâmetro `latest_months` pode sobrescrever `latest_years`.

## 6. Fluxo de dados / estado / eventos
O módulo não mantém estado mutável; funciona como um *lookup table* puro. O fluxo típico é:

```
Cliente → get_dataset_runner(id) → runner(settings, storage, logger, …) → dict de resultados
```

Nenhum evento interno ou cache é manipulado aqui.

## 7. Conexões com outros arquivos do projeto
- **Importações de runners:**  
  - `forest_pipelines/datasets/cvm/fi_inf_diario.py` → expõe `sync`.  
  - `forest_pipelines/datasets/cvm/fi_doc_extrato.py` → expõe `sync`.  
  - `forest_pipelines/datasets/cvm/fi_cad_registro_fundo_classe.py` → expõe `sync`.  
  - `forest_pipelines/datasets/cvm/fi_cad_nao_adaptados_rcvm175.py` → expõe `sync`.  
  - `forest_pipelines/datasets/cvm/fi_cad_icvm555_hist.py` → expõe `sync`.  
  - `forest_pipelines/datasets/cvm/fii_doc_inf_trimestral.py` → expõe `sync`.  

> **Observação:** Não há referências externas declaradas (ex.: UI, API) no trecho fornecido; portanto, não é possível inferir quem consome `get_dataset_runner` sem analisar outros módulos.

## 8. Pontos de atenção, riscos e melhorias recomendadas
| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Acoplamento ao atributo `sync`** | Quebra de compatibilidade se o runner mudar de nome ou assinatura. | Definir uma interface abstrata (e.g., `class BaseRunner(Protocol): def __call__(self, ...) -> dict:`) e validar os objetos ao registrar. |
| **Ausência de tipagem explícita nos runners** | Reduz a capacidade de análise estática. | Importar tipos de retorno dos módulos de dataset ou usar `Protocol` para descrever a assinatura esperada. |
| **Mensagens de erro genéricas** | Dificulta depuração quando o `dataset_id` é construído dinamicamente. | Incluir sugestões de IDs válidos (`list(RUNNERS.keys())`) na exceção. |
| **Documentação de parâmetros do runner** | A docstring menciona parâmetros, mas não há validação. | Criar wrapper que verifica a presença de `settings`, `storage`, `logger` antes de delegar ao runner. |
| **Escalabilidade do registro** | O dicionário estático exige modificação manual para novos datasets. | Implementar registro automático via decorador (`@register_dataset(id)`) nos módulos de dataset. |

Implementar as melhorias acima aumentará a robustez, a manutenibilidade e a clareza do registro de datasets.
