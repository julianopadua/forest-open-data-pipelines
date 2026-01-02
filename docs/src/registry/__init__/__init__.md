# 📄 Documentação do módulo `registry/__init__.py`

---

## 1. Visão geral e responsabilidade
O módulo `registry.__init__` atua como ponto de entrada da sub‑pacote **registry**.  
Sua única responsabilidade é expor, de forma controlada, a função `get_dataset_runner` proveniente de `forest_pipelines.registry.datasets`.  
Ao centralizar esse export, o pacote permite que consumidores importem a funcionalidade principal do registro de datasets sem precisar conhecer a estrutura interna de diretórios.

---

## 2. Posicionamento na arquitetura
- **Camada**: *Domínio / Core* – faz parte da lógica de negócios que gerencia a descoberta e execução de datasets.
- **Tipo**: *Facade* (fachada) – simplifica o acesso a recursos internos do sub‑pacote `registry`.
- **Escopo**: Interno ao projeto `forest_pipelines`; não há exposição direta à camada de UI ou a serviços externos.

---

## 3. Interfaces e exports
```python
__all__ = ["get_dataset_runner"]
```
- **Exportação pública**: `get_dataset_runner`
  - Tipo: *callable* (função)
  - Propósito: retorna um *runner* configurado para um dataset específico, conforme definido em `forest_pipelines.registry.datasets`.

Qualquer importação do sub‑pacote `registry` (e.g., `from forest_pipelines.registry import get_dataset_runner`) será limitada a esse nome, evitando vazamento de símbolos internos.

---

## 4. Dependências e acoplamentos
| Tipo | Módulo/Package | Natureza |
|------|----------------|----------|
| Interna | `forest_pipelines.registry.datasets` | **Forte** – a única dependência; a função exportada é importada diretamente. |
| Externa | Nenhuma | O módulo não depende de bibliotecas de terceiros nem de APIs externas. |

O acoplamento é **unidirecional**: `registry.__init__` depende de `datasets`, mas o inverso não ocorre.

---

## 5. Leitura guiada do código (top‑down)

```python
# src/forest_pipelines/registry/__init__.py
from forest_pipelines.registry.datasets import get_dataset_runner

__all__ = ["get_dataset_runner"]
```

1. **Importação** – a linha `from ... import get_dataset_runner` traz a implementação concreta da função de registro de datasets.  
   - **Decisão de design**: manter a implementação em `datasets.py` permite que a lógica de carregamento e validação de datasets evolua independentemente da fachada.
2. **Definição de `__all__`** – restringe o namespace exportado ao nome listado, garantindo encapsulamento e evitando importações acidentais de símbolos auxiliares que possam ser adicionados futuramente ao módulo.

Não há lógica adicional, laços ou condições; o módulo é deliberadamente minimalista para reduzir superfície de erro.

---

## 6. Fluxo de dados / estado / eventos
O módulo **não** mantém estado nem gera eventos.  
Ele apenas encaminha a chamada para `get_dataset_runner`, que por sua vez (não descrito aqui) pode:
- Receber parâmetros de configuração,
- Instanciar objetos de runner,
- Retornar um objeto pronto para execução.

Qualquer fluxo de dados ocorre dentro da implementação de `get_dataset_runner`, fora do escopo deste arquivo.

---

## 7. Conexões com outros arquivos do projeto
- **Importa**: `forest_pipelines.registry.datasets`  
  - Documentação: [datasets module](../datasets.md) *(link fictício – substituir pelo caminho real)*
- **É importado por**: Nenhum módulo atualmente registrado como dependente direto.  
  - Caso futuros componentes precisem de acesso ao runner de datasets, deverão importar via `forest_pipelines.registry`.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas
| Item | Descrição | Recomendações |
|------|-----------|---------------|
| **Acoplamento forte** | A fachada depende exclusivamente de `get_dataset_runner`. Qualquer mudança de assinatura ou remoção da função quebrará importações. | - Versionar a API de `datasets`.<br>- Considerar um wrapper interno que valide a presença da função antes de exportá‑la. |
| **Visibilidade limitada** | `__all__` restringe a exportação, mas não impede importações explícitas (`from ...datasets import ...`). | - Documentar claramente que `get_dataset_runner` é a única interface pública suportada. |
| **Ausência de testes unitários** | Não há cobertura de teste para a fachada. | - Incluir teste que verifica `registry.get_dataset_runner` aponta para a mesma referência de `datasets.get_dataset_runner`. |
| **Escalabilidade da fachada** | Caso o sub‑pacote registre outras funcionalidades (e.g., `list_datasets`, `validate_dataset`), o `__all__` precisará ser atualizado. | - Avaliar a adoção de um padrão de registro automático (e.g., `__getattr__` em Python 3.7+). |

---
