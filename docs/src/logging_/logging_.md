## 1. Visão geral e responsabilidade  

O módulo **`logging_.py`** fornece uma única função, `get_logger`, responsável por criar e configurar um objeto `logging.Logger` padronizado para gravação de logs de um dataset específico.  
A configuração inclui:

* nível de log **INFO**;  
* saída simultânea para **stdout** (via `StreamHandler`) e para um arquivo de texto (via `FileHandler`);  
* formatação unificada contendo timestamp UTC, nível, nome do logger e mensagem.

## 2. Onde este arquivo se encaixa na arquitetura  

- **Camada:** Utilitário / Infraestrutura (serviços de apoio).  
- **Domínio:** Não pertence a nenhum domínio de negócio; serve a toda a aplicação que precise de logging consistente.  
- **UI / API:** Não expõe interface de usuário nem endpoints.  

## 3. Interfaces e exports  

| Export | Tipo | Descrição |
|--------|------|-----------|
| `get_logger(logs_dir: Path, dataset_id: str) -> logging.Logger` | Função | Cria e devolve um logger configurado para o dataset identificado por `dataset_id`, armazenando os arquivos de log em `logs_dir`. |

## 4. Dependências e acoplamentos  

| Tipo | Módulo | Motivo |
|------|--------|--------|
| **Externa** | `logging` (stdlib) | API padrão de logging do Python. |
| **Externa** | `datetime.datetime` | Geração da data UTC para o nome do arquivo. |
| **Externa** | `pathlib.Path` | Manipulação segura de caminhos de arquivos. |
| **Interna** | Nenhuma | O módulo não importa nenhum código do próprio repositório. |

O acoplamento é **baixo**: a única dependência externa é a biblioteca padrão, garantindo portabilidade.

## 5. Leitura guiada do código (top‑down)

```python
def get_logger(logs_dir: Path, dataset_id: str) -> logging.Logger:
    logger = logging.getLogger(dataset_id)          # 1. Obtém (ou cria) logger nomeado
    logger.setLevel(logging.INFO)                  # 2. Define nível padrão
    logger.handlers.clear()                        # 3. Remove handlers preexistentes

    logs_dir.mkdir(parents=True, exist_ok=True)    # 4. Garante existência do diretório
    date = datetime.utcnow().strftime("%Y-%m-%d")  # 5. Data UTC para nome do arquivo
    log_file = logs_dir / f"{dataset_id}_{date}.log"

    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s - %(message)s")

    sh = logging.StreamHandler()                   # 6. Handler para stdout
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_file, encoding="utf-8")  # 7. Handler para arquivo
    fh.setFormatter(fmt)

    logger.addHandler(sh)                          # 8. Anexa stdout
    logger.addHandler(fh)                          # 9. Anexa arquivo
    return logger                                   # 10. Retorna logger configurado
```

**Decisões de implementação relevantes**

| Decisão | Justificativa |
|---------|---------------|
| `logger.handlers.clear()` | Garante que chamadas subsequentes a `get_logger` não acumulem handlers, evitando duplicação de mensagens. |
| `logs_dir.mkdir(parents=True, exist_ok=True)` | Cria hierarquia completa de diretórios de forma idempotente. |
| Timestamp em UTC (`datetime.utcnow()`) | Padroniza logs independentemente do fuso horário da máquina de execução. |
| Formato `"%(... )sZ"` | O sufixo `Z` indica horário UTC, facilitando correlação com sistemas externos. |
| Codificação `"utf-8"` no `FileHandler` | Prevê suporte a caracteres não‑ASCII nos registros. |

## 6. Fluxo de dados/estado/eventos  

1. **Entrada:** `logs_dir` (caminho onde armazenar arquivos) e `dataset_id` (identificador do dataset).  
2. **Processamento:** Criação/limpeza do logger, montagem do caminho do arquivo (`{dataset_id}_{YYYY-MM-DD}.log`).  
3. **Saída:** Instância de `logging.Logger` pronta para uso; o estado interno do logger inclui dois handlers (stream e file).  
4. **Eventos:** Cada chamada ao logger gera eventos de log que são enviados simultaneamente para stdout e para o arquivo de log.

## 7. Conexões com outros arquivos do projeto  

- **Importado por:** *Nenhum* (não há referências externas no momento).  
- **Importa:** *Nenhum* módulo interno do repositório.  

> **Observação:** Caso futuros módulos precisem de logging, deverão importar `get_logger` a partir de `src/forest_pipelines/logging_.py`.

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Multiplicidade de chamadas** | Se `get_logger` for invocado repetidamente com o mesmo `dataset_id`, o logger será reconfigurado a cada chamada (handlers são limpos). Isso pode ser custoso em loops intensivos. | Cachear o logger por `dataset_id` usando `functools.lru_cache` ou dicionário interno. |
| **Rotação de arquivos** | O nome do arquivo inclui apenas a data (YYYY‑MM‑DD). Em ambientes de alta frequência, o arquivo pode crescer indefinidamente ao longo do dia. | Integrar `logging.handlers.TimedRotatingFileHandler` ou `RotatingFileHandler` para limitar tamanho ou criar arquivos por hora. |
| **Formato de timestamp** | O `Formatter` usa `%(asctime)s` que, por padrão, inclui milissegundos locais antes da conversão para UTC (`Z`). Pode gerar confusão. | Substituir por `datetime.utcnow().isoformat()` via `logging.Formatter(fmt, datefmt='%Y-%m-%dT%H:%M:%S.%fZ')`. |
| **Nível de log fixo** | O nível está hard‑coded como `INFO`. Projetos que precisem de `DEBUG` ou `ERROR` não podem ajustar sem modificar o código. | Expor parâmetro opcional `level: int = logging.INFO`. |
| **Falha ao criar diretório** | `mkdir` pode lançar exceção de permissão. Não há tratamento de erro. | Envolver em `try/except` e propagar exceção customizada (`LoggingSetupError`). |

Implementar as melhorias acima aumentará a robustez, a performance e a flexibilidade do mecanismo de logging centralizado.
