# 📄 Documentação do módulo `cli.py`

---

## 1. Visão geral e responsabilidade
O arquivo **`src/forest_pipelines/cli.py`** implementa a interface de linha de comando (CLI) da aplicação *forest‑pipelines*.  
Ele expõe o comando `sync`, que orquestra a execução de um *dataset runner* (pipeline de extração/transformação) e a publicação do manifesto resultante em um bucket Supabase.  

> **Objetivo principal:** automatizar, via CLI, a sincronização de datasets configurados, garantindo registro de logs e disponibilidade pública do manifesto JSON.

---

## 2. Onde este arquivo se encaixa na arquitetura
| Camada / Domínio | Papel |
|------------------|-------|
| **Interface de Usuário (UI) – camada de apresentação** | CLI (entrada de usuário, parsing de argumentos). |
| **Orquestração / Aplicação** | Coordena serviços de logging, storage e execução de pipelines. |
| **Utilitário** | Usa helpers genéricos (`get_logger`, `load_settings`, `SupabaseStorage`). |

Não contém lógica de negócio de domínio; apenas compõe serviços já existentes.

---

## 3. Interfaces e exports (o que ele expõe)

| Export | Tipo | Descrição |
|--------|------|-----------|
| `app` | `typer.Typer` | Instância da aplicação Typer que registra o comando `sync`. |
| `sync` | `function` | Função decorada com `@app.command()`; ponto de entrada da CLI. |
| `__main__` guard | - | Executa `app()` quando o módulo é invocado diretamente (`python -m forest_pipelines.cli`). |

Nenhum outro símbolo é exportado.

---

## 4. Dependências e acoplamentos

| Tipo | Módulo | Motivo da dependência |
|------|--------|-----------------------|
| **Externa** | `json`, `pathlib.Path`, `typer` | Manipulação de JSON, caminhos de arquivos e definição da CLI. |
| **Interna** | `forest_pipelines.logging_.get_logger` | Cria logger configurado por `settings.logs_dir`. |
| | `forest_pipelines.registry.datasets.get_dataset_runner` | Recupera a classe/função que executa o pipeline do dataset solicitado. |
| | `forest_pipelines.settings.load_settings` | Carrega configuração YAML da aplicação. |
| | `forest_pipelines.storage.supabase_storage.SupabaseStorage` | Abstração de armazenamento remoto (Supabase). |

> **Acoplamento:** O módulo depende fortemente de `get_dataset_runner` e `SupabaseStorage`. Qualquer mudança na assinatura desses componentes pode quebrar a CLI.

---

## 5. Leitura guiada do código (top‑down)

```python
# Instancia a aplicação Typer (sem autocompletar)
app = typer.Typer(add_completion=False)
```

### 5.1 Definição do comando `sync`

```python
@app.command()
def sync(
    dataset_id: str = typer.Argument(..., help="Ex: cvm_fi_inf_diario"),
    config_path: str = typer.Option("configs/app.yml", help="Caminho do config principal"),
    latest_months: int | None = typer.Option(None, help="Sobrescreve latest_months do dataset"),
) -> None:
```

* **Parâmetros**  
  * `dataset_id` – identificador obrigatório do dataset.  
  * `config_path` – caminho opcional para o arquivo YAML de configuração (padrão `configs/app.yml`).  
  * `latest_months` – sobrescreve a configuração `latest_months` do dataset, se fornecido.

### 5.2 Carregamento de configuração e logger

```python
settings = load_settings(config_path)
logger = get_logger(settings.logs_dir, dataset_id)
```

* `load_settings` lê o YAML e devolve um objeto (não detalhado aqui).  
* `get_logger` cria um logger que grava em `settings.logs_dir` e inclui o `dataset_id` no nome do logger.

### 5.3 Inicialização do storage Supabase

```python
storage = SupabaseStorage.from_env(
    logger=logger,
    bucket_open_data=settings.supabase_bucket_open_data,
)
```

* `from_env` lê variáveis de ambiente necessárias (ex.: `SUPABASE_URL`, `SUPABASE_KEY`).  
* O bucket usado vem da configuração `supabase_bucket_open_data`.

### 5.4 Execução do runner do dataset

```python
runner = get_dataset_runner(dataset_id)
manifest = runner(
    settings=settings,
    storage=storage,
    logger=logger,
    latest_months=latest_months,
)
```

* `get_dataset_runner` devolve uma *callable* (classe ou função) responsável por processar o dataset.  
* O runner recebe os mesmos objetos de contexto (settings, storage, logger) e retorna um **manifest** – dicionário contendo, entre outros, a chave `bucket_prefix`.

### 5.5 Publicação do manifesto

```python
manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
manifest_path = f"{manifest['bucket_prefix'].rstrip('/')}/manifest.json"

storage.upload_bytes(
    object_path=manifest_path,
    data=manifest_bytes,
    content_type="application/json",
    upsert=True,
)
```

* Serializa o dicionário para JSON UTF‑8.  
* Constrói o caminho no bucket usando `bucket_prefix`.  
* `upload_bytes` grava o arquivo, sobrescrevendo (`upsert=True`) se já existir.

### 5.6 Log de conclusão

```python
logger.info("Manifest publicado: %s", storage.public_url(manifest_path))
logger.info("OK")
```

* Exibe a URL pública do manifesto e indica sucesso.

### 5.7 Execução direta

```python
if __name__ == "__main__":
    app()
```

Permite chamar a CLI via `python -m forest_pipelines.cli`.

---

## 6. Fluxo de dados/estado/eventos

1. **Entrada** – argumentos da linha de comando (`dataset_id`, `config_path`, `latest_months`).  
2. **Configuração** – carregada a partir de YAML; fornece diretórios, credenciais e parâmetros de bucket.  
3. **Logger** – estado interno (arquivo de log) associado ao dataset.  
4. **Storage** – objeto que encapsula a conexão Supabase; mantém credenciais em ambiente.  
5. **Runner** – recebe todos os objetos acima e produz um *manifest* (estrutura de metadados).  
6. **Upload** – bytes do manifesto são enviados ao bucket; o caminho final é registrado no log.  

Não há eventos assíncronos nem estado persistente além do upload.

---

## 7. Conexões com outros arquivos do projeto

| Módulo importado | Caminho relativo | Propósito |
|------------------|------------------|-----------|
| `forest_pipelines.logging_` | `src/forest_pipelines/logging_.py` | Função `get_logger`. |
| `forest_pipelines.registry.datasets` | `src/forest_pipelines/registry/datasets.py` | Função `get_dataset_runner`. |
| `forest_pipelines.settings` | `src/forest_pipelines/settings.py` | Função `load_settings`. |
| `forest_pipelines.storage.supabase_storage` | `src/forest_pipelines/storage/supabase_storage.py` | Classe `SupabaseStorage`. |

> **Nota:** Não há arquivos que importem `cli.py`; ele é ponto de entrada da aplicação.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas

| Área | Observação | Recomendações |
|------|------------|---------------|
| **Validação de parâmetros** | `dataset_id` é passado diretamente ao runner sem verificação de existência. | Inserir checagem antecipada (`if not runner: raise typer.Exit(...)`). |
| **Tratamento de exceções** | Nenhum `try/except` captura falhas de I/O (ex.: falha ao ler config, upload). | Envolver blocos críticos em `try/except` e usar `logger.error` + `typer.Exit` com código de erro. |
| **Dependência de variáveis de ambiente** | `SupabaseStorage.from_env` pode lançar exceção se variáveis ausentes. | Documentar variáveis necessárias e validar antes da criação do storage. |
| **Teste de integração** | O comando executa I/O externo (Supabase). | Criar mocks para `SupabaseStorage` e `runner` em testes unitários da CLI. |
| **Extensibilidade** | Apenas um comando (`sync`). | Estruturar o módulo para permitir adição de novos sub‑comandos (ex.: `validate`, `list`). |
| **Documentação de saída** | O manifesto é publicado, mas seu schema não está descrito aqui. | Referenciar ou incluir link para a especificação do manifesto. |

--- 

*Esta documentação foi gerada com base no código-fonte apresentado, sem suposições adicionais.*
