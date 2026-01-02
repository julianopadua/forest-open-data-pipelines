# 📄 Documentação Técnica – `datasets/cvm/fi_doc_extrato.py`

---

## 1. Visão geral e responsabilidade
Este módulo implementa a extração, filtragem e sincronização dos arquivos **Extrato FI** disponibilizados pelo portal de dados da CVM.  
Ele:

* Carrega a configuração do dataset a partir de um arquivo YAML.
* Descobre, via HTTP, os recursos (CSV e arquivos de metadados) associados ao dataset.
* Seleciona os arquivos relevantes de acordo com parâmetros de “anos recentes” e inclusão do arquivo corrente.
* Faz download dos arquivos selecionados, calcula hash SHA‑256 e tamanho, e os envia para um storage configurado.
* Constrói e devolve um *manifest* que descreve os artefatos armazenados.

---

## 2. Onde este arquivo se encaixa na arquitetura
| Camada            | Domínio / Responsabilidade                     |
|-------------------|-----------------------------------------------|
| **Data Ingestion**| Coleta de dados externos (CVM)                |
| **Domain**        | Dataset *cvm_fi_doc_extrato* (Fundos de Investimento – extrato) |
| **Utilitário**    | Funções auxiliares de download (`stream_download`) e construção de manifest (`build_manifest`). |

O módulo não contém lógica de apresentação (UI) nem de orquestração de pipelines; ele é um **componente de ingestão** que pode ser invocado por um CLI ou por um orquestrador de pipelines.

---

## 3. Interfaces e exports (o que ele expõe)

| Nome                | Tipo                | Descrição |
|---------------------|---------------------|-----------|
| `DatasetCfg`        | `@dataclass(frozen=True)` | Estrutura imutável que representa a configuração do dataset. |
| `load_dataset_cfg`  | `Callable[[Path, str], DatasetCfg]` | Carrega a configuração a partir de `<datasets_dir>/<dataset_id>.yml`. |
| `extract_resource_urls` | `Callable[[str], list[str]]` | Recupera todas as URLs de recursos a partir da página HTML do dataset. |
| `pick_urls`         | `Callable[[list[str], int, bool], tuple[list[tuple[str, str]], str | None]]` | Filtra URLs por ano, inclui o arquivo corrente e identifica o arquivo de metadados. |
| `sync`              | `Callable[[Any, Any, Any, int | None], dict[str, Any]]` | Função principal que executa o fluxo completo e devolve o manifest. |

Nenhum outro símbolo é exportado (não há `__all__` explícito, mas apenas os itens acima são de interesse público).

---

## 4. Dependências e acoplamentos

| Tipo | Biblioteca / Módulo | Motivo do uso |
|------|----------------------|---------------|
| **Externa** | `requests` | HTTP GET da página de recursos. |
|              | `beautifulsoup4` (`bs4`) | Parse de HTML para extrair links. |
|              | `pyyaml` (`yaml`) | Leitura de arquivos de configuração YAML. |
|              | `dataclasses`, `pathlib`, `re`, `typing` | Utilitários padrão da linguagem. |
| **Interna** | `forest_pipelines.http.stream_download` | Download com streaming, cálculo de SHA‑256 e tamanho. |
|              | `forest_pipelines.manifests.build_manifest` | Geração do objeto de manifest padrão do projeto. |

O módulo **não** depende de outros componentes de ingestão ou de camada de negócios; seu acoplamento externo se restringe a duas funções utilitárias citadas acima.

---

## 5. Leitura guiada do código (top‑down)

### 5.1 Constantes
```python
RE_YEAR_CSV = re.compile(r"extrato_fi_(\d{4})\.csv$", re.IGNORECASE)
```
Expressão regular que identifica arquivos CSV nomeados `extrato_fi_<ANO>.csv`.

### 5.2 `DatasetCfg`
```python
@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    latest_years: int
    include_current: bool
```
Objeto imutável que agrupa todas as informações necessárias para a sincronização.

### 5.3 `load_dataset_cfg`
* Lê `<datasets_dir>/<dataset_id>.yml`.
* Preenche campos faltantes com valores padrão ou lança `ValueError` se `source_dataset_url` ou `bucket_prefix` estiverem ausentes.
* Converte `latest_years` para `int` e `include_current` para `bool`.

### 5.4 `extract_resource_urls`
1. `GET` na URL do dataset (timeout 60 s).  
2. Levanta exceção caso o status não seja 2xx (`raise_for_status`).  
3. Usa **BeautifulSoup** para selecionar elementos `<a>` com a classe `resource-url-analytics`.  
4. Retorna a lista **ordenada e deduplicada** de URLs absolutas.

### 5.5 `pick_urls`
* Percorre a lista de URLs e classifica cada recurso:
  * **Meta** – arquivos `.txt` contendo “meta_extrato_fi”.
  * **Atual** – arquivo exatamente `extrato_fi.csv`.
  * **Anual** – arquivos que casam com `RE_YEAR_CSV`.
* Ordena os recursos anuais por ano decrescente e mantém no máximo `latest_years` (mínimo 1).  
* Monta a lista final `items` obedecendo `include_current`.  
* Retorna `items` (tuplas `(periodo, url)`) e `meta_url` (ou `None`).

### 5.6 `sync`
Fluxo principal:

1. **Carrega configuração** (`load_dataset_cfg`).  
2. Determina `ly` (anos/meses a considerar) a partir de argumento `latest_months` ou da configuração.  
3. **Descobre URLs** (`extract_resource_urls`).  
4. **Filtra URLs** (`pick_urls`).  
5. **Itera sobre os recursos selecionados**:
   * Define caminho local (`settings.data_dir / "cvm_fi_doc_extrato" / filename`).  
   * Faz download via `stream_download`, obtendo `sha256` e `size_bytes`.  
   * Constrói `object_path` no bucket (`{bucket_prefix}/data/{folder}/{filename}`).  
   * Envia ao storage (`upload_file`) com MIME `text/csv; charset=utf-8`.  
   * Gera `public_url` e adiciona um dicionário ao array `items`.
6. **Processa metadados** (se houver) de forma análoga, usando MIME `text/plain; charset=utf-8`.  
7. **Constrói o manifest** com `build_manifest`, passando `items` e `meta`.  
8. Retorna o manifest (dicionário).

> **Invariantes**  
> * Cada recurso baixado tem seu hash e tamanho registrados.  
> * O caminho no bucket segue a convenção `{bucket_prefix}/data/<periodo>/<filename>` ou `{bucket_prefix}/meta/<filename>`.  
> * O manifest sempre contém `dataset_id`, `title` e `source_dataset_url` conforme a configuração.

---

## 6. Fluxo de dados / estado / eventos

| Etapa | Entrada | Processamento | Saída |
|-------|---------|---------------|-------|
| Configuração | `settings.datasets_dir`, `dataset_id` | `load_dataset_cfg` | `DatasetCfg` |
| Descoberta | `DatasetCfg.source_dataset_url` | `requests.get` → `BeautifulSoup` | Lista de URLs |
| Seleção | Lista de URLs, `latest_years`, `include_current` | `pick_urls` | `items_urls` (periodo, url) + `meta_url` |
| Download | Cada `url` | `stream_download` (stream → arquivo local) | Arquivo local, `sha256`, `size_bytes` |
| Upload | Arquivo local, `object_path` | `storage.upload_file` | Objeto armazenado no bucket |
| Manifest | Dados de upload + metadados | `build_manifest` | Dicionário de manifest |

Não há estado persistente interno ao módulo; todas as variáveis são locais à execução da função `sync`.

---

## 7. Conexões com outros arquivos do projeto

| Módulo | Tipo de vínculo | Comentário |
|--------|----------------|------------|
| `forest_pipelines.http` (`stream_download`) | Importação de função utilitária | Responsável pelo download com cálculo de hash. |
| `forest_pipelines.manifests` (`build_manifest`) | Importação de função de construção de manifest | Gera a estrutura de saída padrão do projeto. |
| **Nenhum** | Exportado para outros módulos | O arquivo não é importado por nenhum outro componente (conforme metadados atuais). |

> **Observação:** Caso novos módulos passem a consumir `sync` ou `load_dataset_cfg`, será necessário atualizar a documentação de dependências.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas

| Área | Risco / Limitação | Recomendações |
|------|-------------------|---------------|
| **Validação de configuração** | Falha silenciosa se campos opcionais (`title`) forem omitidos; apenas `source_dataset_url` e `bucket_prefix` são obrigatórios. | Documentar explicitamente os campos esperados no YAML e validar tipos (ex.: `latest_years` > 0). |
| **Resiliência de rede** | `requests.get` usa timeout fixo de 60 s; não há retry automático. | Implementar política de retry (ex.: `urllib3.util.retry.Retry`) ou usar `httpx` com back‑off. |
| **Parsing HTML** | Depende da classe CSS `resource-url-analytics`; alterações na página da CVM podem quebrar a extração. | Isolar a lógica de extração em função testável e incluir teste de integração que verifica a presença da classe. |
| **Seleção de anos** | `latest_years` mínimo é 1, mas a lógica `max(1, latest_years)` pode incluir mais arquivos que o usuário espera se houver múltiplos arquivos por ano. | Garantir que a fonte da CVM realmente produz um único CSV por ano ou adaptar a filtragem para deduplicar por ano. |
| **MIME Types** | MIME hard‑coded (`text/csv; charset=utf-8`, `text/plain; charset=utf-8`). | Detectar MIME dinamicamente ou parametrizar via configuração. |
| **Teste unitário** | Funções dependem de I/O externo (HTTP, filesystem, storage). | Mockar `requests`, `stream_download` e `storage` em testes unitários; incluir cobertura de caminhos de erro (ex.: URL inexistente, falha de upload). |
| **Documentação de manifest** | O formato retornado por `build_manifest` não é descrito aqui. | Referenciar a documentação de `build_manifest` ou incluir exemplo de saída. |
| **Tipagem** | Função `sync` usa `Any` para `settings`, `storage` e `logger`. | Definir protocolos (PEP‑544) ou classes base para melhorar a tipagem estática. |

--- 

*Esta documentação segue as diretrizes de estilo solicitadas: linguagem pt‑BR, tom acadêmico/técnico, uso de Markdown estruturado e blocos de código limitados.*
