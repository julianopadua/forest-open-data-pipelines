# 📄 Documentação – `datasets/cvm/fii_doc_inf_trimestral.py`

---

## 1. Visão geral e responsabilidade
Este módulo implementa a **sincronização** dos arquivos de informação trimestral de Fundos de Investimento Imobiliário (FII) disponibilizados pela CVM.  
Ele:

1. Carrega a configuração do dataset a partir de um arquivo YAML.  
2. Descobre, via scraping, as URLs dos recursos (ZIPs) publicados no portal da CVM.  
3. Seleciona os arquivos dos últimos *N* anos (ou meses, por compatibilidade) e, opcionalmente, o arquivo de metadados.  
4. Faz download em streaming, calcula hash SHA‑256 e tamanho, e envia os artefatos para o storage configurado.  
5. Constrói e devolve um **manifest** descrevendo os itens armazenados.

---

## 2. Onde este arquivo se encaixa na arquitetura
| Camada / Domínio | Papel |
|------------------|-------|
| **Data Ingestion** (pipeline de coleta) | Responsável por obter dados externos (CVM) e materializá‑los em storage interno. |
| **Domain** – *cvm* | Concentra lógica específica de datasets da CVM. |
| **Util / Infra** | Usa utilitários genéricos (`stream_download`, `build_manifest`) que abstraem HTTP e geração de manifestos. |

Não há exposição direta a UI ou camada de aplicação; o módulo é invocado por scripts/CLI que orquestram pipelines.

---

## 3. Interfaces e exports (o que ele expõe)

| Nome | Tipo | Descrição |
|------|------|-----------|
| `DatasetCfg` | `@dataclass(frozen=True)` | Estrutura imutável que contém `id`, `title`, `source_dataset_url`, `bucket_prefix` e `latest_years`. |
| `load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg` | Função | Lê o arquivo YAML de configuração e devolve um `DatasetCfg`. |
| `extract_resource_urls(dataset_url: str) -> list[str]` | Função | Faz *GET* na página do dataset e retorna todas as URLs de recursos encontradas. |
| `pick_latest_year_zip_urls(urls: list[str], latest_years: int) -> tuple[list[tuple[str, str]], str | None]` | Função | Filtra URLs de arquivos ZIP por ano, ordena decrescente e devolve até `latest_years` pares `(year, url)` + URL de metadados (se houver). |
| `sync(settings: Any, storage: Any, logger: Any, latest_months: int | None = None) -> dict[str, Any]` | Função | Orquestra todo o fluxo descrito acima e retorna o manifesto gerado. |

Nenhum outro símbolo é exportado (não há `__all__` explícito, mas apenas os itens acima são de interesse público).

---

## 4. Dependências e acoplamentos

| Tipo | Biblioteca / Módulo | Motivo |
|------|----------------------|--------|
| **Externa** | `re`, `dataclasses`, `pathlib`, `typing`, `requests`, `yaml`, `bs4` (BeautifulSoup) | Operações de regex, I/O de arquivos, tipagem, HTTP, parsing YAML e HTML. |
| **Interna** | `forest_pipelines.http.stream_download` | Função genérica que realiza download em streaming e calcula SHA‑256/tamanho. |
| **Interna** | `forest_pipelines.manifests.build_manifest` | Constrói o dicionário de manifesto padronizado. |

O módulo **não** depende de outros datasets ou de lógica de negócio específica; seu acoplamento está limitado a utilitários de infraestrutura.

---

## 5. Leitura guiada do código (top‑down)

### 5.1 Constantes
```python
RE_YEAR_ZIP = re.compile(r"inf_trimestral_fii_(\d{4})\.zip$", re.IGNORECASE)
```
Regex que captura o ano (4 dígitos) no nome do arquivo ZIP de dados trimestrais.

### 5.2 `DatasetCfg`
```python
@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    latest_years: int
```
Objeto imutável usado em todo o fluxo para evitar inconsistências de configuração.

### 5.3 `load_dataset_cfg`
* Lê `<datasets_dir>/<dataset_id>.yml`.  
* Preenche campos ausentes com valores padrão (`id` = `dataset_id`, `title` = `id`).  
* Resolve `source_dataset_url` a partir de `source_dataset_url` ou `dataset_slug`.  
* Valida presença de `bucket_prefix`.  
* Converte `latest_years` para `int` (padrão 5).  
* Levanta `ValueError` caso requisitos essenciais falhem.

### 5.4 `extract_resource_urls`
* Executa `requests.get` com timeout de 60 s.  
* Usa BeautifulSoup para selecionar links com a classe CSS `resource-url-analytics`.  
* Filtra apenas URLs absolutas (`http*`).  
* Retorna lista **ordenada e deduplicada**.

### 5.5 `pick_latest_year_zip_urls`
* Percorre a lista de URLs recebida.  
* Identifica o *meta* ZIP (`meta_inf_trimestral_fii`) e o armazena separadamente.  
* Aplica `RE_YEAR_ZIP` para extrair o ano dos demais arquivos.  
* Ordena por ano decrescente e devolve até `max(1, latest_years)` pares `(year, url)`.  
* Retorna também a URL do meta (ou `None`).

### 5.6 `sync`
```python
def sync(settings: Any, storage: Any, logger: Any,
         latest_months: int | None = None) -> dict[str, Any]:
```
1. **Carrega configuração** (`load_dataset_cfg`).  
2. Determina número de períodos a considerar (`ly`).  
3. **Descobre URLs** (`extract_resource_urls`).  
4. **Seleciona arquivos** (`pick_latest_year_zip_urls`).  
5. **Itera sobre os ZIPs**:
   * Define caminho local (`settings.data_dir / "cvm_fii_doc_inf_trimestral" / filename`).  
   * Faz download via `stream_download`.  
   * Constrói caminho de objeto no bucket (`{bucket_prefix}/data/{year}/{filename}`).  
   * Faz upload (`storage.upload_file`) com MIME `application/zip`.  
   * Obtém URL pública (`storage.public_url`).  
   * Popula dicionário de item (inclui `sha256`, `size_bytes`, etc.).
6. **Processa meta**, se presente, seguindo lógica análoga (bucket `meta/`).  
7. **Constrói manifesto** usando `build_manifest` com `items` e `meta`.  
8. Retorna o manifesto.

**Invariantes**:
* Sempre há ao menos um ZIP de dados (garantido por `max(1, latest_years)`).  
* O caminho de bucket segue a estrutura `{bucket_prefix}/data/{year}/...` para dados e `{bucket_prefix}/meta/...` para metadados.  
* O download é feito em **streaming**, evitando carregamento completo em memória.

---

## 6. Fluxo de dados / estado / eventos

1. **Entrada**:  
   * `settings` – objeto contendo `datasets_dir` (Path) e `data_dir` (Path).  
   * `storage` – abstração de backend de armazenamento (ex.: S3).  
   * `logger` – logger padrão (ex.: `logging.Logger`).  
   * `latest_months` – opcional, sobrescreve `cfg.latest_years`.

2. **Processamento**:  
   * Leitura de configuração → URLs → filtragem → download → upload → geração de metadados.

3. **Saída**:  
   * Dicionário de manifesto contendo:
     * `dataset_id`, `title`, `source_dataset_url`, `bucket_prefix`.
     * Lista `items` (um por ano) com atributos de integridade e localização.
     * Opcional `meta` com informações do arquivo de metadados.

Não há estado persistente interno ao módulo; todo estado transitório reside em objetos temporários (`dl`, `items`).

---

## 7. Conexões com outros arquivos do projeto

| Módulo importado | Propósito | Link (relativo) |
|------------------|-----------|-----------------|
| `forest_pipelines.http.stream_download` | Realiza download em streaming, calcula SHA‑256 e tamanho. | `../http.py` |
| `forest_pipelines.manifests.build_manifest` | Gera estrutura de manifesto padronizada. | `../manifests/build_manifest.py` |

> **Observação**: Não há importações internas adicionais nem exportações para outros módulos; a função `sync` costuma ser chamada por scripts de orquestração (ex.: CLI ou scheduler) que não constam neste repositório.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas

| Área | Risco / Limitação | Recomendações |
|------|-------------------|---------------|
| **Validação de configuração** | Falha silenciosa se campos opcionais (`title`) forem omitidos; apenas `source_dataset_url` ou `dataset_slug` são obrigatórios. | Documentar schema YAML e, se possível, validar com `jsonschema` ou `pydantic`. |
| **Scraping HTML** | Dependente da estrutura da página da CVM (`a.resource-url-analytics`). Alterações no front‑end podem quebrar a extração. | Isolar lógica de parsing em módulo separado e incluir testes de integração contra página de exemplo. |
| **Timeouts e retries** | `requests.get` usa timeout único de 60 s e não há política de retry. | Incorporar `urllib3.util.retry.Retry` ou `tenacity` para tentativas automáticas. |
| **Limite de anos** | `max(1, latest_years)` garante ao menos um arquivo, mas pode baixar dados desnecessários se `latest_years` for 0 por engano. | Validar `latest_years` > 0 na carga da configuração. |
| **Upload idempotente** | `storage.upload_file(..., upsert=True)` assume que o backend aceita sobrescrita; comportamento pode variar. | Documentar contrato esperado do objeto `storage`. |
| **Manuseio de erros de download** | `stream_download` pode lançar exceções que não são capturadas, interrompendo o pipeline inteiro. | Envolver download em bloco `try/except`, registrar falhas e continuar com os demais arquivos. |
| **Testabilidade** | Funções dependem de I/O externo (HTTP, filesystem, storage). | Injetar abstrações (ex.: `http_client`, `fs`) e escrever testes unitários com mocks. |
| **Tipagem** | Parâmetros `settings`, `storage`, `logger` são tipados como `Any`. | Definir protocolos (`Protocol`) ou classes base para melhorar a verificação estática. |

Implementar as melhorias acima aumentará a robustez, a manutenibilidade e a capacidade de teste do módulo.
