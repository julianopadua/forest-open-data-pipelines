## 1. Visão geral e responsabilidade  

O módulo **`datasets/cvm/fi_inf_diario.py`** implementa a extração, download e publicação dos arquivos ZIP contendo informações diárias de fundos de investimento (FI) disponibilizados pela CVM.  
Ele lê a configuração do dataset, identifica os recursos mais recentes na página da CVM, realiza o download em streaming, envia os artefatos para um bucket de armazenamento público e gera um *manifest* descrevendo os itens publicados.

---

## 2. Posicionamento na arquitetura  

| Camada / Domínio | Descrição |
|------------------|-----------|
| **Data Ingestion** (pipeline de ingestão) | Responsável por coletar dados externos (CVM) e disponibilizá‑los em armazenamento interno. |
| **Utilitário** | Usa funções auxiliares de `forest_pipelines.http` (download streaming) e `forest_pipelines.manifests.build_manifest` (construção de manifest). |
| **Nenhum UI** | Não há interação direta com camada de apresentação. |

O módulo está localizado na sub‑pasta `datasets/cvm`, indicando que pertence ao domínio de **datasets públicos da CVM**.

---

## 3. Interfaces e exports  

| Export | Tipo | Descrição |
|--------|------|-----------|
| `DatasetCfg` | `@dataclass(frozen=True)` | Estrutura imutável que agrupa as propriedades de configuração do dataset (id, title, URL fonte, prefixo de bucket, número de meses a considerar). |
| `load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg` | Função | Carrega e valida o arquivo YAML de configuração (`{dataset_id}.yml`). |
| `extract_resource_urls(dataset_url: str) -> list[str]` | Função | Faz *scraping* da página do dataset e devolve a lista de URLs de recursos (ZIPs e meta‑arquivo). |
| `pick_latest_zip_urls(urls: list[str], latest_months: int) -> tuple[list[tuple[str, str]], str | None]` | Função | Filtra URLs de arquivos ZIP, extrai o período (YYYY‑MM) e devolve os *N* mais recentes, além da URL do arquivo meta (se houver). |
| `sync(settings: Any, storage: Any, logger: Any, latest_months: int | None = None) -> dict[str, Any]` | Função | Orquestra todo o fluxo: carrega configuração, obtém recursos, baixa arquivos, envia ao bucket e retorna o manifest final. |

Nenhum objeto é exportado como classe ou módulo adicional.

---

## 4. Dependências e acoplamentos  

| Tipo | Biblioteca / Módulo | Motivo |
|------|----------------------|--------|
| **Externa** | `re`, `dataclasses`, `pathlib`, `typing`, `requests`, `yaml`, `bs4` (BeautifulSoup) | Operações de regex, I/O de arquivos, tipagem, requisições HTTP, parsing de YAML e HTML. |
| **Interna** | `forest_pipelines.http.stream_download` | Função de download em streaming que devolve objeto com `file_path`, `sha256` e `size_bytes`. |
| **Interna** | `forest_pipelines.manifests.build_manifest` | Constrói o dicionário de manifest a partir dos itens coletados. |

O módulo não depende de nenhum outro dataset ou camada de negócio; o acoplamento está restrito a utilitários de I/O e ao contrato esperado de `storage` (métodos `upload_file` e `public_url`) e `logger`.

---

## 5. Leitura guiada do código (top‑down)

1. **Importações e regex**  
   ```python
   RE_ZIP = re.compile(r"inf_diario_fi_(\d{6})\.zip$", re.IGNORECASE)
   ```  
   Regex captura o período `YYYYMM` presente no nome do arquivo ZIP.

2. **`DatasetCfg`** – dataclass imutável que garante consistência de configuração ao longo da execução.

3. **`load_dataset_cfg`**  
   - Lê `<datasets_dir>/<dataset_id>.yml`.  
   - Suporta dois formatos: completo (campo `source_dataset_url`) ou “slug‑based” (campo `dataset_slug`).  
   - Valida presença de `bucket_prefix`.  
   - Converte `latest_months` para `int` (padrão 12).  
   - Lança `ValueError` caso campos obrigatórios estejam ausentes.

4. **`extract_resource_urls`**  
   - Executa `GET` com timeout de 60 s.  
   - Usa BeautifulSoup para selecionar anchors com classe `resource-url-analytics`.  
   - Filtra apenas URLs que começam com `http`.  
   - Retorna lista ordenada e deduplicada.

5. **`pick_latest_zip_urls`**  
   - Percorre URLs, identifica arquivos meta (`meta_inf_diario_fi*.txt`) e arquivos ZIP via `RE_ZIP`.  
   - Constrói tupla `(periodo, url)`.  
   - Ordena decrescentemente por período e devolve os *N* mais recentes (`latest_months`).  
   - Retorna também a URL do meta‑arquivo (ou `None`).

6. **`sync`** – ponto de entrada principal.  
   - Carrega configuração (`load_dataset_cfg`).  
   - Determina número de meses a processar (`lm`).  
   - Obtém todas as URLs (`extract_resource_urls`).  
   - Seleciona os ZIPs e meta (`pick_latest_zip_urls`).  
   - **Loop ZIPs**:  
     - Define caminho local (`settings.data_dir / "cvm_fi_inf_diario" / filename`).  
     - Faz download via `stream_download`.  
     - Constrói caminho no bucket (`{bucket_prefix}/data/{period}/{filename}`).  
     - Faz upload (`storage.upload_file`) com MIME `application/zip`.  
     - Registra informações (SHA‑256, tamanho, URLs) em `items`.  
   - **Meta (opcional)**: processo análogo, porém MIME `text/plain; charset=utf-8` e caminho `{bucket_prefix}/meta/{filename}`.  
   - Invoca `build_manifest` com todos os itens e o objeto meta, retornando o dicionário final.

**Decisões de implementação relevantes**  
- **Imutabilidade** da configuração (`DatasetCfg`) evita alterações inesperadas.  
- **Separação de responsabilidades**: extração de URLs, filtragem de períodos e sincronização são funções distintas, facilitando testes unitários.  
- **Uso de `sorted(set(...))`** garante ordem determinística e elimina duplicatas.  
- **Não captura exceções** dentro de `sync`; permite propagação do traceback para depuração, mas evita exposição de credenciais (`repr(storage)` foi sanitizado no dataclass).  

---

## 6. Fluxo de dados / estado  

```
YAML config  ──► DatasetCfg ──► settings (paths) ──►
   │                                 │
   ▼                                 ▼
extract_resource_urls ──► lista de URLs (ZIP + meta)
   │                                 │
   ▼                                 ▼
pick_latest_zip_urls ──► [(period, zip_url), ...] + meta_url
   │                                 │
   ▼                                 ▼
Para cada zip_url:
   stream_download ──► arquivo local + hash/tamanho
   storage.upload_file ──► objeto no bucket
   → coleta metadados → items[]
   (mesmo fluxo para meta_url, se houver)
   │
   ▼
build_manifest ──► dicionário manifest (retorno)
```

O estado interno é mantido apenas nas variáveis locais (`items`, `meta_obj`). Não há mutabilidade global.

---

## 7. Conexões com outros arquivos do projeto  

| Módulo referenciado | Tipo de vínculo | Comentário |
|---------------------|----------------|------------|
| `forest_pipelines.http.stream_download` | Importação de função utilitária | Responsável por download em streaming e cálculo de SHA‑256. |
| `forest_pipelines.manifests.build_manifest` | Importação de função de construção de manifest | Centraliza a estrutura de saída esperada pelos pipelines de publicação. |
| `settings` (objeto passado a `sync`) | Dependência de configuração externa | Deve possuir atributos `datasets_dir` (Path) e `data_dir` (Path). |
| `storage` (objeto passado a `sync`) | Dependência de camada de persistência | Precisa implementar `upload_file` e `public_url`. |
| `logger` (objeto passado a `sync`) | Dependência de logging | Utiliza métodos `info`. |

Nenhum outro módulo importa este arquivo; ele funciona como ponto de entrada autônomo para o dataset *cvm_fi_inf_diario*.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Risco / Observação | Sugestão de melhoria |
|------|--------------------|----------------------|
| **Validação de YAML** | `yaml.safe_load` pode retornar `None` → `raw` tratado, mas campos ausentes geram `ValueError`. | Manter a validação atual; considerar uso de schema (e.g., `cerberus` ou `pydantic`) para mensagens de erro mais detalhadas. |
| **Timeout fixo de 60 s** | Em redes lentas o download da página pode falhar. | Tornar o timeout configurável via `settings`. |
| **Dependência de estrutura HTML** | Seleção `a.resource-url-analytics` assume layout estável da CVM. | Implementar fallback ou teste de regressão que avise caso a classe CSS seja alterada. |
| **Hard‑coded MIME types** | `application/zip` e `text/plain; charset=utf-8` são fixos. | Extrair MIME a partir da extensão ou cabeçalhos HTTP para maior robustez. |
| **Sem retry** | Falha em `requests.get` ou `stream_download` aborta o pipeline. | Incorporar política de retry exponencial (e.g., `urllib3.util.retry`). |
| **Limite de `latest_months`** | Caso `latest_months` > número de arquivos disponíveis, a função ainda funciona, mas pode gerar logs confusos. | Logar explicitamente quando o número de arquivos encontrados for menor que o solicitado. |
| **Teste unitário** | Funções puras (`extract_resource_urls`, `pick_latest_zip_urls`) são testáveis, mas dependem de rede. | Mockar `requests.get` e `stream_download` nos testes. |
| **Documentação de tipos** | Anotações genéricas `Any` para `settings`, `storage`, `logger`. | Definir protocolos (PEP 544) ou interfaces mínimas para melhorar a tipagem estática. |

Implementar as melhorias acima aumentará a resiliência, a manutenibilidade e a clareza do módulo.
