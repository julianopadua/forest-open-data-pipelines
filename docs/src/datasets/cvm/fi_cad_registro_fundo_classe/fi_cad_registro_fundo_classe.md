## 1. Visão geral e responsabilidade  

Este módulo implementa a **sincronização** do dataset *cvm_fi_cad_registro_fundo_classe* disponibilizado pela CVM (Comissão de Valores Mobiliários).  
Ele:

* Carrega a configuração do dataset a partir de um arquivo YAML.  
* Descobre, na página web do dataset, as URLs dos arquivos de dados e de metadados.  
* Faz download dos arquivos (streaming) e os envia para um storage configurado.  
* Constrói e devolve um *manifest* que descreve os artefatos armazenados.

O objetivo é garantir que a versão “atual” dos arquivos esteja sempre disponível no bucket definido, com metadados de integridade (SHA‑256, tamanho) e URLs públicas.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada / Domínio | Papel |
|------------------|-------|
| **Datasets → CVM** | Conector específico para o dataset *fi_cad_registro_fundo_classe* da CVM. |
| **Camada de ingestão** | Responsável por obter dados externos, normalizar e armazenar. |
| **Utilitário de manifesto** | Usa `forest_pipelines.manifests.build_manifest` para gerar o registro de versão. |

Não há dependência direta de UI ou de camada de aplicação; o módulo é invocado por scripts de orquestração (ex.: CLI ou scheduler) que fornecem os objetos `settings`, `storage` e `logger`.

---

## 3. Interfaces e exports (o que ele expõe)

| Nome | Tipo | Descrição |
|------|------|-----------|
| `DatasetCfg` | `@dataclass(frozen=True)` | Estrutura imutável que contém `id`, `title`, `source_dataset_url` e `bucket_prefix`. |
| `load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg` | Função | Lê o arquivo YAML de configuração e devolve um `DatasetCfg`. |
| `extract_resource_urls(dataset_url: str) -> list[str]` | Função | Faz *scraping* da página do dataset e retorna todas as URLs de recursos encontradas. |
| `find_by_filename(urls: list[str], filename: str) -> str` | Função | Busca, entre as URLs, a que corresponde exatamente ao nome de arquivo informado. |
| `sync(settings: Any, storage: Any, logger: Any, latest_months: int | None = None) -> dict[str, Any]` | Função | Orquestra todo o fluxo descrito na Visão geral e devolve o manifesto gerado. |

Nenhum outro símbolo é exportado (não há `__all__` explícito, mas apenas os itens acima são importáveis de forma pública).

---

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Biblioteca | Motivo da dependência |
|------|----------------------|-----------------------|
| **Externa** | `requests` | HTTP GET da página do dataset. |
|  | `beautifulsoup4` (`bs4`) | Parsing HTML para extrair links. |
|  | `yaml` (`PyYAML`) | Leitura de arquivos de configuração. |
|  | `dataclasses`, `pathlib`, `typing` | Tipagem e manipulação de caminhos. |
| **Interna** | `forest_pipelines.http.stream_download` | Função utilitária que realiza download em streaming e calcula SHA‑256 e tamanho. |
|  | `forest_pipelines.manifests.build_manifest` | Constrói o dicionário de manifesto a partir dos artefatos. |
| **Objetos de runtime** | `settings`, `storage`, `logger` (tipados como `Any`) | São injetados pelo chamador; a implementação concreta não pode ser inferida a partir deste código. |

O módulo tem **acoplamento baixo** com a camada de storage (interface genérica) e **acoplamento alto** com a estrutura de configuração (exige campos específicos no YAML).

---

## 5. Leitura guiada do código (top‑down)

1. **Importações** – trazem apenas o necessário; não há importação circular.  
2. **`DatasetCfg`** – dataclass imutável que garante consistência após carregamento.  
3. **`load_dataset_cfg`**  
   * Monta o caminho `<datasets_dir>/<dataset_id>.yml`.  
   * Usa `yaml.safe_load`; se o arquivo estiver vazio, `raw` será `{}`.  
   * Preenche `id` e `title` com valores padrão (`dataset_id` ou `id`).  
   * Determina `source_dataset_url` a partir de `source_dataset_url` ou, alternativamente, de `dataset_slug` (concatena com `https://dados.cvm.gov.br/dataset/`).  
   * Valida a presença de `source_dataset_url` e `bucket_prefix`, lançando `ValueError` caso faltem.  
4. **`extract_resource_urls`**  
   * Executa `GET` com timeout de 60 s; levanta exceção HTTP se falhar.  
   * Analisa o HTML com `BeautifulSoup`.  
   * Seleciona elementos `<a>` com classe `resource-url-analytics`, coleta `href` que iniciam com `http`.  
   * Remove duplicatas e devolve lista ordenada.  
5. **`find_by_filename`**  
   * Percorre a lista de URLs, extrai o nome de arquivo (parte final antes de `?`).  
   * Comparação case‑insensitive; retorna a primeira correspondência ou lança `ValueError`.  
6. **`sync`** – ponto de entrada principal:  
   * Carrega a configuração usando `load_dataset_cfg`.  
   * Define nomes esperados dos arquivos zip (`registro_fundo_classe.zip` e `meta_registro_fundo_classe.zip`).  
   * Obtém todas as URLs do dataset e seleciona as corretas via `find_by_filename`.  
   * **Download de dados**:  
     - Cria caminho local sob `settings.data_dir/...`.  
     - Usa `stream_download` (retorna objeto com `file_path`, `sha256`, `size_bytes`).  
     - Faz upload para `storage` no caminho `<bucket_prefix>/data/atual/<filename>`.  
     - Captura URL pública.  
   * **Download de metadados**: fluxo análogo, porém caminho de bucket `<bucket_prefix>/meta/<filename>`.  
   * Monta lista `items` (apenas o artefato de dados) e dicionário `meta_obj`.  
   * Chama `build_manifest` com todos os parâmetros e devolve o manifesto.  

**Invariantes**:  
* `source_dataset_url` nunca é `None` após `load_dataset_cfg`.  
* As URLs retornadas por `extract_resource_urls` são únicas e ordenadas.  
* O nome dos arquivos esperados é fixo; qualquer mudança no dataset exigirá alteração de código.

---

## 6. Fluxo de dados/estado/eventos  

```
settings (datasets_dir, data_dir) ──► load_dataset_cfg ──► DatasetCfg
                                   │
                                   ▼
                     extract_resource_urls (HTTP GET) ──► list[URL]
                                   │
                                   ▼
                     find_by_filename ──► data_url, meta_url
                                   │
                                   ▼
               stream_download (data_url) ──► local file, sha256, size
                                   │
                                   ▼
               storage.upload_file (bucket_path) ──► objeto no bucket
                                   │
                                   ▼
               storage.public_url ──► URL pública
                                   │
                                   ▼
               build_manifest ──► dict (manifest)
```

O módulo não mantém estado interno entre chamadas; todo o estado é passado explicitamente via parâmetros e objetos retornados.

---

## 7. Conexões com outros arquivos do projeto  

| Módulo referenciado | Tipo de ligação | Comentário |
|---------------------|-----------------|------------|
| `forest_pipelines.http.stream_download` | Importação de função utilitária | Responsável pelo download em streaming e cálculo de hash. |
| `forest_pipelines.manifests.build_manifest` | Importação de função de construção de manifesto | Gera o dicionário final usado por pipelines downstream. |
| `settings` (objeto passado a `sync`) | Dependência de runtime | Espera atributos `datasets_dir` e `data_dir`; definição não está neste arquivo. |
| `storage` (objeto passado a `sync`) | Dependência de runtime | Deve implementar `upload_file` e `public_url`; interface não especificada aqui. |
| `logger` (objeto passado a `sync`) | Dependência de runtime | Usa método `info`. |

> **Nota:** Não há importações internas adicionais; o módulo é autônomo exceto pelas dependências acima.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Hard‑coded filenames** (`registro_fundo_classe.zip`, `meta_registro_fundo_classe.zip`) | Quebra de compatibilidade se a CVM alterar os nomes. | Externalizar para a configuração YAML (ex.: `data_filename`, `meta_filename`). |
| **Parsing HTML frágil** – depende da classe CSS `resource-url-analytics`. | Mudança no layout da página pode impedir a extração de URLs. | Implementar fallback (ex.: buscar por links que terminem em `.zip`) ou usar API oficial, se disponível. |
| **Tipagem genérica (`Any`) para `settings`, `storage`, `logger`** | Reduz a capacidade de análise estática e pode ocultar erros de contrato. | Definir protocolos (PEP 544) ou classes base com atributos esperados. |
| **Ausência de tratamento de exceções de rede** (ex.: `requests.get` pode falhar). | Falha abrupta da pipeline. | Envolver chamadas de rede em `try/except`, com política de retry/backoff. |
| **Uso de `yaml.safe_load` sem validação de esquema** | Configurações incompletas ou malformadas podem gerar erros posteriores. | Validar contra schema (ex.: `jsonschema`) e reportar mensagens claras. |
| **Upload sem verificação de integridade pós‑upload** | Possível corrupção no bucket sem detecção. | Após `upload_file`, ler o objeto e comparar hash ou usar recursos de verificação do storage. |
| **Dependência de `requests` + `BeautifulSoup`** – duas bibliotecas externas. | Aumenta o tamanho do ambiente e pode gerar conflitos de versão. | Avaliar uso de `httpx` + `lxml` ou API JSON da CVM, se existir. |

Implementar as melhorias acima aumentará a robustez, a manutenibilidade e a testabilidade do módulo.
