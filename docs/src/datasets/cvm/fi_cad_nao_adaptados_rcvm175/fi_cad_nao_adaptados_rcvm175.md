# Documentação do módulo `fi_cad_nao_adaptados_rcvm175.py`

---

## 1. Visão geral e responsabilidade  

Este módulo implementa a extração, download e publicação dos recursos do dataset **CVM – FI Cad Não Adaptados RCVM 175**.  
Ele:

1. Carrega a configuração do dataset a partir de um arquivo YAML.  
2. Descobre, na página web do dataset, as URLs dos arquivos de dados (`cad_fi.csv`) e de metadados (`meta_cad_fi.txt`).  
3. Faz download desses arquivos usando a rotina de streaming da camada de HTTP.  
4. Persiste os arquivos em um storage (ex.: bucket S3) e gera URLs públicas.  
5. Constrói e devolve um *manifest* que descreve os artefatos produzidos.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada / Domínio | Papel |
|------------------|-------|
| **Datasets** (sub‑pacote `forest_pipelines.datasets.cvm`) | Conjunto de pipelines de ingestão de datasets da CVM. |
| **Camada de integração / ETL** | Orquestra a coleta de recursos externos, o armazenamento e a geração de metadados. |
| **Utilitário** | Não contém lógica de UI nem de negócio; serve como adaptador entre a fonte HTTP e o storage. |

---

## 3. Interfaces e exports (o que ele expõe)

| Nome | Tipo | Descrição |
|------|------|-----------|
| `DatasetCfg` | `@dataclass(frozen=True)` | Estrutura imutável que agrupa as propriedades essenciais do dataset (id, título, URL da fonte, prefixo de bucket). |
| `load_dataset_cfg(datasets_dir: Path, dataset_id: str) -> DatasetCfg` | Função | Lê o arquivo YAML de configuração e devolve um `DatasetCfg`. Levanta `ValueError` se campos obrigatórios estiverem ausentes. |
| `extract_resource_urls(dataset_url: str) -> list[str]` | Função | Faz *GET* na página do dataset e devolve a lista de URLs de recursos encontradas. |
| `find_by_filename(urls: list[str], filename: str) -> str` | Função | Busca, na lista de URLs, a que corresponde exatamente ao nome de arquivo informado (case‑insensitive). |
| `sync(settings: Any, storage: Any, logger: Any, latest_months: int | None = None) -> dict[str, Any]` | Função | Pipeline completo de sincronização; retorna o *manifest* gerado por `build_manifest`. |

Nenhum outro símbolo é exportado (não há `__all__` explícito, mas apenas os itens acima são importáveis publicamente).

---

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Biblioteca | Motivo |
|------|----------------------|--------|
| **Externa** | `requests` | Realiza requisição HTTP à página do dataset. |
|  | `beautifulsoup4` (`bs4`) | Faz parsing HTML para extrair links de recursos. |
|  | `yaml` (`PyYAML`) | Carrega o arquivo de configuração em YAML. |
|  | `dataclasses`, `pathlib`, `typing` | Utilitários padrão da linguagem. |
| **Interna** | `forest_pipelines.http.stream_download` | Função de download com streaming que devolve objeto contendo `file_path`, `sha256` e `size_bytes`. |
|  | `forest_pipelines.manifests.build_manifest` | Constrói o dicionário de manifesto a partir dos itens coletados. |
| **Acoplamento** | `settings` (qualquer objeto) | Espera atributos `datasets_dir` e `data_dir`. Não há tipagem explícita; o contrato é implícito. |
|  | `storage` (qualquer objeto) | Precisa implementar `upload_file(path, local_path, mime, upsert)` e `public_url(path)`. |
|  | `logger` (qualquer objeto) | Deve possuir método `info(msg, *args)`. |

O módulo não depende de outros componentes do projeto além das duas funções citadas acima.

---

## 5. Leitura guiada do código (top‑down)

### 5.1 Definição da configuração  

```python
@dataclass(frozen=True)
class DatasetCfg:
    id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
```

*Imutabilidade* garante que a configuração não seja alterada após carregada.

### 5.2 `load_dataset_cfg`

1. Constrói o caminho `<datasets_dir>/<dataset_id>.yml`.  
2. Carrega o YAML com `yaml.safe_load`.  
3. Preenche campos faltantes (`id`, `title`) usando valores padrão.  
4. Determina a URL da fonte:  
   - Usa `source_dataset_url` se presente.  
   - Caso contrário, compõe a partir de `dataset_slug` (`https://dados.cvm.gov.br/dataset/{slug}`).  
5. Valida a presença de `source_dataset_url` e `bucket_prefix`, lançando `ValueError` se ausentes.  
6. Retorna a instância `DatasetCfg`.

### 5.3 `extract_resource_urls`

1. `GET` na `dataset_url` com timeout de 60 s.  
2. Verifica status (`raise_for_status`).  
3. Analisa o HTML com `BeautifulSoup`.  
4. Seleciona elementos `<a>` com a classe `resource-url-analytics`.  
5. Coleta atributos `href` que iniciam com `http`.  
6. Remove duplicatas e devolve lista ordenada.

### 5.4 `find_by_filename`

Percorre a lista de URLs, extrai o nome do arquivo (parte final do caminho, sem query string) e compara, ignorando caixa. Se não encontrar, levanta `ValueError`.

### 5.5 `sync`

| Etapa | Ação |
|-------|------|
| **Carregamento de cfg** | `load_dataset_cfg(settings.datasets_dir, "cvm_fi_cad_nao_adaptados_rcvm175")` |
| **Descoberta de URLs** | `extract_resource_urls` → `find_by_filename` para `cad_fi.csv` e `meta_cad_fi.txt` |
| **Download de dados** | `stream_download(data_url, local_path)` |
| **Upload de dados** | `storage.upload_file(object_path, local_path, "text/csv; charset=utf-8", upsert=True)` |
| **Public URL** | `storage.public_url(object_path)` |
| **Montagem de item** | Dicionário com metadados (SHA‑256, tamanho, caminhos, URLs). |
| **Download e upload de metadados** | Idêntico ao fluxo de dados, porém MIME `text/plain`. |
| **Construção do manifesto** | `build_manifest` recebe `dataset_id`, `title`, `source_dataset_url`, `bucket_prefix`, lista `items` e objeto `meta`. |
| **Retorno** | Dicionário de manifesto. |

Observações de implementação:

* O parâmetro `latest_months` é mantido apenas por compatibilidade com a CLI; não é usado.  
* O caminho local de armazenamento segue a estrutura `settings.data_dir / "cvm_fi_cad_nao_adaptados_rcvm175" / <filename>`.  
* O prefixo de bucket (`cfg.bucket_prefix`) é concatenado com sub‑caminhos `data/atual/` e `meta/`.  

---

## 6. Fluxo de dados / estado / eventos  

1. **Entrada**:  
   - `settings.datasets_dir` (Path) – diretório contendo o YAML de configuração.  
   - `settings.data_dir` (Path) – raiz onde os arquivos baixados são temporariamente armazenados.  
   - `storage` – abstração de backend de objetos (ex.: S3).  
   - `logger` – coletor de logs.  

2. **Processamento**:  
   - Leitura de configuração → descoberta de URLs → download (stream) → cálculo de hash SHA‑256 e tamanho → upload.  

3. **Saída**:  
   - Dicionário de manifesto contendo metadados de cada artefato (data e meta).  
   - Arquivos persistidos no storage com URLs públicas.  

Não há estado mutável compartilhado entre chamadas; cada execução de `sync` é idempotente (o upload usa `upsert=True`).  

---

## 7. Conexões com outros arquivos do projeto  

| Módulo | Tipo de vínculo | Comentário |
|--------|----------------|------------|
| `forest_pipelines.http.stream_download` | Importação de função | Responsável por download com streaming e cálculo de hash. |
| `forest_pipelines.manifests.build_manifest` | Importação de função | Gera o dicionário de manifesto padronizado. |
| (Nenhum outro módulo importa este arquivo) | — | O módulo funciona como ponto de entrada independente para o dataset específico. |

*Links para a documentação dos módulos importados* (não disponíveis no enunciado) devem ser inseridos quando a documentação do projeto for gerada.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Risco / Impacto | Recomendações |
|------|-----------------|---------------|
| **Validação de YAML** | O código assume que o arquivo existe e contém YAML válido; `yaml.safe_load` pode retornar `None`. Já tratado, mas erros de sintaxe geram exceção não capturada. | Envolver a leitura em `try/except yaml.YAMLError` e propagar erro mais amigável. |
| **Dependência de estrutura HTML** | `extract_resource_urls` depende da classe CSS `resource-url-analytics`. Alterações na página da CVM quebrarão a extração. | Isolar a lógica de parsing em função testável e incluir fallback ou teste de integração periódico. |
| **Hard‑coded filenames** | `data_filename` e `meta_filename` são fixos. Caso o dataset altere nomes, o pipeline falhará. | Tornar os nomes configuráveis via YAML (ex.: `data_filename`, `meta_filename`). |
| **Uso de `Any` nos parâmetros** | Falta de tipagem explícita para `settings`, `storage` e `logger` dificulta análise estática. | Definir protocolos (PEP 544) ou classes de interface para esses objetos. |
| **Timeout fixo de 60 s** | Em ambientes com conexão lenta, o `requests.get` pode falhar. | Tornar o timeout configurável ou implementar retry com back‑off. |
| **Upload sem verificação de integridade** | O código confia que `stream_download` calculou o hash corretamente; não há verificação pós‑upload. | Opcionalmente, validar o hash no storage (ex.: usando ETag). |
| **Ausência de testes unitários** | Nenhum teste está presente no repositório (não inferido). | Criar testes que mockam `requests`, `stream_download` e `storage` para validar cada etapa. |

--- 

*Esta documentação foi gerada com base no código-fonte fornecido e nas convenções do projeto.*
