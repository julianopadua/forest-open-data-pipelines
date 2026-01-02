## 1. Visão geral e responsabilidade  

O módulo **`fi_cad_icvm555_hist.py`** implementa a rotina de sincronização do dataset *cvm_fi_cad_icvm555_hist* proveniente do portal de dados da CVM.  
Sua responsabilidade principal é:

* Ler a configuração do dataset a partir de um arquivo YAML.  
* Descobrir, via scraping, as URLs dos recursos “data” e “meta”.  
* Efetuar download streaming dos arquivos ZIP.  
* Persistir os arquivos em storage configurado (bucket) e gerar URLs públicas.  
* Construir e devolver um *manifest* que descreve os artefatos baixados.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada / Domínio | Descrição |
|------------------|-----------|
| **Domain – Dados externos** | Trata de um dataset público da CVM (dados financeiros). |
| **Application – Pipeline de ingestão** | É parte da cadeia de pipelines que traz dados brutos para o data lake. |
| **Infrastructure – Storage / HTTP** | Utiliza abstrações de `forest_pipelines.http` e `forest_pipelines.manifests`. |
| **Utilitário** | Contém funções auxiliares (`load_dataset_cfg`, `extract_resource_urls`, `find_by_filename`). |

Não há UI nem camada de apresentação; o módulo é puramente de backend.

---

## 3. Interfaces e exports (o que ele expõe)

| Export | Tipo | Descrição |
|--------|------|-----------|
| `DatasetCfg` | `@dataclass` (immutável) | Estrutura de configuração do dataset (id, title, source URL, bucket prefix). |
| `load_dataset_cfg` | `def(Path, str) -> DatasetCfg` | Carrega e valida a configuração a partir de `<datasets_dir>/<dataset_id>.yml`. |
| `extract_resource_urls` | `def(str) -> list[str]` | Faz *GET* na página do dataset e devolve URLs de recursos marcados com a classe CSS `resource-url-analytics`. |
| `find_by_filename` | `def(list[str], str) -> str` | Busca, entre as URLs, a que corresponde exatamente ao nome de arquivo informado. |
| `sync` | `def(Any, Any, Any, int|None=None) -> dict[str, Any]` | Orquestra todo o fluxo de download, upload e geração de manifest. É a única função chamada por pipelines externos. |

Nenhum outro símbolo é exportado.

---

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Biblioteca | Motivo da dependência |
|------|----------------------|-----------------------|
| **Externa** | `requests` | HTTP GET da página do dataset. |
| | `beautifulsoup4` (`bs4`) | Parsing HTML para extrair URLs. |
| | `pyyaml` (`yaml`) | Leitura segura do arquivo de configuração YAML. |
| | `dataclasses` | Definição imutável de `DatasetCfg`. |
| | `pathlib.Path` | Manipulação de caminhos de arquivos. |
| **Interna** | `forest_pipelines.http.stream_download` | Função de download streaming que devolve objeto com `file_path`, `sha256`, `size_bytes`. |
| | `forest_pipelines.manifests.build_manifest` | Construtor de manifest padronizado para o dataset. |
| **Acoplamento** | `settings` (qualquer objeto) | Espera atributos `datasets_dir` e `data_dir`. Não há tipagem explícita; a interface é implícita. |
| | `storage` (qualquer objeto) | Precisa dos métodos `upload_file` e `public_url`. Também implícito. |
| | `logger` | Usa método `info`. |

O módulo depende de contratos implícitos (`settings`, `storage`, `logger`), o que gera acoplamento fraco a implementações concretas, porém sem verificação estática.

---

## 5. Leitura guiada do código (top‑down)

1. **Importações** – trazem apenas o necessário; não há imports circulares.  
2. **`DatasetCfg`** – dataclass *frozen* garante imutabilidade após carregamento.  
3. **`load_dataset_cfg`**  
   * Constrói caminho `<datasets_dir>/<dataset_id>.yml`.  
   * Usa `yaml.safe_load`; se o arquivo estiver vazio, `raw` será `{}`.  
   * Preenche `id` e `title` com valores padrão (`dataset_id`).  
   * Resolve `source_dataset_url` a partir de `source_dataset_url` ou, alternativamente, `dataset_slug`.  
   * Valida presença de `bucket_prefix`; lança `ValueError` caso falte.  
4. **`extract_resource_urls`**  
   * `GET` com timeout de 60 s; levanta exceção HTTP se falhar.  
   * `BeautifulSoup` parseia o HTML.  
   * Seleciona links com a classe CSS `resource-url-analytics`, filtra por `http` e devolve lista única e ordenada.  
5. **`find_by_filename`**  
   * Percorre a lista de URLs, extrai o nome do arquivo (parte final antes de `?`).  
   * Comparação case‑insensitive; lança `ValueError` se não encontrar.  
6. **`sync`** – ponto de entrada da pipeline:  
   * Carrega configuração (`cvm_fi_cad_icvm555_hist.yml`).  
   * Define nomes esperados dos arquivos ZIP (`cad_fi_hist.zip`, `meta_cad_fi.zip`).  
   * Obtém todas as URLs do dataset e seleciona as corretas via `find_by_filename`.  
   * **Download data**:  
     - `stream_download` grava em `<data_dir>/cvm_fi_cad_icvm555_hist/<data_filename>`.  
     - Upload para `storage` em `<bucket_prefix>/data/atual/<data_filename>`.  
     - Gera URL pública e registra metadados (SHA‑256, tamanho, etc.) em `items`.  
   * **Download meta**: processo análogo, porém caminho de bucket `<bucket_prefix>/meta/`.  
   * Constrói o *manifest* chamando `build_manifest` com:  
     - `dataset_id`, `title`, `source_dataset_url`, `bucket_prefix`.  
     - Lista `items` (apenas o data).  
     - Dicionário `meta` (arquivo meta).  
   * Retorna o dicionário de manifest.  

**Invariantes**:  
* As URLs encontradas são sempre únicas (`sorted(set(urls))`).  
* Os nomes de arquivos são fixos; qualquer mudança no dataset exigirá atualização do código.  

**Decisões de implementação**:  
* Uso de *scraping* ao invés de API pública, presumindo que a CVM não oferece endpoint estruturado.  
* `stream_download` abstrai a lógica de chunked download e cálculo de hash, mantendo o módulo focado em orquestração.  

---

## 6. Fluxo de dados / estado / eventos  

```
settings.datasets_dir ──► load_dataset_cfg ──► DatasetCfg
settings.data_dir ──► stream_download (data) ──► arquivo local
settings.data_dir ──► stream_download (meta) ──► arquivo local
storage.upload_file ──► bucket (data) / bucket (meta)
storage.public_url ──► URLs públicas (retornadas no manifest)
```

Não há eventos assíncronos; todas as chamadas são síncronas e bloqueantes. O único estado mutável temporário são os arquivos baixados no disco local, que são descartados após o upload (não há limpeza explícita no código).

---

## 7. Conexões com outros arquivos do projeto  

* **`forest_pipelines.http.stream_download`** – responsável pelo download streaming e cálculo de hash.  
* **`forest_pipelines.manifests.build_manifest`** – gera o objeto de manifesto padronizado.  

Nenhum outro módulo importa este arquivo; ele é um ponto de entrada de pipeline (geralmente chamado por scripts de orquestração).  

*(Links para a documentação dos módulos acima não foram fornecidos.)*

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Risco / Impacto | Recomendações |
|------|-----------------|---------------|
| **Parsing HTML** | Dependente da estrutura da página da CVM; mudanças de classe CSS ou layout quebram `extract_resource_urls`. | Avaliar uso de API oficial ou criar camada de adaptação que possa ser configurada via selectors. |
| **Hard‑coded filenames** | Qualquer alteração nos nomes dos arquivos no portal requer modificação de código. | Externalizar nomes de arquivos para o YAML de configuração. |
| **Contratos implícitos (`settings`, `storage`, `logger`)** | Falta de tipagem estática pode gerar erros em tempo de execução. | Definir Protocols (PEP 544) ou dataclasses para validar a interface esperada. |
| **Ausência de limpeza de arquivos temporários** | Arquivos locais permanecem após execução, consumindo espaço. | Implementar remoção (`Path.unlink`) após upload bem‑sucedido. |
| **Sem tratamento de falhas de download parcial** | `stream_download` pode falhar; o código não tenta re‑tentar nem limpa arquivos corrompidos. | Envolver download em bloco `try/except` com política de retry e limpeza. |
| **Uso de `requests` síncrono** | Bloqueia a thread durante download da página; pode ser ineficiente em pipelines paralelas. | Considerar `httpx` async ou thread pool para paralelismo leve. |
| **Validação mínima da resposta HTML** | Caso a página retorne erro 200 com conteúdo inesperado, `extract_resource_urls` retornará lista vazia e `find_by_filename` lançará erro genérico. | Verificar presença de pelo menos um link antes de prosseguir e emitir mensagem de erro mais clara. |

Implementar as melhorias acima aumentará a robustez, a manutenibilidade e a observabilidade do pipeline de ingestão.
