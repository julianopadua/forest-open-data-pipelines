# Documentação Técnica – `src/forest_pipelines/http.py`

---

## 1. Visão geral e responsabilidade
Este módulo fornece utilitários de **download HTTP** com suporte a streaming, cálculo de hash SHA‑256 e registro de tamanho em bytes. A única funcionalidade pública é a função `stream_download`, que garante a criação do diretório de destino, grava o conteúdo em disco de forma incremental e devolve um objeto imutável `DownloadResult` contendo metadados do arquivo baixado.

---

## 2. Onde este arquivo se encaixa na arquitetura
- **Camada:** Utilitários / Infraestrutura (acesso a recursos externos).  
- **Domínio:** Não pertence a um domínio de negócio específico; serve como apoio genérico a pipelines que necessitam baixar artefatos (ex.: modelos, datasets).  
- **Ponto de integração:** Pode ser invocado por módulos de orquestração de pipelines ou por scripts de preparação de ambiente.

---

## 3. Interfaces e exports (o que ele expõe)

| Nome | Tipo | Descrição |
|------|------|-----------|
| `DownloadResult` | `@dataclass(frozen=True)` | Contém `file_path: Path`, `size_bytes: int` e `sha256: str`. Representa o resultado de um download. |
| `stream_download(url: str, out_path: Path, timeout_s: int = 120) -> DownloadResult` | Função | Realiza download streaming de `url` para `out_path`, calcula SHA‑256 e tamanho total. Levanta exceções de `requests` em caso de falha. |

```python
@dataclass(frozen=True)
class DownloadResult:
    file_path: Path
    size_bytes: int
    sha256: str
```

```python
def stream_download(url: str, out_path: Path, timeout_s: int = 120) -> DownloadResult:
    ...
```

---

## 4. Dependências e acoplamentos

| Tipo | Biblioteca | Motivo |
|------|------------|--------|
| **Externa** | `requests` | Cliente HTTP robusto com suporte a streaming. |
| **Externa** | `hashlib` | Implementação padrão de SHA‑256. |
| **Externa** | `pathlib.Path` | Manipulação de caminhos de forma portátil. |
| **Interna** | Nenhuma | O módulo não depende de outros componentes do repositório. |

O acoplamento é **baixo**: a única dependência externa é a API pública de `requests`. Substituições (ex.: `httpx`) exigiriam apenas alteração da implementação interna.

---

## 5. Leitura guiada do código (top‑down)

1. **Importações** – `__future__` habilita anotações de tipo adiantadas; demais imports são utilitários padrão.  
2. **`DownloadResult`** – Classe imutável (`frozen=True`) garante que os metadados não sejam alterados após a criação.  
3. **`stream_download`** –  
   - **Criação de diretório**: `out_path.parent.mkdir(parents=True, exist_ok=True)` assegura que o caminho de destino exista.  
   - **Inicialização de hash e contador**: `hashlib.sha256()` e `size = 0`.  
   - **Requisição HTTP**: `requests.get(..., stream=True, timeout=timeout_s)` abre a conexão em modo streaming; o contexto `with` garante fechamento da conexão.  
   - **Validação de status**: `r.raise_for_status()` propaga erros HTTP (4xx/5xx).  
   - **Loop de escrita**: `r.iter_content(chunk_size=1024*1024)` lê blocos de 1 MiB; blocos vazios são ignorados. Cada chunk é gravado em disco, incorporado ao hash (`h.update(chunk)`) e contabilizado em `size`.  
   - **Retorno**: Instancia `DownloadResult` com caminho absoluto, tamanho total e hash hexadecimal.

**Invariantes**  
- O diretório de saída sempre existe antes da escrita.  
- O hash SHA‑256 reflete exatamente o conteúdo gravado, pois a atualização ocorre imediatamente após cada escrita.  
- O tamanho em bytes corresponde à soma dos comprimentos dos chunks processados.

---

## 6. Fluxo de dados/estado/eventos

```
URL  ──► HTTP GET (stream) ──► Chunk (bytes) ──► 
   │                                 │
   ├─► Escrita em disco (out_path)   ├─► Atualiza hash (sha256)
   └─► Incrementa contador (size_bytes)
```

Não há eventos assíncronos nem estado compartilhado; toda a operação ocorre de forma síncrona dentro da chamada da função.

---

## 7. Conexões com outros arquivos do projeto
- **Importado por:** *Nenhum* (conforme análise estática do repositório).  
- **Importa:** *Nenhum* módulo interno.  

> **Observação:** Caso futuros módulos precisem de download, deverão importar `stream_download` a partir deste caminho (`forest_pipelines.http`).

---

## 8. Pontos de atenção, riscos e melhorias recomendadas

| Item | Descrição | Recomendações |
|------|-----------|---------------|
| **Timeout fixo** | O parâmetro `timeout_s` aplica o mesmo timeout para conexão e leitura, o que pode ser insuficiente para arquivos muito grandes. | Expor parâmetros separados (`connect_timeout`, `read_timeout`) ou permitir `timeout: tuple[float, float]`. |
| **Tamanho de chunk** | O tamanho de 1 MiB pode ser inadequado em ambientes com memória limitada. | Tornar `chunk_size` configurável ou adaptar dinamicamente ao tamanho do arquivo (via cabeçalho `Content-Length`). |
| **Tratamento de exceções** | Apenas `raise_for_status` é propagado; erros de I/O (ex.: disco cheio) não são capturados. | Envolver a escrita em bloco `try/except` e lançar exceção customizada (`DownloadError`). |
| **Verificação de integridade** | O hash é calculado, mas não há mecanismo para comparar contra um valor esperado. | Permitir parâmetro opcional `expected_sha256: str | None` e validar ao final, lançando erro em caso de divergência. |
| **Testabilidade** | Dependência direta de `requests.get` dificulta mocks em testes unitários. | Injetar cliente HTTP (ex.: `session: requests.Session | None`) ou usar `requests.get` via wrapper. |
| **Compatibilidade com HTTP/2** | `requests` não suporta HTTP/2 nativamente. | Avaliar migração para `httpx` se houver necessidade de HTTP/2 ou multiplexação. |

Implementar as melhorias acima aumentará a robustez, configurabilidade e testabilidade do módulo, alinhando‑o a boas práticas de engenharia de software.
