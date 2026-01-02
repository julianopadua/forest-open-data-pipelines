# storage/supabase_storage.py – Documentação Técnica

---

## 1. Visão geral e responsabilidade  

O módulo **`supabase_storage.py`** encapsula a interação com o serviço de armazenamento da Supabase.  
Ele fornece uma classe **`SupabaseStorage`** que:

* cria um cliente Supabase a partir de credenciais de ambiente;  
* disponibiliza métodos para upload de arquivos (a partir de caminho local ou de bytes) com controle de *upsert*;  
* gera URLs públicas para objetos armazenados.  

A classe foi projetada para ser utilizada por pipelines que precisam persistir artefatos em um bucket configurado.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada | Domínio | Propósito |
|--------|---------|-----------|
| **Infraestrutura / Persistência** | **Armazenamento de dados** | Abstrai a API de storage da Supabase, permitindo que camadas superiores (ex.: pipelines de processamento) não dependam diretamente da biblioteca `supabase`. |

Ele não contém lógica de negócio nem de apresentação; atua como um *adapter* para o serviço externo.

---

## 3. Interfaces e exports  

```python
@dataclass
class SupabaseStorage:
    supabase_url: str
    service_role_key: str = field(repr=False)
    bucket: str = "open-data"
    logger: Any = None

    @classmethod
    def from_env(cls, logger: Any, bucket_open_data: str) -> "SupabaseStorage": ...
    @property
    def client(self): ...
    def upload_file(self, object_path: str, local_path: str,
                    content_type: str, upsert: bool = True) -> None: ...
    def upload_bytes(self, object_path: str, data: bytes,
                     content_type: str, upsert: bool = True) -> None: ...
    def public_url(self, object_path: str) -> str: ...
```

*Exportado*: a classe `SupabaseStorage`. Não há funções ou variáveis globais adicionais.

---

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Biblioteca | Motivo |
|------|----------------------|--------|
| **Externa** | `supabase` (função `create_client`) | Cria o cliente HTTP que comunica com a API da Supabase. |
| **Externa** | `os` | Leitura de variáveis de ambiente. |
| **Externa** | `dataclasses`, `typing` | Definição da classe e tipagem. |
| **Interna** | Nenhuma | O módulo não importa outros componentes do repositório. |

O acoplamento externo está limitado à API pública da biblioteca `supabase`; mudanças nessa API podem impactar o módulo.

---

## 5. Leitura guiada do código (top‑down)

1. **Importações e definição da dataclass**  
   ```python
   from __future__ import annotations
   import os
   from dataclasses import dataclass, field
   from typing import Any
   from supabase import create_client
   ```

2. **Construtor implícito (`@dataclass`)**  
   - `supabase_url` e `service_role_key` são obrigatórios.  
   - `service_role_key` tem `repr=False` para evitar exposição em logs/tracebacks.  
   - `bucket` tem valor padrão `"open-data"`.  
   - `logger` aceita qualquer objeto que implemente o método `info`.

3. **`from_env` – fábrica baseada em variáveis de ambiente**  
   - Lê `SUPABASE_URL` e `SUPABASE_SERVICE_ROLE_KEY`.  
   - Valida presença; lança `RuntimeError` caso falte.  
   - Recebe `bucket_open_data` (valor esperado em `configs/app.yml` ou similar) e valida.  
   - Retorna uma instância configurada.

4. **`client` – propriedade lazy**  
   - Cada acesso cria um novo cliente via `create_client(self.supabase_url, self.service_role_key)`.  
   - Mantém a chave apenas no objeto (não em `repr`).

5. **`upload_file`**  
   - Converte flag `upsert` para string `"true"`/`"false"` (exigência da API Supabase).  
   - Abre o arquivo local em modo binário e chama `self.client.storage.from_(self.bucket).upload(...)`.  
   - Opcionalmente registra a operação via `logger.info`.

6. **`upload_bytes`**  
   - Idêntico a `upload_file`, porém aceita um `bytes` já carregado em memória.  
   - Não abre arquivo; passa o objeto `bytes` diretamente ao método `upload`.

7. **`public_url`**  
   - Constrói a URL pública do objeto usando o padrão da Supabase:  
     ```
     {base_url}/storage/v1/object/public/{bucket}/{path}
     ```
   - Normaliza barras finais/iniciais para evitar duplicação.

---

## 6. Fluxo de dados / estado / eventos  

| Operação | Entrada | Transformação | Saída / efeito colateral |
|----------|---------|---------------|--------------------------|
| `from_env` | `logger`, `bucket_open_data` + variáveis de ambiente | Validação e criação de objeto | Instância `SupabaseStorage`. |
| `upload_file` | `object_path`, `local_path`, `content_type`, `upsert` | Leitura de arquivo → chamada HTTP → (opcional) log | Upload realizado no bucket; nenhum valor retornado. |
| `upload_bytes` | `object_path`, `data`, `content_type`, `upsert` | Chamada HTTP direta → (opcional) log | Upload realizado; sem retorno. |
| `public_url` | `object_path` | Formatação de string | URL pública (string). |

O objeto não mantém estado mutável além das credenciais e do nome do bucket; cada chamada ao cliente gera uma nova conexão HTTP.

---

## 7. Conexões com outros arquivos do projeto  

*Este módulo não é importado por nenhum outro arquivo do repositório (conforme análise atual).*

Caso seja utilizado futuramente, a importação típica será:

```python
from forest_pipelines.storage.supabase_storage import SupabaseStorage
```

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Descrição | Recomendações |
|------|-----------|---------------|
| **Exposição de credenciais** | `service_role_key` permanece em memória e pode ser logado inadvertidamente. | Garantir que `logger` nunca registre objetos que contenham a chave; considerar uso de `SecretStr` ou similar. |
| **Criação de cliente a cada acesso** | A propriedade `client` cria um novo cliente a cada chamada, o que pode gerar overhead de conexão. | Cachear o cliente (ex.: atributo privado `_client`) após a primeira criação, respeitando a necessidade de renovação de token, se houver. |
| **Tratamento de erros da API Supabase** | O código não captura exceções lançadas por `upload`. | Envolver chamadas em `try/except`, registrar falhas e propagar exceções customizadas. |
| **Validação de `content_type`** | Não há verificação de MIME type válido. | Opcionalmente validar contra lista de tipos permitidos ou usar biblioteca `mimetypes`. |
| **Limite de tamanho de arquivo** | Não há controle de tamanho; uploads muito grandes podem falhar. | Documentar limites esperados ou implementar chunked upload, se suportado. |
| **Testabilidade** | Dependência direta de `supabase.create_client` dificulta testes unitários. | Injetar fábrica de cliente ou usar *mock* nas camadas superiores. |
| **Documentação de tipos** | `logger` tipado como `Any`. | Substituir por `logging.Logger` ou protocolo que exponha `info`. |

Implementar as melhorias acima aumentará a robustez, a performance e a manutenibilidade do módulo.
