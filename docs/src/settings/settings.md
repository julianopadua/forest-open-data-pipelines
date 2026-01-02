# 📄 Documentação – `settings.py`

> **Arquivo:** `src/forest_pipelines/settings.py`  
> **Linguagem:** Python 3 (PEP 484, `dataclasses`)  

---

## 1. Visão geral e responsabilidade  

Este módulo centraliza a carga e a validação das configurações de execução da aplicação.  
Ele:

* Lê um arquivo YAML de configuração (`app.yml` ou equivalente).  
* Carrega variáveis de ambiente via **python‑dotenv**.  
* Constrói caminhos absolutos para diretórios críticos (raiz do repositório, dados, logs, datasets).  
* Garante a existência dos diretórios de dados e logs.  
* Encapsula todas as informações em um objeto imutável `Settings`, facilitando a injeção de dependências em todo o código‑base.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada / Domínio | Função |
|------------------|--------|
| **Config / Infraestrutura** | Responsável por provisionar parâmetros de ambiente e caminhos de arquivos que outras camadas (ex.: pipelines, serviços, UI) consomem. |
| **Utilitário** | Não contém lógica de negócio; apenas transforma dados de configuração em objetos de uso geral. |

---

## 3. Interfaces e exports (o que ele expõe)

| Nome | Tipo | Descrição |
|------|------|-----------|
| `Settings` | `@dataclass(frozen=True)` | Estrutura imutável contendo: `root`, `data_dir`, `logs_dir`, `datasets_dir` (todos `Path`) e `supabase_bucket_open_data` (`str`). |
| `load_settings` | `Callable[[str], Settings]` | Função pública que recebe o caminho para o YAML de configuração e devolve uma instância de `Settings`. |

> **Exportação implícita:** o módulo exporta apenas os dois símbolos acima; tudo o mais (imports, funções auxiliares) permanece interno.

---

## 4. Dependências e acoplamentos  

| Tipo | Biblioteca | Motivo |
|------|-------------|--------|
| **Externa** | `os` | Acesso a variáveis de ambiente. |
|  | `pathlib.Path` | Manipulação segura de caminhos de arquivos. |
|  | `yaml.safe_load` (PyYAML) | Parseamento do arquivo de configuração. |
|  | `dotenv.load_dotenv` (python‑dotenv) | Carregamento automático de `.env`. |
|  | `dataclasses.dataclass` | Definição de objeto imutável. |
| **Interna** | *Nenhuma* | O módulo não importa outros pacotes internos do projeto. |

O acoplamento externo é limitado a bibliotecas de propósito geral; não há dependência direta de outros módulos da aplicação.

---

## 5. Leitura guiada do código (top‑down)

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv
```

1. **Importações** – `__future__` garante suporte a anotações de tipo avançadas.  
2. **`@dataclass(frozen=True)`** – Define `Settings` como objeto **imutável**, evitando alterações acidentais após a carga.  
3. **`load_settings(config_path: str) -> Settings`** – Função principal.  

### Etapas internas de `load_settings`

| Etapa | Ação | Racional |
|-------|------|----------|
| **1. Carregar .env** | `load_dotenv()` | Permite sobrescrever variáveis de ambiente sem alterar o YAML. |
| **2. Determinar raiz** | `root = Path(config_path).resolve().parent.parent` | Assume que o YAML está em `.../configs/app.yml`; subir dois níveis chega ao diretório raiz do repositório. |
| **3. Ler YAML** | `yaml.safe_load(f)` | Usa carregamento seguro (sem execução de tags arbitrárias). |
| **4. Construir caminhos** | `root / cfg["app"]["data_dir"]` etc. | Concatena a raiz com sub‑caminhos declarados no YAML, garantindo caminhos absolutos. |
| **5. Resolver bucket Supabase** | `bucket_env = cfg["supabase"]["bucket_open_data_env"]; bucket = os.getenv(bucket_env, "open-data")` | O nome da variável de ambiente é configurável; fallback para `"open-data"` caso não exista. |
| **6. Garantir diretórios** | `mkdir(parents=True, exist_ok=True)` | Cria `data_dir` e `logs_dir` se ainda não existirem, evitando falhas posteriores de I/O. |
| **7. Instanciar `Settings`** | `return Settings(...)` | Retorna objeto imutável contendo todas as informações consolidadas. |

**Invariantes observadas**

* `root`, `data_dir`, `logs_dir` e `datasets_dir` são sempre `Path` resolvidos e existentes (exceto `datasets_dir`, que não é criado automaticamente).  
* `supabase_bucket_open_data` nunca é `None`; sempre tem valor padrão `"open-data"`.

---

## 6. Fluxo de dados/estado/eventos  

1. **Entrada** – `config_path` (string) apontando para o arquivo YAML.  
2. **Processamento** – Leitura de variáveis de ambiente, parsing do YAML, montagem de caminhos, criação de diretórios.  
3. **Saída** – Instância de `Settings` (estado imutável) que pode ser propagada por injeção de dependência ou armazenada em um singleton de configuração.  

Não há eventos assíncronos nem mutabilidade posterior ao retorno.

---

## 7. Conexões com outros arquivos do projeto  

* **Importado por** – *Nenhum* (conforme análise estática atual). Caso futuros módulos precisem de configuração, deverão importar `load_settings` ou `Settings` a partir deste caminho.  
* **Importa** – *Nenhum* módulo interno; apenas bibliotecas padrão e de terceiros.  

> **Observação:** Se o projeto evoluir e houver dependências circulares, será necessário revisar a localização deste módulo (ex.: movê‑lo para um pacote `core/config`).  

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Hard‑coded suposição de estrutura** | O cálculo de `root` presume que o YAML está em `configs/` duas pastas abaixo da raiz. Alterações na organização de arquivos quebrarão a lógica. | Tornar o cálculo configurável ou validar a posição esperada com mensagens de erro claras. |
| **Criação automática de diretórios** | `data_dir` e `logs_dir` são criados silenciosamente; falhas de permissão podem gerar exceções inesperadas. | Capturar `OSError` e relatar falha de criação de forma explícita. |
| **Variável de ambiente opcional** | Fallback `"open-data"` pode mascarar erros de configuração. | Opcionalmente, registrar (log) quando o fallback for usado, facilitando depuração. |
| **Ausência de validação de schema** | O YAML é carregado sem verificação de chaves obrigatórias; chaves ausentes gerarão `KeyError`. | Integrar validação de schema (ex.: `jsonschema` ou `pydantic`) antes de acessar os campos. |
| **`datasets_dir` não é criado** | Se o diretório não existir, código que o utiliza pode falhar. | Avaliar se a criação automática é desejada ou, ao menos, documentar a responsabilidade do chamador. |
| **Tipagem limitada** | Anotações de tipo são genéricas (`dict` retornado por `yaml.safe_load`). | Substituir por tipos mais específicos (`TypedDict` ou `pydantic.BaseModel`). |

Implementar as melhorias acima aumentará a robustez e a manutenibilidade do módulo de configuração.
