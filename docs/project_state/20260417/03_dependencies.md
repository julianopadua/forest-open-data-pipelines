# Dependências

## Origem

Definidas em [`pyproject.toml`](../../../pyproject.toml). **Não há** `requirements.txt` nem lockfile no repositório (pip/uv resolve em tempo de instalação).

| Pacote | Versão declarada | Uso provável no projeto |
| --- | --- | --- |
| `requests` | >=2.31 | HTTP para CKAN, APIs, downloads. |
| `beautifulsoup4` | >=4.12 | Parsing HTML (CVM, notícias). |
| `PyYAML` | >=6.0.1 | Configs `app.yml`, datasets, reports. |
| `python-dotenv` | >=1.0.1 | `load_dotenv()` em `settings.py`. |
| `typer` | >=0.12 | CLI `forest-pipelines`. |
| `pydantic` | >=2.7 | Modelos em datasets (ex.: notícias). |
| `supabase` | >=2.6.0 | Cliente Storage. |
| `pandas` | >=2.2 | Agregações em relatórios / leitura tabular. |
| `pyarrow` | >=15.0.0 | Stack pandas / leitura eficiente. |
| `groq` | (sem pin) | Cliente LLM para Groq. |

## Dev

- `pytest>=8.0` (extra `[dev]`).

## Bibliotecas críticas

- **supabase + requests:** caminho crítico para publicação; falhas aparecem em produção/CI.
- **pandas + pyarrow:** memória e CPU em `bdqueimadas_overview`.

## Aparentemente não utilizadas

- Nenhuma dependência principal claramente órfã sem grep; `pydantic` uso concentrado em `noticias_agricolas` / modelos.

## Riscos de versão

- **`groq` sem lower bound:** builds futuras podem quebrar compatibilidade.
- **Sem lockfile:** CI `pip install -e .` pode resolver versões diferentes em datas diferentes — **dívida de reprodutibilidade** (severidade média).

## Package manager

- **setuptools** como build backend; instalação editável padrão.
- **Alternativa 1 — `uv lock`:** facilidade alta, lock rápido, gratuito; melhora reprodutibilidade.
- **Alternativa 2 — `pip-tools` (`pip-compile`):** facilidade média; gratuito; `requirements.txt` pinado.
- **Alternativa 3 — Poetry:** facilidade média; lock nativo; adiciona ferramenta ao projeto.
