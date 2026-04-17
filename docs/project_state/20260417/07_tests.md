# Testes

## Ferramentas

- **pytest** configurado em [`pyproject.toml`](../../../pyproject.toml) (`testpaths = ["tests"]`).
- Dependência opcional: `pip install -e ".[dev]"`.

## Cobertura atual

- **Não há** `pytest-cov` nem relatório de cobertura configurado no repositório.
- Arquivos de teste identificados: [`tests/test_noticias_agricolas_parsers.py`](../../../tests/test_noticias_agricolas_parsers.py) e fixtures em `tests/fixtures/noticias_agricolas/`.

## Tipos de testes existentes

- **Unitários** focados em parsers HTML/merge/datas do pipeline **Notícias Agrícolas** — boa base para regressão de scraping frágil.

## Lacunas

| Área sem testes aparentes | Risco |
| --- | --- |
| CLI (`cli.py`) | Regressão em comandos/flags |
| `SupabaseStorage` | Comportamento de retry e URLs |
| Runners CVM/EIA/INPE | Quebra por mudança de site upstream |
| Relatório bdqueimadas | Lógica pesada sem teste |
| `publish_report_package` | Contrato JSON |

## Testes frágeis ou ausentes

- Scrapers são **inerentemente frágeis**; os testes de notícias mitigam com HTML fixo em fixtures — padrão correto.

## Prioridades de cobertura sugeridas

1. Testes de **smoke** com mocks para `SupabaseStorage` (sem rede).
2. Golden file pequeno para **build_manifest**.
3. Opcional: integração marcada `@pytest.mark.integration` com credenciais em CI (segredo).
