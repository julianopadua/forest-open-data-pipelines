# src/forest_pipelines/cli_help.py
"""Textos longos para o Typer (--help global e docstrings dos comandos)."""
from __future__ import annotations


def short_command_summary(doc: str) -> str:
    """Primeira linha do texto longo (tabela de subcomandos no --help)."""
    line = doc.strip().split("\n", 1)[0].strip()
    return line if len(line) < 120 else line[:117] + "..."


def _bullet_list(items: list[str]) -> str:
    return "\n".join(f"  • {x}" for x in items)


def build_app_help() -> str:
    """Conteúdo do `forest-pipelines --help` (listas de IDs atualizadas via registries)."""
    from forest_pipelines.audits.registry import RUNNERS as AUDIT_RUNNERS
    from forest_pipelines.registry.datasets import RUNNERS as DATASET_RUNNERS
    from forest_pipelines.reports.registry.reports import RUNNERS as REPORT_RUNNERS

    datasets = sorted(DATASET_RUNNERS.keys())
    reports = sorted(REPORT_RUNNERS.keys())
    audits = sorted(AUDIT_RUNNERS.keys())

    return f"""\
forest-open-data-pipelines — CLI para sincronizar datasets abertos, gerar relatórios e auditar fontes.

Comandos estão agrupados por painel no --help. Cada subcomando tem documentação própria.

Requisitos comuns
  • Config: arquivo YAML principal (padrão configs/app.yml) com data_dir, logs, datasets_dir, etc.
  • Variáveis de ambiente (sync, build-report): SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY (obrigatórias).
    O nome do bucket vem da env cujo nome está em app.yml em supabase.bucket_open_data_env
    (ex.: SUPABASE_BUCKET_OPEN_DATA); se ausente, o default é o bucket "open-data".
  • sync, build-report e anp-publish publicam no Storage do Supabase (mesmas envs);
    anp-catalog e anp-compact podem só gerar arquivos locais.

IDs registrados — sync (dataset_id)
{_bullet_list(datasets)}

IDs registrados — build-report (report_id)
{_bullet_list(reports)}

IDs registrados — audit-dataset (dataset_id)
{_bullet_list(audits)}
"""


# --- Docstrings por comando (primeira linha = resumo curto para uma linha no --help) ---

ANP_CATALOG_DOC = """\
Catálogo de links CSV da organização ANP no dados.gov.br (API CKAN pública).

Consulta package_search com fq=organization:<UUID>, paginação start/rows (50 por página),
extrai recursos com format CSV e grava anp_catalogo_supabase.json e .csv no diretório de saída.

Saídas: JSON (lista de dicts) + CSV com colunas dataset_title, file_name, download_url.

Exemplos:
  forest-pipelines anp-catalog
  forest-pipelines anp-catalog --limit 5 --output-dir ./out
"""


ANP_COMPACT_DOC = """\
Converte um snapshot exportado do portal (anp.json com registros/totalRegistros) em JSON compacto
para modelagem/Supabase: metadados do conjunto, temas/tags, merge de resourcesAcessoRapido ∪ resourcesFormatado.

Saída padrão: anp_catalog_compact.json (UTF-8, acentos literais). Validação opcional com JSON Schema v1.

Exemplos:
  forest-pipelines anp-compact src/forest_pipelines/dados_abertos/anp.json -o compact.json
  forest-pipelines anp-compact ./anp.json --no-validate
"""


ANP_PUBLISH_DOC = """\
Envia o JSON compacto do catálogo ANP para o bucket Storage open-data (prefixo padrão anp/catalog):
anp_catalog_compact.json e manifest.json com URLs públicas.

Requer as mesmas variáveis que sync/build-report (SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, bucket via app.yml).

Exemplos:
  forest-pipelines anp-publish ./anp_catalog_compact.json
  forest-pipelines anp-publish ./compact.json --bucket-prefix anp/catalog/v1
"""


SYNC_DOC = """\
Executa o pipeline registrado para o dataset_id: download/transformação conforme a fonte (EIA, INPE, CVM, INMET, feeds),
upload dos artefatos para o bucket Supabase configurado e publicação de manifest.json no prefixo do dataset.

Tipos de arquivo variam por fonte: comuns são CSV, ZIP e XLS no storage; feeds podem publicar JSON.
O manifest.json descreve o pacote publicado no prefixo do bucket.

Opção --latest-months: repassada ao runner quando suportada (janela temporal); ignorada se o dataset não usar.

Exemplos:
  forest-pipelines sync eia_petroleum_weekly
  forest-pipelines sync inpe_bdqueimadas_focos --config-path configs/app.yml
  forest-pipelines sync noticias_agricolas_news --latest-months 3
"""


BUILD_REPORT_DOC = """\
Gera o pacote estático de um relatório (HTML, assets, manifest) e publica no Storage via publish_report_package.

Hoje há um report registrado; o comando registra no log URLs públicas do manifest e do relatório.

Exemplo:
  forest-pipelines build-report bdqueimadas_overview
"""


AUDIT_DATASET_DOC = """\
Roda a auditoria registrada para o dataset (validações, resumo). Grava Markdown e JSON de resumo em disco
(paths logados ao final). Não publica no Supabase Storage como o sync.

Exemplo:
  forest-pipelines audit-dataset inpe_bdqueimadas_focos
"""


PUBLISH_CATALOG_DOC = """\
Gera e publica os catálogos consolidados (open_data_catalog.json e reports_catalog.json) no bucket Storage,
sob o prefixo catalog/ por padrão. O frontend do portal consome esses JSONs e elimina arrays hardcoded.

Fontes (SSOT):
  • configs/catalog/open_data.yml — datasets não-ANP (UI metadata: category, segment, subcategory, source)
  • configs/catalog/reports.yml — catálogo de relatórios (slug, título, layout, hero, etc.)
  • anp_catalog_compact.json (na raiz do repo) — catálogo ANP já transformado (gerado por anp-compact)

Se o arquivo compacto da ANP não existir, o catálogo é publicado sem os datasets ANP e uma warning é
registrada no envelope (generation_status=success_partial_fallback).

Exemplos:
  forest-pipelines publish-catalog
  forest-pipelines publish-catalog --bucket-prefix catalog/v1
  forest-pipelines publish-catalog --anp-compact ./some-other/anp_catalog_compact.json
"""
