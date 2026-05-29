from __future__ import annotations

from forest_pipelines.datasets.anp.govbr import (
    ANP_DATASET_IDS,
    AnpHttpOptions,
    AnpRunnerOptions,
    CatalogDatasetCfg,
    ResourceLink,
    build_manifest_from_detail_page,
    discover_collections,
    extract_page_freshness_labels,
    extract_resource_links,
    parse_govbr_freshness_label,
    resolve_final_url,
)
from forest_pipelines.profiling import ProfileOptions, profile_cache_from_manifest, use_profile_cache
from forest_pipelines.registry.datasets import RUNNERS


HUB_HTML = """
<html>
  <body>
    <nav><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/menu">Menu</a></nav>
    <main id="content-core">
      <h1>Dados abertos</h1>
      <h2>Dados disponíveis em formato aberto</h2>
      <p>Acervo de Dados Técnicos</p>
      <p><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/acervo-de-dados-tecnicos">Veja os dados abertos do Acervo de Dados Técnicos.</a></p>
      <p>Produção de petróleo e gás natural por estado e localização</p>
      <p><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado-e-localizacao">Veja as informações.</a></p>
      <p><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/home/pda.pdf">Plano</a></p>
      <h2>Saiba Mais</h2>
      <p><a href="https://external.example/dados/conjuntos-dados/anp">Portal antigo</a></p>
    </main>
  </body>
</html>
"""


DETAIL_HTML = """
<html>
  <body>
    <main id="content-core">
      <h1>Produção de petróleo e gás natural por estado e localização</h1>
      <span class="documentPublished"><span>Publicado em</span><span class="value">26/10/2020 09h56</span></span>
      <span class="documentModified"><span>Atualizado em</span><span class="value">30/04/2026 15h14</span></span>
      <h3>Produção de petróleo</h3>
      <ul>
        <li><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/ppgn-el/metadados-producao-petroleo.pdf">Metadados - Produção de petróleo</a> (atualizado em 10/3/2026)</li>
        <li><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/ppgn-el/producao-petroleo-m3-1997-2026.csv">Produção de petróleo 1997-2026</a> (atualizado em 30/4/2026)</li>
      </ul>
      <h3>Consulta oficial</h3>
      <p><a href="https://cdp.anp.gov.br/ords/r/cdp_apex/consulta-dados-publicos-cdp/consulta-de-pocos">Consulta de poços</a></p>
      <p><a href="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/outro-dataset">Outro dataset</a></p>
    </main>
  </body>
</html>
"""


class ProfileResponse:
    status_code = 200
    headers = {"Content-Type": "text/csv"}

    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "ProfileResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        yield self.body


def test_discover_collections_ignores_navigation_and_files() -> None:
    links = discover_collections(HUB_HTML)

    assert [link.slug for link in links] == [
        "acervo-de-dados-tecnicos",
        "producao-de-petroleo-e-gas-natural-por-estado-e-localizacao",
    ]
    assert links[0].title == "Acervo de Dados Técnicos"


def test_registry_anp_ids_match_runner_ids() -> None:
    for dataset_id in ANP_DATASET_IDS:
        assert dataset_id in RUNNERS
    anp_runners = [key for key in RUNNERS if key.startswith("anp_")]
    assert sorted(anp_runners) == sorted(ANP_DATASET_IDS)


def test_extract_resource_links_keeps_files_and_indirect_official_pages() -> None:
    resources = extract_resource_links(
        DETAIL_HTML,
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado-e-localizacao",
    )

    filenames = [resource.filename for resource in resources]
    assert filenames == [
        "metadados-producao-petroleo.pdf",
        "producao-petroleo-m3-1997-2026.csv",
        "consulta-de-pocos.html",
    ]
    assert resources[0].kind == "metadata"
    assert resources[1].kind == "data"
    assert resources[1].period == "1997"
    assert resources[2].direct_download is False


def test_extract_page_freshness_labels_reads_govbr_spans() -> None:
    labels = extract_page_freshness_labels(DETAIL_HTML)

    assert labels == {
        "published_label": "26/10/2020 09h56",
        "modified_label": "30/04/2026 15h14",
    }


def test_parse_govbr_freshness_label_handles_date_and_time() -> None:
    signal = parse_govbr_freshness_label(
        "30/04/2026 15h14",
        method="test",
    )

    assert signal is not None
    assert signal.precision == "datetime"
    assert signal.raw_label == "30/04/2026 15h14"


def test_extract_resource_links_keeps_item_updated_label_with_time() -> None:
    html = DETAIL_HTML.replace("atualizado em 30/4/2026", "atualizado em 30/4/2026 08h30")

    resources = extract_resource_links(
        html,
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado-e-localizacao",
    )

    assert resources[1].updated_label == "30/4/2026 08h30"


def test_anp_resource_freshness_prefers_item_label(monkeypatch) -> None:
    signals = []

    def fake_profile_source_url(source_url: str, **kwargs):
        return {
            "profile_status": "ok",
            "profile_warnings": [],
        }

    def fake_profiled_item(**kwargs):
        signals.append(kwargs["freshness_signal"])
        return {
            "kind": "data",
            "period": kwargs["period"],
            "filename": kwargs["filename"],
            "source_url": kwargs["source_url"],
            "title": kwargs["title"],
            "profile_status": "ok",
            "profile_warnings": [],
        }

    monkeypatch.setattr("forest_pipelines.datasets.anp.govbr.profile_source_url", fake_profile_source_url)
    monkeypatch.setattr("forest_pipelines.datasets.anp.govbr.profiled_item", fake_profiled_item)

    build_manifest_from_detail_page(
        cfg=CatalogDatasetCfg(
            id="anp_teste",
            slug="teste",
            title="Teste",
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste",
            bucket_prefix="anp/teste",
        ),
        html=DETAIL_HTML,
        resources=[
            ResourceLink(
                source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/data.csv",
                filename="data.csv",
                title="Dados",
                section="Secao",
                period="2026",
                kind="data",
                direct_download=True,
                updated_label="29/04/2026",
            )
        ],
        logger=None,
        options=AnpRunnerOptions(http=AnpHttpOptions(), profile=ProfileOptions()),
    )

    assert signals[0].method == "anp_resource_updated_label"
    assert signals[0].raw_label == "29/04/2026"


def test_anp_resource_freshness_falls_back_to_page_label(monkeypatch) -> None:
    signals = []

    monkeypatch.setattr("forest_pipelines.datasets.anp.govbr.profiled_item", lambda **kwargs: signals.append(kwargs["freshness_signal"]) or {
        "kind": "data",
        "period": kwargs["period"],
        "filename": kwargs["filename"],
        "source_url": kwargs["source_url"],
        "title": kwargs["title"],
        "profile_status": "ok",
        "profile_warnings": [],
    })

    build_manifest_from_detail_page(
        cfg=CatalogDatasetCfg(
            id="anp_teste",
            slug="teste",
            title="Teste",
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste",
            bucket_prefix="anp/teste",
        ),
        html=DETAIL_HTML,
        resources=[
            ResourceLink(
                source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/data.csv",
                filename="data.csv",
                title="Dados",
                section="Secao",
                period="2026",
                kind="data",
                direct_download=True,
            )
        ],
        logger=None,
        options=AnpRunnerOptions(http=AnpHttpOptions(), profile=ProfileOptions()),
    )

    assert signals[0].method == "anp_page_modified_label"
    assert signals[0].raw_label == "30/04/2026 15h14"


def test_build_manifest_places_metadata_and_documentation(monkeypatch) -> None:
    def fake_profile(source_url: str, **kwargs):
        return {
            "size_bytes": 10,
            "sha256": "0" * 64,
            "content_type": "text/plain",
            "format": kwargs.get("filename", "x").split(".")[-1],
            "profiled_at": "2026-05-28T00:00:00Z",
            "profile_status": "ok",
            "profile_warnings": [],
        }

    monkeypatch.setattr("forest_pipelines.datasets.anp.govbr.profile_source_url", fake_profile)
    monkeypatch.setattr("forest_pipelines.datasets.anp.govbr.profiled_item", lambda **kwargs: {
        "kind": "data",
        "period": kwargs["period"],
        "filename": kwargs["filename"],
        "source_url": kwargs["source_url"],
        "title": kwargs["title"],
        **fake_profile(kwargs["source_url"], filename=kwargs["filename"]),
    })

    resources = [
        ResourceLink(
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/meta.pdf",
            filename="meta.pdf",
            title="Metadados",
            section="Secao",
            period="current",
            kind="metadata",
            direct_download=True,
        ),
        ResourceLink(
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/data.csv",
            filename="data.csv",
            title="Dados",
            section="Secao",
            period="2026",
            kind="data",
            direct_download=True,
        ),
        ResourceLink(
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/manual.pdf",
            filename="manual.pdf",
            title="Manual",
            section="Secao",
            period="current",
            kind="documentation",
            direct_download=True,
        ),
    ]

    manifest = build_manifest_from_detail_page(
        cfg=CatalogDatasetCfg(
            id="anp_teste",
            slug="teste",
            title="Teste",
            source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste",
            bucket_prefix="anp/teste",
        ),
        html="<main id='content-core'><h1>Teste ANP</h1></main>",
        resources=resources,
        logger=None,
        options=AnpRunnerOptions(http=AnpHttpOptions(), profile=ProfileOptions()),
    )

    assert manifest["schema_version"] == "2.0"
    assert manifest["title"] == "Teste ANP"
    assert manifest["meta"]["metadata_file"]["filename"] == "meta.pdf"
    assert manifest["meta"]["custom_tags"]["documentation_files"][0]["filename"] == "manual.pdf"
    assert manifest["items"][0]["source_url"].endswith("data.csv")


def test_anp_fresh_resource_reuses_cached_profile(monkeypatch) -> None:
    def fail_get(*args, **kwargs):
        raise AssertionError("network should not be used")

    monkeypatch.setattr("forest_pipelines.profiling.requests.get", fail_get)
    source_url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/data.csv"
    manifest_cache = {
        "items": [
            {
                "source_url": source_url,
                "filename": "data.csv",
                "row_count": 7,
                "profiled_at": "2026-05-29T00:00:00Z",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }

    with use_profile_cache(profile_cache_from_manifest(manifest_cache)):
        manifest = build_manifest_from_detail_page(
            cfg=CatalogDatasetCfg(
                id="anp_teste",
                slug="teste",
                title="Teste",
                source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste",
                bucket_prefix="anp/teste",
            ),
            html=DETAIL_HTML,
            resources=[
                ResourceLink(
                    source_url=source_url,
                    filename="data.csv",
                    title="Dados",
                    section="Secao",
                    period="2026",
                    kind="data",
                    direct_download=True,
                    updated_label="28/05/2026",
                )
            ],
            logger=None,
            options=AnpRunnerOptions(http=AnpHttpOptions(), profile=ProfileOptions()),
        )

    assert manifest["items"][0]["row_count"] == 7


def test_anp_stale_resource_reprofiles(monkeypatch) -> None:
    monkeypatch.setattr(
        "forest_pipelines.profiling.requests.get",
        lambda *args, **kwargs: ProfileResponse(b"a,b\n1,2\n3,4\n"),
    )
    source_url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/data.csv"
    manifest_cache = {
        "items": [
            {
                "source_url": source_url,
                "filename": "data.csv",
                "row_count": 7,
                "profiled_at": "2026-05-28T00:00:00Z",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ]
    }

    with use_profile_cache(profile_cache_from_manifest(manifest_cache)):
        manifest = build_manifest_from_detail_page(
            cfg=CatalogDatasetCfg(
                id="anp_teste",
                slug="teste",
                title="Teste",
                source_url="https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste",
                bucket_prefix="anp/teste",
            ),
            html=DETAIL_HTML,
            resources=[
                ResourceLink(
                    source_url=source_url,
                    filename="data.csv",
                    title="Dados",
                    section="Secao",
                    period="2026",
                    kind="data",
                    direct_download=True,
                    updated_label="29/05/2026",
                )
            ],
            logger=None,
            options=AnpRunnerOptions(http=AnpHttpOptions(), profile=ProfileOptions()),
        )

    assert manifest["items"][0]["row_count"] == 2


def test_resolve_final_url_uses_meta_refresh(monkeypatch) -> None:
    class Response:
        status_code = 405
        url = "https://www.gov.br/start"
        headers = {"content-type": "text/html"}
        text = '<meta http-equiv="refresh" content="0; url=/final.csv">'

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("requests.head", lambda *args, **kwargs: Response())
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response())

    final = resolve_final_url("https://www.gov.br/start", AnpHttpOptions())

    assert final == "https://www.gov.br/final.csv"
