from __future__ import annotations

from types import SimpleNamespace

from forest_pipelines.datasets.inpe import bdqueimadas_boletins_integrados as module


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.headers = {"Content-Type": "text/html; charset=utf-8"}

    def raise_for_status(self) -> None:
        return None


def test_parse_boletim_pdf_link_extracts_month_period_absolute_url() -> None:
    resource = module.parse_boletim_pdf_link(
        "03_2024.pdf",
        "https://dataserver-coids.inpe.br/queimadas/queimadas/Boletins-Integrados/2024/",
    )

    assert resource is not None
    assert resource.period == "2024-03"
    assert resource.year == "2024"
    assert resource.month == "03"
    assert resource.filename == "03_2024.pdf"
    assert resource.url.endswith("/2024/03_2024.pdf")


def test_extract_pdf_urls_walks_year_directories(monkeypatch) -> None:
    pages = {
        "https://example.test/boletins/": """
            <a href="../">Voltar</a>
            <a href="2024/">Diretório 2024</a>
            <a href="2025/">Diretório 2025</a>
        """,
        "https://example.test/boletins/2024/": """
            <a href="../">Voltar</a>
            <a href="01_2024.pdf">01_2024.pdf</a>
            <a href="not-a-boletim.txt">not-a-boletim.txt</a>
        """,
        "https://example.test/boletins/2025/": """
            <a href="../">Voltar</a>
            <a href="02_2025.pdf">02_2025.pdf</a>
        """,
    }

    def fake_get(url: str, timeout: int) -> FakeResponse:
        assert timeout == 60
        return FakeResponse(pages[url])

    monkeypatch.setattr(module.requests, "get", fake_get)

    resources = module.extract_pdf_urls("https://example.test/boletins/")

    assert [resource.period for resource in resources] == ["2025-02", "2024-01"]
    assert [resource.filename for resource in resources] == ["02_2025.pdf", "01_2024.pdf"]


def test_sync_indexes_source_urls_without_uploading_pdfs(monkeypatch, tmp_path) -> None:
    cfg_dir = tmp_path / "configs" / "datasets" / "inpe"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "bdqueimadas_boletins_integrados.yml").write_text(
        "\n".join(
            [
                "id: inpe_bdqueimadas_boletins_integrados",
                'title: "INPE - BDQueimadas - Boletins Integrados"',
                'source_url: "https://example.test/boletins/"',
                "bucket_prefix: inpe/bdqueimadas/boletins_integrados",
            ]
        ),
        encoding="utf-8",
    )

    resources = [
        module.BoletimResource(
            period="2025-02",
            year="2025",
            month="02",
            filename="02_2025.pdf",
            url="https://example.test/boletins/2025/02_2025.pdf",
        )
    ]
    monkeypatch.setattr(module, "extract_pdf_urls", lambda source_url: resources)
    monkeypatch.setattr(
        module,
        "profiled_item",
        lambda **kw: {
            "kind": "data",
            "period": kw["period"],
            "filename": kw["filename"],
            "title": kw["title"],
            "source_url": kw["source_url"],
            "sha256": "abc",
            "size_bytes": 10,
            "profile_status": "ok",
            "profile_warnings": [],
        },
    )

    class FakeStorage:
        def __init__(self) -> None:
            self.uploads = []

        def upload_file(self, object_path, local_path, content_type, upsert=True) -> None:
            self.uploads.append((object_path, local_path, content_type, upsert))

        def public_url(self, object_path):
            return f"https://storage.test/{object_path}"

    storage = FakeStorage()
    settings = SimpleNamespace(
        datasets_dir=tmp_path / "configs" / "datasets",
        data_dir=tmp_path / "data",
    )
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)

    manifest = module.sync(settings=settings, storage=storage, logger=logger)

    assert manifest["dataset_id"] == "inpe_bdqueimadas_boletins_integrados"
    assert manifest["bucket_prefix"] == "inpe/bdqueimadas/boletins_integrados"
    assert manifest["items"][0]["period"] == "2025-02"
    assert manifest["items"][0]["kind"] == "data"
    assert manifest["items"][0]["title"] == "Boletim integrado 02/2025"
    assert manifest["items"][0]["sha256"] == "abc"
    assert manifest["items"][0]["size_bytes"] == 10
    assert manifest["items"][0]["source_url"] == "https://example.test/boletins/2025/02_2025.pdf"
    assert storage.uploads == []


def test_validate_source_urls_blocks_when_no_public_source_url() -> None:
    try:
        module.validate_source_urls([])
    except RuntimeError as exc:
        assert "Nenhum PDF público" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
