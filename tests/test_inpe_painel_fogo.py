from __future__ import annotations

from types import SimpleNamespace

from forest_pipelines.datasets.inpe import bdqueimadas_painel_fogo as module


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.headers = {"Content-Type": "text/html; charset=utf-8"}

    def raise_for_status(self) -> None:
        return None


def test_parse_painel_pdf_link_extracts_month_period() -> None:
    resource = module.parse_painel_pdf_link(
        "Painel_Qmd_03_2024.pdf",
        "https://dataserver-coids.inpe.br/queimadas/queimadas/Painel-Fogo/2024/",
    )

    assert resource is not None
    assert resource.period == "2024-03"
    assert resource.year == "2024"
    assert resource.month == "03"
    assert resource.filename == "Painel_Qmd_03_2024.pdf"
    assert resource.url.endswith("/2024/Painel_Qmd_03_2024.pdf")


def test_extract_painel_pdf_urls_walks_year_directories(monkeypatch) -> None:
    pages = {
        "https://example.test/painel/": """
            <a href="../">Voltar</a>
            <a href="2024/">Diretório 2024</a>
            <a href="2025/">Diretório 2025</a>
        """,
        "https://example.test/painel/2024/": """
            <a href="../">Voltar</a>
            <a href="Painel_Qmd_01_2024.pdf">Painel_Qmd_01_2024.pdf</a>
            <a href="not-a-painel.txt">not-a-painel.txt</a>
        """,
        "https://example.test/painel/2025/": """
            <a href="../">Voltar</a>
            <a href="Painel_Qmd_02_2025.pdf">Painel_Qmd_02_2025.pdf</a>
        """,
    }

    def fake_get(url: str, timeout: int) -> FakeResponse:
        assert timeout == 60
        return FakeResponse(pages[url])

    monkeypatch.setattr(module.requests, "get", fake_get)

    resources = module.extract_painel_pdf_urls("https://example.test/painel/")

    assert [resource.period for resource in resources] == ["2025-02", "2024-01"]
    assert [resource.filename for resource in resources] == [
        "Painel_Qmd_02_2025.pdf",
        "Painel_Qmd_01_2024.pdf",
    ]


def test_sync_indexes_source_urls_without_uploading_pdfs(monkeypatch, tmp_path) -> None:
    cfg_dir = tmp_path / "configs" / "datasets" / "inpe"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "bdqueimadas_painel_fogo.yml").write_text(
        "\n".join(
            [
                "id: inpe_bdqueimadas_painel_fogo",
                'title: "INPE - BDQueimadas - Painel-Fogo"',
                'source_url: "https://example.test/painel/"',
                "bucket_prefix: inpe/bdqueimadas/painel_fogo",
            ]
        ),
        encoding="utf-8",
    )

    resources = [
        module.PainelResource(
            period="2025-02",
            year="2025",
            month="02",
            filename="Painel_Qmd_02_2025.pdf",
            url="https://example.test/painel/2025/Painel_Qmd_02_2025.pdf",
        )
    ]
    monkeypatch.setattr(module, "extract_painel_pdf_urls", lambda source_url: resources)
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

    assert manifest["dataset_id"] == "inpe_bdqueimadas_painel_fogo"
    assert manifest["bucket_prefix"] == "inpe/bdqueimadas/painel_fogo"
    assert manifest["items"][0]["period"] == "2025-02"
    assert manifest["items"][0]["kind"] == "data"
    assert manifest["items"][0]["title"] == "Painel de queimadas 02/2025"
    assert manifest["items"][0]["sha256"] == "abc"
    assert manifest["items"][0]["size_bytes"] == 10
    assert manifest["items"][0]["source_url"] == "https://example.test/painel/2025/Painel_Qmd_02_2025.pdf"
    assert storage.uploads == []


def test_validate_source_urls_blocks_when_empty() -> None:
    try:
        module.validate_source_urls([])
    except RuntimeError as exc:
        assert "Nenhum PDF público" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
