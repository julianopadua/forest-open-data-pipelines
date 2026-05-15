from __future__ import annotations

from types import SimpleNamespace

from forest_pipelines.datasets.mma import cnuc_unidades_conservacao as module


def test_is_allowed_download_url_accepts_mma_host() -> None:
    assert module.is_allowed_download_url(
        "https://dados.mma.gov.br/dataset/x/resource/y/download/file.csv"
    )
    assert not module.is_allowed_download_url("https://example.com/a.csv")


def test_period_from_resource_prefers_last_modified() -> None:
    assert (
        module.period_from_resource({"last_modified": "2024-03-01T12:00:00", "url": ""})
        == "2024-03-01"
    )


def test_build_manifest_items_skips_urls_in_skip_set() -> None:
    res = [
        {
            "name": "A",
            "format": "CSV",
            "url": "https://dados.mma.gov.br/x/download/a.csv",
            "last_modified": "2023-01-02T00:00:00",
            "size": 0,
        },
        {
            "name": "B",
            "format": "CSV",
            "url": "https://dados.mma.gov.br/x/download/b.csv",
            "last_modified": "2024-01-02T00:00:00",
            "size": 0,
        },
    ]
    items = module.build_manifest_items(res, skip_urls={res[1]["url"]})
    assert len(items) == 1
    assert items[0]["filename"] == "a.csv"
    assert items[0]["public_url"] == res[0]["url"]


def test_sync_indexes_external_urls_without_upload(monkeypatch, tmp_path) -> None:
    cfg_dir = tmp_path / "configs" / "datasets" / "mma"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "cnuc_unidades_conservacao.yml").write_text(
        "\n".join(
            [
                "id: mma_cnuc_unidades_conservacao",
                'title: "MMA - CNUC - Unidades de Conservação"',
                'source_url: "https://dados.mma.gov.br/dataset/unidadesdeconservacao"',
                "bucket_prefix: mma/cnuc/unidades_conservacao",
                "ckan_package_id: unidadesdeconservacao",
            ]
        ),
        encoding="utf-8",
    )

    package = {
        "metadata_modified": "2026-04-16T12:00:00",
        "resources": [
            {
                "name": "Dicionário de Dados - Unidades de Conservação",
                "format": "PDF",
                "url": "https://dados.mma.gov.br/dataset/x/res/d/download/dicionario.pdf",
                "last_modified": "2025-01-01T00:00:00",
                "size": 100,
            },
            {
                "name": "CNUC_2026_03",
                "format": "CSV",
                "url": "https://dados.mma.gov.br/dataset/x/res/d/download/cnuc.csv",
                "last_modified": "2026-03-15T00:00:00",
                "size": 0,
            },
            {
                "name": "ZIP externo",
                "format": "ZIP",
                "url": "https://sharepoint.example.com/file.zip",
                "last_modified": "2026-03-15T00:00:00",
                "size": 0,
            },
        ],
    }

    monkeypatch.setattr(module, "fetch_ckan_package", lambda package_id: package)

    class FakeStorage:
        def __init__(self) -> None:
            self.uploads: list[tuple[Any, ...]] = []

        def upload_file(self, *args: Any, **kwargs: Any) -> None:
            self.uploads.append((args, kwargs))

    storage = FakeStorage()
    settings = SimpleNamespace(datasets_dir=tmp_path / "configs" / "datasets")
    logger = SimpleNamespace(info=lambda *a, **k: None)

    manifest = module.sync(settings=settings, storage=storage, logger=logger)

    assert manifest["dataset_id"] == "mma_cnuc_unidades_conservacao"
    assert manifest["generation_status"] == "success_partial_fallback"
    assert any("omitido" in w for w in manifest["warnings"])
    assert len(manifest["items"]) == 1
    assert manifest["items"][0]["sha256"] == "external"
    assert manifest["items"][0]["filename"] == "cnuc.csv"
    assert manifest["meta"]["metadata_file"]["filename"] == "dicionario.pdf"
    assert storage.uploads == []
