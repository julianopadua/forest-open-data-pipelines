from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from forest_pipelines.datasets.cvm import ckan_dataset
from forest_pipelines.registry.datasets import RUNNERS


class Logger:
    def info(self, *args: object, **kwargs: object) -> None:
        return None


def test_cvm_catalog_entries_have_runner_and_config() -> None:
    repo = Path(__file__).resolve().parents[1]
    catalog_path = repo / "configs" / "catalog" / "open_data.yml"
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    entries = [
        entry
        for entry in catalog["datasets"]
        if isinstance(entry, dict) and entry.get("source_id") == "cvm"
    ]

    assert len(entries) == 56
    for entry in entries:
        dataset_id = entry["id"]
        config_path = repo / "configs" / "datasets" / "cvm" / f"{dataset_id.removeprefix('cvm_')}.yml"
        assert dataset_id in RUNNERS
        assert config_path.exists()
        assert entry["source_url"].startswith("https://dados.cvm.gov.br/dataset/")
        assert entry["manifest_path"].endswith("/manifest.json")


def test_cvm_ckan_runner_filters_metadata_and_builds_manifest(tmp_path: Path, monkeypatch: Any) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "cvm"
    config_dir.mkdir(parents=True)
    (config_dir / "processo_sancionador.yml").write_text(
        "\n".join(
            [
                "id: cvm_processo_sancionador",
                'title: "CVM - Processos Sancionadores"',
                "ckan_package_id: processo-sancionador",
                'source_dataset_url: "https://dados.cvm.gov.br/dataset/processo-sancionador"',
                "bucket_prefix: cvm/processo_sancionador",
                "include_meta: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch(package_id: str) -> dict[str, Any]:
        assert package_id == "processo-sancionador"
        return {
            "metadata_modified": "2026-05-20T12:00:00",
            "resources": [
                {
                    "name": "Processos sancionadores",
                    "url": "https://dados.cvm.gov.br/dados/processo_sancionador.zip",
                    "format": "ZIP",
                },
                {
                    "name": "Meta processo sancionador",
                    "url": "https://dados.cvm.gov.br/dados/meta_processo_sancionador.zip",
                    "format": "ZIP",
                },
                {
                    "name": "Mirror externo",
                    "url": "https://example.com/processo_sancionador.zip",
                    "format": "ZIP",
                },
            ],
        }

    def fake_profile_source_url(source_url: str, **kwargs: Any) -> dict[str, Any]:
        return {
            "size_bytes": 10,
            "sha256": "abc",
            "format": Path(source_url).suffix.lstrip("."),
            "profile_status": "ok",
            "profile_warnings": [],
        }

    def fake_profiled_item(**kwargs: Any) -> dict[str, Any]:
        return {
            "kind": kwargs["kind"],
            "period": kwargs["period"],
            "filename": kwargs["filename"],
            "source_url": kwargs["source_url"],
            "title": kwargs["title"],
            **fake_profile_source_url(kwargs["source_url"]),
        }

    monkeypatch.setattr(ckan_dataset, "fetch_ckan_package", fake_fetch)
    monkeypatch.setattr(ckan_dataset, "profile_source_url", fake_profile_source_url)
    monkeypatch.setattr(ckan_dataset, "profiled_item", fake_profiled_item)

    manifest = ckan_dataset.sync_dataset(
        dataset_id="cvm_processo_sancionador",
        settings=SimpleNamespace(datasets_dir=datasets_dir),
        storage=None,
        logger=Logger(),
    )

    assert manifest["dataset_id"] == "cvm_processo_sancionador"
    assert manifest["source_dataset_url"] == "https://dados.cvm.gov.br/dataset/processo-sancionador"
    assert [item["filename"] for item in manifest["items"]] == ["processo_sancionador.zip"]
    assert manifest["meta"]["metadata_file"]["filename"] == "meta_processo_sancionador.zip"
    assert manifest["meta"]["custom_tags"]["ckan_resource_count"] == 3
    assert manifest["warnings"] == ["1 recurso(s) CKAN fora dos filtros do dataset foram omitidos do manifest."]
