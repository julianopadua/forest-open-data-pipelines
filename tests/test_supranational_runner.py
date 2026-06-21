from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from forest_pipelines.datasets.supranational import runner
from forest_pipelines.registry.datasets import RUNNERS


class Logger:
    def info(self, *args: object, **kwargs: object) -> None:
        return None

    def warning(self, *args: object, **kwargs: object) -> None:
        return None


def test_supranational_catalog_entries_have_runner_and_config() -> None:
    repo = Path(__file__).resolve().parents[1]
    catalog = yaml.safe_load((repo / "configs" / "catalog" / "open_data.yml").read_text())
    entries = [
        entry
        for entry in catalog["datasets"]
        if isinstance(entry, dict) and entry["id"] in runner.SUPRANATIONAL_DATASET_IDS
    ]

    assert len(entries) == len(runner.SUPRANATIONAL_DATASET_IDS)
    assert len({entry["id"] for entry in entries}) == len(entries)
    assert len({entry["slug"] for entry in entries}) == len(entries)
    for dataset_id in runner.SUPRANATIONAL_DATASET_IDS:
        config_path = repo / "configs" / "datasets" / "supranational" / f"{dataset_id}.yml"
        assert dataset_id in RUNNERS
        assert config_path.exists()
    for entry in entries:
        assert entry["manifest_path"].startswith("supranational/")
        assert entry["manifest_path"].endswith("/manifest.json")
        assert entry["source_url"].startswith("https://")


def test_static_runner_builds_manifest_with_strict_meta(tmp_path: Path, monkeypatch: Any) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "supranational"
    config_dir.mkdir(parents=True)
    (config_dir / "un_wpp_2024_bulk_csv.yml").write_text(
        "\n".join(
            [
                "id: un_wpp_2024_bulk_csv",
                'title: "UN WPP 2024 - Bulk CSV"',
                "protocol: static_files",
                'source_dataset_url: "https://population.un.org/wpp/downloads/"',
                "bucket_prefix: supranational/un/wpp_2024_bulk_csv",
                'source_agency: "United Nations - Population Division"',
                'notes: "Official bulk CSV files."',
                "allowed_hosts:",
                "  - population.un.org",
                "geographic_scope: global",
                "temporal_granularity: annual",
                'dataset_version: "2024"',
                "profile_mode: headers",
                "resources:",
                '  - title: "Demographic indicators"',
                '    period: "2024"',
                "    filename: WPP2024_Demographic_Indicators_Medium.csv.gz",
                '    source_url: "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz"',
                '    source_page_url: "https://population.un.org/wpp/downloads/"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        runner,
        "_profile_headers",
        lambda source_url, filename, mode="headers": {
            "size_bytes": 10,
            "format": "csv.gz",
            "profile_status": "skipped",
            "profile_warnings": [],
        },
    )

    manifest = runner.sync_dataset(
        dataset_id="un_wpp_2024_bulk_csv",
        settings=SimpleNamespace(datasets_dir=datasets_dir),
        storage=None,
        logger=Logger(),
    )

    assert manifest["schema_version"] == "2.0"
    assert manifest["dataset_id"] == "un_wpp_2024_bulk_csv"
    assert manifest["items"][0]["source_url"].startswith("https://population.un.org/")
    assert set(manifest["meta"]) == {"source_agency", "notes", "custom_tags"}
    assert manifest["meta"]["custom_tags"]["provider_family"] == "supranational"
    assert manifest["meta"]["custom_tags"]["dataset_version"] == "2024"


def test_url_policy_rejects_non_contract_urls() -> None:
    allowed = ("energydata.info",)

    assert runner._url_allowed(
        "https://energydata.info/dataset/foo/resource/bar/download/data.zip",
        allowed,
    )
    assert not runner._url_allowed("http://energydata.info/data.zip", allowed)
    assert not runner._url_allowed("https://energydata.info/datastore_search?resource_id=x", allowed)
    assert not runner._url_allowed("https://energydata.info/preview/data.zip", allowed)
    assert not runner._url_allowed("https://energydata.info/data.zip?token=secret", allowed)
    assert not runner._url_allowed("https://example.test/data.zip", allowed)


def test_catalog_config_rejects_unallowlisted_api_url(tmp_path: Path) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "supranational"
    config_dir.mkdir(parents=True)
    (config_dir / "energydata_brazil_road_network.yml").write_text(
        "\n".join(
            [
                "id: energydata_brazil_road_network",
                'title: "EnergyData.info - Brazil Road Network"',
                "protocol: ckan_files",
                'source_dataset_url: "https://energydata.info/dataset/brazil-road-network-federal-and-state-highways"',
                "bucket_prefix: supranational/energydata/brazil_road_network",
                'source_agency: "World Bank Group - EnergyData.info"',
                'notes: "Official CKAN resources."',
                "allowed_hosts:",
                "  - energydata.info",
                'ckan_api_url: "https://example.test/api/3/action/package_show?id=road-network"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="URL is not accepted"):
        runner.load_dataset_cfg(datasets_dir, "energydata_brazil_road_network")


def test_ckan_runner_filters_license_hosts_and_datastore(tmp_path: Path, monkeypatch: Any) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "supranational"
    config_dir.mkdir(parents=True)
    (config_dir / "energydata_brazil_road_network.yml").write_text(
        "\n".join(
            [
                "id: energydata_brazil_road_network",
                'title: "EnergyData.info - Brazil Road Network"',
                "protocol: ckan_files",
                'source_dataset_url: "https://energydata.info/dataset/brazil-road-network-federal-and-state-highways"',
                "bucket_prefix: supranational/energydata/brazil_road_network",
                'source_agency: "World Bank Group - EnergyData.info"',
                'notes: "Official CKAN resources."',
                "allowed_hosts:",
                "  - energydata.info",
                "  - datacatalogfiles.worldbank.org",
                "accepted_license_ids:",
                "  - CC-BY-4.0",
                'ckan_api_url: "https://energydata.info/api/3/action/package_show?id=6f74ce08-b5e9-463c-97e1-60a772db4f60"',
                "ckan_package_id: 6f74ce08-b5e9-463c-97e1-60a772db4f60",
                "profile_mode: headers",
                "resource_exclude:",
                '  - "datastore"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        runner,
        "_fetch_json",
        lambda url: {
            "success": True,
            "result": {
                "private": False,
                "state": "active",
                "isopen": True,
                "license_id": "CC-BY-4.0",
                "resources": [
                    {
                        "name": "Federal highways",
                        "url": "https://datacatalogfiles.worldbank.org/ddh-published/0038536/DR0047126/br-federal-highways-2020.zip",
                        "format": "ZIP",
                        "state": "active",
                    },
                    {
                        "name": "Datastore",
                        "url": "https://energydata.info/api/3/action/datastore_search?resource_id=x",
                        "format": "JSON",
                        "state": "active",
                    },
                    {
                        "name": "External",
                        "url": "https://example.test/data.zip",
                        "format": "ZIP",
                        "state": "active",
                    },
                ],
            },
        },
    )
    monkeypatch.setattr(
        runner,
        "_profile_headers",
        lambda source_url, filename, mode="headers": {
            "size_bytes": 10,
            "format": "zip",
            "profile_status": "skipped",
            "profile_warnings": [],
        },
    )

    manifest = runner.sync_dataset(
        dataset_id="energydata_brazil_road_network",
        settings=SimpleNamespace(datasets_dir=datasets_dir),
        storage=None,
        logger=Logger(),
    )

    assert [item["filename"] for item in manifest["items"]] == ["br-federal-highways-2020.zip"]
    assert manifest["warnings"] == ["2 CKAN resource(s) were omitted by URL, license, or format filters."]
    assert manifest["meta"]["custom_tags"]["accepted_license_ids"] == ["CC-BY-4.0"]


def test_ckan_runner_rejects_unaccepted_license(tmp_path: Path, monkeypatch: Any) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "supranational"
    config_dir.mkdir(parents=True)
    (config_dir / "energydata_brazil_road_network.yml").write_text(
        "\n".join(
            [
                "id: energydata_brazil_road_network",
                'title: "EnergyData.info - Brazil Road Network"',
                "protocol: ckan_files",
                'source_dataset_url: "https://energydata.info/dataset/brazil-road-network-federal-and-state-highways"',
                "bucket_prefix: supranational/energydata/brazil_road_network",
                'source_agency: "World Bank Group - EnergyData.info"',
                'notes: "Official CKAN resources."',
                "allowed_hosts:",
                "  - energydata.info",
                "accepted_license_ids:",
                "  - CC-BY-4.0",
                'ckan_api_url: "https://energydata.info/api/3/action/package_show?id=6f74ce08-b5e9-463c-97e1-60a772db4f60"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        runner,
        "_fetch_json",
        lambda url: {
            "success": True,
            "result": {
                "private": False,
                "state": "active",
                "isopen": True,
                "license_id": "notspecified",
                "resources": [],
            },
        },
    )

    with pytest.raises(RuntimeError, match="license not accepted"):
        runner.sync_dataset(
            dataset_id="energydata_brazil_road_network",
            settings=SimpleNamespace(datasets_dir=datasets_dir),
            storage=None,
            logger=Logger(),
        )


def test_faostat_catalog_uses_official_file_location(tmp_path: Path, monkeypatch: Any) -> None:
    datasets_dir = tmp_path / "datasets"
    config_dir = datasets_dir / "supranational"
    config_dir.mkdir(parents=True)
    (config_dir / "faostat_qcl.yml").write_text(
        "\n".join(
            [
                "id: faostat_qcl",
                'title: "FAOSTAT - Crops and Livestock Products"',
                "protocol: bulk_catalog",
                'source_dataset_url: "https://www.fao.org/faostat/en/#data/QCL"',
                "bucket_prefix: supranational/faostat/qcl",
                'source_agency: "FAO - FAOSTAT"',
                'notes: "Official bulk catalog."',
                "allowed_hosts:",
                "  - www.fao.org",
                "  - bulks-faostat.fao.org",
                'faostat_catalog_url: "https://bulks-faostat.fao.org/production/datasets_E.xml"',
                "faostat_dataset_code: QCL",
                "profile_mode: headers",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        runner,
        "_fetch_text",
        lambda url: """
        <Datasets>
          <Dataset>
            <DatasetCode>QCL</DatasetCode>
            <DatasetName>Crops and livestock products</DatasetName>
            <FileLocation>https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip</FileLocation>
          </Dataset>
        </Datasets>
        """,
    )
    monkeypatch.setattr(
        runner,
        "_profile_headers",
        lambda source_url, filename, mode="headers": {
            "size_bytes": 10,
            "format": "zip",
            "profile_status": "skipped",
            "profile_warnings": [],
        },
    )

    manifest = runner.sync_dataset(
        dataset_id="faostat_qcl",
        settings=SimpleNamespace(datasets_dir=datasets_dir),
        storage=None,
        logger=Logger(),
    )

    assert manifest["items"][0]["source_url"].startswith("https://bulks-faostat.fao.org/")
    assert manifest["items"][0]["filename"] == "Production_Crops_Livestock_E_All_Data_(Normalized).zip"
    assert manifest["meta"]["custom_tags"]["upstream_dataset_id"] == "QCL"
