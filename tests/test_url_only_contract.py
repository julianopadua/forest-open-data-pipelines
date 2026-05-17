from __future__ import annotations

import inspect
import sys
from pathlib import Path

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.registry.datasets import RUNNERS
from forest_pipelines.cli import _merge_incremental_manifest_items


def test_registered_dataset_runners_do_not_upload_payload_files() -> None:
    repo = Path(__file__).resolve().parents[1]
    src = repo / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    offenders: list[str] = []
    for dataset_id, runner in RUNNERS.items():
        module = inspect.getmodule(runner)
        if module is None or module.__file__ is None:
            continue
        path = Path(module.__file__)
        if "datasets" not in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "storage.upload_file(" in text or "stream_download(" in text:
            offenders.append(f"{dataset_id}: {path.relative_to(repo)}")

    assert offenders == []


def test_manifest_builder_removes_legacy_storage_urls() -> None:
    manifest = build_manifest(
        "dataset",
        "Dataset",
        "https://source.test/page",
        "source/dataset",
        [
            {
                "kind": "data",
                "period": "2024",
                "filename": "data.csv",
                "public_url": "https://storage.test/data.csv",
                "storage_path": "source/dataset/data.csv",
            }
        ],
        meta={
            "metadata_file": {
                "filename": "dictionary.txt",
                "public_url": "https://storage.test/dictionary.txt",
                "storage_path": "source/dataset/dictionary.txt",
            }
        },
    )

    item = manifest["items"][0]
    metadata_file = manifest["meta"]["metadata_file"]
    assert item["source_url"] == "https://storage.test/data.csv"
    assert metadata_file["source_url"] == "https://storage.test/dictionary.txt"
    assert "public_url" not in item
    assert "storage_path" not in item
    assert "public_url" not in metadata_file
    assert "storage_path" not in metadata_file


def test_incremental_manifest_merge_retains_existing_source_urls() -> None:
    class Logger:
        def info(self, *args: object, **kwargs: object) -> None:
            return None

    current = build_manifest(
        "dataset",
        "Dataset",
        "https://source.test/page",
        "source/dataset",
        [
            {
                "kind": "data",
                "period": "2025",
                "filename": "new.csv",
                "source_url": "https://source.test/new.csv",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ],
        meta={},
    )
    existing = build_manifest(
        "dataset",
        "Dataset",
        "https://source.test/page",
        "source/dataset",
        [
            {
                "kind": "data",
                "period": "2024",
                "filename": "old.csv",
                "source_url": "https://source.test/old.csv",
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ],
        meta={},
    )

    merged = _merge_incremental_manifest_items(
        current_manifest=current,
        existing_manifest=existing,
        logger=Logger(),
    )

    assert [item["source_url"] for item in merged["items"]] == [
        "https://source.test/new.csv",
        "https://source.test/old.csv",
    ]
