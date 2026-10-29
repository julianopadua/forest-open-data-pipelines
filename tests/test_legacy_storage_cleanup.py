from __future__ import annotations

import json

from forest_pipelines.storage.legacy_cleanup import (
    audit_references,
    is_legacy_object,
    list_objects,
)


class FakeBucket:
    def __init__(self, referenced: bool = False) -> None:
        self.referenced = referenced

    def list(self, prefix: str, _options: dict) -> list[dict]:
        folders = {
            "": [{"name": "inpe", "id": None, "metadata": None},
                 {"name": "catalog", "id": None, "metadata": None}],
            "inpe": [{"name": "focos", "id": None, "metadata": None}],
            "inpe/focos": [{"name": "data", "id": None, "metadata": None},
                           {"name": "manifest.json", "id": "manifest", "metadata": {"size": 100}}],
            "inpe/focos/data": [{"name": "2021.zip", "id": "zip", "metadata": {"size": 700}}],
            "catalog": [{"name": "open_data_catalog.json", "id": "catalog", "metadata": {"size": 20}}],
        }
        return folders.get(prefix, [])

    def download(self, path: str) -> bytes:
        if path == "inpe/focos/manifest.json":
            url = (
                "https://project.supabase.co/storage/v1/object/public/open-data/inpe/focos/data/2021.zip"
                if self.referenced else "https://dataserver-coids.inpe.br/focos_2021.zip"
            )
            return json.dumps({"items": [{"source_url": url}]}).encode()
        return b"{}"


def test_cleanup_plan_selects_legacy_data_only() -> None:
    bucket = FakeBucket()
    objects = list_objects(bucket)
    candidates = [obj for obj in objects if is_legacy_object(obj.path)]

    assert [(obj.path, obj.size_bytes) for obj in candidates] == [
        ("inpe/focos/data/2021.zip", 700)
    ]
    assert audit_references(bucket, "open-data", objects) == set()


def test_cleanup_blocks_referenced_storage_object() -> None:
    bucket = FakeBucket(referenced=True)
    objects = list_objects(bucket)

    assert audit_references(bucket, "open-data", objects) == {
        "inpe/focos/data/2021.zip"
    }


def test_cleanup_never_selects_report_or_manifest() -> None:
    assert not is_legacy_object("reports/bdqueimadas/live/report.json")
    assert not is_legacy_object("inpe/bdqueimadas/manifest.json")
    assert is_legacy_object("cvm/fi/doc/entrega/data/2025-04/file.zip")
    assert is_legacy_object("news/noticias-agricolas/snapshots/2026/10/28/file.json")
