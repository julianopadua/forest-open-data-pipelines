# tests/test_anp_catalog_compact.py
from __future__ import annotations

import json
from pathlib import Path

import pytest

from forest_pipelines.dados_abertos.anp_catalog_compact import (
    compact_schema_path,
    infer_resource_kind,
    load_anp_snapshot,
    nfc_text,
    transform_anp_snapshot,
    validate_compact_envelope,
    write_compact_catalog,
)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
MINIMAL_SNAPSHOT = FIXTURE_DIR / "anp_snapshot_minimal.json"


def test_infer_resource_kind() -> None:
    assert infer_resource_kind("Metadados", "PDF") == "documentation"
    assert infer_resource_kind("foo", "CSV") == "data"
    assert infer_resource_kind("relatorio", "PDF") == "documentation"
    assert infer_resource_kind("x", "FOO") == "other"


def test_nfc_text_normalizes_strings() -> None:
    import unicodedata

    s = unicodedata.normalize("NFD", "ação")
    assert nfc_text(s) == unicodedata.normalize("NFC", "ação")


def test_transform_minimal_matches_schema_and_utf8(tmp_path: Path) -> None:
    data = load_anp_snapshot(MINIMAL_SNAPSHOT)
    env = transform_anp_snapshot(data)
    assert env["schema_version"] == "1"
    assert env["source_total_registros"] == 1
    assert len(env["datasets"]) == 1
    ds = env["datasets"][0]
    assert ds["slug"] == "exemplo-dataset"
    assert "acentuação" in ds["title"]
    assert ds["extra_fields"]["periodicidade"] == "MENSAL"
    assert ds["extra_fields"]["cobertura_espacial"] == "FEDERAL"

    res = {r["resource_id"]: r for r in ds["resources"]}
    assert res["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]["sources"] == ["acesso_rapido", "formatado"]
    assert res["cccccccc-cccc-cccc-cccc-cccccccccccc"]["sources"] == ["formatado"]
    assert res["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]["sources"] == ["acesso_rapido"]

    out = tmp_path / "out.json"
    write_compact_catalog(out, env)
    raw = out.read_text(encoding="utf-8")
    assert "Exemplo com acentuação" in raw

    validate_compact_envelope(env)


def test_schema_file_exists() -> None:
    p = compact_schema_path()
    assert p.is_file()
    assert "anp_catalog_compact.v1.schema.json" in p.name


@pytest.mark.skipif(
    not (Path(__file__).resolve().parents[1] / "src" / "forest_pipelines" / "dados_abertos" / "anp.json").is_file(),
    reason="large snapshot not in working tree",
)
def test_transform_full_anp_json_validates() -> None:
    repo = Path(__file__).resolve().parents[1]
    big = repo / "src" / "forest_pipelines" / "dados_abertos" / "anp.json"
    data = load_anp_snapshot(big)
    env = transform_anp_snapshot(data)
    validate_compact_envelope(env)
    assert len(env["datasets"]) >= 1
