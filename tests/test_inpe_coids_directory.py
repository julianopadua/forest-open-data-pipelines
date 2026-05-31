from __future__ import annotations

from types import SimpleNamespace

from forest_pipelines.datasets.inpe import bdqueimadas_focos_coids as coids_runner
from forest_pipelines.datasets.inpe.coids_directory import (
    CoidsEntry,
    discover_files,
    parse_directory_entries,
)


def test_parse_directory_entries_ignores_navigation_and_sort_links() -> None:
    html = """
    <html><body>
      <a href="../">Parent Directory</a>
      <a href="?C=N;O=D">Name</a>
      <a href="AC/">AC/</a>
      <a href="focos_diario_br_20260531.csv">focos_diario_br_20260531.csv</a> 2026-05-31 09:05 143K
    </body></html>
    """

    entries = parse_directory_entries(html, "https://example.test/focos/")

    assert [entry.name for entry in entries] == ["AC", "focos_diario_br_20260531.csv"]
    assert entries[0].is_dir is True
    assert entries[1].is_dir is False
    assert entries[1].url == "https://example.test/focos/focos_diario_br_20260531.csv"


def test_discover_files_recurses_directories(monkeypatch) -> None:
    pages = {
        "https://example.test/root/": [
            CoidsEntry(name="AC", url="https://example.test/root/AC/", is_dir=True),
            CoidsEntry(name="readme.txt", url="https://example.test/root/readme.txt", is_dir=False),
        ],
        "https://example.test/root/AC/": [
            CoidsEntry(name="focos_ac_ref_2025.zip", url="https://example.test/root/AC/focos_ac_ref_2025.zip", is_dir=False),
        ],
    }

    monkeypatch.setattr(
        "forest_pipelines.datasets.inpe.coids_directory.fetch_directory_entries",
        lambda url, timeout_s=60: pages[url],
    )

    entries = discover_files("https://example.test/root/", recursive=True, max_depth=2)

    assert [entry.filename for entry in entries] == ["focos_ac_ref_2025.zip", "readme.txt"]


def test_entry_period_strategies() -> None:
    daily = CoidsEntry(
        name="focos_diario_br_20260531.csv",
        url="https://example.test/focos_diario_br_20260531.csv",
        is_dir=False,
    )
    ten_min = CoidsEntry(
        name="focos_10min_20260531_0910.csv",
        url="https://example.test/focos_10min_20260531_0910.csv",
        is_dir=False,
    )
    state = CoidsEntry(
        name="focos_mt_ref_2025.zip",
        url="https://example.test/EstadosBr_sat_ref/MT/focos_mt_ref_2025.zip",
        is_dir=False,
    )

    assert coids_runner.entry_period(daily, "daily") == "2026-05-31"
    assert coids_runner.entry_period(ten_min, "ten_min") == "2026-05-31 0910"
    assert coids_runner.entry_period(state, "annual_state") == "MT/2025"


def test_coids_sync_preserves_source_urls_without_upload(monkeypatch, tmp_path) -> None:
    cfg_dir = tmp_path / "configs" / "datasets" / "inpe"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "bdqueimadas_focos_diario_brasil.yml").write_text(
        "\n".join(
            [
                "id: inpe_bdqueimadas_focos_diario_brasil",
                'title: "Diario Brasil"',
                'source_url: "https://example.test/diario/Brasil/"',
                "bucket_prefix: inpe/bdqueimadas/focos/diario/brasil",
                "period_strategy: daily",
            ]
        ),
        encoding="utf-8",
    )
    entry = CoidsEntry(
        name="focos_diario_br_20260531.csv",
        url="https://example.test/diario/Brasil/focos_diario_br_20260531.csv",
        is_dir=False,
        size_label="143K",
        last_modified_label="2026-05-31 09:05",
    )
    monkeypatch.setattr(coids_runner, "discover_files", lambda *args, **kwargs: [entry])
    monkeypatch.setattr(
        coids_runner,
        "profiled_item",
        lambda **kwargs: {
            "kind": "data",
            "period": kwargs["period"],
            "filename": kwargs["filename"],
            "source_url": kwargs["source_url"],
            "profile_status": "ok",
            "profile_warnings": [],
        },
    )

    manifest = coids_runner.make_sync("bdqueimadas_focos_diario_brasil")(
        settings=SimpleNamespace(datasets_dir=tmp_path / "configs" / "datasets"),
        storage=SimpleNamespace(),
        logger=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    assert manifest["dataset_id"] == "inpe_bdqueimadas_focos_diario_brasil"
    assert manifest["items"][0]["source_url"] == entry.url
    assert manifest["items"][0]["release_time"] == "2026-05-31 09:05"
    assert manifest["items"][0]["source_size_label"] == "143K"
