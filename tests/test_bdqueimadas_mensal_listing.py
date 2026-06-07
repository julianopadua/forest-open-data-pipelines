from __future__ import annotations

from pathlib import Path

from forest_pipelines.datasets.inpe import bdqueimadas_mensal_listing as listing


def test_ensure_mensal_files_refreshes_only_forced_month(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cache_dir = tmp_path / "mensal"
    cache_dir.mkdir()
    jan = cache_dir / "focos_mensal_br_202601.csv"
    may = cache_dir / "focos_mensal_br_202605.csv"
    jan.write_text("jan-cache\n", encoding="utf-8")
    may.write_text("may-cache\n", encoding="utf-8")

    links = [
        (202601, "focos_mensal_br_202601.csv", "https://example.test/jan.csv"),
        (202602, "focos_mensal_br_202602.csv", "https://example.test/feb.csv"),
        (202605, "focos_mensal_br_202605.csv", "https://example.test/may.csv"),
        (202606, "focos_mensal_br_202606.csv", "https://example.test/jun.csv"),
    ]
    downloads: list[str] = []

    def fake_extract_mensal_links(_base_url: str):
        return links

    def fake_stream_download(url: str, out_path: Path):
        downloads.append(url)
        out_path.write_text(f"downloaded {url}\n", encoding="utf-8")

    monkeypatch.setattr(listing, "extract_mensal_links", fake_extract_mensal_links)
    monkeypatch.setattr(listing, "stream_download", fake_stream_download)

    files = listing.ensure_mensal_files_for_year(
        base_url="https://example.test/",
        year=2026,
        cache_dir=cache_dir,
        skip_download=False,
        months=range(1, 6),
        force_download_months=[5],
    )

    assert [(month, path.name) for month, path in files] == [
        (1, "focos_mensal_br_202601.csv"),
        (2, "focos_mensal_br_202602.csv"),
        (5, "focos_mensal_br_202605.csv"),
    ]
    assert downloads == [
        "https://example.test/feb.csv",
        "https://example.test/may.csv",
    ]
    assert jan.read_text(encoding="utf-8") == "jan-cache\n"
    assert may.read_text(encoding="utf-8") == "downloaded https://example.test/may.csv\n"
    assert not (cache_dir / "focos_mensal_br_202606.csv").exists()
