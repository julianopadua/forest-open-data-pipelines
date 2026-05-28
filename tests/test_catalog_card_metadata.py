from forest_pipelines.catalog.build import build_open_data_catalog, build_reports_catalog


def test_open_data_catalog_uses_base_dataset_entries(tmp_path):
    base_path = tmp_path / "open_data.yml"
    base_path.write_text(
        """
datasets:
  - id: anp_teste
    category_title: "Mercado de commodities"
    segment_title: "Energia"
    subcategory_title: "Petróleo e gás"
    source_id: anp
    source_title: ANP
    slug: teste
    title: "Teste ANP"
    description: "Entrada ANP normal no catalogo base."
    manifest_path: anp/teste/manifest.json
    source_url: "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/teste"
""".lstrip(),
        encoding="utf-8",
    )

    warnings: list[str] = []
    env = build_open_data_catalog(
        base_config_path=base_path,
        warnings_bucket=warnings,
    )

    dataset = env["datasets"][0]
    assert dataset["id"] == "anp_teste"
    assert dataset["manifest_path"] == "anp/teste/manifest.json"
    assert warnings == []


def test_reports_catalog_adds_card_metadata_from_report_document(tmp_path):
    reports_path = tmp_path / "reports.yml"
    reports_path.write_text(
        """
reports:
  - id: bdqueimadas_overview
    slug: bdqueimadas-overview
    title: BDQueimadas
    description: Fallback curto
    description_en: Short fallback
    source_title: INPE
    category_title: Meio ambiente
    manifest_path: reports/bdqueimadas/overview/manifest.json
    stable_report_path: reports/bdqueimadas/overview/report.json
    tags: [queimadas]
""".lstrip(),
        encoding="utf-8",
    )

    def loader(path: str):
        assert path == "reports/bdqueimadas/overview/report.json"
        return {
            "generated_at": "2026-05-22T12:00:00Z",
            "summary": {
                "pt": "Resumo em portugues para o card.",
                "en": "English summary for the card.",
            },
            "coverage": {
                "first_year": 2003,
                "latest_year": 2026,
                "year_range": "2003-2026",
                "latest_period": "2026-05",
            },
        }

    warnings: list[str] = []
    env = build_reports_catalog(
        reports_config_path=reports_path,
        warnings_bucket=warnings,
        report_loader=loader,
    )

    report = env["reports"][0]
    assert report["generated_at"] == "2026-05-22T12:00:00Z"
    assert report["coverage"]["year_range"] == "2003-2026"
    assert report["excerpt"] == "Resumo em portugues para o card."
    assert report["excerpt_en"] == "English summary for the card."
