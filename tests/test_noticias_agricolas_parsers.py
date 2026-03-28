# tests/test_noticias_agricolas_parsers.py
from __future__ import annotations

from pathlib import Path

from forest_pipelines.datasets.noticias_agricolas.article_parser import (
    extract_source_article_id,
    parse_article_html,
)
from forest_pipelines.datasets.noticias_agricolas.dates import parse_published_line, to_iso8601_z
from forest_pipelines.datasets.noticias_agricolas.list_parser import parse_category_list_html
from forest_pipelines.datasets.noticias_agricolas.merge import merge_listings_by_url
from forest_pipelines.datasets.noticias_agricolas.models import NewsListItem
from forest_pipelines.datasets.noticias_agricolas.text_cleanup import first_useful_paragraph

FIX = Path(__file__).resolve().parent / "fixtures" / "noticias_agricolas"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def test_parse_category_list_first_five() -> None:
    html = _read("category_list.html")
    items = parse_category_list_html(
        html,
        category_slug="clima",
        category_label="Clima",
        base_url="https://www.noticiasagricolas.com.br",
        limit=5,
    )
    assert len(items) == 2
    assert items[0].rank_within_category == 1
    assert "417968" in items[0].url
    assert items[0].listing_date_ddmmyyyy == "27/03/2026"
    assert items[0].listing_time_hhmm == "07:38"
    assert items[1].listing_date_ddmmyyyy == "26/03/2026"


def test_parse_article_full_lead_content_tags_image() -> None:
    html = _read("article_full.html")
    art = parse_article_html(
        html,
        url="https://www.noticiasagricolas.com.br/noticias/clima/x.html",
        base_url="https://www.noticiasagricolas.com.br",
        listing_date_ddmmyyyy="27/03/2026",
        listing_time_hhmm="07:38",
    )
    assert art.title == "Título da matéria"
    assert "Lead do texto" in (art.lead or "")
    assert "Primeiro parágrafo" in art.content_text
    assert "Subtítulo" in art.content_text
    assert art.tags == ["Clima", "Agro"]
    assert art.image_url == "https://cdn.example.com/img/main.jpg"
    assert art.published_at_iso.startswith("2026-03-27")
    assert art.published_at_iso.endswith("Z")


def test_parse_article_minimal_no_image_no_tags() -> None:
    html = _read("article_minimal.html")
    art = parse_article_html(
        html,
        url="https://www.noticiasagricolas.com.br/x/12345-slug.html",
        base_url="https://www.noticiasagricolas.com.br",
        listing_date_ddmmyyyy="15/01/2025",
        listing_time_hhmm="09:00",
    )
    assert art.image_url is None
    assert art.tags == []
    assert art.lead is None
    fp = first_useful_paragraph(art.content_text)
    assert fp is not None
    assert "Único parágrafo" in fp


def test_parse_published_line_iso() -> None:
    dt = parse_published_line("Publicado em 27/03/2026 07:38")
    assert dt is not None
    iso = to_iso8601_z(dt)
    assert iso == "2026-03-27T10:38:00Z"


def test_extract_source_article_id() -> None:
    assert extract_source_article_id(
        "https://www.noticiasagricolas.com.br/noticias/clima/417968-calor.html"
    ) == "417968"
    assert extract_source_article_id("https://example.com/foo.html") is None


def test_merge_dedup_url_two_categories() -> None:
    rows = [
        NewsListItem(
            url="https://www.noticiasagricolas.com.br/noticias/clima/1-x.html",
            title="T",
            category_slug="clima",
            category_label="Clima",
            rank_within_category=2,
            listing_date_ddmmyyyy="01/01/2026",
            listing_time_hhmm="10:00",
        ),
        NewsListItem(
            url="https://www.noticiasagricolas.com.br/noticias/clima/1-x.html",
            title="T",
            category_slug="meio-ambiente",
            category_label="Meio Ambiente",
            rank_within_category=1,
            listing_date_ddmmyyyy="01/01/2026",
            listing_time_hhmm="10:00",
        ),
    ]
    order = ["clima", "codigo-florestal", "meio-ambiente"]
    merged = merge_listings_by_url(rows, order)
    assert len(merged) == 1
    m = next(iter(merged.values()))
    assert set(m.category_slugs) == {"clima", "meio-ambiente"}
    assert m.rank_within_category == 1
    assert m.primary_slug == "clima"
