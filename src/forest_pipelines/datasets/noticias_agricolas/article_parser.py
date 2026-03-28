# src/forest_pipelines/datasets/noticias_agricolas/article_parser.py
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from forest_pipelines.datasets.noticias_agricolas.dates import (
    combine_listing_datetime,
    parse_published_line,
    to_iso8601_z,
)
from forest_pipelines.datasets.noticias_agricolas.models import ArticleDetail
from forest_pipelines.datasets.noticias_agricolas.text_cleanup import normalize_body_text, normalize_paragraph_text

_RE_ARTICLE_ID = re.compile(r"/(\d{4,})-")


def extract_source_article_id(url: str) -> str | None:
    m = _RE_ARTICLE_ID.search(url)
    return m.group(1) if m else None


def _meta_content(soup: BeautifulSoup, *, prop: str | None = None, name: str | None = None) -> str | None:
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
    else:
        tag = soup.find("meta", attrs={"name": name})
    if not tag or not isinstance(tag, Tag):
        return None
    c = tag.get("content")
    return str(c).strip() if c else None


def _extract_image_url(soup: BeautifulSoup, base_url: str, materia: Tag | None) -> str | None:
    for prop in ("og:image",):
        u = _meta_content(soup, prop=prop)
        if u and u.startswith("http"):
            return u
    tw = _meta_content(soup, name="twitter:image")
    if tw and tw.startswith("http"):
        return tw
    if materia:
        for img in materia.find_all("img", src=True):
            src = img.get("src", "").strip()
            if not src or "doubleclick" in src or "googlesyndication" in src:
                continue
            return urljoin(base_url, src)
    return None


def _materia_blocks(materia: Tag) -> list[str]:
    lines: list[str] = []
    for el in materia.find_all(["p", "h2", "h3"], recursive=True):
        parent_classes = []
        p = el.parent
        while p and isinstance(p, Tag):
            cls = " ".join(p.get("class", []) if isinstance(p.get("class"), list) else [])
            parent_classes.append(cls)
            p = p.parent
        joined = " ".join(parent_classes).lower()
        if "comentarios" in joined or "newsletter" in joined:
            continue
        text = el.get_text(" ", strip=True)
        text = normalize_paragraph_text(text)
        if text:
            lines.append(text)
    return lines


def parse_article_html(
    html: str,
    *,
    url: str,
    base_url: str,
    listing_date_ddmmyyyy: str,
    listing_time_hhmm: str,
) -> ArticleDetail:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1", class_="page-title")
    title = h1.get_text(" ", strip=True) if h1 else ""

    datas_el = soup.find("div", class_="datas")
    datas_raw = datas_el.get_text(" ", strip=True) if datas_el else ""

    lead_el = soup.find("div", class_="lead")
    lead = lead_el.get_text(" ", strip=True) if lead_el else None
    if lead == "":
        lead = None

    materia = soup.find("div", class_="materia")
    if materia and isinstance(materia, Tag):
        block_lines = _materia_blocks(materia)
        content_text = normalize_body_text(block_lines)
    else:
        content_text = ""

    tags: list[str] = []
    tags_wrap = soup.find("div", class_="tags")
    if tags_wrap:
        for a in tags_wrap.select("ul li a"):
            t = a.get_text(" ", strip=True)
            if t:
                tags.append(t)

    image_url = _extract_image_url(soup, base_url, materia if isinstance(materia, Tag) else None)

    dt_pub = parse_published_line(datas_raw) if datas_raw else None
    if dt_pub is None:
        dt_pub = combine_listing_datetime(listing_date_ddmmyyyy, listing_time_hhmm)
    if dt_pub is None:
        # Last resort: keep empty string for published_at in validation layer
        published_iso = ""
    else:
        published_iso = to_iso8601_z(dt_pub)

    return ArticleDetail(
        url=url,
        title=title,
        lead=lead,
        content_text=content_text,
        tags=tags,
        image_url=image_url,
        published_at_iso=published_iso,
        datas_raw=datas_raw,
    )
