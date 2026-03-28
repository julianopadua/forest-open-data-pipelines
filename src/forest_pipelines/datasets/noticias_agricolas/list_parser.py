# src/forest_pipelines/datasets/noticias_agricolas/list_parser.py
from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from forest_pipelines.datasets.noticias_agricolas.models import NewsListItem


def parse_category_list_html(
    html: str,
    *,
    category_slug: str,
    category_label: str,
    base_url: str,
    limit: int = 5,
) -> list[NewsListItem]:
    """
    Parse the first page of a category: first ``limit`` ``li.horizontal.com-hora`` items.
    Date comes from the nearest preceding ``h3`` (DD/MM/YYYY).
    """
    soup = BeautifulSoup(html, "html.parser")
    lis = soup.select("li.horizontal.com-hora")[:limit]
    out: list[NewsListItem] = []
    rank = 0
    for li in lis:
        rank += 1
        a = li.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        h2 = a.find("h2")
        title = h2.get_text(" ", strip=True) if h2 else ""
        hora = a.find("span", class_="hora")
        time_s = hora.get_text(strip=True) if hora else "00:00"
        h3 = li.find_previous("h3")
        date_s = h3.get_text(strip=True) if h3 else ""
        out.append(
            NewsListItem(
                url=abs_url,
                title=title,
                category_slug=category_slug,
                category_label=category_label,
                rank_within_category=rank,
                listing_date_ddmmyyyy=date_s,
                listing_time_hhmm=time_s,
            )
        )
    return out
