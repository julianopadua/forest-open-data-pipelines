# src/forest_pipelines/datasets/noticias_agricolas/merge.py
from __future__ import annotations

from collections import defaultdict
from urllib.parse import urlparse, urlunparse

from forest_pipelines.datasets.noticias_agricolas.models import MergedListing, NewsListItem


def normalize_url_key(url: str) -> str:
    p = urlparse(url.strip())
    path = p.path.rstrip("/") or "/"
    netloc = (p.netloc or "").lower()
    scheme = (p.scheme or "https").lower()
    return urlunparse((scheme, netloc, path, "", "", ""))


def merge_listings_by_url(
    rows: list[NewsListItem],
    category_order: list[str],
) -> dict[str, MergedListing]:
    """
    Deduplicate by URL. Preserve category slugs in YAML order first, then extras.
    ``rank_within_category`` is the minimum rank seen across duplicate rows.
    """
    by_key: dict[str, list[NewsListItem]] = defaultdict(list)
    for r in rows:
        by_key[normalize_url_key(r.url)].append(r)

    out: dict[str, MergedListing] = {}
    for key, group in by_key.items():
        seen: set[str] = set()
        slugs: list[str] = []
        labels: list[str] = []
        for slug in category_order:
            for row in group:
                if row.category_slug == slug and slug not in seen:
                    seen.add(slug)
                    slugs.append(slug)
                    labels.append(row.category_label)
                    break
        for row in group:
            if row.category_slug not in seen:
                seen.add(row.category_slug)
                slugs.append(row.category_slug)
                labels.append(row.category_label)

        rank = min(x.rank_within_category for x in group)
        primary_slug = slugs[0]
        primary_label = labels[0]
        ref = group[0]
        title_hint = next((x.title for x in group if (x.title or "").strip()), "")
        out[key] = MergedListing(
            url=ref.url,
            category_slugs=slugs,
            category_labels=labels,
            rank_within_category=rank,
            primary_slug=primary_slug,
            primary_label=primary_label,
            listing_date_ddmmyyyy=ref.listing_date_ddmmyyyy,
            listing_time_hhmm=ref.listing_time_hhmm,
            title_from_listing=title_hint,
        )
    return out
