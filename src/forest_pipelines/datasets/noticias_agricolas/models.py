# src/forest_pipelines/datasets/noticias_agricolas/models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class NewsListItem:
    """One row from a category listing page (first page only)."""

    url: str
    title: str
    category_slug: str
    category_label: str
    rank_within_category: int
    listing_date_ddmmyyyy: str
    listing_time_hhmm: str


@dataclass
class ArticleDetail:
    """Parsed article page before assembly into the public feed item."""

    url: str
    title: str
    lead: str | None
    content_text: str
    tags: list[str]
    image_url: str | None
    published_at_iso: str
    datas_raw: str


@dataclass
class CategoryConfig:
    slug: str
    label: str
    url: str


@dataclass
class PendingFeedItem:
    """Work-in-progress row combining listing metadata with optional article detail."""

    url: str
    category_slug: str
    category_label: str
    rank_within_category: int
    listing_date_ddmmyyyy: str
    listing_time_hhmm: str
    article: ArticleDetail | None = None
    error: str | None = None


@dataclass
class MergedListing:
    """After deduplication by URL across categories."""

    url: str
    category_slugs: list[str]
    category_labels: list[str]
    rank_within_category: int
    primary_slug: str
    primary_label: str
    listing_date_ddmmyyyy: str
    listing_time_hhmm: str
    title_from_listing: str
