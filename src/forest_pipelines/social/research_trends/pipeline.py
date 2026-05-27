"""Orchestrator for the research-trends social deck.

Two modes:
  - historical: full date range since `from_date` (default 2000-01-01), sorted
    by citation count. Best for long-term retrospectives.
  - recent: a bounded date window (e.g. last 30 days), sorted by publication
    date descending. Best for weekly / monthly social posts.

Topic is configurable via `TopicConfig`. The default ships Brazilian wildfire
research; the same code can drive other topics (ML in environmental science,
remote sensing of deforestation, etc.) by adding a new `TopicConfig` entry.

The cache key encodes topic + mode + window + sort, so changing any of those
forces a fresh OpenAlex pull.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from forest_pipelines.social.research_trends.charts import (
    render_publications_per_year,
    render_top_bars,
    render_trends_vs_publications,
)
from forest_pipelines.social.research_trends.crossref_client import CrossrefClient
from forest_pipelines.social.research_trends.google_trends_client import GoogleTrendsClient
from forest_pipelines.social.research_trends.openalex_client import OpenAlexClient

LOG = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
APP_ROOT = REPO_ROOT / "apps" / "social-post-templates"
DEFAULT_CACHE_DIR = REPO_ROOT / "data" / "research_trends" / "cache"
DEFAULT_OUT_DIR = APP_ROOT / "public" / "generated"
DEFAULT_MANIFEST = APP_ROOT / "examples" / "research-trends.manifest.json"
DEFAULT_PUBLIC_MANIFEST = APP_ROOT / "public" / "examples" / "research-trends.manifest.json"

CONCEPT_WILDFIRE = "C2776775217"
CONCEPT_FOREST_FIRE = "C84111414"


# ── Topic configuration ──────────────────────────────────────────────────────


@dataclass(slots=True, frozen=True)
class TopicConfig:
    """Defines what 'topic' means for one run of the pipeline.

    Add a new instance to TOPICS to drive a different deck without changing the
    rest of the code. `slug` is also baked into the cache key.

    `required_terms` is a topical guardrail: after OpenAlex returns works, we
    drop any whose title and abstract contain NONE of these terms. This
    prevents off-topic works (Nickel Ores, intracranial pressure, ...) from
    sneaking in via tangential keyword mentions in the API's broad search.
    """

    slug: str
    cover_title: str
    cover_topic_tag: str  # short chrome tag, e.g. "Pesquisa & Dados"
    cover_series_label: str
    body_subject_short: str  # "queimadas", used in body_text sentences
    body_subject_phrase: str  # "queimadas no Brasil", longer noun phrase
    search_query: str  # OpenAlex `search=...`
    extra_filters: list[str] = field(default_factory=list)  # filter clauses (no date filters here)
    excluded_concepts: frozenset[str] = frozenset()
    google_trends_terms: tuple[str, ...] = ()
    required_terms: tuple[str, ...] = ()  # post-filter on title + abstract (any-of, case-insensitive)


TOPIC_BRAZIL_WILDFIRE = TopicConfig(
    slug="brazil-wildfire",
    cover_title="Pesquisa sobre queimadas no Brasil",
    cover_topic_tag="Pesquisa & Dados",
    cover_series_label="Tendências de pesquisa",
    body_subject_short="queimadas",
    body_subject_phrase="queimadas no Brasil",
    search_query="wildfire OR queimadas OR forest fire OR bushfire",
    extra_filters=["authorships.institutions.country_code:BR"],
    excluded_concepts=frozenset({CONCEPT_WILDFIRE, CONCEPT_FOREST_FIRE}),
    google_trends_terms=("queimadas",),
    required_terms=(
        "wildfire",
        "wildfires",
        "forest fire",
        "forest fires",
        "bushfire",
        "queimada",
        "queimadas",
        "incêndio florestal",
        "incêndios florestais",
        "fire regime",
        "burned area",
        "biomass burning",
    ),
)

TOPIC_ML_ENVIRONMENT = TopicConfig(
    slug="ml-environment",
    cover_title="Machine learning em pesquisa ambiental",
    cover_topic_tag="ML & Meio Ambiente",
    cover_series_label="Tendências de pesquisa",
    body_subject_short="machine learning ambiental",
    body_subject_phrase="machine learning em estudos ambientais",
    search_query="(machine learning OR deep learning) AND (environment OR ecology OR forest OR climate)",
    extra_filters=[],
    excluded_concepts=frozenset(),
    google_trends_terms=("machine learning environment",),
    required_terms=(
        "machine learning",
        "deep learning",
        "neural network",
        "neural networks",
        "random forest",
        "convolutional",
        "transformer",
    ),
)

TOPICS: dict[str, TopicConfig] = {
    TOPIC_BRAZIL_WILDFIRE.slug: TOPIC_BRAZIL_WILDFIRE,
    TOPIC_ML_ENVIRONMENT.slug: TOPIC_ML_ENVIRONMENT,
}


# ── Chart keys ───────────────────────────────────────────────────────────────

CHART_PUB_YEAR = "publications-per-year"
CHART_TRENDS = "google-trends"
CHART_INSTITUTIONS = "top-institutions"
CHART_CONCEPTS = "top-concepts"
CHART_VENUES = "top-venues"


# ── Pipeline config ──────────────────────────────────────────────────────────


@dataclass(slots=True)
class PipelineConfig:
    topic: TopicConfig
    mode: str  # "historical" or "recent"
    from_date: date
    to_date: date | None  # None means open-ended (now)
    sort: str
    mailto: str
    cache_dir: Path
    out_dir: Path
    manifest_path: Path
    public_manifest_path: Path
    max_openalex_pages: int | None
    crossref_sample: int
    skip_google_trends: bool


# ── Filter / cache helpers ───────────────────────────────────────────────────


def _build_filter(topic: TopicConfig, from_date: date, to_date: date | None) -> str:
    clauses = list(topic.extra_filters)
    clauses.append(f"from_publication_date:{from_date.isoformat()}")
    if to_date is not None:
        clauses.append(f"to_publication_date:{to_date.isoformat()}")
    return ",".join(clauses)


def _cache_key(
    topic: TopicConfig, mode: str, from_date: date, to_date: date | None, sort: str
) -> str:
    parts = [
        topic.slug,
        mode,
        from_date.isoformat(),
        to_date.isoformat() if to_date else "open",
        sort.replace(":", "_"),
    ]
    return "_".join(parts)


# ── Topical post-filter ──────────────────────────────────────────────────────


def _reconstruct_abstract(work: dict[str, Any]) -> str:
    """OpenAlex stores abstracts as an inverted index. Reconstruct a flat string."""
    idx = work.get("abstract_inverted_index") or {}
    if not idx:
        return ""
    #idx maps word tokens to positions; membership only needs token keys.
    return " ".join(idx.keys())


def _topical_filter(
    works: list[dict[str, Any]], required_terms: tuple[str, ...]
) -> list[dict[str, Any]]:
    """Drop works whose title and abstract don't mention any of the required terms.

    Case-insensitive substring match. This is the safety net that catches
    works the OpenAlex `search=` parameter returned for tangential reasons
    (references list, related-work mentions, etc.) when sorted by date.
    """
    if not required_terms:
        return works
    terms_lower = tuple(t.lower() for t in required_terms)
    kept: list[dict[str, Any]] = []
    for work in works:
        title = (work.get("title") or "").lower()
        abstract = _reconstruct_abstract(work).lower()
        haystack = f"{title} {abstract}"
        if any(t in haystack for t in terms_lower):
            kept.append(work)
    return kept


# ── Aggregations ─────────────────────────────────────────────────────────────


@dataclass(slots=True)
class Aggregation:
    publications_per_year: list[dict[str, Any]]
    top_institutions: list[dict[str, Any]]
    top_concepts: list[dict[str, Any]]
    top_venues: list[dict[str, Any]]
    top_cited_works: list[dict[str, Any]]
    total_works: int
    current_year_count: int
    current_year: int


def _primary_brazilian_institution(work: dict[str, Any]) -> str:
    for authorship in work.get("authorships") or []:
        for inst in authorship.get("institutions") or []:
            if inst.get("country_code") == "BR" and inst.get("display_name"):
                return inst["display_name"]
    # Fall back to first institution available.
    for authorship in work.get("authorships") or []:
        for inst in authorship.get("institutions") or []:
            if inst.get("display_name"):
                return inst["display_name"]
    return "instituição não informada"


def _primary_concept(work: dict[str, Any], excluded: frozenset[str]) -> str:
    for concept in work.get("concepts") or []:
        cid = (concept.get("id") or "").rsplit("/", 1)[-1]
        if cid in excluded:
            continue
        name = concept.get("display_name")
        if name:
            return name
    return "tema não informado"


def _primary_venue(work: dict[str, Any]) -> str:
    host = (work.get("primary_location") or {}).get("source") or {}
    return host.get("display_name") or "veículo não informado"


def _full_title(work: dict[str, Any]) -> str:
    return (work.get("title") or "").strip() or "trabalho sem título"


def _aggregate(works: list[dict[str, Any]], topic: TopicConfig) -> Aggregation:
    pubs_by_year: Counter[int] = Counter()
    inst_by_id: dict[str, dict[str, Any]] = {}
    concepts_by_id: dict[str, dict[str, Any]] = {}
    venues_by_id: dict[str, dict[str, Any]] = {}

    excluded = topic.excluded_concepts

    for work in works:
        year = work.get("publication_year")
        if isinstance(year, int):
            pubs_by_year[year] += 1

        for authorship in work.get("authorships") or []:
            for inst in authorship.get("institutions") or []:
                # Respect the country filter from extra_filters by also
                # honouring it here, but only when the topic restricts to BR.
                # For other topics we accept any institution.
                country_filtered = any(
                    "country_code:BR" in f for f in topic.extra_filters
                )
                if country_filtered and inst.get("country_code") != "BR":
                    continue
                key = inst.get("id") or inst.get("display_name")
                if not key:
                    continue
                row = inst_by_id.setdefault(
                    key,
                    {"id": key, "label": inst.get("display_name") or key, "count": 0},
                )
                row["count"] += 1

        for concept in work.get("concepts") or []:
            cid = concept.get("id")
            if not cid:
                continue
            cid_short = cid.rsplit("/", 1)[-1]
            if cid_short in excluded:
                continue
            row = concepts_by_id.setdefault(
                cid_short,
                {
                    "id": cid_short,
                    "label": concept.get("display_name") or cid_short,
                    "count": 0,
                },
            )
            row["count"] += 1

        host = (work.get("primary_location") or {}).get("source") or {}
        vid = host.get("id") or host.get("display_name")
        if vid:
            row = venues_by_id.setdefault(
                vid,
                {"id": vid, "label": host.get("display_name") or vid, "count": 0},
            )
            row["count"] += 1

    pubs_series = [{"year": y, "count": pubs_by_year[y]} for y in sorted(pubs_by_year)]
    top_institutions = sorted(inst_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]
    top_concepts = sorted(concepts_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]
    top_venues = sorted(venues_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]

    top_cited = sorted(
        (w for w in works if w.get("title")),
        key=lambda w: int(w.get("cited_by_count") or 0),
        reverse=True,
    )[:5]
    top_cited_works = [
        {
            "title": _full_title(w),
            "year": w.get("publication_year"),
            "citations": int(w.get("cited_by_count") or 0),
            "primary_institution": _primary_brazilian_institution(w),
            "primary_concept": _primary_concept(w, excluded),
            "primary_venue": _primary_venue(w),
            "doi": (w.get("doi") or "").replace("https://doi.org/", "") or None,
        }
        for w in top_cited
    ]

    today = date.today()
    current_year = today.year
    current_year_count = pubs_by_year.get(current_year, 0)

    return Aggregation(
        publications_per_year=pubs_series,
        top_institutions=top_institutions,
        top_concepts=top_concepts,
        top_venues=top_venues,
        top_cited_works=top_cited_works,
        total_works=len(works),
        current_year_count=current_year_count,
        current_year=current_year,
    )


# ── Crossref validation ──────────────────────────────────────────────────────


def _validate_with_crossref(
    works: list[dict[str, Any]], client: CrossrefClient, *, sample: int
) -> list[dict[str, Any]]:
    cited_sorted = sorted(
        (w for w in works if w.get("doi")),
        key=lambda w: int(w.get("cited_by_count") or 0),
        reverse=True,
    )[:sample]
    out: list[dict[str, Any]] = []
    for work in cited_sorted:
        doi = (work.get("doi") or "").replace("https://doi.org/", "")
        oa_year = work.get("publication_year")
        cr = client.fetch_work(doi)
        if cr is None:
            out.append({"doi": doi, "status": "not_found"})
            continue
        issued = (cr.get("issued") or {}).get("date-parts", [[None]])
        cr_year = issued[0][0] if issued and issued[0] else None
        match = cr_year == oa_year
        out.append(
            {
                "doi": doi,
                "status": "ok" if match else "year_mismatch",
                "openalex_year": oa_year,
                "crossref_year": cr_year,
                "title_openalex": (work.get("title") or "")[:120],
                "title_crossref": ((cr.get("title") or [""])[0])[:120],
            }
        )
    return out


# ── Chart rendering ──────────────────────────────────────────────────────────


def _render_charts(
    agg: Aggregation,
    yearly_counts_10y: dict[int, int],
    trends_payload: dict[str, Any] | None,
    out_dir: Path,
    topic: TopicConfig,
    highlight_year: int,
) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    paths[CHART_PUB_YEAR] = out_dir / f"research-{CHART_PUB_YEAR}.png"
    render_publications_per_year(
        yearly_counts_10y,
        paths[CHART_PUB_YEAR],
        title=f"Publicações por ano sobre {topic.body_subject_phrase}",
        highlight_year=highlight_year,
    )

    pubs_series_for_trends = [{"year": y, "count": c} for y, c in sorted(yearly_counts_10y.items())]
    if trends_payload and trends_payload.get("series") and pubs_series_for_trends:
        first_term = next(iter(trends_payload["series"].keys()))
        paths[CHART_TRENDS] = out_dir / f"research-{CHART_TRENDS}.png"
        render_trends_vs_publications(
            trends_payload["series"][first_term],
            pubs_series_for_trends,
            paths[CHART_TRENDS],
            title="Interesse público vs produção científica",
            trends_label=f"Google Trends · {first_term}",
        )

    paths[CHART_INSTITUTIONS] = out_dir / f"research-{CHART_INSTITUTIONS}.png"
    render_top_bars(
        agg.top_institutions,
        paths[CHART_INSTITUTIONS],
        title="Instituições com mais publicações",
    )

    paths[CHART_CONCEPTS] = out_dir / f"research-{CHART_CONCEPTS}.png"
    render_top_bars(
        agg.top_concepts,
        paths[CHART_CONCEPTS],
        title="Temas mais frequentes",
    )

    paths[CHART_VENUES] = out_dir / f"research-{CHART_VENUES}.png"
    render_top_bars(
        agg.top_venues,
        paths[CHART_VENUES],
        title="Periódicos e veículos com mais publicações",
    )

    return paths


# ── Body text helpers ────────────────────────────────────────────────────────


def _format_top_cited_lines(
    items: list[dict[str, Any]], *, field_key: str, limit: int = 3
) -> str:
    """Render the top-cited works as a multi-line list.

    Each line uses the full title, paired with year and the side field (e.g.
    institution / concept / venue) joined with a comma. The slide CSS uses
    `white-space: pre-line` so the line breaks survive into the rendered PNG.
    """
    if not items:
        return ""
    lines = []
    for i, item in enumerate(items[:limit], 1):
        right = item.get(field_key) or ""
        year = item.get("year") or "s/d"
        title = (item.get("title") or "").strip() or "trabalho sem título"
        lines.append(f"{i}. {title} ({year}), {right}")
    return "\n".join(lines)


def _pluralize_pt(n: int, singular: str, plural: str) -> str:
    return singular if n == 1 else plural


def _cover_summary(agg: Aggregation, config: PipelineConfig) -> str:
    today = date.today()
    today_human = today.strftime("%d/%m/%Y")
    if config.mode == "recent":
        window_days = (today - config.from_date).days
        works_word = _pluralize_pt(agg.total_works, "trabalho indexado", "trabalhos indexados")
        return (
            f"Últimos {window_days} dias: {agg.total_works} {works_word} pelo OpenAlex sobre "
            f"{config.topic.body_subject_phrase} (até {today_human})."
        )
    cy = agg.current_year
    cy_count = agg.current_year_count
    if cy_count > 0:
        pub_word = _pluralize_pt(cy_count, "publicação", "publicações")
        return (
            f"{cy} registra {cy_count} {pub_word} sobre {config.topic.body_subject_phrase} "
            f"indexadas no OpenAlex até {today_human}."
        )
    if agg.publications_per_year:
        latest = agg.publications_per_year[-1]
        pub_word = _pluralize_pt(latest["count"], "publicação", "publicações")
        return (
            f"Último ano com dados: {latest['year']} ({latest['count']} {pub_word} sobre "
            f"{config.topic.body_subject_phrase} indexadas no OpenAlex)."
        )
    return f"Sem publicações indexadas pelo OpenAlex no recorte atual ({today_human})."


def _publications_body(
    yearly_counts: dict[int, int], highlight_year: int, topic: TopicConfig
) -> str:
    if not yearly_counts:
        return (
            "Nenhum trabalho indexado pelo OpenAlex nos últimos 10 anos para a "
            "estratégia de busca atual. Revise o tema ou amplie os filtros."
        )
    cy_count = yearly_counts.get(highlight_year, 0)
    total_10y = sum(yearly_counts.values())
    today = date.today()
    is_partial = highlight_year == today.year and today.month < 12
    base = (
        f"Últimos 10 anos: {total_10y} trabalhos sobre {topic.body_subject_phrase} "
        f"indexados pelo OpenAlex."
    )
    if is_partial:
        base += (
            f" Em {highlight_year} (ano em curso), {cy_count} trabalhos até o momento."
        )
    else:
        prior_count = yearly_counts.get(highlight_year - 1, 0)
        base += f" Em {highlight_year}, {cy_count} trabalhos."
        if prior_count > 0:
            delta = (cy_count - prior_count) / prior_count * 100.0
            sign = "alta" if delta >= 0 else "queda"
            base += f" {sign.capitalize()} de {abs(delta):.1f}% sobre {highlight_year - 1}."
    return base


def _top_with_papers_body(
    agg: Aggregation,
    items: list[dict[str, Any]],
    *,
    suffix: str,
    article: str,
    top_field: str,
) -> str:
    if not items:
        return "Sem dados suficientes no recorte atual."
    head = (
        f"{article} {suffix} mais frequente é {items[0]['label']}, com {items[0]['count']} "
        f"trabalhos no recorte.\n\nMais citados no recorte:\n"
    )
    return head + _format_top_cited_lines(agg.top_cited_works, field_key=top_field, limit=3)


# ── Manifest emission ────────────────────────────────────────────────────────


def _build_manifest(
    agg: Aggregation,
    yearly_counts_10y: dict[int, int],
    chart_paths: dict[str, Path],
    trends_payload: dict[str, Any] | None,
    config: PipelineConfig,
) -> dict[str, Any]:
    topic = config.topic
    today = date.today()
    published_at = today.strftime("%b %Y").capitalize()

    def img(key: str) -> str:
        return f"/generated/{chart_paths[key].name}" if key in chart_paths else ""

    slides: list[dict[str, Any]] = [
        {
            "type": "cover",
            "slots": {
                "topic_tag": topic.cover_topic_tag,
                "published_at": published_at,
                "series_label": topic.cover_series_label,
                "title": topic.cover_title,
                "summary": _cover_summary(agg, config),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "topic_tag": topic.cover_topic_tag,
                "published_at": published_at,
                "caption": f"Publicações por ano · OpenAlex (últimos 10 anos)",
                "image_url": img(CHART_PUB_YEAR),
                "body_text": _publications_body(yearly_counts_10y, today.year, topic),
            },
        },
    ]

    if CHART_TRENDS in chart_paths:
        slides.append(
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic.cover_topic_tag,
                    "published_at": published_at,
                    "caption": "Interesse público · Google Trends + OpenAlex",
                    "image_url": img(CHART_TRENDS),
                    "body_text": (
                        f"Compara o interesse de busca no Google (Brasil) com a contagem anual "
                        f"de trabalhos sobre {topic.body_subject_phrase} indexados pelo OpenAlex."
                    ),
                },
            }
        )

    slides.extend(
        [
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic.cover_topic_tag,
                    "published_at": published_at,
                    "caption": "Top 10 instituições · OpenAlex",
                    "image_url": img(CHART_INSTITUTIONS),
                    "body_text": _top_with_papers_body(
                        agg,
                        agg.top_institutions,
                        suffix="instituição",
                        article="A",
                        top_field="primary_institution",
                    ),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic.cover_topic_tag,
                    "published_at": published_at,
                    "caption": "Temas mais frequentes · OpenAlex concepts",
                    "image_url": img(CHART_CONCEPTS),
                    "body_text": _top_with_papers_body(
                        agg,
                        agg.top_concepts,
                        suffix="tema",
                        article="O",
                        top_field="primary_concept",
                    ),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic.cover_topic_tag,
                    "published_at": published_at,
                    "caption": "Top 10 periódicos e veículos · OpenAlex",
                    "image_url": img(CHART_VENUES),
                    "body_text": _top_with_papers_body(
                        agg,
                        agg.top_venues,
                        suffix="veículo",
                        article="O",
                        top_field="primary_venue",
                    ),
                },
            },
            {
                "type": "cta",
                "slots": {
                    "topic_tag": topic.cover_topic_tag,
                    "published_at": published_at,
                    "cta_kicker": "Quer continuar acompanhando?",
                    "cta_headline": "Mais análises e dados",
                    "cta_subline": "Acesse o portal de dados abertos do instituto.",
                    "cta_url": "institutoforest.org",
                },
            },
        ]
    )
    global_slots = {
        "topic_tag": topic.cover_topic_tag,
        "published_at": published_at,
    }
    for slide in slides:
        slots = slide.get("slots", {})
        slots.pop("topic_tag", None)
        slots.pop("published_at", None)

    return {
        "theme": "white",
        "runId": f"research-trends-{config.mode}-{topic.slug}",
        "sizes": {
            "topicTagPx": 24,
            "datePx": 26,
            "pageNumberPx": 24,
            "logoHeightPx": 54,
        },
        "globalSlots": global_slots,
        "slides": slides,
        "sources": {
            "openalex": "https://api.openalex.org/works",
            "crossref": "https://api.crossref.org/works/{doi}",
            "google_trends": "pytrends (Google Trends, Brazil)",
        },
        "query": {
            "topic": topic.slug,
            "search": topic.search_query,
            "filter": _build_filter(topic, config.from_date, config.to_date),
            "sort": config.sort,
            "mode": config.mode,
            "from_date": config.from_date.isoformat(),
            "to_date": config.to_date.isoformat() if config.to_date else None,
            "generated_at": date.today().isoformat(),
            "total_works": agg.total_works,
        },
    }


# ── Top-level run ────────────────────────────────────────────────────────────


def run(config: PipelineConfig) -> None:
    LOG.info(
        "research_trends.start topic=%s mode=%s from=%s to=%s sort=%s",
        config.topic.slug,
        config.mode,
        config.from_date,
        config.to_date,
        config.sort,
    )
    config.cache_dir.mkdir(parents=True, exist_ok=True)

    openalex = OpenAlexClient(mailto=config.mailto, cache_dir=config.cache_dir)
    crossref = CrossrefClient(mailto=config.mailto, cache_dir=config.cache_dir)

    cache_key = _cache_key(
        config.topic, config.mode, config.from_date, config.to_date, config.sort
    )
    filter_str = _build_filter(config.topic, config.from_date, config.to_date)

    raw_works = list(
        openalex.iter_works(
            search=config.topic.search_query,
            filter_str=filter_str,
            sort=config.sort,
            cache_key=cache_key,
            max_pages=config.max_openalex_pages,
        )
    )
    LOG.info("research_trends.openalex_loaded count=%d", len(raw_works))
    works = _topical_filter(raw_works, config.topic.required_terms)
    dropped = len(raw_works) - len(works)
    if dropped > 0:
        LOG.info(
            "research_trends.topical_filter kept=%d dropped=%d (off-topic by title/abstract)",
            len(works),
            dropped,
        )
    if not works:
        LOG.warning(
            "research_trends.no_results - 0 works after topical filter (raw=%d). "
            "Conferir search=%r e required_terms=%s do tópico.",
            len(raw_works),
            config.topic.search_query,
            list(config.topic.required_terms),
        )

    agg = _aggregate(works, config.topic)

    # Always pull a 10-year yearly histogram for the topic via OpenAlex
        #group_by is one cheap request and decouples the bar chart from
    # the mode's date window. Without this, recent-mode runs would render a
    # single-bar chart (only the current year), which is misleading.
    today = date.today()
    histogram_end = today.year
    histogram_start = histogram_end - 9
    histogram_filter = ",".join(
        list(config.topic.extra_filters)
        + [
            f"from_publication_date:{histogram_start}-01-01",
            f"to_publication_date:{histogram_end}-12-31",
        ]
    )
    histogram_cache_key = f"{config.topic.slug}_{histogram_start}_{histogram_end}"
    yearly_counts_10y = openalex.count_by_year(
        search=config.topic.search_query,
        filter_str=histogram_filter,
        cache_key=histogram_cache_key,
    )
    LOG.info(
        "research_trends.yearly_counts_loaded years=%d total=%d",
        len(yearly_counts_10y),
        sum(yearly_counts_10y.values()),
    )

    if not config.skip_google_trends and config.topic.google_trends_terms:
        trends_client = GoogleTrendsClient(cache_dir=config.cache_dir)
        trends_payload = trends_client.interest_over_time(list(config.topic.google_trends_terms))
    else:
        trends_payload = None

    chart_paths = _render_charts(
        agg,
        yearly_counts_10y,
        trends_payload,
        config.out_dir,
        config.topic,
        highlight_year=histogram_end,
    )
    LOG.info("research_trends.charts_rendered count=%d", len(chart_paths))

    validation = _validate_with_crossref(works, crossref, sample=config.crossref_sample)
    audit_path = config.cache_dir / f"crossref_validation_{config.topic.slug}_{config.mode}.json"
    audit_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2))
    LOG.info("research_trends.crossref_audited n=%d path=%s", len(validation), audit_path.name)

    pub_year_spec = [
        {"year": y, "count": c, "highlight": y == today.year}
        for y, c in sorted(yearly_counts_10y.items())
    ]
    for key, data in (
        (CHART_PUB_YEAR, pub_year_spec),
        (CHART_INSTITUTIONS, agg.top_institutions),
        (CHART_CONCEPTS, agg.top_concepts),
        (CHART_VENUES, agg.top_venues),
        ("top-cited-works", agg.top_cited_works),
    ):
        (config.out_dir / f"chart_spec-research-{key}.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2)
        )

    manifest = _build_manifest(agg, yearly_counts_10y, chart_paths, trends_payload, config)
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    config.public_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.public_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    LOG.info("research_trends.manifest_written path=%s", config.manifest_path)


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _resolve_window(args: argparse.Namespace) -> tuple[str, date, date | None]:
    """Resolve mode + date window from CLI flags. Returns (mode, from_date, to_date)."""
    today = date.today()
    if args.window_days is not None:
        # window_days implies recent
        to_d = args.to_date or today
        from_d = to_d - timedelta(days=args.window_days)
        return "recent", from_d, to_d
    if args.from_date is not None and args.to_date is not None:
        mode = args.mode or "recent"
        return mode, args.from_date, args.to_date
    if args.from_date is not None:
        mode = args.mode or "historical"
        return mode, args.from_date, args.to_date  # to_date stays None
    if args.to_date is not None:
        mode = args.mode or "recent"
        from_d = args.to_date - timedelta(days=30)
        return mode, from_d, args.to_date
    #no explicit window flags; fall back to mode defaults.
    mode = args.mode or "historical"
    if mode == "recent":
        return mode, today - timedelta(days=30), today
    return "historical", date(2000, 1, 1), None


def _default_sort(mode: str) -> str:
    return "publication_date:desc" if mode == "recent" else "cited_by_count:desc"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m forest_pipelines.social.research_trends",
        description=(
            "Build the research-trends social deck. Two modes: historical "
            "(all-time since --from-date) and recent (bounded window, ideal "
            "for weekly/monthly posts). Topic is swappable via --topic."
        ),
    )
    p.add_argument(
        "--topic",
        choices=list(TOPICS.keys()),
        default=TOPIC_BRAZIL_WILDFIRE.slug,
        help=f"Tema da busca (default: {TOPIC_BRAZIL_WILDFIRE.slug}).",
    )
    p.add_argument(
        "--mode",
        choices=["historical", "recent"],
        default=None,
        help="historical (default sem flags de janela) ou recent (default com --window-days).",
    )
    p.add_argument("--from-date", type=_parse_date, default=None, metavar="YYYY-MM-DD")
    p.add_argument("--to-date", type=_parse_date, default=None, metavar="YYYY-MM-DD")
    p.add_argument(
        "--window-days",
        type=int,
        default=None,
        help="Atalho para modo recent: from_date = to_date (default hoje) - N dias.",
    )
    p.add_argument(
        "--sort",
        default=None,
        help=(
            "Ordenação OpenAlex (default: cited_by_count:desc no historical, "
            "publication_date:desc no recent)."
        ),
    )
    p.add_argument(
        "--mailto",
        default=os.environ.get("FOREST_POLITE_EMAIL", "julianofpadua@gmail.com"),
    )
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST)
    p.add_argument("--public-manifest-path", type=Path, default=DEFAULT_PUBLIC_MANIFEST)
    p.add_argument(
        "--max-openalex-pages",
        type=int,
        default=None,
        help="Cap OpenAlex paging (each page = 200 works). Default: unlimited.",
    )
    p.add_argument("--crossref-sample", type=int, default=20)
    p.add_argument("--skip-google-trends", action="store_true")
    p.add_argument(
        "--refresh",
        action="store_true",
        help="Apaga caches do tema/modo/janela antes de rodar.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    topic = TOPICS[args.topic]
    mode, from_date, to_date = _resolve_window(args)
    sort = args.sort or _default_sort(mode)

    if args.refresh:
        prefix = _cache_key(topic, mode, from_date, to_date, sort)
        deleted: list[Path] = []
        target = args.cache_dir / f"openalex_{prefix}.json"
        if target.exists():
            target.unlink()
            deleted.append(target)
        # Group-by year cache (keyed by topic+window range).
        for path in args.cache_dir.glob(f"openalex_groupby_{topic.slug}_*.json"):
            path.unlink()
            deleted.append(path)
        # Google Trends cache (not keyed by date window).
        for path in args.cache_dir.glob("google_trends_*.json"):
            path.unlink()
            deleted.append(path)
        LOG.info("research_trends.cache_cleared n=%d", len(deleted))

    config = PipelineConfig(
        topic=topic,
        mode=mode,
        from_date=from_date,
        to_date=to_date,
        sort=sort,
        mailto=args.mailto,
        cache_dir=args.cache_dir,
        out_dir=args.out_dir,
        manifest_path=args.manifest_path,
        public_manifest_path=args.public_manifest_path,
        max_openalex_pages=args.max_openalex_pages,
        crossref_sample=args.crossref_sample,
        skip_google_trends=args.skip_google_trends,
    )
    run(config)
    return 0


# ── Compatibility: keep an importable hash helper for tests ─────────────────


def _hash_key(parts: list[str]) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:12]
