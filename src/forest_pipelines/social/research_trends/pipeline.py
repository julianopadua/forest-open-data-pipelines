"""Orchestrator for the research-trends social deck.

Pulls Brazilian wildfire research metadata from OpenAlex, validates the top
sample of DOIs against Crossref, optionally overlays Google Trends search
interest, renders six PNG charts and emits the manifest the white-theme
composer loads via `?preset=research-trends`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from forest_pipelines.social.research_trends.charts import (
    render_open_access_share,
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

# OpenAlex concept IDs are stable identifiers — see https://api.openalex.org/concepts.
CONCEPT_WILDFIRE = "C2776775217"  # Wildfire
CONCEPT_FOREST_FIRE = "C84111414"  # Forest fire

DEFAULT_OPENALEX_FILTER = (
    f"concepts.id:{CONCEPT_WILDFIRE}|{CONCEPT_FOREST_FIRE},"
    "authorships.institutions.country_code:BR,"
    "from_publication_date:2000-01-01"
)


# ── Chart keys / titles ──────────────────────────────────────────────────────

CHART_PUB_YEAR = "publications-per-year"
CHART_TRENDS = "google-trends"
CHART_INSTITUTIONS = "top-institutions"
CHART_CONCEPTS = "top-concepts"
CHART_VENUES = "top-venues"
CHART_OA_SHARE = "open-access-share"


@dataclass(slots=True)
class PipelineConfig:
    mailto: str
    cache_dir: Path
    out_dir: Path
    manifest_path: Path
    public_manifest_path: Path
    max_openalex_pages: int | None
    crossref_sample: int
    skip_google_trends: bool


# ── Aggregations ─────────────────────────────────────────────────────────────


def _aggregate(works: list[dict[str, Any]]) -> dict[str, Any]:
    pubs_by_year: Counter[int] = Counter()
    inst_by_id: dict[str, dict[str, Any]] = {}
    concepts_by_id: dict[str, dict[str, Any]] = {}
    venues_by_id: dict[str, dict[str, Any]] = {}
    oa_by_year: dict[int, dict[str, int]] = {}

    excluded_concepts = {CONCEPT_WILDFIRE, CONCEPT_FOREST_FIRE}

    for work in works:
        year = work.get("publication_year")
        if not isinstance(year, int):
            continue
        pubs_by_year[year] += 1

        oa_flag = bool((work.get("open_access") or {}).get("is_oa"))
        slot = oa_by_year.setdefault(year, {"oa": 0, "total": 0})
        slot["total"] += 1
        if oa_flag:
            slot["oa"] += 1

        for authorship in work.get("authorships", []) or []:
            for inst in authorship.get("institutions", []) or []:
                if inst.get("country_code") != "BR":
                    continue
                key = inst.get("id") or inst.get("display_name")
                if not key:
                    continue
                row = inst_by_id.setdefault(
                    key, {"id": key, "label": inst.get("display_name") or key, "count": 0}
                )
                row["count"] += 1

        for concept in work.get("concepts", []) or []:
            cid = concept.get("id")
            if not cid:
                continue
            cid_short = cid.rsplit("/", 1)[-1]
            if cid_short in excluded_concepts:
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
                {
                    "id": vid,
                    "label": host.get("display_name") or vid,
                    "count": 0,
                },
            )
            row["count"] += 1

    pubs_series = [
        {"year": y, "count": pubs_by_year[y]} for y in sorted(pubs_by_year)
    ]
    oa_series = [
        {
            "year": y,
            "oa_pct": round(100.0 * v["oa"] / v["total"], 2) if v["total"] else 0.0,
            "oa": v["oa"],
            "total": v["total"],
        }
        for y, v in sorted(oa_by_year.items())
    ]
    top_institutions = sorted(inst_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]
    top_concepts = sorted(concepts_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]
    top_venues = sorted(venues_by_id.values(), key=lambda r: r["count"], reverse=True)[:10]

    return {
        "publications_per_year": pubs_series,
        "open_access_share": oa_series,
        "top_institutions": top_institutions,
        "top_concepts": top_concepts,
        "top_venues": top_venues,
    }


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
    agg: dict[str, Any], trends_payload: dict[str, Any] | None, out_dir: Path
) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    paths[CHART_PUB_YEAR] = out_dir / f"research-{CHART_PUB_YEAR}.png"
    render_publications_per_year(
        agg["publications_per_year"],
        paths[CHART_PUB_YEAR],
        title="Publicações por ano sobre queimadas no Brasil",
    )

    if trends_payload and trends_payload.get("series") and agg["publications_per_year"]:
        first_term = next(iter(trends_payload["series"].keys()))
        paths[CHART_TRENDS] = out_dir / f"research-{CHART_TRENDS}.png"
        render_trends_vs_publications(
            trends_payload["series"][first_term],
            agg["publications_per_year"],
            paths[CHART_TRENDS],
            title="Interesse público vs produção científica",
            trends_label=f"Google Trends · {first_term}",
        )

    paths[CHART_INSTITUTIONS] = out_dir / f"research-{CHART_INSTITUTIONS}.png"
    render_top_bars(
        agg["top_institutions"],
        paths[CHART_INSTITUTIONS],
        title="Instituições brasileiras com mais publicações",
    )

    paths[CHART_CONCEPTS] = out_dir / f"research-{CHART_CONCEPTS}.png"
    render_top_bars(
        agg["top_concepts"],
        paths[CHART_CONCEPTS],
        title="Temas mais frequentes nos trabalhos",
    )

    paths[CHART_VENUES] = out_dir / f"research-{CHART_VENUES}.png"
    render_top_bars(
        agg["top_venues"],
        paths[CHART_VENUES],
        title="Periódicos e veículos com mais publicações",
    )

    paths[CHART_OA_SHARE] = out_dir / f"research-{CHART_OA_SHARE}.png"
    render_open_access_share(
        agg["open_access_share"],
        paths[CHART_OA_SHARE],
        title="Share de Open Access ao longo do tempo",
    )

    return paths


# ── Manifest emission ────────────────────────────────────────────────────────


def _build_manifest(
    agg: dict[str, Any],
    chart_paths: dict[str, Path],
    trends_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    pubs = agg["publications_per_year"]
    latest = pubs[-1] if pubs else None
    prior = pubs[-2] if len(pubs) >= 2 else None
    oa_latest = agg["open_access_share"][-1] if agg["open_access_share"] else None

    def img(key: str) -> str:
        return f"/generated/{chart_paths[key].name}" if key in chart_paths else ""

    def body(text: str) -> str:
        return text

    topic = "Pesquisa & Dados"
    published_at = ""
    if latest:
        published_at = str(latest["year"])

    slides: list[dict[str, Any]] = [
        {
            "type": "cover",
            "slots": {
                "topic_tag": topic,
                "published_at": published_at,
                "series_label": "Tendências de pesquisa",
                "title": "Pesquisa sobre queimadas no Brasil",
                "summary": (
                    "Cruzamento entre OpenAlex, Crossref e Google Trends — instituições, "
                    "temas, veículos e interesse público desde 2000."
                ),
            },
        },
        {
            "type": "body_chart",
            "slots": {
                "topic_tag": topic,
                "published_at": published_at,
                "caption": "Publicações por ano · OpenAlex",
                "image_url": img(CHART_PUB_YEAR),
                "body_text": _publications_body(latest, prior),
            },
        },
    ]

    if CHART_TRENDS in chart_paths:
        slides.append(
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "caption": "Interesse público · Google Trends + OpenAlex",
                    "image_url": img(CHART_TRENDS),
                    "body_text": (
                        "Compara o interesse do público (Google Trends, Brasil) com a "
                        "produção científica indexada pela OpenAlex no mesmo período."
                    ),
                },
            }
        )

    slides.extend(
        [
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "caption": "Top 10 instituições brasileiras · OpenAlex",
                    "image_url": img(CHART_INSTITUTIONS),
                    "body_text": _top_body(agg["top_institutions"], suffix="instituição"),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "caption": "Temas mais frequentes · OpenAlex concepts",
                    "image_url": img(CHART_CONCEPTS),
                    "body_text": _top_body(agg["top_concepts"], suffix="tema"),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "caption": "Top 10 periódicos e veículos · OpenAlex",
                    "image_url": img(CHART_VENUES),
                    "body_text": _top_body(agg["top_venues"], suffix="veículo"),
                },
            },
            {
                "type": "body_chart",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "caption": "Open Access · OpenAlex",
                    "image_url": img(CHART_OA_SHARE),
                    "body_text": _oa_body(oa_latest),
                },
            },
            {
                "type": "cta",
                "slots": {
                    "topic_tag": topic,
                    "published_at": published_at,
                    "cta_kicker": "Quer continuar acompanhando?",
                    "cta_headline": "Mais análises e dados",
                    "cta_subline": "Acesse o portal de dados abertos do instituto.",
                    "cta_url": "institutoforest.org",
                },
            },
        ]
    )

    return {
        "theme": "white",
        "runId": "research-trends",
        "sizes": {
            "topicTagPx": 24,
            "datePx": 26,
            "pageNumberPx": 24,
            "logoHeightPx": 54,
        },
        "slides": slides,
        "sources": {
            "openalex": "https://api.openalex.org/works (concepts: wildfire, forest fire; BR institutions)",
            "crossref": "https://api.crossref.org/works/{doi}",
            "google_trends": "pytrends (Google Trends, Brazil, last 10 years)",
        },
    }


def _publications_body(latest: dict[str, Any] | None, prior: dict[str, Any] | None) -> str:
    if not latest:
        return "Sem dados suficientes para resumir publicações por ano."
    if prior and prior.get("count"):
        delta = (latest["count"] - prior["count"]) / prior["count"] * 100.0
        sign = "alta" if delta >= 0 else "queda"
        return (
            f"Em {latest['year']}, {latest['count']} trabalhos foram publicados, uma "
            f"{sign} de {abs(delta):.1f}% sobre {prior['year']}."
        )
    return f"Em {latest['year']}, {latest['count']} trabalhos foram publicados."


def _top_body(items: list[dict[str, Any]], *, suffix: str) -> str:
    if not items:
        return "Sem dados suficientes."
    top = items[0]
    return f"O {suffix} mais frequente é {top['label']}, com {top['count']} trabalhos."


def _oa_body(latest: dict[str, Any] | None) -> str:
    if not latest:
        return "Sem dados de Open Access disponíveis."
    return (
        f"Em {latest['year']}, {latest['oa_pct']:.1f}% das publicações eram Open Access "
        f"({latest['oa']} de {latest['total']})."
    )


# ── Top-level run ────────────────────────────────────────────────────────────


def run(config: PipelineConfig) -> None:
    LOG.info("research_trends.start mailto=%s", config.mailto)
    config.cache_dir.mkdir(parents=True, exist_ok=True)

    openalex = OpenAlexClient(mailto=config.mailto, cache_dir=config.cache_dir)
    crossref = CrossrefClient(mailto=config.mailto, cache_dir=config.cache_dir)

    works = list(
        openalex.iter_works(
            filter_str=DEFAULT_OPENALEX_FILTER,
            cache_key="brazil_wildfire_research",
            max_pages=config.max_openalex_pages,
        )
    )
    LOG.info("research_trends.openalex_loaded count=%d", len(works))

    agg = _aggregate(works)

    if not config.skip_google_trends:
        trends_client = GoogleTrendsClient(cache_dir=config.cache_dir)
        trends_payload = trends_client.interest_over_time(["queimadas", "wildfire Brazil"])
    else:
        trends_payload = None

    chart_paths = _render_charts(agg, trends_payload, config.out_dir)
    LOG.info("research_trends.charts_rendered count=%d", len(chart_paths))

    validation = _validate_with_crossref(works, crossref, sample=config.crossref_sample)
    audit_path = config.cache_dir / "crossref_validation.json"
    audit_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2))
    LOG.info("research_trends.crossref_audited n=%d", len(validation))

    # Per-chart raw aggregation specs (for reproducibility).
    for key, data in (
        (CHART_PUB_YEAR, agg["publications_per_year"]),
        (CHART_INSTITUTIONS, agg["top_institutions"]),
        (CHART_CONCEPTS, agg["top_concepts"]),
        (CHART_VENUES, agg["top_venues"]),
        (CHART_OA_SHARE, agg["open_access_share"]),
    ):
        (config.out_dir / f"chart_spec-research-{key}.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2)
        )

    manifest = _build_manifest(agg, chart_paths, trends_payload)
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    config.public_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    config.public_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    LOG.info("research_trends.manifest_written path=%s", config.manifest_path)


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m forest_pipelines.social.research_trends",
        description=(
            "Build research-trends social deck (Brazilian wildfire research). "
            "Pulls OpenAlex, validates with Crossref, overlays Google Trends."
        ),
    )
    p.add_argument(
        "--mailto",
        default=os.environ.get("FOREST_POLITE_EMAIL", "julianofpadua@gmail.com"),
        help="Contact email for OpenAlex + Crossref polite-pool headers.",
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
    p.add_argument(
        "--skip-google-trends",
        action="store_true",
        help="Skip the Google Trends fetch; the overlay chart will be omitted.",
    )
    p.add_argument(
        "--refresh",
        action="store_true",
        help="Delete the OpenAlex + Google Trends caches before running.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.refresh:
        for path in args.cache_dir.glob("openalex_*.json"):
            path.unlink()
        for path in args.cache_dir.glob("google_trends_*.json"):
            path.unlink()
        LOG.info("research_trends.cache_cleared")

    config = PipelineConfig(
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
