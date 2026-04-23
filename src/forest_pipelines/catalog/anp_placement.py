"""Port of scripts/generate-anp-catalog.mjs placement logic to Python.

Deterministic categorization of ANP datasets from the compact envelope
into (category_title, segment_title, subcategory_title).

Kept isolated so the ruleset can evolve without touching the publish flow.
"""
from __future__ import annotations

import unicodedata
from typing import Any, TypedDict


class Placement(TypedDict, total=False):
    category_title: str
    segment_title: str
    subcategory_title: str


# Legacy placements for the first 15 ANP datasets. Explicit wins over heuristics.
LEGACY_PLACEMENT_BY_SLUG: dict[str, Placement] = {
    "tancagem-do-abastecimento-nacional-de-combustiveis": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Abastecimento e mercado",
    },
    "serie-historica-de-precos-de-combustiveis-e-de-glp": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Abastecimento e mercado",
    },
    "rodadas-de-licitacoes-de-petroleo-e-gas-natural": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "resultado-de-poco": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "relacao-de-concessionarios": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "registro-de-leos-e-graxas-lubrificantes": {
        "category_title": "Outros",
        "subcategory_title": "Comércio e serviços",
    },
    "programa-de-monitoramento-dos-lubrificantes-pml": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Lubrificantes",
    },
    "producao-de-petroleo-e-gas-natural-por-estado-e-localizacao": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "producao-de-petroleo-e-gas-natural-por-poco": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "producao-de-biocombustiveis": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Biocombustíveis e renováveis",
    },
    "processamento-de-petroleo-e-producao-de-derivados": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "previso-de-investimentos-exploratrios": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Petróleo e gás",
    },
    "prestadores-de-servicos-de-apoio-administrativo": {
        "category_title": "Outros",
        "subcategory_title": "Administração",
    },
    "pontos-de-abastecimento-autorizados": {
        "category_title": "Mercado de commodities",
        "segment_title": "Energia",
        "subcategory_title": "Abastecimento e mercado",
    },
    "pesquisa-e-desenvolvimento-e-inovacao-pdi": {
        "category_title": "Outros",
        "subcategory_title": "Pesquisa e desenvolvimento",
    },
}


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    stripped = unicodedata.normalize("NFD", str(value))
    ascii_only = "".join(c for c in stripped if unicodedata.category(c) != "Mn")
    return ascii_only.lower()


def _has_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(n in haystack for n in needles)


def placement_for_dataset(ds: dict[str, Any]) -> Placement:
    """Mirror of placementForDataset() from scripts/generate-anp-catalog.mjs."""
    slug = ds.get("slug") or ""
    legacy = LEGACY_PLACEMENT_BY_SLUG.get(slug)
    if legacy:
        return legacy

    slug_n = _normalize(slug)
    title_n = _normalize(ds.get("title"))
    text = f"{slug_n} {title_n}"
    themes = [
        _normalize(t.get("title"))
        for t in (ds.get("themes") or [])
        if isinstance(t, dict) and t.get("title")
    ]
    theme_text = " ".join(themes)

    admin_needles = (
        "apoio-administrativo",
        "prestadores-de-servicos",
        "multas-aplicadas",
        "autorizacoes",
        "fiscalizacao",
        "conteudo-local",
        "aditamento-de-conteudo-local",
    )
    if _has_any(text, admin_needles) or _has_any(theme_text, ("planejamento e gestao",)):
        return {
            "category_title": "Outros",
            "subcategory_title": "Administração",
        }

    if _has_any(text, ("lubrificante", "oleos-e-graxas")):
        return {
            "category_title": "Mercado de commodities",
            "segment_title": "Energia",
            "subcategory_title": "Lubrificantes",
        }

    if _has_any(text, ("bio", "etanol", "biodiesel")):
        return {
            "category_title": "Mercado de commodities",
            "segment_title": "Energia",
            "subcategory_title": "Biocombustíveis e renováveis",
        }

    upstream_needles = (
        "petroleo",
        "gas-natural",
        "gasodutos",
        "poco",
        "exploracao",
        "producao",
        "blocos",
        "bacias",
        "amostras-de-rochas",
        "dados-tecnicos",
        "acervo-de-dados-tecnicos",
        "investimentos-exploratorios",
        "contratos-de-exploracao",
        "incidentes-de-exploracao-e-producao",
        "rodadas-de-licitacoes",
        "concessionarios",
        "participacoes-governamentais",
        "aquisicao-processamento-e-estudo-de-dados",
    )
    if _has_any(text, upstream_needles) or _has_any(theme_text, ("energia",)):
        return {
            "category_title": "Mercado de commodities",
            "segment_title": "Energia",
            "subcategory_title": "Petróleo e gás",
        }

    downstream_needles = (
        "abastecimento",
        "precos-de-combustiveis",
        "vendas-de-derivados",
        "movimentacao",
        "importacoes-e-exportacoes",
        "distribuidores",
        "revendedores",
        "revendas-de-gas",
        "armazenagem",
        "tancagem",
        "terminais",
        "pmqc",
        "qualidade-dos-combustiveis",
        "pontos-de-abastecimento",
        "comercializacao-de-gas",
        "painel-de-produtores-de-derivados",
        "processamento-de-petroleo-e-producao-de-derivados",
    )
    if _has_any(text, downstream_needles) or _has_any(
        theme_text, ("abastecimento", "comercio e servicos", "economia e financas")
    ):
        return {
            "category_title": "Mercado de commodities",
            "segment_title": "Energia",
            "subcategory_title": "Abastecimento e mercado",
        }

    if _has_any(text, ("anuario-estatistico", "anurio-estatstico")):
        return {
            "category_title": "Outros",
            "subcategory_title": "Pesquisa e desenvolvimento",
        }

    return {
        "category_title": "Outros",
        "subcategory_title": "Geral",
    }


def anp_id_from_slug(slug: str) -> str:
    return f"anp_{str(slug).replace('-', '_')}"
