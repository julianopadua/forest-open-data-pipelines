from __future__ import annotations

import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import unquote, urljoin, urlparse
from zoneinfo import ZoneInfo

import requests
import yaml
from bs4 import BeautifulSoup, Tag

from forest_pipelines.manifests.build_manifest import build_manifest
from forest_pipelines.profiling import (
    FreshnessSignal,
    ProfileOptions,
    profiled_item,
    profile_source_url,
    warning,
)

ANP_HUB_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
USER_AGENT = "ForestOpenDataDiscovery/1.0 (+https://institutoforest.org)"
BLOCKED_LEGACY_HOST = ".".join(("dados", "gov", "br"))
DOWNLOAD_SUFFIXES = {
    ".csv",
    ".zip",
    ".xls",
    ".xlsx",
    ".json",
    ".xml",
    ".txt",
    ".pdf",
    ".doc",
    ".docx",
    ".ods",
    ".odt",
    ".shp",
    ".geojson",
    ".gpkg",
    ".kml",
}
DATA_SUFFIXES = DOWNLOAD_SUFFIXES - {".pdf"}
METADATA_RE = re.compile(r"(meta|metadado|metadados|dicionario|dicionário|layout|readme)", re.I)
GOVBR_DATE_LABEL_RE = re.compile(
    r"([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})(?:\s+([0-9]{1,2})h([0-9]{2}))?",
    re.I,
)
UPDATED_RE = re.compile(
    r"atualizado em\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}(?:\s+[0-9]{1,2}h[0-9]{2})?)",
    re.I,
)
PUBLISHED_RE = re.compile(
    r"publicado em\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}(?:\s+[0-9]{1,2}h[0-9]{2})?)",
    re.I,
)
PERIOD_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?:[-_]?([01]\d))?(?!\d)")
ANP_DATASET_PREFIX = "/anp/pt-br/centrais-de-conteudo/dados-abertos"
GOVBR_TZ = ZoneInfo("America/Sao_Paulo")


@dataclass(frozen=True)
class AnpHttpOptions:
    timeout_s: int = 30
    retry_count: int = 3
    delay_min_s: float = 0.25
    delay_max_s: float = 0.50


@dataclass(frozen=True)
class AnpRunnerOptions:
    http: AnpHttpOptions
    profile: ProfileOptions


@dataclass(frozen=True)
class CollectionLink:
    slug: str
    title: str
    url: str


@dataclass(frozen=True)
class ResourceLink:
    source_url: str
    filename: str
    title: str
    section: str
    period: str
    kind: str
    direct_download: bool
    updated_label: str | None = None


@dataclass(frozen=True)
class CatalogDatasetCfg:
    id: str
    slug: str
    title: str
    source_url: str
    bucket_prefix: str
    source_agency: str = "ANP - Agência Nacional do Petróleo, Gás Natural e Biocombustíveis"
    notes: str = "Indexação URL-only via páginas oficiais gov.br da ANP."


ANP_DATASET_IDS: tuple[str, ...] = (
    "anp_acervo_de_dados_tecnicos",
    "anp_acoes_de_fiscalizacao_do_abastecimento",
    "anp_aditamento_de_conteudo_local",
    "anp_amostras_de_rochas_e_fluidos",
    "anp_anuario_estatistico_brasileiro_do_petroleo_gas_natural_e_biocombustiveis",
    "anp_aquisicao_processamento_e_estudo_de_dados",
    "anp_autorizacoes_de_gas_natural",
    "anp_blocos_com_fase_exploratoria_encerrada",
    "anp_capacidade_de_armazenagem_de_terminais",
    "anp_comercializacao_de_gas_natural",
    "anp_dados_cadastrais_das_revendas_de_gas_liquefeito_de_petroleo_glp",
    "anp_dados_cadastrais_dos_revendedores_varejistas_de_combustiveis_automotivos",
    "anp_gestao_de_contratos_de_exploracao_e_producao__dados_de_ep",
    "anp_dados_georreferenciados_das_bacias_sedimentares_brasileiras",
    "anp_distribuidores_de_combustiveis_liquidos",
    "anp_fase_de_exploracao",
    "anp_fase_de_desenvolvimento_e_producao",
    "anp_fiscalizacao_de_conteudo_local",
    "anp_importacoes_e_exportacoes",
    "anp_dados_de_incidentes_de_exploracao_e_producao_de_petroleo_e_gas_natural",
    "anp_movimentacao_de_derivados_de_petroleo_e_biocombustiveis",
    "anp_movimentacao_dos_terminais_aquaviarios",
    "anp_dados_consolidados_de_movimentacao_de_gas_natural_em_gasodutos_de_transporte",
    "anp_multas_aplicadas___vencimento_a_partir_de_2016",
    "anp_participacoes_governamentais",
    "anp_pesquisa_e_desenvolvimento_e_inovacao_pdi",
    "anp_pontos_de_abastecimento_autorizados",
    "anp_pmqc___programa_de_monitoramento_da_qualidade_dos_combustiveis",
    "anp_programa_de_monitoramento_dos_lubrificantes_pml",
    "anp_prestadores_de_servicos_de_apoio_administrativo",
    "anp_previso_de_investimentos_exploratrios",
    "anp_processamento_de_petroleo_e_producao_de_derivados",
    "anp_producao_de_biocombustiveis",
    "anp_producao_de_petroleo_e_gas_natural_por_estado_e_localizacao",
    "anp_producao_de_petroleo_e_gas_natural_por_poco",
    "anp_relacao_de_concessionarios",
    "anp_registro_de_leos_e_graxas_lubrificantes",
    "anp_resultado_de_poco",
    "anp_rodadas_de_licitacoes_de_petroleo_e_gas_natural",
    "anp_serie_historica_de_precos_de_combustiveis_e_de_glp",
    "anp_tancagem_do_abastecimento_nacional_de_combustiveis",
    "anp_vendas_de_derivados_de_petroleo_e_biocombustiveis",
)


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_govbr_freshness_label(
    raw_label: str | None,
    *,
    method: str,
) -> FreshnessSignal | None:
    text = _clean_text(raw_label)
    match = GOVBR_DATE_LABEL_RE.search(text)
    if not match:
        return None
    day, month, year, hour, minute = match.groups()
    precision = "datetime" if hour is not None and minute is not None else "date"
    source_modified_at = datetime(
        int(year),
        int(month),
        int(day),
        int(hour or 0),
        int(minute or 0),
        tzinfo=GOVBR_TZ,
    )
    return FreshnessSignal(
        source_modified_at=source_modified_at,
        precision=precision,
        method=method,
        raw_label=match.group(0),
    )


def _label_from_span(soup: BeautifulSoup, class_name: str) -> str | None:
    span = soup.find("span", class_=class_name)
    if not isinstance(span, Tag):
        return None
    value = span.find("span", class_="value")
    if isinstance(value, Tag):
        text = _clean_text(value.get_text(" "))
        if text:
            return text
    text = _clean_text(span.get_text(" "))
    match = GOVBR_DATE_LABEL_RE.search(text)
    return match.group(0) if match else None


def extract_page_freshness_labels(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    labels: dict[str, str] = {}
    published = _label_from_span(soup, "documentPublished")
    modified = _label_from_span(soup, "documentModified")
    text = _clean_text(_article_root(soup).get_text(" "))
    if not published:
        match = PUBLISHED_RE.search(text)
        published = match.group(1) if match else None
    if not modified:
        match = UPDATED_RE.search(text)
        modified = match.group(1) if match else None
    if published:
        labels["published_label"] = published
    if modified:
        labels["modified_label"] = modified
    return labels


def _ascii_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    ascii_only = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    slug = re.sub(r"[^A-Za-z0-9]+", "-", ascii_only.lower()).strip("-")
    return slug or "download"


def _netloc_key(netloc: str) -> str:
    n = netloc.lower()
    return n[4:] if n.startswith("www.") else n


def _is_allowed_official_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = _netloc_key(parsed.netloc)
    if host == BLOCKED_LEGACY_HOST or host.endswith(f".{BLOCKED_LEGACY_HOST}"):
        return False
    return host == "gov.br" or host.endswith(".gov.br") or host == "anp.gov.br" or host.endswith(".anp.gov.br")


def _is_anp_page(url: str) -> bool:
    parsed = urlparse(url)
    return _netloc_key(parsed.netloc) == "gov.br" and parsed.path.startswith(ANP_DATASET_PREFIX)


def _filename_from_url(url: str, fallback: str) -> str:
    path = unquote(urlparse(url.split("?", 1)[0]).path)
    name = Path(path).name
    if name and "." in name:
        return name
    safe = _ascii_slug(fallback)
    return f"{safe}.html"


def _suffix(filename: str) -> str:
    lower = filename.lower()
    for ext in sorted(DOWNLOAD_SUFFIXES, key=len, reverse=True):
        if lower.endswith(ext):
            return ext
    return Path(lower).suffix


def _article_root(soup: BeautifulSoup) -> Tag:
    for selector in ("#content-core", "main", "article", "#content"):
        found = soup.select_one(selector)
        if isinstance(found, Tag):
            return found
    body = soup.body
    return body if isinstance(body, Tag) else soup


def _heading_text(root: Tag) -> str:
    h1 = root.find("h1")
    if isinstance(h1, Tag):
        text = _clean_text(h1.get_text(" "))
        if text:
            return text
    return ""


def _slug_from_collection_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = Path(path).name
    return _ascii_slug(unquote(slug))


def _link_title(anchor: Tag, fallback_url: str) -> str:
    label = _clean_text(anchor.get_text(" "))
    if label and not label.lower().startswith("veja "):
        return label
    parent = anchor.parent
    for previous in anchor.find_all_previous(limit=8):
        if not isinstance(previous, Tag):
            continue
        if previous.name in {"li", "p", "h2", "h3"}:
            text = _clean_text(previous.get_text(" "))
            text = re.split(r"\bVeja\b", text, maxsplit=1, flags=re.I)[0].strip()
            if text and not text.lower().startswith("veja "):
                return text
    if isinstance(parent, Tag):
        text = _clean_text(parent.get_text(" "))
        text = re.split(r"\bVeja\b", text, maxsplit=1, flags=re.I)[0].strip()
        if text and not text.lower().startswith("veja "):
            return text
    return _slug_from_collection_url(fallback_url).replace("-", " ").title()


def _iter_content_links(root: Tag) -> Iterable[Tag]:
    for anchor in root.find_all("a", href=True):
        if isinstance(anchor, Tag):
            yield anchor


def discover_collections(html: str, base_url: str = ANP_HUB_URL) -> list[CollectionLink]:
    soup = BeautifulSoup(html, "html.parser")
    root = _article_root(soup)
    seen: set[str] = set()
    collections: list[CollectionLink] = []
    for anchor in _iter_content_links(root):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        url = urljoin(base_url, href).split("#", 1)[0]
        parsed = urlparse(url)
        if not _is_anp_page(url):
            continue
        if parsed.path.rstrip("/") in {ANP_DATASET_PREFIX, f"{ANP_DATASET_PREFIX}/dados-abertos"}:
            continue
        if "/arquivos/" in parsed.path:
            continue
        if url in seen:
            continue
        seen.add(url)
        collections.append(
            CollectionLink(
                slug=_slug_from_collection_url(url),
                title=_link_title(anchor, url),
                url=url,
            )
        )
    return collections


def _period_from_text(*parts: str) -> str:
    text = " ".join(parts)
    match = PERIOD_RE.search(text)
    if not match:
        return "current"
    year, month = match.groups()
    return f"{year}-{month}" if month else year


def _updated_label_near(anchor: Tag) -> str | None:
    parent = anchor.parent
    text = _clean_text(parent.get_text(" ")) if isinstance(parent, Tag) else ""
    match = UPDATED_RE.search(text)
    return match.group(1) if match else None


def _resource_kind(filename: str, label: str) -> str:
    target = f"{filename} {label}"
    if METADATA_RE.search(target):
        return "metadata"
    ext = _suffix(filename)
    if ext in DATA_SUFFIXES:
        return "data"
    if ext == ".pdf":
        return "documentation"
    return "data"


def _should_keep_indirect(url: str) -> bool:
    if not _is_allowed_official_url(url):
        return False
    if _is_anp_page(url):
        return False
    return True


def extract_resource_links(html: str, page_url: str) -> list[ResourceLink]:
    soup = BeautifulSoup(html, "html.parser")
    root = _article_root(soup)
    resources: list[ResourceLink] = []
    seen: set[str] = set()
    section = ""
    for element in root.descendants:
        if not isinstance(element, Tag):
            continue
        if element.name in {"h2", "h3", "h4"}:
            section = _clean_text(element.get_text(" "))
            continue
        if element.name != "a" or not element.get("href"):
            continue
        href = str(element.get("href") or "").strip()
        source_url = urljoin(page_url, href).split("#", 1)[0]
        if source_url in seen or not _is_allowed_official_url(source_url):
            continue
        label = _clean_text(element.get_text(" "))
        filename = _filename_from_url(source_url, label or section or "download")
        ext = _suffix(filename)
        direct = ext in DOWNLOAD_SUFFIXES
        if not direct and not _should_keep_indirect(source_url):
            continue
        if "compartilhe" in label.lower() or "copiar" in label.lower():
            continue
        seen.add(source_url)
        title = label or filename
        resources.append(
            ResourceLink(
                source_url=source_url,
                filename=filename,
                title=title,
                section=section,
                period=_period_from_text(filename, label, section),
                kind=_resource_kind(filename, label),
                direct_download=direct,
                updated_label=_updated_label_near(element),
            )
        )
    return resources


def _metadata_sort_key(resource: ResourceLink) -> tuple[int, str]:
    text = f"{resource.filename} {resource.title}".lower()
    return (0 if METADATA_RE.search(text) else 1, resource.filename)


def split_manifest_resources(resources: list[ResourceLink]) -> tuple[ResourceLink | None, list[ResourceLink], list[ResourceLink]]:
    metadata_candidates = [
        resource
        for resource in resources
        if _suffix(resource.filename) == ".pdf" and resource.kind in {"metadata", "documentation"}
    ]
    metadata_file = sorted(metadata_candidates, key=_metadata_sort_key)[0] if metadata_candidates else None
    documentation = [
        resource
        for resource in resources
        if resource.kind in {"metadata", "documentation"} and resource != metadata_file
    ]
    data = [resource for resource in resources if resource not in documentation and resource != metadata_file]
    return metadata_file, data, documentation


def extract_page_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    root = _article_root(soup)
    return _heading_text(root) or _heading_text(soup) or "ANP - Dados abertos"


def load_anp_runner_options(datasets_dir: Path) -> AnpRunnerOptions:
    path = datasets_dir / "anp" / "govbr.yml"
    raw: dict[str, Any] = {}
    if path.is_file():
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raw = {}
    http_raw = raw.get("http") if isinstance(raw.get("http"), dict) else {}
    profile_raw = raw.get("profile") if isinstance(raw.get("profile"), dict) else {}
    return AnpRunnerOptions(
        http=AnpHttpOptions(
            timeout_s=int(http_raw.get("timeout_s") or 30),
            retry_count=int(http_raw.get("retry_count") or 3),
            delay_min_s=float(http_raw.get("delay_min_s") or 0.25),
            delay_max_s=float(http_raw.get("delay_max_s") or 0.50),
        ),
        profile=ProfileOptions(
            timeout_s=int(profile_raw.get("timeout_s") or 180),
            max_archive_members=int(profile_raw.get("max_archive_members") or 8),
        ),
    )


def load_catalog_dataset_cfg(root: Path, dataset_id: str) -> CatalogDatasetCfg:
    path = root / "configs" / "catalog" / "open_data.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for entry in raw.get("datasets") or []:
        if not isinstance(entry, dict) or entry.get("id") != dataset_id:
            continue
        return CatalogDatasetCfg(
            id=str(entry["id"]),
            slug=str(entry["slug"]),
            title=str(entry["title"]),
            source_url=str(entry["source_url"]),
            bucket_prefix=str(entry["manifest_path"]).removesuffix("/manifest.json"),
        )
    raise FileNotFoundError(f"Entrada ANP nao encontrada no catalogo: {dataset_id}")


def fetch_html(url: str, options: AnpHttpOptions, logger: Any = None) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    last_exc: Exception | None = None
    for attempt in range(max(options.retry_count, 1)):
        try:
            if attempt:
                delay = min(options.delay_max_s, options.delay_min_s * (2 ** attempt))
                time.sleep(delay)
            response = requests.get(url, headers=headers, timeout=options.timeout_s)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_exc = exc
            if logger:
                logger.warning("ANP gov.br fetch failed url=%s attempt=%s error=%s", url, attempt + 1, type(exc).__name__)
    raise RuntimeError(f"Falha ao baixar pagina ANP: {url}") from last_exc


def resolve_final_url(url: str, options: AnpHttpOptions, logger: Any = None) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    try:
        response = requests.head(url, headers=headers, timeout=options.timeout_s, allow_redirects=True)
        if response.status_code < 400:
            return response.url
    except Exception as exc:
        if logger:
            logger.info("ANP HEAD resolve failed url=%s error=%s", url, type(exc).__name__)
    try:
        response = requests.get(
            url,
            headers={**headers, "Range": "bytes=0-0"},
            timeout=options.timeout_s,
            allow_redirects=True,
            stream=True,
        )
        final_url = response.url
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type.lower():
            text = response.text[:4096]
            soup = BeautifulSoup(text, "html.parser")
            meta = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if isinstance(meta, Tag):
                content = str(meta.get("content") or "")
                match = re.search(r"url=([^;]+)", content, re.I)
                if match:
                    return urljoin(final_url, match.group(1).strip())
        return final_url
    except Exception as exc:
        if logger:
            logger.info("ANP GET resolve failed url=%s error=%s", url, type(exc).__name__)
        return url


def _resource_freshness_signal(
    resource: ResourceLink,
    page_modified_signal: FreshnessSignal | None,
) -> FreshnessSignal | None:
    resource_signal = parse_govbr_freshness_label(
        resource.updated_label,
        method="anp_resource_updated_label",
    )
    return resource_signal or page_modified_signal


def _resource_to_document_file(
    resource: ResourceLink,
    logger: Any,
    options: ProfileOptions,
    page_modified_signal: FreshnessSignal | None,
) -> dict[str, Any]:
    return {
        "filename": resource.filename,
        "source_url": resource.source_url,
        "title": resource.title,
        **profile_source_url(
            resource.source_url,
            filename=resource.filename,
            logger=logger,
            options=options,
            freshness_signal=_resource_freshness_signal(resource, page_modified_signal),
        ),
    }


def _resource_to_item(
    resource: ResourceLink,
    logger: Any,
    options: ProfileOptions,
    page_modified_signal: FreshnessSignal | None,
) -> dict[str, Any]:
    if not resource.direct_download:
        return {
            "kind": "data",
            "period": resource.period,
            "filename": resource.filename,
            "source_url": resource.source_url,
            "title": resource.title,
            "format": _suffix(resource.filename).lstrip(".") or "html",
            "profile_status": "skipped",
            "profile_warnings": [
                warning("indirect_official_resource", "Official ANP resource is an interactive page, not a direct file.")
            ],
        }
    return profiled_item(
        source_url=resource.source_url,
        filename=resource.filename,
        period=resource.period,
        title=resource.title,
        kind="data",
        logger=logger,
        options=options,
        freshness_signal=_resource_freshness_signal(resource, page_modified_signal),
    )


def build_manifest_from_detail_page(
    *,
    cfg: CatalogDatasetCfg,
    html: str,
    resources: list[ResourceLink] | None,
    logger: Any,
    options: AnpRunnerOptions,
) -> dict[str, Any]:
    title = extract_page_title(html) or cfg.title
    discovered = resources if resources is not None else extract_resource_links(html, cfg.source_url)
    page_freshness_labels = extract_page_freshness_labels(html)
    page_modified_signal = parse_govbr_freshness_label(
        page_freshness_labels.get("modified_label"),
        method="anp_page_modified_label",
    )
    metadata_resource, data_resources, documentation_resources = split_manifest_resources(discovered)
    metadata_file = (
        _resource_to_document_file(
            metadata_resource,
            logger,
            options.profile,
            page_modified_signal,
        )
        if metadata_resource
        else None
    )
    items = [
        _resource_to_item(resource, logger, options.profile, page_modified_signal)
        for resource in sorted(data_resources, key=lambda r: (r.period, r.filename), reverse=True)
    ]
    documentation_files = [
        {
            "filename": resource.filename,
            "source_url": resource.source_url,
            "title": resource.title,
            "kind": resource.kind,
        }
        for resource in documentation_resources
    ]
    resource_updated_labels = {
        resource.source_url: resource.updated_label
        for resource in discovered
        if resource.updated_label
    }
    warnings: list[str] = []
    if not items:
        warnings.append("Nenhum recurso de dados direto foi descoberto na pagina gov.br da ANP.")
    if documentation_files:
        warnings.append(f"{len(documentation_files)} documento(s) oficial(is) foram publicados em meta.custom_tags.documentation_files.")
    meta: dict[str, Any] = {
        "source_agency": cfg.source_agency,
        "notes": cfg.notes,
        "custom_tags": {
            "govbr_hub_url": ANP_HUB_URL,
            "govbr_slug": cfg.slug,
            "discovered_resource_count": len(discovered),
            "documentation_files": documentation_files,
            "page_freshness_labels": page_freshness_labels,
            "resource_updated_labels": resource_updated_labels,
        },
    }
    if metadata_file:
        meta["metadata_file"] = metadata_file
    return build_manifest(
        dataset_id=cfg.id,
        title=title,
        source_dataset_url=cfg.source_url,
        bucket_prefix=cfg.bucket_prefix,
        items=items,
        meta=meta,
        warnings=warnings,
    )


def sync_dataset(
    *,
    dataset_id: str,
    settings: Any,
    storage: Any,
    logger: Any,
    latest_months: int | None = None,
) -> dict[str, Any]:
    del storage, latest_months
    cfg = load_catalog_dataset_cfg(settings.root, dataset_id)
    options = load_anp_runner_options(settings.datasets_dir)
    logger.info("ANP gov.br detail page: %s", cfg.source_url)
    html = fetch_html(cfg.source_url, options.http, logger)
    resources = extract_resource_links(html, cfg.source_url)
    resolved: list[ResourceLink] = []
    for resource in resources:
        final_url = resolve_final_url(resource.source_url, options.http, logger)
        if final_url != resource.source_url and _is_allowed_official_url(final_url):
            resource = ResourceLink(
                source_url=final_url,
                filename=_filename_from_url(final_url, resource.title),
                title=resource.title,
                section=resource.section,
                period=resource.period,
                kind=resource.kind,
                direct_download=_suffix(_filename_from_url(final_url, resource.title)) in DOWNLOAD_SUFFIXES,
                updated_label=resource.updated_label,
            )
        resolved.append(resource)
    return build_manifest_from_detail_page(
        cfg=cfg,
        html=html,
        resources=resolved,
        logger=logger,
        options=options,
    )


def make_sync(dataset_id: str) -> Callable[..., dict[str, Any]]:
    def sync(
        settings: Any,
        storage: Any,
        logger: Any,
        latest_months: int | None = None,
    ) -> dict[str, Any]:
        return sync_dataset(
            dataset_id=dataset_id,
            settings=settings,
            storage=storage,
            logger=logger,
            latest_months=latest_months,
        )

    return sync
