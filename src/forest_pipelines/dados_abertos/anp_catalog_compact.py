# src/forest_pipelines/dados_abertos/anp_catalog_compact.py
"""
Compact ANP catalog JSON for Supabase — field contract (schema_version \"1\").

Dataset-level keys:
  package_id, slug, title, notes_plain, description_html,
  organization_id, organization_slug, organization_esfera, organization_uf, organization_municipio,
  maintainer, maintainer_email, license_code, license_title, version, metadata_created,
  themes, tags, extra_fields, is_open_data, visibility, private, has_dcat_badge, source_exported_at

Resource-level keys (nested under each dataset as \"resources\"):
  resource_id, url, format, name, description, kind, sources

kind: \"data\" | \"documentation\" | \"other\"
sources: subset of {\"acesso_rapido\", \"formatado\"}

Unicode: strings are normalized to NFC; JSON output uses UTF-8 with ensure_ascii=False.
Optional post-step if mojibake appears downstream: fix double UTF-8 decoding (not applied here).
"""
from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

SCHEMA_VERSION = "1"

ResourceKind = Literal["data", "documentation", "other"]
ResourceSource = Literal["acesso_rapido", "formatado"]

# Map portal extras keys (camelCase) → stable snake_case for Supabase-friendly columns
_EXTRAS_KEY_MAP: dict[str, str] = {
    "atualizacaoVersao": "atualizacao_versao",
    "coberturaEspacial": "cobertura_espacial",
    "reuso": "reuso",
    "descontinuado": "descontinuado",
    "existePrevisaoAbertura": "existe_previsao_abertura",
    "ultimaAtualizacaoMetadados": "ultima_atualizacao_metadados",
    "periodicidade": "periodicidade",
    "observanciaLegal": "observancia_legal",
    "granularidadeEspacial": "granularidade_espacial",
    "ultimaAtualizacaoDados": "ultima_atualizacao_dados",
}


def nfc_text(value: Any) -> Any:
    """Apply Unicode NFC to all strings in nested dict/list structures."""
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [nfc_text(x) for x in value]
    if isinstance(value, dict):
        return {k: nfc_text(v) for k, v in value.items()}
    return value


def _norm_url_key(url: str) -> str:
    u = url.strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        return urlunparse((p.scheme.lower(), netloc, path, "", p.query, ""))
    except Exception:
        return u.lower()


def infer_resource_kind(name: str, format_str: str | None) -> ResourceKind:
    fmt = (format_str or "").strip().upper()
    n = (name or "").strip().lower()
    if fmt == "PDF" and ("metadad" in n or n in ("metadados", "metadado")):
        return "documentation"
    if fmt in ("CSV", "XLSX", "XLS", "ZIP", "JSON", "XML", "GEOJSON", "SHP", "PARQUET"):
        return "data"
    if fmt == "PDF":
        return "documentation"
    if fmt in ("HTML", "DOC", "DOCX", "ODT", "TXT", "RTF"):
        return "documentation"
    return "other"


def _resource_from_flat(
    *,
    rid: Any,
    url: Any,
    fmt: Any,
    name: Any,
    description: Any,
    source: ResourceSource | None = None,
) -> dict[str, Any] | None:
    url_s = str(url or "").strip()
    if not url_s:
        return None
    rid_s = str(rid or "").strip() or None
    out: dict[str, Any] = {
        "resource_id": rid_s,
        "url": unicodedata.normalize("NFC", url_s),
        "format": unicodedata.normalize("NFC", str(fmt or "").strip()),
        "name": unicodedata.normalize("NFC", str(name or "").strip() or "(sem nome)"),
        "description": _optional_nfc_str(description),
        "kind": infer_resource_kind(str(name or ""), str(fmt or "")),
    }
    if source is not None:
        out["sources"] = [source]
    return out


def _optional_nfc_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    return unicodedata.normalize("NFC", s)


def _resource_key(rid: str, url: str) -> str:
    rid = rid.strip()
    if rid:
        return f"id:{rid}"
    return f"url:{_norm_url_key(url)}"


def _merge_resource_lists(
    acesso_rapido: list[dict[str, Any]] | None,
    formatado_top: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Union: order follows acesso_rapido first; formatado adds or merges sources."""
    merged: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}

    def upsert(raw: dict[str, Any], source: ResourceSource) -> None:
        r = _resource_from_flat(
            rid=raw.get("id"),
            url=raw.get("url"),
            fmt=raw.get("format"),
            name=raw.get("name"),
            description=raw.get("description"),
            source=None,
        )
        if not r:
            return
        rid = str(r.get("resource_id") or "").strip()
        url = str(r.get("url") or "")
        key = _resource_key(rid, url)
        if key not in by_key:
            r["sources"] = [source]
            by_key[key] = r
            merged.append(r)
        else:
            ex = by_key[key]
            srcs = sorted(set((ex.get("sources") or []) + [source]))
            ex["sources"] = srcs

    for raw in acesso_rapido or []:
        if isinstance(raw, dict):
            upsert(raw, "acesso_rapido")

    for raw in formatado_top or []:
        if isinstance(raw, dict):
            upsert(raw, "formatado")

    return merged


def _extras_snake_key(k: str) -> str:
    return _EXTRAS_KEY_MAP.get(k, re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower())


def _extract_extra_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Merge extrasFormatado then overlay portal `extras` object (object wins on conflict)."""
    out: dict[str, str] = {}
    fmt_list = record.get("extrasFormatado")
    if isinstance(fmt_list, list):
        for item in fmt_list:
            if not isinstance(item, dict):
                continue
            k = item.get("key")
            v = item.get("value")
            if k is None or v is None:
                continue
            sk = _extras_snake_key(str(k))
            out[sk] = unicodedata.normalize("NFC", str(v).strip())
    extras = record.get("extras")
    if isinstance(extras, dict):
        for k, v in extras.items():
            sk = _extras_snake_key(str(k))
            if v is None:
                continue
            out[sk] = unicodedata.normalize("NFC", str(v).strip())
    return out


def _themes(record: dict[str, Any]) -> list[dict[str, str | None]]:
    raw = record.get("temasAcessoRapido")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str | None]] = []
    for t in raw:
        if not isinstance(t, dict):
            continue
        out.append(
            {
                "id": str(t.get("id") or "").strip() or None,
                "slug": str(t.get("name") or "").strip() or None,
                "title": _optional_nfc_str(t.get("title")),
            }
        )
    return out


def _tags(record: dict[str, Any]) -> list[dict[str, str | None]]:
    raw = record.get("tagsAcessoRapido")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str | None]] = []
    for t in raw:
        if not isinstance(t, dict):
            continue
        out.append(
            {
                "id": str(t.get("id") or "").strip() or None,
                "name": _optional_nfc_str(t.get("name")),
            }
        )
    return out


def transform_dataset(record: dict[str, Any]) -> dict[str, Any]:
    """Map one portal `registros[]` object to compact dataset + resources."""
    resources_top: list[dict[str, Any]] = []
    rf = record.get("resourcesFormatado")
    if isinstance(rf, list):
        for item in rf:
            if isinstance(item, dict):
                resources_top.append(
                    {
                        "id": item.get("id"),
                        "format": item.get("format"),
                        "url": item.get("url"),
                        "name": item.get("name"),
                        "description": item.get("description"),
                    }
                )

    merged_resources = _merge_resource_lists(
        record.get("resourcesAcessoRapido") if isinstance(record.get("resourcesAcessoRapido"), list) else None,
        resources_top,
    )

    notes = _optional_nfc_str(record.get("notes"))
    desc_html = _optional_nfc_str(record.get("markdownNotes"))

    return {
        "package_id": str(record.get("id") or "").strip(),
        "slug": str(record.get("name") or "").strip(),
        "title": unicodedata.normalize("NFC", str(record.get("title") or "").strip() or "(sem título)"),
        "notes_plain": notes,
        "description_html": desc_html,
        "organization_id": str(record.get("organizationId") or "").strip() or None,
        "organization_slug": str(record.get("organizationName") or "").strip() or None,
        "organization_esfera": _optional_nfc_str(record.get("organizationEsfera")),
        "organization_uf": _optional_nfc_str(record.get("organizationUf")),
        "organization_municipio": _optional_nfc_str(record.get("organizationMunicipio")),
        "maintainer": _optional_nfc_str(record.get("maintainer")),
        "maintainer_email": _optional_nfc_str(record.get("maintainerEmail")),
        "license_code": _optional_nfc_str(record.get("licenca")),
        "license_title": _optional_nfc_str(record.get("tituloLicenca")),
        "version": _optional_nfc_str(record.get("version")),
        "metadata_created": _optional_nfc_str(record.get("metadataCreated")),
        "themes": _themes(record),
        "tags": _tags(record),
        "extra_fields": _extract_extra_fields(record),
        "is_open_data": bool(record.get("dadosAbertos")) if record.get("dadosAbertos") is not None else None,
        "visibility": _optional_nfc_str(record.get("visibilidade")),
        "private": record.get("privado") if isinstance(record.get("privado"), bool) else None,
        "has_dcat_badge": bool(record.get("possuiSeloDcat")) if record.get("possuiSeloDcat") is not None else None,
        "source_exported_at": _optional_nfc_str(record.get("dataAtualizacao")),
        "resources": merged_resources,
    }


def transform_anp_snapshot(data: dict[str, Any], *, generated_at: datetime | None = None) -> dict[str, Any]:
    """
    Build compact envelope from portal snapshot: {\"registros\": [...], \"totalRegistros\": N}.
    """
    gen = generated_at or datetime.now(timezone.utc)
    if gen.tzinfo is None:
        gen = gen.replace(tzinfo=timezone.utc)

    registros = data.get("registros")
    if not isinstance(registros, list):
        raise ValueError("Snapshot must contain 'registros' array")

    datasets = [transform_dataset(r) for r in registros if isinstance(r, dict)]

    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": gen.isoformat(),
        "source_total_registros": data.get("totalRegistros"),
        "datasets": nfc_text(datasets),
    }
    return envelope


def load_anp_snapshot(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)


def write_compact_catalog(path: Path, envelope: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(envelope, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def compact_schema_path() -> Path:
    return Path(__file__).resolve().parent / "schemas" / "anp_catalog_compact.v1.schema.json"


def validate_compact_envelope(envelope: dict[str, Any]) -> None:
    """Raise jsonschema.ValidationError if envelope does not match schema v1."""
    import jsonschema

    schema = json.loads(compact_schema_path().read_text(encoding="utf-8"))
    jsonschema.validate(instance=envelope, schema=schema)


def run_compact_from_paths(input_path: Path, output_path: Path) -> None:
    data = load_anp_snapshot(input_path)
    envelope = transform_anp_snapshot(data)
    write_compact_catalog(output_path, envelope)
