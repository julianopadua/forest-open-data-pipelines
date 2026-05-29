from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import zipfile
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator, Literal
from urllib.parse import unquote, urlparse

import pandas as pd
import requests

ProfileStatus = Literal["ok", "partial", "failed", "skipped"]
FreshnessPrecision = Literal["date", "datetime"]

TABULAR_SUFFIXES = {".csv", ".txt", ".tsv"}
EXCEL_SUFFIXES = {".xls", ".xlsx"}
ARCHIVE_SUFFIXES = {".zip"}
JSON_SUFFIXES = {".json", ".geojson"}
XML_SUFFIXES = {".xml"}
PDF_SUFFIXES = {".pdf"}
GEOSPATIAL_SUFFIXES = {".tif", ".tiff", ".shp", ".gpkg", ".kml"}

PROFILE_CACHE_FIELDS = {
    "size_bytes",
    "sha256",
    "row_count",
    "column_count",
    "columns",
    "content_type",
    "format",
    "last_modified",
    "profiled_at",
    "profile_status",
    "profile_warnings",
    "archive_profile",
}

_PROFILE_CACHE: ContextVar[dict[str, dict[str, Any]] | None] = ContextVar(
    "forest_profile_cache",
    default=None,
)


@dataclass(frozen=True)
class ProfileOptions:
    timeout_s: int = 180
    keep_local: bool = False
    max_archive_members: int = 8


@dataclass(frozen=True)
class FreshnessSignal:
    source_modified_at: datetime
    precision: FreshnessPrecision
    method: str
    raw_label: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def filename_from_url(url: str, fallback: str = "download") -> str:
    try:
        path = unquote(urlparse(url).path)
        name = Path(path).name
        return name or fallback
    except Exception:
        return fallback


def warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _is_url_only_sentinel(profile: dict[str, Any]) -> bool:
    # Items emitted by the legacy URL-only contract carry sentinel checksums
    # and zero sizes. They are not a real profile; reusing them across syncs
    # would suppress every subsequent reprofile and freeze the cache to
    # placeholder data. Treat them as a cache miss so the runner downloads
    # the resource and produces real profile_status, sha256, size_bytes,
    # content_type, format, last_modified, profiled_at, and where applicable
    # row_count / column_count / columns / archive_profile.
    sha = profile.get("sha256")
    if isinstance(sha, str) and sha.strip().lower() == "external":
        return True
    if profile.get("size_bytes") == 0 and not profile.get("sha256"):
        return True
    if profile.get("profile_status") == "skipped" and not profile.get("profiled_at"):
        return True
    return False


def profile_cache_from_manifest(manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(manifest, dict):
        return {}

    candidates: list[dict[str, Any]] = []
    items = manifest.get("items")
    if isinstance(items, list):
        candidates.extend(item for item in items if isinstance(item, dict))

    meta = manifest.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("metadata_file"), dict):
        candidates.append(meta["metadata_file"])

    cache: dict[str, dict[str, Any]] = {}
    for raw in candidates:
        source_url = raw.get("source_url")
        if not isinstance(source_url, str) or not source_url.strip():
            continue
        profile = {
            key: value
            for key, value in raw.items()
            if key in PROFILE_CACHE_FIELDS and value is not None
        }
        if not profile:
            continue
        if _is_url_only_sentinel(profile):
            continue
        cache[source_url] = profile
    return cache


@contextmanager
def use_profile_cache(cache: dict[str, dict[str, Any]] | None) -> Iterator[None]:
    token = _PROFILE_CACHE.set(dict(cache) if cache else None)
    try:
        yield
    finally:
        _PROFILE_CACHE.reset(token)


def _profile_cache_hit(source_url: str) -> dict[str, Any] | None:
    cache = _PROFILE_CACHE.get()
    if not cache:
        return None
    profile = cache.get(source_url)
    return dict(profile) if isinstance(profile, dict) else None


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_http_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = parsedate_to_datetime(value.strip())
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _source_signal_allows_cache(
    cached: dict[str, Any],
    freshness_signal: FreshnessSignal,
) -> bool:
    profiled_at = _parse_iso_datetime(cached.get("profiled_at"))
    if profiled_at is None:
        return False
    source_modified_at = freshness_signal.source_modified_at
    if source_modified_at.tzinfo is None:
        source_modified_at = source_modified_at.replace(tzinfo=timezone.utc)
    else:
        source_modified_at = source_modified_at.astimezone(timezone.utc)
    if freshness_signal.precision == "date":
        return profiled_at.date() >= source_modified_at.date()
    return profiled_at >= source_modified_at


def freshness_signal_allows_cache(
    cached_profile: dict[str, Any],
    freshness_signal: FreshnessSignal,
) -> bool:
    return _source_signal_allows_cache(cached_profile, freshness_signal)


def _same_http_datetime(left: Any, right: Any) -> bool:
    parsed_left = _parse_http_datetime(left)
    parsed_right = _parse_http_datetime(right)
    if parsed_left is not None and parsed_right is not None:
        return parsed_left == parsed_right
    return str(left or "").strip() == str(right or "").strip()


def _int_header(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _http_headers_allow_cache(cached: dict[str, Any], headers: Any) -> bool:
    response_last_modified = headers.get("Last-Modified") if headers else None
    cached_last_modified = cached.get("last_modified")
    if not response_last_modified or not cached_last_modified:
        return False
    if not _same_http_datetime(response_last_modified, cached_last_modified):
        return False
    response_size = _int_header(headers.get("Content-Length") if headers else None)
    cached_size = cached.get("size_bytes")
    if response_size is not None and cached_size is not None:
        return response_size == cached_size
    return True


def _format_from_filename(filename: str) -> str:
    suffix = Path(filename).suffix.lower().lstrip(".")
    return suffix or "unknown"


def _hash_file(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                continue
            size += len(chunk)
            h.update(chunk)
    return size, h.hexdigest()


def _text_sample(path: Path, size: int = 65536) -> str:
    raw = path.read_bytes()[:size]
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _detect_delimiter(sample: str, suffix: str) -> str:
    if suffix == ".tsv":
        return "\t"
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def _profile_delimited(path: Path, suffix: str) -> dict[str, Any]:
    sample = _text_sample(path)
    delimiter = _detect_delimiter(sample, suffix)
    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            columns = next(reader)
        except StopIteration:
            return {
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "profile_status": "partial",
                "profile_warnings": [warning("empty_tabular_data", "No rows were found.")],
            }
        row_count = sum(1 for _ in reader)

    columns = [str(col).strip() for col in columns]
    result: dict[str, Any] = {
        "row_count": int(row_count),
        "column_count": len(columns),
        "columns": columns,
    }
    if row_count == 0:
        result["profile_status"] = "partial"
        result["profile_warnings"] = [
            warning("empty_tabular_data", "Only the header row was found.")
        ]
    return result


def _profile_excel(path: Path) -> dict[str, Any]:
    try:
        xl = pd.ExcelFile(path)
        sheet = xl.sheet_names[0]
        header = pd.read_excel(path, sheet_name=sheet, nrows=0)
        df = pd.read_excel(path, sheet_name=sheet)
    except Exception as exc:
        return {
            "profile_status": "partial",
            "profile_warnings": [
                warning("unsupported_format", f"Excel profile failed: {type(exc).__name__}.")
            ],
        }
    columns = [str(col) for col in header.columns]
    return {
        "row_count": int(len(df)),
        "column_count": len(columns),
        "columns": columns,
    }


def _profile_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "profile_status": "partial",
            "profile_warnings": [
                warning("unsupported_format", f"JSON profile failed: {type(exc).__name__}.")
            ],
        }
    if isinstance(data, list):
        return {"row_count": len(data)}
    if isinstance(data, dict):
        return {"column_count": len(data.keys()), "columns": sorted(str(k) for k in data.keys())}
    return {}


def _profile_xml(path: Path) -> dict[str, Any]:
    try:
        import xml.etree.ElementTree as ET

        root = ET.parse(path).getroot()
        return {"columns": [root.tag], "column_count": 1}
    except Exception as exc:
        return {
            "profile_status": "partial",
            "profile_warnings": [
                warning("unsupported_format", f"XML profile failed: {type(exc).__name__}.")
            ],
        }


def _profile_archive_member(zf: zipfile.ZipFile, member: str, tmp_dir: Path) -> dict[str, Any]:
    suffix = Path(member).suffix.lower()
    out = tmp_dir / Path(member).name
    with zf.open(member) as src, out.open("wb") as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            if not chunk:
                continue
            dst.write(chunk)
    member_profile = _profile_path(out, filename=member)
    out.unlink(missing_ok=True)
    return {
        "filename": member,
        "size_bytes": zf.getinfo(member).file_size,
        "format": suffix.lstrip(".") or "unknown",
        "row_count": member_profile.get("row_count"),
        "column_count": member_profile.get("column_count"),
        "columns": member_profile.get("columns"),
        "profile_status": member_profile.get("profile_status", "ok"),
        "profile_warnings": member_profile.get("profile_warnings", []),
    }


def _profile_archive(path: Path, options: ProfileOptions) -> dict[str, Any]:
    warnings: list[dict[str, str]] = []
    try:
        with zipfile.ZipFile(path) as zf:
            infos = [info for info in zf.infolist() if not info.is_dir()]
            members = [info.filename for info in infos]
            tabular = [
                info.filename
                for info in infos
                if Path(info.filename).suffix.lower() in TABULAR_SUFFIXES | EXCEL_SUFFIXES
            ]
            selected = tabular[: options.max_archive_members]
            if len(tabular) > len(selected):
                warnings.append(
                    warning(
                        "archive_member_skipped",
                        "Some archive members were not profiled because of the configured limit.",
                    )
                )
            with tempfile.TemporaryDirectory() as tmp:
                tmp_dir = Path(tmp)
                member_profiles = [
                    _profile_archive_member(zf, member, tmp_dir) for member in selected
                ]
            row_count = sum(
                int(mp.get("row_count") or 0)
                for mp in member_profiles
                if mp.get("row_count") is not None
            )
            archive_profile = {
                "member_count": len(members),
                "members": members[: options.max_archive_members],
                "uncompressed_size_bytes": sum(info.file_size for info in infos),
                "tabular_members": member_profiles,
            }
            result: dict[str, Any] = {
                "archive_profile": archive_profile,
                "row_count": row_count if member_profiles else None,
            }
            if warnings:
                result["profile_status"] = "partial"
                result["profile_warnings"] = warnings
            return result
    except Exception as exc:
        return {
            "profile_status": "partial",
            "profile_warnings": [
                warning("unsupported_format", f"Archive profile failed: {type(exc).__name__}.")
            ],
        }


def _profile_path(path: Path, *, filename: str) -> dict[str, Any]:
    suffix = Path(filename).suffix.lower()
    if suffix in TABULAR_SUFFIXES:
        return _profile_delimited(path, suffix)
    if suffix in EXCEL_SUFFIXES:
        return _profile_excel(path)
    if suffix in JSON_SUFFIXES:
        return _profile_json(path)
    if suffix in XML_SUFFIXES:
        return _profile_xml(path)
    if suffix in PDF_SUFFIXES | GEOSPATIAL_SUFFIXES:
        return {}
    return {
        "profile_status": "skipped",
        "profile_warnings": [
            warning("unsupported_format", "No parser is available for this format.")
        ],
    }


def profile_downloaded_file(
    path: Path,
    *,
    source_url: str,
    filename: str | None = None,
    content_type: str | None = None,
    last_modified: str | None = None,
    options: ProfileOptions | None = None,
) -> dict[str, Any]:
    opts = options or ProfileOptions()
    name = filename or path.name or filename_from_url(source_url)
    size_bytes, sha256 = _hash_file(path)
    result: dict[str, Any] = {
        "size_bytes": size_bytes,
        "sha256": sha256,
        "content_type": content_type,
        "format": _format_from_filename(name),
        "last_modified": last_modified,
        "profiled_at": now_iso(),
        "profile_status": "ok",
        "profile_warnings": [],
    }
    if Path(name).suffix.lower() in ARCHIVE_SUFFIXES:
        file_profile = _profile_archive(path, opts)
    else:
        file_profile = _profile_path(path, filename=name)
    result.update({k: v for k, v in file_profile.items() if v is not None})
    if "profile_status" not in file_profile:
        result["profile_status"] = "ok"
    if "profile_warnings" not in file_profile:
        result["profile_warnings"] = []
    return result


def _profile_response_body(
    response: Any,
    *,
    source_url: str,
    filename: str,
    options: ProfileOptions,
    logger: Any = None,
) -> dict[str, Any]:
    tmp_path: Path | None = None
    try:
        content_type = response.headers.get("Content-Type")
        last_modified = response.headers.get("Last-Modified")
        with tempfile.NamedTemporaryFile(
            prefix="forest-profile-",
            suffix=Path(filename).suffix,
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                tmp.write(chunk)
        profile = profile_downloaded_file(
            tmp_path,
            source_url=source_url,
            filename=filename,
            content_type=content_type,
            last_modified=last_modified,
            options=options,
        )
        if logger:
            logger.info(
                "Profile result: %s bytes=%s status=%s",
                source_url,
                profile.get("size_bytes"),
                profile.get("profile_status"),
            )
        return profile
    finally:
        if tmp_path and not options.keep_local:
            tmp_path.unlink(missing_ok=True)


def profile_source_url(
    source_url: str,
    *,
    filename: str | None = None,
    logger: Any = None,
    options: ProfileOptions | None = None,
    freshness_signal: FreshnessSignal | None = None,
) -> dict[str, Any]:
    cached = _profile_cache_hit(source_url)
    opts = options or ProfileOptions()
    name = filename or filename_from_url(source_url)

    if cached is not None and freshness_signal is not None:
        if _source_signal_allows_cache(cached, freshness_signal):
            if logger:
                logger.info(
                    "Profile cache fresh: %s method=%s raw=%s",
                    source_url,
                    freshness_signal.method,
                    freshness_signal.raw_label,
                )
            return cached
        if logger:
            logger.info(
                "Profile cache stale: %s method=%s raw=%s",
                source_url,
                freshness_signal.method,
                freshness_signal.raw_label,
            )

    headers: dict[str, str] = {}
    if cached is not None and freshness_signal is None:
        last_modified = cached.get("last_modified")
        if isinstance(last_modified, str) and last_modified.strip():
            headers["If-Modified-Since"] = last_modified.strip()

    try:
        if logger:
            logger.info("Profiling source URL: %s", source_url)
        with requests.get(
            source_url,
            stream=True,
            timeout=opts.timeout_s,
            headers=headers or None,
        ) as response:
            if cached is not None and freshness_signal is None:
                if getattr(response, "status_code", None) == 304:
                    if logger:
                        logger.info("Profile cache fresh by HTTP 304: %s", source_url)
                    return cached
                if _http_headers_allow_cache(cached, response.headers):
                    if logger:
                        logger.info("Profile cache fresh by HTTP headers: %s", source_url)
                    return cached
            response.raise_for_status()
            return _profile_response_body(
                response,
                source_url=source_url,
                filename=name,
                options=opts,
                logger=logger,
            )
    except AssertionError:
        raise
    except Exception as exc:
        if logger:
            logger.warning("Profile failed for %s: %s", source_url, exc)
        return {
            "profiled_at": now_iso(),
            "profile_status": "failed",
            "profile_warnings": [
                warning("download_timeout", f"Profiling failed: {type(exc).__name__}.")
            ],
        }


def profiled_item(
    *,
    source_url: str,
    filename: str | None = None,
    period: str = "current",
    title: str | None = None,
    kind: str = "data",
    release_time: str | None = None,
    logger: Any = None,
    options: ProfileOptions | None = None,
    freshness_signal: FreshnessSignal | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    name = filename or filename_from_url(source_url)
    item: dict[str, Any] = {
        "kind": kind,
        "period": period,
        "filename": name,
        "source_url": source_url,
    }
    if title:
        item["title"] = title
    if release_time:
        item["release_time"] = release_time
    if extra:
        item.update(extra)
    item.update(
        profile_source_url(
            source_url=source_url,
            filename=name,
            logger=logger,
            options=options,
            freshness_signal=freshness_signal,
        )
    )
    return item
