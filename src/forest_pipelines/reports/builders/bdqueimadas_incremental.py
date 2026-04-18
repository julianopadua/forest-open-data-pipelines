# src/forest_pipelines/reports/builders/bdqueimadas_incremental.py
from __future__ import annotations

import csv
import hashlib
import json
import re
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

RE_YEAR = re.compile(r"(\d{4})")

CACHE_SCHEMA_VERSION = 2
ALL_BIOMES_VALUE = "__all__"

BIOME_LABELS: dict[str, dict[str, str]] = {
    "AMAZÔNIA": {"pt": "Amazônia", "en": "Amazon"},
    "CERRADO": {"pt": "Cerrado", "en": "Cerrado"},
    "CAATINGA": {"pt": "Caatinga", "en": "Caatinga"},
    "MATA ATLÂNTICA": {"pt": "Mata Atlântica", "en": "Atlantic Forest"},
    "PAMPA": {"pt": "Pampa", "en": "Pampa"},
    "PANTANAL": {"pt": "Pantanal", "en": "Pantanal"},
}


def build_incremental_year_caches(
    storage: Any,
    cache_prefix: str,
    zip_files: list[Path],
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
    logger: Any,
) -> dict[str, Any]:
    cache_prefix = cache_prefix.rstrip("/")
    manifest_path = f"{cache_prefix}/incremental_manifest.json"
    build_signature = _build_signature(
        datetime_candidates=datetime_candidates,
        state_candidates=state_candidates,
        biome_candidates=biome_candidates,
    )

    manifest = _download_json(storage, manifest_path, logger)
    manifest_files = _extract_manifest_files(
        manifest=manifest,
        build_signature=build_signature,
    )

    year_payloads: list[dict[str, Any]] = []
    new_manifest_files: dict[str, Any] = {}

    reused_count = 0
    rebuilt_count = 0

    for zip_path in sorted(zip_files):
        inferred_year = _extract_year_from_name(zip_path.name)
        if inferred_year is None:
            logger.warning("Arquivo ignorado sem ano inferível no nome: %s", zip_path.name)
            continue

        fingerprint = _build_source_fingerprint(zip_path)
        cache_object_path = f"{cache_prefix}/yearly/{inferred_year}.json"
        cached_entry = manifest_files.get(zip_path.name, {})

        payload: dict[str, Any] | None = None

        if (
            cached_entry.get("fingerprint") == fingerprint
            and cached_entry.get("build_signature") == build_signature
            and cached_entry.get("cache_object_path") == cache_object_path
        ):
            cached_payload = _download_json(storage, cache_object_path, logger)
            if _is_valid_year_payload(
                payload=cached_payload,
                inferred_year=inferred_year,
                fingerprint=fingerprint,
                build_signature=build_signature,
            ):
                payload = cached_payload

        if payload is None:
            logger.info("Reprocessando agregado anual: %s", zip_path.name)

            payload = _build_year_payload(
                zip_path=zip_path,
                datetime_candidates=datetime_candidates,
                state_candidates=state_candidates,
                biome_candidates=biome_candidates,
            )
            payload["cache_schema_version"] = CACHE_SCHEMA_VERSION
            payload["build_signature"] = build_signature
            payload["fingerprint"] = fingerprint

            storage.upload_bytes(
                object_path=cache_object_path,
                data=_to_bytes(payload),
                content_type="application/json",
                upsert=True,
            )
            rebuilt_count += 1
        else:
            logger.info("Reutilizando agregado anual em cache: %s", zip_path.name)
            reused_count += 1

        year_payloads.append(payload)
        new_manifest_files[zip_path.name] = {
            "year": inferred_year,
            "cache_object_path": cache_object_path,
            "build_signature": build_signature,
            "fingerprint": fingerprint,
            "row_count": int(payload.get("row_count", 0)),
            "processed_at": payload.get("processed_at"),
        }

    new_manifest = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "build_signature": build_signature,
        "cache_prefix": cache_prefix,
        "files": new_manifest_files,
        "stats": {
            "files_total": len(new_manifest_files),
            "reused_count": reused_count,
            "rebuilt_count": rebuilt_count,
        },
    }

    storage.upload_bytes(
        object_path=manifest_path,
        data=_to_bytes(new_manifest),
        content_type="application/json",
        upsert=True,
    )

    return {
        "year_payloads": sorted(
            year_payloads,
            key=lambda item: int(item.get("inferred_year", 0)),
        ),
        "cache_stats": {
            "cache_prefix": cache_prefix,
            "files_total": len(new_manifest_files),
            "reused_count": reused_count,
            "rebuilt_count": rebuilt_count,
        },
    }


def consolidate_year_payloads(
    year_payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    monthly_all_df = _merge_sum_frames(
        [pd.DataFrame(item.get("monthly_all", [])) for item in year_payloads],
        key_cols=["period", "year"],
    )
    monthly_by_biome_df = _merge_sum_frames(
        [pd.DataFrame(item.get("monthly_by_biome", [])) for item in year_payloads],
        key_cols=["period", "year", "biome"],
    )
    annual_all_df = _merge_sum_frames(
        [pd.DataFrame(item.get("annual_all", [])) for item in year_payloads],
        key_cols=["year"],
    )
    annual_by_biome_df = _merge_sum_frames(
        [pd.DataFrame(item.get("annual_by_biome", [])) for item in year_payloads],
        key_cols=["year", "biome"],
    )
    state_year_all_df = _merge_sum_frames(
        [pd.DataFrame(item.get("state_year_all", [])) for item in year_payloads],
        key_cols=["year", "state"],
    )
    state_year_by_biome_df = _merge_sum_frames(
        [pd.DataFrame(item.get("state_year_by_biome", [])) for item in year_payloads],
        key_cols=["year", "state", "biome"],
    )

    available_biomes = sorted(
        {
            str(biome).strip()
            for item in year_payloads
            for biome in item.get("available_biomes", [])
            if str(biome).strip()
        }
    )

    yearly_file_stats = [
        {
            "file_name": item.get("file_name"),
            "file_size_bytes": int(item.get("file_size_bytes", 0)),
            "inferred_year": int(item.get("inferred_year", 0)),
            "row_count": int(item.get("row_count", 0)),
            "month_span_min": item.get("month_span_min"),
            "month_span_max": item.get("month_span_max"),
            "detected_datetime_column": item.get("detected_datetime_column"),
            "detected_state_column": item.get("detected_state_column"),
            "detected_biome_column": item.get("detected_biome_column"),
            "available_biomes": item.get("available_biomes", []),
        }
        for item in sorted(year_payloads, key=lambda x: int(x.get("inferred_year", 0)))
    ]

    total_rows_processed = int(
        sum(int(item.get("row_count", 0)) for item in year_payloads)
    )

    return {
        "monthly_all_df": monthly_all_df.sort_values(["period"]).reset_index(drop=True),
        "monthly_by_biome_df": monthly_by_biome_df.sort_values(["period", "biome"]).reset_index(drop=True),
        "annual_all_df": annual_all_df.sort_values(["year"]).reset_index(drop=True),
        "annual_by_biome_df": annual_by_biome_df.sort_values(["year", "biome"]).reset_index(drop=True),
        "state_year_all_df": state_year_all_df.sort_values(["year", "state"]).reset_index(drop=True),
        "state_year_by_biome_df": state_year_by_biome_df.sort_values(["year", "state", "biome"]).reset_index(drop=True),
        "available_biomes": available_biomes,
        "yearly_file_stats": yearly_file_stats,
        "total_rows_processed": total_rows_processed,
    }


def combine_all_and_biome_records(
    all_df: pd.DataFrame,
    by_biome_df: pd.DataFrame,
    sort_cols: list[str],
) -> list[dict[str, Any]]:
    frames: list[pd.DataFrame] = []

    if not all_df.empty:
        all_part = all_df.copy()
        all_part["biome"] = ALL_BIOMES_VALUE
        frames.append(all_part)

    if not by_biome_df.empty:
        frames.append(by_biome_df.copy())

    if not frames:
        return []

    merged = pd.concat(frames, ignore_index=True)
    effective_sort_cols = [col for col in [*sort_cols, "biome"] if col in merged.columns]
    if effective_sort_cols:
        merged = merged.sort_values(effective_sort_cols).reset_index(drop=True)

    return _df_to_records(merged)


def biome_label_i18n(biome: str) -> dict[str, str]:
    key = str(biome).strip().upper()
    labels = BIOME_LABELS.get(key)
    if labels:
        return labels
    return {
        "pt": str(biome).strip(),
        "en": str(biome).strip(),
    }


def _extract_manifest_files(
    manifest: dict[str, Any] | None,
    build_signature: str,
) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        return {}

    if manifest.get("cache_schema_version") != CACHE_SCHEMA_VERSION:
        return {}

    if manifest.get("build_signature") != build_signature:
        return {}

    files = manifest.get("files")
    return files if isinstance(files, dict) else {}


def _build_signature(
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> str:
    payload = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "datetime_candidates": datetime_candidates,
        "state_candidates": state_candidates,
        "biome_candidates": biome_candidates,
        "aggregations": [
            "monthly_all",
            "monthly_by_biome",
            "annual_all",
            "annual_by_biome",
            "state_year_all",
            "state_year_by_biome",
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _build_source_fingerprint(zip_path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(zip_path) as zf:
        member = _pick_member(zf)
        info = zf.getinfo(member)

    return {
        "zip_name": zip_path.name,
        "zip_size_bytes": int(zip_path.stat().st_size),
        "member_name": member,
        "member_crc": int(info.CRC),
        "member_file_size": int(info.file_size),
        "member_compress_size": int(info.compress_size),
    }


def _is_valid_year_payload(
    payload: dict[str, Any] | None,
    inferred_year: int,
    fingerprint: dict[str, Any],
    build_signature: str,
) -> bool:
    if not isinstance(payload, dict):
        return False

    required_keys = {
        "monthly_all",
        "monthly_by_biome",
        "annual_all",
        "annual_by_biome",
        "state_year_all",
        "state_year_by_biome",
    }

    if payload.get("cache_schema_version") != CACHE_SCHEMA_VERSION:
        return False

    if payload.get("build_signature") != build_signature:
        return False

    if payload.get("fingerprint") != fingerprint:
        return False

    if int(payload.get("inferred_year", 0)) != inferred_year:
        return False

    return all(key in payload for key in required_keys)


def _build_year_payload(
    zip_path: Path,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> dict[str, Any]:
    subset, detected_columns = _read_zip_subset(
        zip_path=zip_path,
        datetime_candidates=datetime_candidates,
        state_candidates=state_candidates,
        biome_candidates=biome_candidates,
    )

    if subset.empty:
        return {
            "file_name": zip_path.name,
            "file_size_bytes": int(zip_path.stat().st_size),
            "inferred_year": _extract_year_from_name(zip_path.name),
            "row_count": 0,
            "month_span_min": None,
            "month_span_max": None,
            "detected_datetime_column": detected_columns["datetime"],
            "detected_state_column": detected_columns["state"],
            "detected_biome_column": detected_columns["biome"],
            "available_biomes": [],
            "monthly_all": [],
            "monthly_by_biome": [],
            "annual_all": [],
            "annual_by_biome": [],
            "state_year_all": [],
            "state_year_by_biome": [],
            "processed_at": _now_iso(),
        }

    monthly_all = (
        subset.groupby(["period_month", "year"])
        .size()
        .rename("value")
        .reset_index()
        .rename(columns={"period_month": "period"})
    )
    monthly_all["period"] = monthly_all["period"].astype(str)

    monthly_by_biome = (
        subset.dropna(subset=["biome"])
        .groupby(["period_month", "year", "biome"])
        .size()
        .rename("value")
        .reset_index()
        .rename(columns={"period_month": "period"})
    )
    monthly_by_biome["period"] = monthly_by_biome["period"].astype(str)

    annual_all = (
        subset.groupby(["year"])
        .size()
        .rename("value")
        .reset_index()
    )

    annual_by_biome = (
        subset.dropna(subset=["biome"])
        .groupby(["year", "biome"])
        .size()
        .rename("value")
        .reset_index()
    )

    state_year_all = (
        subset.dropna(subset=["state"])
        .groupby(["year", "state"])
        .size()
        .rename("value")
        .reset_index()
    )

    state_year_by_biome = (
        subset.dropna(subset=["state", "biome"])
        .groupby(["year", "state", "biome"])
        .size()
        .rename("value")
        .reset_index()
    )

    available_biomes = sorted(
        {
            str(biome).strip()
            for biome in subset["biome"].dropna().unique().tolist()
            if str(biome).strip()
        }
    )

    return {
        "file_name": zip_path.name,
        "file_size_bytes": int(zip_path.stat().st_size),
        "inferred_year": _extract_year_from_name(zip_path.name),
        "row_count": int(len(subset)),
        "month_span_min": str(subset["period_month"].min()),
        "month_span_max": str(subset["period_month"].max()),
        "detected_datetime_column": detected_columns["datetime"],
        "detected_state_column": detected_columns["state"],
        "detected_biome_column": detected_columns["biome"],
        "available_biomes": available_biomes,
        "monthly_all": _df_to_records(monthly_all),
        "monthly_by_biome": _df_to_records(monthly_by_biome),
        "annual_all": _df_to_records(annual_all),
        "annual_by_biome": _df_to_records(annual_by_biome),
        "state_year_all": _df_to_records(state_year_all),
        "state_year_by_biome": _df_to_records(state_year_by_biome),
        "processed_at": _now_iso(),
    }


def _read_zip_subset(
    zip_path: Path,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> tuple[pd.DataFrame, dict[str, str]]:
    with zipfile.ZipFile(zip_path) as zf:
        member = _pick_member(zf)
        delimiter = _detect_delimiter(zf, member)
        columns = _detect_columns(
            zf=zf,
            member=member,
            datetime_candidates=datetime_candidates,
            state_candidates=state_candidates,
            biome_candidates=biome_candidates,
            delimiter=delimiter,
        )

        dt_col = columns["datetime"]
        state_col = columns["state"]
        biome_col = columns["biome"]

        df = _read_member_csv(
            zf=zf,
            member=member,
            delimiter=delimiter,
            usecols=[dt_col, state_col, biome_col],
        )

    df = df.rename(
        columns={
            dt_col: "raw_datetime",
            state_col: "raw_state",
            biome_col: "raw_biome",
        }
    ).copy()

    out = _normalized_focos_subset_from_raw_columns(df)
    return out, {
        "datetime": dt_col,
        "state": state_col,
        "biome": biome_col,
    }


def _normalized_focos_subset_from_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(
        df["raw_datetime"].astype("string").str.strip(),
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )

    state = (
        df["raw_state"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    )

    biome = (
        df["raw_biome"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    )

    out = pd.DataFrame(
        {
            "datetime": dt,
            "state": state,
            "biome": biome,
        }
    ).dropna(subset=["datetime"])

    out["year"] = out["datetime"].dt.year.astype(int)
    out["period_month"] = out["datetime"].dt.to_period("M").astype(str)

    return out[["datetime", "year", "period_month", "state", "biome"]]


def _pick_member(zf: zipfile.ZipFile) -> str:
    members = [
        name
        for name in zf.namelist()
        if not name.endswith("/") and Path(name).suffix.lower() in {".csv", ".txt"}
    ]
    if not members:
        raise FileNotFoundError("ZIP sem arquivo CSV/TXT legível.")
    members.sort()
    return members[0]


def _detect_delimiter(zf: zipfile.ZipFile, member: str) -> str:
    with zf.open(member) as f:
        sample = f.read(4096).decode("utf-8", errors="ignore")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def detect_columns_from_header(
    available: list[str],
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> dict[str, str]:
    dt_col = _pick_column(available, datetime_candidates)
    state_col = _pick_column(available, state_candidates)
    biome_col = _pick_column(available, biome_candidates)

    if dt_col is None:
        raise KeyError(
            f"Não foi possível identificar a coluna temporal. "
            f"Candidatas testadas: {datetime_candidates}. "
            f"Colunas disponíveis: {available}"
        )

    if state_col is None:
        raise KeyError(
            f"Não foi possível identificar a coluna de UF/estado. "
            f"Candidatas testadas: {state_candidates}. "
            f"Colunas disponíveis: {available}"
        )

    if biome_col is None:
        raise KeyError(
            f"Não foi possível identificar a coluna de bioma. "
            f"Candidatas testadas: {biome_candidates}. "
            f"Colunas disponíveis: {available}"
        )

    return {
        "datetime": dt_col,
        "state": state_col,
        "biome": biome_col,
    }


def _detect_columns(
    zf: zipfile.ZipFile,
    member: str,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
    delimiter: str,
) -> dict[str, str]:
    header_df = _read_member_csv(
        zf=zf,
        member=member,
        delimiter=delimiter,
        nrows=0,
    )
    return detect_columns_from_header(
        list(header_df.columns),
        datetime_candidates,
        state_candidates,
        biome_candidates,
    )


def _pick_column(available: list[str], candidates: list[str]) -> str | None:
    normalized_map = {_normalize(col): col for col in available}
    for candidate in candidates:
        norm = _normalize(candidate)
        if norm in normalized_map:
            return normalized_map[norm]
    return None


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.casefold())


def _read_member_csv(
    zf: zipfile.ZipFile,
    member: str,
    delimiter: str,
    usecols: list[str] | None = None,
    nrows: int | None = None,
) -> pd.DataFrame:
    encodings = ["utf-8", "latin-1", "cp1252"]

    last_error: Exception | None = None
    for encoding in encodings:
        try:
            with zf.open(member) as f:
                return pd.read_csv(
                    f,
                    sep=delimiter,
                    encoding=encoding,
                    usecols=usecols,
                    nrows=nrows,
                    dtype="string",
                    low_memory=False,
                    on_bad_lines="skip",
                )
        except Exception as e:  # noqa: BLE001
            last_error = e
            continue

    raise RuntimeError(f"Falha ao ler {member} com encodings suportados.") from last_error


def _detect_delimiter_path(path: Path) -> str:
    with open(path, "rb") as f:
        sample = f.read(4096).decode("utf-8", errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def _read_path_csv(
    path: Path,
    delimiter: str,
    usecols: list[str] | None = None,
    nrows: int | None = None,
) -> pd.DataFrame:
    encodings = ["utf-8", "latin-1", "cp1252"]
    last_error: Exception | None = None
    for encoding in encodings:
        try:
            return pd.read_csv(
                path,
                sep=delimiter,
                encoding=encoding,
                usecols=usecols,
                nrows=nrows,
                dtype="string",
                low_memory=False,
                on_bad_lines="skip",
            )
        except Exception as e:  # noqa: BLE001
            last_error = e
            continue
    raise RuntimeError(f"Falha ao ler {path} com encodings suportados.") from last_error


def read_focos_subset_brasil_file(
    path: Path,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> pd.DataFrame:
    """Lê um CSV (ou ZIP com um CSV interno) no mesmo formato dos focos INPE; retorna subset com datetime válido."""
    suf = path.suffix.lower()
    if suf == ".zip":
        with zipfile.ZipFile(path) as zf:
            member = _pick_member(zf)
            delimiter = _detect_delimiter(zf, member)
            columns = _detect_columns(
                zf=zf,
                member=member,
                datetime_candidates=datetime_candidates,
                state_candidates=state_candidates,
                biome_candidates=biome_candidates,
                delimiter=delimiter,
            )
            dt_col = columns["datetime"]
            state_col = columns["state"]
            biome_col = columns["biome"]
            df = _read_member_csv(
                zf=zf,
                member=member,
                delimiter=delimiter,
                usecols=[dt_col, state_col, biome_col],
            )
    elif suf == ".csv":
        delimiter = _detect_delimiter_path(path)
        header_df = _read_path_csv(path, delimiter, nrows=0)
        columns = detect_columns_from_header(
            list(header_df.columns),
            datetime_candidates,
            state_candidates,
            biome_candidates,
        )
        dt_col = columns["datetime"]
        state_col = columns["state"]
        biome_col = columns["biome"]
        df = _read_path_csv(
            path,
            delimiter,
            usecols=[dt_col, state_col, biome_col],
        )
    else:
        raise ValueError(f"Extensão não suportada para focos mensais: {path}")

    df = df.rename(
        columns={
            dt_col: "raw_datetime",
            state_col: "raw_state",
            biome_col: "raw_biome",
        }
    ).copy()
    return _normalized_focos_subset_from_raw_columns(df)


def count_focos_rows_brasil_file(
    path: Path,
    datetime_candidates: list[str],
    state_candidates: list[str],
    biome_candidates: list[str],
) -> int:
    """Número de focos (linhas válidas) alinhado ao agregado nacional do report."""
    subset = read_focos_subset_brasil_file(
        path,
        datetime_candidates,
        state_candidates,
        biome_candidates,
    )
    return int(len(subset))


def _extract_year_from_name(filename: str) -> int | None:
    match = RE_YEAR.search(filename)
    if not match:
        return None
    return int(match.group(1))


def _merge_sum_frames(frames: list[pd.DataFrame], key_cols: list[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=[*key_cols, "value"])

    merged = pd.concat(non_empty, ignore_index=True)
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce").fillna(0).astype(int)

    return (
        merged.groupby(key_cols, as_index=False)["value"]
        .sum()
        .sort_values(key_cols)
        .reset_index(drop=True)
    )


def _download_json(
    storage: Any,
    object_path: str,
    logger: Any,
) -> dict[str, Any] | None:
    data = storage.download_bytes(object_path)
    if not data:
        return None

    try:
        parsed = json.loads(data.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception as e:  # noqa: BLE001
        logger.warning("Falha ao decodificar JSON de cache em %s. erro=%s", object_path, e)
        return None


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        clean: dict[str, Any] = {}
        for key, value in row.items():
            if pd.isna(value):
                clean[key] = None
            elif isinstance(value, pd.Timestamp):
                clean[key] = value.isoformat()
            elif hasattr(value, "item"):
                clean[key] = value.item()
            else:
                clean[key] = value
        out.append(clean)
    return out


def _to_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")