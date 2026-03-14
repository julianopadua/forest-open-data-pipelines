# src/forest_pipelines/audits/utils.py
from __future__ import annotations

import csv
import json
import re
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

RE_YEAR = re.compile(r"(\d{4})")


def extract_year_from_name(filename: str) -> int | None:
    match = RE_YEAR.search(filename)
    if not match:
        return None
    return int(match.group(1))


def pick_archive_member(zf: zipfile.ZipFile) -> str:
    members = [
        name
        for name in zf.namelist()
        if not name.endswith("/") and Path(name).suffix.lower() in {".csv", ".txt"}
    ]
    if not members:
        raise FileNotFoundError("ZIP sem arquivo CSV/TXT legível.")
    members.sort()
    return members[0]


def detect_delimiter(zf: zipfile.ZipFile, member: str) -> str:
    with zf.open(member) as f:
        sample = f.read(8192).decode("utf-8", errors="ignore")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def read_member_csv(
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


def read_header_columns(
    zf: zipfile.ZipFile,
    member: str,
    delimiter: str,
) -> list[str]:
    df = read_member_csv(
        zf=zf,
        member=member,
        delimiter=delimiter,
        nrows=0,
    )
    return list(df.columns)


def read_sample(
    zf: zipfile.ZipFile,
    member: str,
    delimiter: str,
    sample_rows: int,
) -> pd.DataFrame:
    return read_member_csv(
        zf=zf,
        member=member,
        delimiter=delimiter,
        nrows=sample_rows,
    )


def count_member_rows(zf: zipfile.ZipFile, member: str) -> int:
    with zf.open(member) as f:
        total_lines = sum(1 for _ in f)
    return max(total_lines - 1, 0)


def normalize_column_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.casefold())


def pick_best_column(available: list[str], candidates: list[str]) -> str | None:
    normalized_map = {normalize_column_name(col): col for col in available}
    for candidate in candidates:
        normalized = normalize_column_name(candidate)
        if normalized in normalized_map:
            return normalized_map[normalized]
    return None


def infer_series_kind(series: pd.Series) -> str:
    s = series.dropna().astype("string").str.strip()
    s = s[s != ""]
    if s.empty:
        return "empty"

    numeric = pd.to_numeric(s, errors="coerce")
    numeric_ratio = float(numeric.notna().mean())
    if numeric_ratio >= 0.95:
        int_ratio = float(s.str.fullmatch(r"-?\d+").fillna(False).mean())
        if int_ratio >= 0.95:
            return "int"
        return "float"

    dt = pd.to_datetime(s, errors="coerce", dayfirst=True, format="mixed")
    dt_ratio = float(dt.notna().mean())
    if dt_ratio >= 0.8:
        return "datetime"

    unique_count = int(s.nunique(dropna=True))
    if unique_count <= 5 and len(s) >= 10:
        return "categorical"

    return "string"


def distinct_preview(series: pd.Series, limit: int = 4) -> list[str]:
    s = (
        series.dropna()
        .astype("string")
        .str.strip()
    )
    s = s[s != ""]
    values = list(dict.fromkeys(s.tolist()))
    return values[:limit]


def top_schema_signature(column_lists: list[list[str]]) -> tuple[list[str], int]:
    counter: Counter[str] = Counter()
    cache: dict[str, list[str]] = {}

    for cols in column_lists:
        key = json.dumps(cols, ensure_ascii=False)
        counter[key] += 1
        cache[key] = cols

    if not counter:
        return [], 0

    most_common_key, freq = counter.most_common(1)[0]
    return cache[most_common_key], int(freq)


def safe_pct(part: int, total: int) -> float | None:
    if total == 0:
        return None
    return (part / total) * 100.0


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [json_ready(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    return value
