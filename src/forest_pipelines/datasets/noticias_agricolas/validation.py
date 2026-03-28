# src/forest_pipelines/datasets/noticias_agricolas/validation.py
from __future__ import annotations

from typing import Any


def validate_feed_item(item: dict[str, Any]) -> list[str]:
    """Return list of validation error messages (empty if OK)."""
    errs: list[str] = []
    if not (item.get("title") or "").strip():
        errs.append("title vazio")
    if not (item.get("url") or "").strip():
        errs.append("url vazio")
    if not (item.get("category_slug") or "").strip():
        errs.append("category_slug vazio")
    pub = item.get("published_at")
    if not pub or not str(pub).strip():
        errs.append("published_at vazio")
    return errs


def validate_feed_for_stable_publish(
    items: list[dict[str, Any]],
    *,
    min_items: int,
) -> list[str]:
    """
    Return human-readable reasons to block stable publish (empty list means OK).
    """
    reasons: list[str] = []
    if len(items) < min_items:
        reasons.append(
            f"quantidade de itens ({len(items)}) abaixo do mínimo configurado ({min_items})"
        )
    for i, it in enumerate(items):
        e = validate_feed_item(it)
        if e:
            reasons.append(f"item[{i}]: " + "; ".join(e))
    return reasons
