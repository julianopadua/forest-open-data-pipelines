# src/forest_pipelines/datasets/noticias_agricolas/text_cleanup.py
from __future__ import annotations

import re

_WS_RE = re.compile(r"[ \t\r\f\v]+")
_MULTI_NL = re.compile(r"\n{3,}")

# Paragraphs that look like CTAs or off-topic boilerplate (not article body).
_CTA_LINE = re.compile(
    r"(?i)^(receba|cadastre|whatsapp|newsletter|"
    r"clique aqui para receber|siga nosso canal|termo de privacidade)\b"
)


def normalize_paragraph_text(s: str) -> str:
    s = s.replace("\xa0", " ").strip()
    s = _WS_RE.sub(" ", s)
    return s


def normalize_body_text(lines: list[str]) -> str:
    cleaned: list[str] = []
    for line in lines:
        n = normalize_paragraph_text(line)
        if not n:
            continue
        if _CTA_LINE.search(n):
            continue
        cleaned.append(n)
    body = "\n\n".join(cleaned)
    body = _MULTI_NL.sub("\n\n", body).strip()
    return body


def first_useful_paragraph(content_text: str) -> str | None:
    for block in content_text.split("\n\n"):
        t = normalize_paragraph_text(block)
        if len(t) >= 40:
            return t
        if len(t) >= 15 and not _CTA_LINE.search(t):
            return t
    return None
