"""Payload helpers for ANP petroleum and natural gas social text."""

from __future__ import annotations

import json
from typing import Any


def payload_to_prompt_block(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)
