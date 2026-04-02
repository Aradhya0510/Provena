"""Deterministic hashing for entity resolution."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def entity_hash(entity_type: str, identifier: dict[str, Any]) -> str:
    """Create a deterministic hash for entity resolution across sources."""
    sorted_id = json.dumps(identifier, sort_keys=True, default=str)
    raw = f"{entity_type}::{sorted_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
