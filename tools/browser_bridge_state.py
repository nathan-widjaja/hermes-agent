#!/usr/bin/env python3
"""Helpers for persisted browser bridge state shared across Hermes runtimes."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)


def get_browser_bridge_state_path() -> Path:
    """Return the persisted browser bridge state file path."""
    return Path(get_hermes_home()) / "state" / "host-bridge" / "state.json"


def load_browser_bridge_state() -> Dict[str, Any]:
    """Load persisted browser bridge state, returning an empty dict on failure."""
    path = get_browser_bridge_state_path()
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("Failed to read browser bridge state from %s: %s", path, exc)
        return {}

    if isinstance(payload, dict):
        return payload

    logger.warning("Browser bridge state at %s was not a JSON object", path)
    return {}


def save_browser_bridge_state(payload: Dict[str, Any]) -> Path:
    """Persist browser bridge state atomically."""
    path = get_browser_bridge_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp_path.replace(path)
    return path


def merge_browser_bridge_state(updates: Dict[str, Any]) -> Dict[str, Any]:
    """Merge updates into persisted browser bridge state and return the result."""
    current = load_browser_bridge_state()
    current.update(updates)
    save_browser_bridge_state(current)
    return current


def clear_browser_bridge_state() -> bool:
    """Delete persisted browser bridge state if it exists."""
    path = get_browser_bridge_state_path()
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def get_browser_bridge_cdp_url() -> str:
    """Return the persisted websocket/browser endpoint, if any."""
    payload = load_browser_bridge_state()
    for key in ("websocket_url", "cdp_url"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""
