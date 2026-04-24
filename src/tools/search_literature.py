"""search_literature: call the EnergyScholar RAG (Project 2) over HTTP."""

from __future__ import annotations

import httpx

from src.utils.config import get_env, load_yaml_config
from src.utils.logging import get_logger

log = get_logger(__name__)


def search_literature(query: str) -> dict:
    env = get_env()
    cfg = load_yaml_config()
    timeout = cfg.get("tools", {}).get("search_literature", {}).get("energyscholar_timeout_seconds", 60)

    url = f"{env.energyscholar_url.rstrip('/')}/query"
    try:
        r = httpx.post(url, json={"question": query}, timeout=timeout)
    except Exception as exc:
        return {"available": False, "reason": f"EnergyScholar unreachable at {url}: {exc!s}"}

    if r.status_code != 200:
        return {
            "available": False,
            "reason": f"EnergyScholar {r.status_code}: {r.text[:300]}",
        }

    payload = r.json()
    return {
        "available": True,
        "answer": payload.get("answer", ""),
        "citations": payload.get("citations", []),
        "model": payload.get("model", ""),
        "provider": payload.get("provider", ""),
        "n_context_chunks": payload.get("n_context_chunks", 0),
    }
