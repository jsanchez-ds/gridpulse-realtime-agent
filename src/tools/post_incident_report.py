"""post_incident_report: Discord webhook + Delta persistence."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from pyspark.sql import SparkSession

from src.utils.config import get_env, load_yaml_config
from src.utils.logging import get_logger

log = get_logger(__name__)

SEVERITY_COLOR = {
    "info": 0x3498DB,       # blue
    "warning": 0xE67E22,    # orange
    "critical": 0xE74C3C,   # red
}

SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


def _post_discord(webhook: str, severity: str, title: str, body: str) -> dict:
    """POST a Discord webhook in 'embed' format — looks like a rich alert card."""
    payload = {
        "username": "GridPulse",
        "embeds": [
            {
                "title": f"[{severity.upper()}] {title}",
                "description": body[:3800],       # Discord embed cap
                "color": SEVERITY_COLOR.get(severity, 0x95A5A6),
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "footer": {"text": "gridpulse-agent"},
            }
        ],
    }
    try:
        r = httpx.post(webhook, json=payload, timeout=10)
        return {"status": r.status_code, "ok": r.is_success}
    except Exception as exc:
        return {"status": 0, "ok": False, "error": str(exc)}


def _persist_delta(record: dict) -> str:
    """Append the report to data/delta/incidents. Returns the path."""
    env = get_env()
    path = Path(env.delta_path) / "incidents"
    path.mkdir(parents=True, exist_ok=True)

    # Small table — use pandas + deltalake for simplicity (no Spark needed).
    from deltalake import write_deltalake

    df = pd.DataFrame([record])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    write_deltalake(str(path), df, mode="append", schema_mode="merge")
    return str(path)


def post_incident_report(severity: str, title: str, body: str) -> dict:
    env = get_env()
    cfg = load_yaml_config()

    suppress_below = cfg.get("tools", {}).get("post_incident_report", {}).get(
        "suppress_below_severity", "warning"
    )
    if SEVERITY_RANK.get(severity, 0) < SEVERITY_RANK.get(suppress_below, 1):
        log.info("incident.suppressed", severity=severity, reason="below threshold")
        discord_status = {"skipped": True, "reason": f"severity below {suppress_below}"}
    elif not env.discord_webhook_url:
        discord_status = {"skipped": True, "reason": "DISCORD_WEBHOOK_URL not set"}
    else:
        discord_status = _post_discord(env.discord_webhook_url, severity, title, body)

    record = {
        "incident_id": str(uuid.uuid4()),
        "timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
        "severity": severity,
        "title": title,
        "body": body,
        "discord_status": json.dumps(discord_status),
    }
    try:
        path = _persist_delta(record)
    except Exception as exc:
        log.warning("incident.delta.failed", error=str(exc))
        path = ""

    return {
        "persisted": bool(path),
        "delta_path": path,
        "discord": discord_status,
        "incident_id": record["incident_id"],
    }
