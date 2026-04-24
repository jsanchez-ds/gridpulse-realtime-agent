"""get_current_load: read the most recent scored event from Delta."""

from __future__ import annotations

from pathlib import Path

from deltalake import DeltaTable

from src.utils.config import get_env
from src.utils.logging import get_logger

log = get_logger(__name__)


def get_current_load(region: str) -> dict:
    env = get_env()
    path = Path(env.delta_path) / "events_scored"
    if not path.exists():
        return {"available": False, "reason": "No streamed events persisted yet"}

    try:
        df = DeltaTable(str(path)).to_pandas()
    except Exception as exc:
        return {"available": False, "reason": f"Delta read failed: {exc!s}"}

    df = df[df["region"] == region]
    if df.empty:
        return {"available": False, "reason": f"No rows for region {region}"}

    df = df.sort_values("timestamp_utc")
    latest = df.iloc[-1]
    return {
        "available": True,
        "region": region,
        "timestamp_utc": str(latest["timestamp_utc"]),
        "load_mw": float(latest["load_mw"]),
        "anomaly_score": float(latest.get("anomaly_score", float("nan"))),
        "is_anomaly": bool(latest.get("is_anomaly", 0)),
    }
