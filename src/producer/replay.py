"""
Event producer — replays historical load data to Kafka as if it were live.

Reads the same Gold Delta table that Project 1 builds, iterates rows in
timestamp order, and publishes each row as a JSON event to the configured
Kafka topic. A configurable `speed` knob lets you replay years in minutes
(e.g. speed=3600 means one real second ≈ one simulated hour).
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from deltalake import DeltaTable

from src.utils.config import get_env
from src.utils.logging import configure_logging, get_logger

log = get_logger(__name__)


def _load_rows(source: str, region: str) -> pd.DataFrame:
    """Load (and lightly normalise) rows from the configured Gold source."""
    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"Replay source not found: {source}")

    if (p / "_delta_log").exists():
        log.info("replay.load.delta", path=str(p))
        df = DeltaTable(str(p)).to_pandas()
    else:
        # fall back: assume parquet directory or csv
        log.info("replay.load.other", path=str(p))
        if p.is_dir():
            df = pd.read_parquet(p)
        elif p.suffix.lower() == ".csv":
            df = pd.read_csv(p)
        else:
            df = pd.read_parquet(p)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df[df["country"] == region].sort_values("timestamp_utc").reset_index(drop=True)
    return df


def _make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 10,
            "compression.type": "zstd",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def _delivery_report(err, msg) -> None:
    if err is not None:
        log.warning("kafka.delivery_failed", error=str(err))


def _row_to_event(row: dict, ingest_iso: str) -> dict:
    """Shrink the row to the fields the streaming job actually needs."""
    return {
        "event_id": f"{row['country']}-{pd.Timestamp(row['timestamp_utc']).value}",
        "timestamp_utc": pd.Timestamp(row["timestamp_utc"]).isoformat(),
        "region": row["country"],
        "load_mw": float(row.get("load_mw_clean", row.get("load_mw"))),
        "hour": int(row.get("hour", 0)),
        "day_of_week": int(row.get("day_of_week", 0)),
        "is_weekend": int(row.get("is_weekend", 0)),
        "load_mw_clean_lag_24": float(row.get("load_mw_clean_lag_24", 0.0)),
        "load_mw_clean_lag_168": float(row.get("load_mw_clean_lag_168", 0.0)),
        "load_mw_clean_roll_mean_24": float(row.get("load_mw_clean_roll_mean_24", 0.0)),
        "load_mw_clean_roll_std_24": float(row.get("load_mw_clean_roll_std_24", 0.0)),
        "ingested_at": ingest_iso,
    }


def run(source: str, region: str, topic: str, speed: float, limit: int | None = None) -> None:
    env = get_env()
    df = _load_rows(source, region)
    if limit:
        df = df.head(limit)

    if df.empty:
        log.warning("replay.empty_source", source=source, region=region)
        return

    producer = _make_producer(env.kafka_bootstrap)

    log.info(
        "replay.start",
        topic=topic,
        bootstrap=env.kafka_bootstrap,
        rows=len(df),
        speed=speed,
        first_ts=str(df["timestamp_utc"].iloc[0]),
        last_ts=str(df["timestamp_utc"].iloc[-1]),
    )

    stopping = {"stop": False}

    def _sig(*_args):
        stopping["stop"] = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    sent = 0
    wall_start = time.time()
    sim_start = df["timestamp_utc"].iloc[0]

    for _, row in df.iterrows():
        if stopping["stop"]:
            break

        # Pace: real seconds between events = (sim seconds) / speed
        elapsed_real = time.time() - wall_start
        elapsed_sim = (row["timestamp_utc"] - sim_start).total_seconds()
        due_real = elapsed_sim / speed
        delay = due_real - elapsed_real
        if delay > 0:
            time.sleep(delay)

        ingest_iso = datetime.now(tz=timezone.utc).isoformat()
        event = _row_to_event(row.to_dict(), ingest_iso)
        producer.produce(
            topic=topic,
            key=event["region"].encode(),
            value=json.dumps(event).encode(),
            callback=_delivery_report,
        )
        sent += 1
        if sent % 100 == 0:
            producer.poll(0)
            log.info("replay.progress", sent=sent, last_ts=event["timestamp_utc"])

    producer.flush(timeout=10)
    log.info("replay.done", sent=sent, elapsed_real_sec=round(time.time() - wall_start, 1))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Replay EIA load data to Kafka")
    ap.add_argument("--source", default=None, help="Path to Gold Delta table (defaults to .env)")
    ap.add_argument("--region", default=None)
    ap.add_argument("--topic", default=None)
    ap.add_argument("--speed", type=float, default=None,
                    help="Speed-up factor (1=real time, 60=60x faster)")
    ap.add_argument("--limit", type=int, default=None, help="Max rows (for smoke tests)")
    return ap.parse_args()


if __name__ == "__main__":
    env = get_env()
    configure_logging(level=env.log_level)
    args = parse_args()
    run(
        source=args.source or env.replay_source,
        region=args.region or env.replay_region,
        topic=args.topic or env.kafka_topic_events,
        speed=args.speed if args.speed is not None else env.replay_speed,
        limit=args.limit,
    )
