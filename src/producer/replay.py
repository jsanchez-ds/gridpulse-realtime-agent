"""
Event producer — replays historical load data as a live stream.

Two transports are supported (selected by STREAM_TRANSPORT in .env):
  - "delta" : appends each event to a Delta table that Spark Structured
              Streaming reads with readStream.format("delta"). No Docker.
  - "kafka" : publishes JSON to a Kafka topic (Redpanda / Confluent).

A configurable `speed` knob lets you replay years in minutes (e.g.
speed=3600 means one real second ≈ one simulated hour).
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from src.utils.config import get_env
from src.utils.logging import configure_logging, get_logger

log = get_logger(__name__)


# ── Source loading ──────────────────────────────────────────────────────────
def _load_rows(source: str, region: str) -> pd.DataFrame:
    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"Replay source not found: {source}")

    if (p / "_delta_log").exists():
        log.info("replay.load.delta", path=str(p))
        df = DeltaTable(str(p)).to_pandas()
    else:
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


def _row_to_event(row: dict, ingest_iso: str) -> dict:
    return {
        "event_id": f"{row['country']}-{pd.Timestamp(row['timestamp_utc']).value}",
        "timestamp_utc": pd.Timestamp(row["timestamp_utc"]),
        "region": row["country"],
        "load_mw": float(row.get("load_mw_clean", row.get("load_mw"))),
        "hour": int(row.get("hour", 0)),
        "day_of_week": int(row.get("day_of_week", 0)),
        "is_weekend": int(row.get("is_weekend", 0)),
        "load_mw_clean_lag_24": float(row.get("load_mw_clean_lag_24", 0.0)),
        "load_mw_clean_lag_168": float(row.get("load_mw_clean_lag_168", 0.0)),
        "load_mw_clean_roll_mean_24": float(row.get("load_mw_clean_roll_mean_24", 0.0)),
        "load_mw_clean_roll_std_24": float(row.get("load_mw_clean_roll_std_24", 0.0)),
        "ingested_at": pd.Timestamp(ingest_iso),
    }


# ── Delta sink ──────────────────────────────────────────────────────────────
class DeltaSink:
    """Appends a small batch of events per micro-tick to a Delta table.

    Spark Structured Streaming picks up each append as a new micro-batch
    via readStream.format('delta'), giving us exactly-once guarantees and
    watermark-friendly timestamps without Kafka.
    """

    def __init__(self, path: str, batch_size: int = 16) -> None:
        self.path = path
        self.batch_size = batch_size
        self._buffer: list[dict] = []
        Path(self.path).mkdir(parents=True, exist_ok=True)

    def publish(self, event: dict) -> None:
        self._buffer.append(event)
        if len(self._buffer) >= self.batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        df = pd.DataFrame(self._buffer)
        write_deltalake(self.path, df, mode="append", schema_mode="merge")
        self._buffer.clear()

    def close(self) -> None:
        self._flush()


# ── Kafka sink (optional) ───────────────────────────────────────────────────
class KafkaSink:
    def __init__(self, bootstrap: str, topic: str) -> None:
        from confluent_kafka import Producer  # lazy import — confluent-kafka is optional

        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "linger.ms": 10,
                "compression.type": "zstd",
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def publish(self, event: dict) -> None:
        # Kafka JSON values can't hold pd.Timestamp directly
        safe = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in event.items()}
        self.producer.produce(
            topic=self.topic,
            key=event["region"].encode(),
            value=json.dumps(safe).encode(),
        )

    def close(self) -> None:
        self.producer.flush(timeout=10)


# ── Main loop ───────────────────────────────────────────────────────────────
def run(
    source: str,
    region: str,
    transport: str,
    topic: str,
    delta_path: str,
    bootstrap: str,
    speed: float,
    limit: int | None = None,
) -> None:
    df = _load_rows(source, region)
    if limit:
        df = df.head(limit)
    if df.empty:
        log.warning("replay.empty_source", source=source, region=region)
        return

    if transport == "kafka":
        sink = KafkaSink(bootstrap=bootstrap, topic=topic)
    else:
        sink = DeltaSink(path=delta_path)

    log.info(
        "replay.start",
        transport=transport,
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

        elapsed_real = time.time() - wall_start
        elapsed_sim = (row["timestamp_utc"] - sim_start).total_seconds()
        due_real = elapsed_sim / speed
        delay = due_real - elapsed_real
        if delay > 0:
            time.sleep(delay)

        ingest_iso = datetime.now(tz=timezone.utc).isoformat()
        event = _row_to_event(row.to_dict(), ingest_iso)
        sink.publish(event)
        sent += 1

        if sent % 100 == 0:
            log.info("replay.progress", sent=sent, last_ts=str(event["timestamp_utc"]))

    sink.close()
    log.info("replay.done", sent=sent, elapsed_real_sec=round(time.time() - wall_start, 1))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Replay EIA load data as a stream")
    ap.add_argument("--source", default=None)
    ap.add_argument("--region", default=None)
    ap.add_argument("--transport", choices=["delta", "kafka"], default=None)
    ap.add_argument("--topic", default=None)
    ap.add_argument("--delta-path", default=None)
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--limit", type=int, default=None)
    return ap.parse_args()


if __name__ == "__main__":
    env = get_env()
    configure_logging(level=env.log_level)
    args = parse_args()
    run(
        source=args.source or env.replay_source,
        region=args.region or env.replay_region,
        transport=args.transport or env.stream_transport,
        topic=args.topic or env.kafka_topic_events,
        delta_path=args.delta_path or env.stream_delta_path,
        bootstrap=args.bootstrap or env.kafka_bootstrap,
        speed=args.speed if args.speed is not None else env.replay_speed,
        limit=args.limit,
    )
