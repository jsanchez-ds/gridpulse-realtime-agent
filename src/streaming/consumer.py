"""
Spark Structured Streaming consumer.

Reads either:
  - a Delta table that the producer appends to (STREAM_TRANSPORT=delta) — DEFAULT
  - a Kafka topic (STREAM_TRANSPORT=kafka) — needs Redpanda / Confluent

…windows the stream, calls the @staging Isolation Forest from Project 1
on each micro-batch, persists results to Delta, and dispatches anomalies
to the GridPulse agent.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import mlflow
import mlflow.sklearn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.config import get_env, load_yaml_config
from src.utils.logging import configure_logging, get_logger

log = get_logger(__name__)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp_utc", TimestampType()),
        StructField("region", StringType()),
        StructField("load_mw", DoubleType()),
        StructField("hour", IntegerType()),
        StructField("day_of_week", IntegerType()),
        StructField("is_weekend", IntegerType()),
        StructField("load_mw_clean_lag_24", DoubleType()),
        StructField("load_mw_clean_lag_168", DoubleType()),
        StructField("load_mw_clean_roll_mean_24", DoubleType()),
        StructField("load_mw_clean_roll_std_24", DoubleType()),
        StructField("ingested_at", TimestampType()),
    ]
)


def _get_spark(app: str = "gridpulse-streaming", with_kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
    )
    extra_packages: list[str] = []
    if with_kafka:
        extra_packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        builder = builder.config(
            "spark.jars.packages", ",".join(extra_packages)
        )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
    except ImportError:
        pass
    return builder.getOrCreate()


def _load_anomaly_model() -> Any:
    env = get_env()
    mlflow.set_tracking_uri(env.mlflow_tracking_uri)
    uri = f"models:/{env.anomaly_model}@{env.anomaly_alias}"
    log.info("anomaly.model.load", uri=uri)
    return mlflow.sklearn.load_model(uri)


def _enrich_with_anomaly(pdf, model, feature_cols: list[str]):
    X = pdf[feature_cols].fillna(pdf[feature_cols].mean(numeric_only=True))
    preds = model.predict(X)
    scores = model.decision_function(X)
    pdf = pdf.copy()
    pdf["is_anomaly"] = (preds == -1).astype(int)
    pdf["anomaly_score"] = scores
    return pdf


def _dispatch_agent(anomaly_rows: list[dict]) -> None:
    if not anomaly_rows:
        return
    try:
        from src.agent.loop import handle_anomaly

        for row in anomaly_rows:
            handle_anomaly(row)
    except Exception as exc:
        log.exception("agent.dispatch.failed", n=len(anomaly_rows), error=str(exc))


def _foreach_batch(batch_df: DataFrame, batch_id: int, model, feature_cols: list[str], delta_path: str) -> None:
    if batch_df.rdd.isEmpty():
        return
    pdf = batch_df.toPandas()
    pdf = _enrich_with_anomaly(pdf, model, feature_cols)
    log.info("batch.done", batch_id=batch_id, rows=len(pdf),
             anomalies=int(pdf["is_anomaly"].sum()))

    spark = batch_df.sparkSession
    enriched = spark.createDataFrame(pdf)
    (
        enriched.write.format("delta")
        .mode("append")
        .save(f"{delta_path}/events_scored")
    )

    anomalies = pdf[pdf["is_anomaly"] == 1]
    if not anomalies.empty:
        spark.createDataFrame(anomalies).write.format("delta").mode("append").save(
            f"{delta_path}/anomalies"
        )
        _dispatch_agent(anomalies.to_dict(orient="records"))


def _build_source(spark: SparkSession, env, cfg) -> DataFrame:
    """Return a streaming DataFrame with the EVENT_SCHEMA regardless of transport."""
    if env.stream_transport == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", env.kafka_bootstrap)
            .option("subscribe", env.kafka_topic_events)
            .option("startingOffsets", "latest")
            .load()
        )
        return (
            raw.selectExpr("CAST(value AS STRING) as json_str")
            .select(F.from_json("json_str", EVENT_SCHEMA).alias("e"))
            .select("e.*")
        )

    # Default: Delta source — the producer appends events to this path.
    Path(env.stream_delta_path).mkdir(parents=True, exist_ok=True)
    # Ensure the table exists before the streaming reader attaches (avoids the
    # "table not found" race on first startup with no producer output yet).
    if not (Path(env.stream_delta_path) / "_delta_log").exists():
        from deltalake import write_deltalake
        import pandas as pd

        empty = pd.DataFrame(
            {
                "event_id": pd.Series([], dtype="string"),
                "timestamp_utc": pd.Series([], dtype="datetime64[ns, UTC]"),
                "region": pd.Series([], dtype="string"),
                "load_mw": pd.Series([], dtype="float64"),
                "hour": pd.Series([], dtype="int64"),
                "day_of_week": pd.Series([], dtype="int64"),
                "is_weekend": pd.Series([], dtype="int64"),
                "load_mw_clean_lag_24": pd.Series([], dtype="float64"),
                "load_mw_clean_lag_168": pd.Series([], dtype="float64"),
                "load_mw_clean_roll_mean_24": pd.Series([], dtype="float64"),
                "load_mw_clean_roll_std_24": pd.Series([], dtype="float64"),
                "ingested_at": pd.Series([], dtype="datetime64[ns, UTC]"),
            }
        )
        write_deltalake(env.stream_delta_path, empty, mode="overwrite")

    return (
        spark.readStream.format("delta")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .load(env.stream_delta_path)
    )


def run() -> None:
    env = get_env()
    cfg = load_yaml_config()
    spark = _get_spark(with_kafka=(env.stream_transport == "kafka"))
    spark.sparkContext.setLogLevel("WARN")

    feature_cols = cfg["features"]["feature_cols"]
    model = _load_anomaly_model()

    Path(env.delta_path).mkdir(parents=True, exist_ok=True)
    checkpoint_dir = cfg["streaming"]["checkpoint_dir"]
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

    parsed = _build_source(spark, env, cfg).withWatermark(
        "timestamp_utc", cfg["streaming"]["watermark"]
    )

    query = (
        parsed.writeStream.outputMode(cfg["streaming"]["output_mode"])
        .foreachBatch(
            lambda df, bid: _foreach_batch(df, bid, model, feature_cols, env.delta_path)
        )
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=cfg["streaming"]["trigger_interval"])
        .start()
    )
    log.info("stream.started", transport=env.stream_transport)
    query.awaitTermination()


if __name__ == "__main__":
    configure_logging(level=get_env().log_level)
    run()
