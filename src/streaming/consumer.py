"""
Spark Structured Streaming consumer.

Subscribes to the `grid_events` topic, parses JSON, windows the stream,
and on each micro-batch calls the @staging Isolation Forest from the MLflow
registry (loaded ONCE per executor) to flag anomalies. Anomalous rows are:
  1. Written to a Delta sink (data/delta/anomalies)
  2. Invoked against the GridPulse agent for incident analysis

The agent call is synchronous within the micro-batch — fine for v1 local
scale (< a few anomalies per batch). For true production, buffer anomalies
to a second Kafka topic and let the agent consume async.
"""

from __future__ import annotations

import json
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


def _get_spark(app: str = "gridpulse-streaming") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder,
            extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"],
        )
    except ImportError:
        pass
    return builder.getOrCreate()


def _load_anomaly_model() -> Any:
    """Load the Project-1 anomaly detector from the local MLflow registry."""
    env = get_env()
    mlflow.set_tracking_uri(env.mlflow_tracking_uri)
    uri = f"models:/{env.anomaly_model}@{env.anomaly_alias}"
    log.info("anomaly.model.load", uri=uri)
    return mlflow.sklearn.load_model(uri)


def _enrich_with_anomaly(pdf, model, feature_cols: list[str]):
    """Run the loaded sklearn model over a pandas batch and tag each row."""
    X = pdf[feature_cols].fillna(pdf[feature_cols].mean(numeric_only=True))
    preds = model.predict(X)        # -1 = anomaly, +1 = normal
    scores = model.decision_function(X)
    pdf = pdf.copy()
    pdf["is_anomaly"] = (preds == -1).astype(int)
    pdf["anomaly_score"] = scores
    return pdf


def _dispatch_agent(anomaly_rows: list[dict]) -> None:
    """Hand anomalies off to the agent. Lazy import so the streaming job
    can still run if the agent package has a problem."""
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

    # Persist everything (append) for the dashboard
    spark = batch_df.sparkSession
    enriched = spark.createDataFrame(pdf)
    (
        enriched.write.format("delta")
        .mode("append")
        .save(f"{delta_path}/events_scored")
    )

    # Split anomalies, persist + dispatch
    anomalies = pdf[pdf["is_anomaly"] == 1]
    if not anomalies.empty:
        spark.createDataFrame(anomalies).write.format("delta").mode("append").save(
            f"{delta_path}/anomalies"
        )
        log.info("batch.anomalies", batch_id=batch_id, n=len(anomalies))
        _dispatch_agent(anomalies.to_dict(orient="records"))


def run() -> None:
    env = get_env()
    cfg = load_yaml_config()
    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")

    feature_cols = cfg["features"]["feature_cols"]
    model = _load_anomaly_model()

    Path(env.delta_path).mkdir(parents=True, exist_ok=True)
    checkpoint_dir = cfg["streaming"]["checkpoint_dir"]
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env.kafka_bootstrap)
        .option("subscribe", env.kafka_topic_events)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withWatermark("timestamp_utc", cfg["streaming"]["watermark"])
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
    log.info("stream.started")
    query.awaitTermination()


if __name__ == "__main__":
    configure_logging(level=get_env().log_level)
    run()
