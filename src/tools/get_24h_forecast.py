"""get_24h_forecast: use Project 1's LightGBM model to produce a 24h ahead forecast.

Falls back to a seasonal-naive baseline (yesterday's 24h) if the model isn't
available locally — keeps the agent useful even when Project 1 isn't set up.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from deltalake import DeltaTable

from src.utils.config import get_env
from src.utils.logging import get_logger

log = get_logger(__name__)

_LGB_FEATURES = [
    "hour", "day_of_week", "day_of_month", "month", "quarter", "week_of_year",
    "is_weekend", "is_holiday_es",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos",
    "load_mw_clean_lag_1", "load_mw_clean_lag_24", "load_mw_clean_lag_48",
    "load_mw_clean_lag_168",
    "load_mw_clean_roll_mean_24", "load_mw_clean_roll_std_24",
    "load_mw_clean_roll_mean_168", "load_mw_clean_roll_std_168",
]


def _naive_baseline(region: str) -> dict:
    """Seasonal-naive: repeat the last 24 observed hours."""
    path = Path(get_env().delta_path) / "events_scored"
    if not path.exists():
        return {"available": False, "reason": "No historical data for baseline"}
    df = DeltaTable(str(path)).to_pandas()
    df = df[df["region"] == region].sort_values("timestamp_utc").tail(24)
    if len(df) < 24:
        return {"available": False, "reason": f"Only {len(df)} rows, need 24"}
    return {
        "available": True,
        "region": region,
        "method": "seasonal_naive_24h",
        "forecast_mw": [float(v) for v in df["load_mw"].tolist()],
        "warning": "Fell back to seasonal-naive (Project 1 model not available)",
    }


def get_24h_forecast(region: str) -> dict:
    env = get_env()
    model_name = "energy-demand-forecaster"

    try:
        mlflow.set_tracking_uri(env.mlflow_tracking_uri)
        model = mlflow.pyfunc.load_model(f"models:/{model_name}@staging")
    except Exception as exc:
        log.info("forecast.model.unavailable", error=str(exc))
        return _naive_baseline(region)

    # Build a synthetic feature frame for the next 24 hours by reading the
    # most recent lags from the Gold source and rolling forward.
    gold_path = Path(env.replay_source)
    if not gold_path.exists():
        log.info("forecast.gold.missing", path=str(gold_path))
        return _naive_baseline(region)

    df = DeltaTable(str(gold_path)).to_pandas()
    df = df[df["country"] == region].sort_values("timestamp_utc")
    if df.empty:
        return _naive_baseline(region)

    last = df.iloc[-1].copy()
    future_rows = []
    for h in range(1, 25):
        ts = pd.Timestamp(last["timestamp_utc"]) + timedelta(hours=h)
        future_rows.append({
            "timestamp_utc": ts,
            "hour": ts.hour,
            "day_of_week": ts.dayofweek,
            "day_of_month": ts.day,
            "month": ts.month,
            "quarter": ts.quarter,
            "week_of_year": int(ts.isocalendar().week),
            "is_weekend": int(ts.dayofweek >= 5),
            "is_holiday_es": 0,
            "hour_sin": float(np.sin(2 * np.pi * ts.hour / 24)),
            "hour_cos": float(np.cos(2 * np.pi * ts.hour / 24)),
            "dow_sin": float(np.sin(2 * np.pi * ts.dayofweek / 7)),
            "dow_cos": float(np.cos(2 * np.pi * ts.dayofweek / 7)),
            "month_sin": float(np.sin(2 * np.pi * ts.month / 12)),
            "month_cos": float(np.cos(2 * np.pi * ts.month / 12)),
            # Autoregressive lags: use the most recent values we have
            "load_mw_clean_lag_1": last.get("load_mw_clean", 0.0),
            "load_mw_clean_lag_24": last.get("load_mw_clean_lag_24", 0.0),
            "load_mw_clean_lag_48": last.get("load_mw_clean_lag_48", 0.0),
            "load_mw_clean_lag_168": last.get("load_mw_clean_lag_168", 0.0),
            "load_mw_clean_roll_mean_24": last.get("load_mw_clean_roll_mean_24", 0.0),
            "load_mw_clean_roll_std_24": last.get("load_mw_clean_roll_std_24", 0.0),
            "load_mw_clean_roll_mean_168": last.get("load_mw_clean_roll_mean_168", 0.0),
            "load_mw_clean_roll_std_168": last.get("load_mw_clean_roll_std_168", 0.0),
        })

    future = pd.DataFrame(future_rows)
    preds = model.predict(future[_LGB_FEATURES])
    return {
        "available": True,
        "region": region,
        "method": "lightgbm_from_mlflow",
        "forecast_mw": [float(p) for p in preds],
        "forecast_start_utc": str(future["timestamp_utc"].iloc[0]),
    }
