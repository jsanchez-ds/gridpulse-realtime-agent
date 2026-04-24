"""Live dashboard — events timeline + anomaly scatter + incident log."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DELTA_PATH = Path(os.getenv("DELTA_PATH", "./data/delta"))

st.set_page_config(page_title="GridPulse", page_icon="⚡", layout="wide")

st.sidebar.title("⚡ GridPulse")
st.sidebar.caption("Real-time grid intelligence agent")
refresh_sec = st.sidebar.slider("Auto-refresh (seconds)", 0, 60, 10)
if refresh_sec > 0:
    st.sidebar.caption(f"Page auto-refreshes every {refresh_sec} s.")

st.title("Live events · anomalies · agent incidents")


def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        from deltalake import DeltaTable

        df = DeltaTable(str(path)).to_pandas()
    except Exception:
        return pd.DataFrame()
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df


events = _load(DELTA_PATH / "events_scored")
anomalies = _load(DELTA_PATH / "anomalies")
incidents = _load(DELTA_PATH / "incidents")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Events scored", f"{len(events):,}")
col2.metric("Anomalies flagged", f"{len(anomalies):,}",
            delta=f"{len(anomalies) / max(len(events),1):.1%} rate" if len(events) else None)
col3.metric("Incidents posted", f"{len(incidents):,}")
col4.metric("Last update (UTC)",
            str(events["timestamp_utc"].max()) if not events.empty else "—")

st.subheader("Load — recent window")
if not events.empty:
    tail = events.sort_values("timestamp_utc").tail(500)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=tail["timestamp_utc"], y=tail["load_mw"],
        mode="lines", name="Load (MW)", line={"width": 1.2},
    ))
    anom = tail[tail.get("is_anomaly", 0) == 1] if "is_anomaly" in tail.columns else tail.iloc[0:0]
    if not anom.empty:
        fig.add_trace(go.Scatter(
            x=anom["timestamp_utc"], y=anom["load_mw"],
            mode="markers", name="Anomalies",
            marker={"color": "red", "size": 8, "symbol": "x"},
        ))
    fig.update_layout(height=400, margin={"t": 20, "b": 10, "l": 10, "r": 10},
                      hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No events yet — start the producer and streaming consumer.")

st.subheader("Recent incidents (agent output)")
if not incidents.empty:
    recent = incidents.sort_values("timestamp_utc", ascending=False).head(20)
    for _, row in recent.iterrows():
        sev = row["severity"]
        badge = {"critical": "🟥", "warning": "🟧", "info": "🟦"}.get(sev, "⬜")
        with st.expander(
            f"{badge}  **[{sev.upper()}]** {row['title']}  ·  "
            f"{str(row['timestamp_utc'])[:19]}"
        ):
            st.markdown(row["body"])
            st.caption(f"incident_id: `{row['incident_id']}`  ·  discord: {row['discord_status']}")
else:
    st.info("No incidents yet. They appear when the agent posts a warning/critical report.")

if refresh_sec > 0:
    # Streamlit's simple auto-refresh — not ideal for prod but fine for demo.
    st.markdown(
        f"<meta http-equiv='refresh' content='{refresh_sec}'>",
        unsafe_allow_html=True,
    )

st.caption(
    "Built with Kafka (Redpanda) · PySpark Structured Streaming · MLflow · "
    "OpenAI-compatible function calling · Delta Lake · Streamlit."
)
