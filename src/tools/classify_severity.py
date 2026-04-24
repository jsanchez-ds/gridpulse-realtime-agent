"""classify_severity: simple heuristic, not LLM-based, so the agent can trust it."""

from __future__ import annotations


def classify_severity(anomaly_score: float, load_mw: float, region: str) -> dict:
    """Classify severity using quantile-ish thresholds on the Isolation-Forest
    score. Lower scores = more anomalous. We keep the thresholds explicit so
    operators can tune them without redeploying."""
    # Thresholds are deliberately conservative — tune to taste.
    if anomaly_score < -0.15:
        severity = "critical"
    elif anomaly_score < -0.05:
        severity = "warning"
    else:
        severity = "info"

    return {
        "severity": severity,
        "anomaly_score": anomaly_score,
        "region": region,
        "load_mw": load_mw,
        "rationale": (
            f"Score {anomaly_score:.4f} on Isolation Forest. "
            f"Thresholds: < -0.15 critical, < -0.05 warning, else info."
        ),
    }
