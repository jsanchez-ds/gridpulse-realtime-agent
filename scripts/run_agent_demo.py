"""
End-to-end demo of the GridPulse agent.

Fabricates a critical anomaly event and feeds it into handle_anomaly().
The agent:
  1. Classifies severity (heuristic tool)
  2. Looks up current load (from whatever's in Delta — may fall back)
  3. Asks Project 1 for a 24h forecast (falls back to seasonal-naive if MLflow unavailable)
  4. Asks Project 2's RAG for literature on the anomaly type
  5. If severity >= warning, posts an incident report to Discord + Delta

This exists so you can validate the full agent + LLM + Discord wiring
WITHOUT needing to bring up Spark Streaming. For the streaming path see
`src/streaming/consumer.py`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from src.agent.loop import handle_anomaly
from src.utils.config import get_env
from src.utils.logging import configure_logging, get_logger

log = get_logger(__name__)


def _fabricate_anomaly(severity: str = "critical", region: str = "CAL") -> dict:
    """Craft an anomaly with a score that matches the requested severity bucket."""
    score_map = {
        "info": -0.02,
        "warning": -0.08,
        "critical": -0.32,
    }
    load_map = {
        "info": 30_000,
        "warning": 48_000,
        "critical": 58_000,
    }
    return {
        "event_id": f"demo-{severity}-{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
        "region": region,
        "load_mw": load_map[severity],
        "anomaly_score": score_map[severity],
        "hour": datetime.now(tz=timezone.utc).hour,
        "day_of_week": datetime.now(tz=timezone.utc).weekday(),
        "is_weekend": int(datetime.now(tz=timezone.utc).weekday() >= 5),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run one agent loop against a fabricated anomaly")
    ap.add_argument(
        "--severity", choices=["info", "warning", "critical"], default="critical",
        help="Which anomaly severity to fabricate",
    )
    ap.add_argument("--region", default="CAL")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(level=get_env().log_level)

    anomaly = _fabricate_anomaly(severity=args.severity, region=args.region)
    log.info("demo.fabricated", **anomaly)

    print("\n" + "=" * 70)
    print(f"  FABRICATED ANOMALY — severity target: {args.severity}")
    print("=" * 70)
    print(json.dumps(anomaly, indent=2))
    print()

    run = handle_anomaly(anomaly)

    print("\n" + "=" * 70)
    print(f"  AGENT RUN COMPLETE")
    print("=" * 70)
    print(f"  iterations         : {run.iterations}")
    print(f"  elapsed_seconds    : {run.elapsed_seconds}")
    print(f"  prompt_tokens      : {run.total_prompt_tokens}")
    print(f"  completion_tokens  : {run.total_completion_tokens}")
    print(f"  hit_max_iterations : {run.hit_max_iterations}")
    print(f"  hit_budget         : {run.hit_budget}")
    print(f"  posted_incident    : {run.posted_incident_id or '(none)'}")
    print()
    print(f"  tool_calls ({len(run.tool_calls)}):")
    for tc in run.tool_calls:
        preview = (tc["result_preview"] or "")[:160].replace("\n", " ")
        # stick to ASCII so Windows cp1252 stdout never explodes
        print(f"    [iter {tc['iteration']}] {tc['name']}({list(tc['args'].keys())})")
        print(f"       -> {preview}")
    print()
    print(f"  final_text: {run.final_text[:500] if run.final_text else '(none)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
