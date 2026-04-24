# ⚡ GridPulse — Real-time Grid Intelligence Agent

> **Kafka → Spark Structured Streaming → MLflow-registered anomaly model → LLM agent with tool use → Discord alert.**

Third project in the energy-forecasting portfolio. Where [`energy-forecasting-databricks`](https://github.com/jsanchez-ds/energy-forecasting-databricks) did classical ML and [`energyscholar-rag`](https://github.com/jsanchez-ds/energyscholar-rag) did LLM/RAG, **GridPulse is the streaming + agent layer that glues them into one system** — the anomaly detector is consumed from Project 1's MLflow registry, the literature lookup tool calls Project 2's HTTP endpoint, and the agent reasons over both.

---

## 🏗️ Architecture

```
┌──────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│  EIA replay      │────▶│   Redpanda        │────▶│  Spark Structured   │
│  (configurable   │     │   (Kafka API      │     │  Streaming          │
│   speed-up)      │     │    compatible)    │     │                     │
└──────────────────┘     └───────────────────┘     └──────────┬──────────┘
                                                              │
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │ Windowed aggregation │
                                                   │ + IsolationForest    │
                                                   │ (Project 1 @staging) │
                                                   └──────────┬───────────┘
                                                              │ on anomaly
                                                              ▼
                              ┌────────────────────────────────────────────────┐
                              │                GridPulse Agent                 │
                              │  ┌─────────────────────────────────────┐       │
                              │  │   System prompt (safety + goals)    │       │
                              │  └─────────────────┬───────────────────┘       │
                              │                    │                           │
                              │  Reason → tool_call → observe → loop (≤ 5)     │
                              │                                                │
                              │  Tools (function calling):                     │
                              │    • get_24h_forecast(region)                  │
                              │    • search_literature(query)  ◀── Project 2   │
                              │    • get_current_load(region)                  │
                              │    • classify_severity(anomaly)                │
                              │    • post_incident_report(text, severity)      │
                              └──────┬───────────────────────────┬─────────────┘
                                     │                           │
                         Delta table │ Discord webhook           │
                                     ▼                           ▼
                         ┌───────────────────────┐    ┌─────────────────────┐
                         │  data/delta/incidents │    │  Discord alert     │
                         └───────────┬───────────┘    └─────────────────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │   Streamlit dashboard │
                         │   (live events + LLM  │
                         │   decision timeline)  │
                         └───────────────────────┘

                ╔══════════════════════════════════════════════╗
                ║  Tracing: Langfuse (optional) + MLflow spans║
                ║  Deploy: Databricks Asset Bundle (v2)        ║
                ╚══════════════════════════════════════════════╝
```

---

## 🎯 What this project proves

| Capability | Evidence |
|---|---|
| **Real-time streaming** | Kafka + Spark Structured Streaming with watermarks |
| **Production ML reuse** | Loads `workspace.default.energy-anomaly-detector@staging` from MLflow at streaming time |
| **Cross-service architecture** | Calls the RAG project over HTTP as a tool |
| **AI agent patterns** | Custom loop with OpenAI-compatible function calling, tool registry, guardrails (max iterations, cost budget) |
| **Provider agnosticism** | Reuses the `LLMClient` abstraction from Project 2 (Groq / Anthropic / OpenAI / OpenRouter interchangeable) |
| **Observability** | Langfuse spans per tool call + MLflow batch stats + Prometheus counters on the streaming job |
| **Data engineering discipline** | Watermarks, exactly-once sinks, idempotent event IDs |
| **Cloud-ready** | Local Redpanda for dev; one-line swap to Confluent Cloud + Databricks Asset Bundle for prod |

---

## 📂 Project structure

```
.
├── src/
│   ├── producer/      # EIA replay → Kafka producer
│   ├── streaming/     # PySpark Structured Streaming job
│   ├── agent/         # Agent loop + tool registry
│   ├── tools/         # Individual tool implementations
│   ├── serving/       # (optional) HTTP side-car for on-demand agent runs
│   └── utils/         # Config, logging, Kafka/Spark factories, LLM client
├── dashboards/        # Streamlit live dashboard
├── docker/            # docker-compose.yml (Redpanda + Langfuse)
├── configs/           # YAML configs
├── scripts/           # bootstrap.sh, env.sh
├── tests/             # pytest suite
└── .github/workflows/ # CI
```

---

## 🚀 Quickstart

### 1. Requirements
- Python 3.11, Java 11 (for PySpark)
- Docker Desktop (for Redpanda)
- LLM API key — Groq or OpenRouter (free tiers fine)
- A Discord webhook URL (optional) for live alerts
- Project 1 MLflow registry with `energy-anomaly-detector@staging` (optional — we ship a local fallback)
- Project 2 EnergyScholar FastAPI running (optional — tool gracefully degrades)

### 2. Setup

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env          # paste LLM key + Discord webhook

docker compose -f docker/docker-compose.yml up -d redpanda
```

### 3. Run

Four terminals (or one `make all`):

```bash
# 1. Spark streaming consumer — waits for events
make stream

# 2. Event producer — replays EIA data at 60x real time
make producer SPEED=60

# 3. Agent HTTP side-car (optional — for manual trigger)
make agent

# 4. Streamlit dashboard
make dashboard
```

Open http://localhost:8503 for the live dashboard.

---

## 📊 Results

_Filled in after the first live run: number of events streamed, anomalies caught, agent decisions taken, wall-clock latencies._

---

## 📜 License

MIT
