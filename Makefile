# ═══════════════════════════════════════════════════════════════════════════
# GridPulse developer Makefile
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: help install lint format test clean redpanda-up redpanda-down \
        producer stream agent dashboard pipeline

help:
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:  ## Install CPU deps into active venv
	pip install --upgrade pip
	pip install -r requirements.txt

lint:
	ruff check src tests
	black --check src tests

format:
	black src tests
	ruff check --fix src tests

test:
	pytest tests/ -v -m "not integration"

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +

# ── Services ──────────────────────────────────────────────────────────────
redpanda-up:  ## Start Redpanda + console
	docker compose -f docker/docker-compose.yml up -d

redpanda-down:
	docker compose -f docker/docker-compose.yml down

producer:  ## Replay EIA events to Kafka (default speed=60x)
	python -m src.producer.replay --speed $(if $(SPEED),$(SPEED),60)

stream:  ## Spark Structured Streaming consumer (Kafka -> anomaly -> agent)
	python -m src.streaming.consumer

agent:  ## FastAPI side-car for manual agent triggers
	uvicorn src.serving.api:app --reload --host 0.0.0.0 --port 8002

dashboard:  ## Live Streamlit dashboard
	streamlit run dashboards/app.py --server.port 8503 --server.headless true

pipeline:  ## bring up the whole dev stack (run each in its own terminal)
	$(MAKE) redpanda-up
	@echo ""
	@echo "Now open 4 terminals and run: make stream  /  make producer  /  make agent  /  make dashboard"
