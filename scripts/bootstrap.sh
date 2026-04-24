#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Bootstrapping GridPulse..."

if [[ ! -d .venv ]]; then
  py -3.11 -m venv .venv 2>/dev/null || python3.11 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/Scripts/activate 2>/dev/null || source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "⚠️  Edit .env: LLM_PROVIDER, *_API_KEY, DISCORD_WEBHOOK_URL"
fi

mkdir -p data/delta data/raw data/checkpoints

echo "✅ Next:"
echo "  1. make redpanda-up"
echo "  2. open 4 terminals:  make stream / make producer / make agent / make dashboard"
