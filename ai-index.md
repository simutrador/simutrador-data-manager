# AI Index (simutrador-server)

A compact entry point for humans and AI agents to find the plan, docs, and how to run/tests.

## Plan of record
- Simulation Engine Implementation Plan: https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/simutrador-server/simulation-engine-implementation-plan.md

## Canonical docs
- WebSocket API v2 (central spec): https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/simutrador-server/ws_api_v2.md
- Pytest Hang Playbook: PYTEST_HANGS.md
- Rate Limiting Plan: RATE_LIMITING_REDESIGN_IMPLEMENTATION_PLAN.md
- Architecture overview: https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/main.md

## How to run (dev)
```bash
uv sync
cp .env.sample .env
uv run python -m src.simutrador_server.main     # both servers
# or individually
uv run python -m src.simutrador_server.auth_server      # 8001
uv run python -m src.simutrador_server.websocket_server # 8003
```

## URLs (local defaults)
- Auth: http://127.0.0.1:8001 (GET /health)
- WebSocket: ws://127.0.0.1:8003 (GET /health, WS /ws/health, WS /ws/simulate)

## Tests & quality
```bash
uv run ruff check --fix src/
uv run pyright src/
uv run pytest -q
```

## Key paths
- src/simutrador_server/
- src/simutrador_server/websocket/
- src/simutrador_server/services/

## Current focus
- See the plan of record above for current task IDs and milestones.

