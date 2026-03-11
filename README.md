# KIS AI Trading v1

KIS Open API based domestic equity trading platform with running local infrastructure,
service processes, persistence, raw event capture, replay packaging, and operator console.

## Stack

- Python 3.11
- FastAPI
- SQLAlchemy 2.0
- Pydantic v2
- MariaDB
- Redis
- NATS JetStream
- MinIO
- Vue 3 + Vite + Pinia

## Repository Layout

- `apps/`: runnable service entrypoints
- `libs/`: shared contracts, domain types, database models, policies, risk logic
- `ops-web/`: operator console frontend
- `tests/`: backend tests
- `infra/`: local observability and infrastructure config
- `kis-open-trading-api/`: upstream KIS reference repo cloned for adapter research

## Quick Start

1. Copy `.env.example` to `.env`.
2. Start the full local stack with `docker compose up -d`.
3. Install backend dependencies with `pip install -e .[dev]`.
4. Initialize the schema with `alembic upgrade head`.
5. Open the main endpoints:

- `http://localhost:8000/health` for the ops API
- `http://localhost:8001/health` for the broker gateway
- `http://localhost:8002/health` for the trading core
- `http://localhost:8003/health` for the market intelligence service
- `http://localhost:8004/health` for the replay runner
- `http://localhost:8005/health` for the shadow live worker
- `http://localhost:5173` for the ops web UI
- `http://localhost:9001` for the MinIO console
- `http://localhost:3000` for Grafana
- `http://localhost:9090` for Prometheus

6. For local frontend-only development, run the operator console with:

```bash
cd ops-web
npm install
npm run dev
```

## Live Trading Guardrails

The repository is now configured around a prod-first workflow. Paper trading is optional and
disabled by default.

- `KIS_ENABLE_PAPER=false` keeps `env=vps` requests blocked.
- `KIS_LIVE_TRADING_ENABLED=false` blocks all live order submission until you explicitly enable it.
- `KIS_LIVE_REQUIRE_ARM=true` requires a separate arm step before live order submission.
- `KIS_LIVE_MAX_ORDER_VALUE_KRW=0` disables the single-order notional cap.
- `KIS_LIVE_ALLOWED_SYMBOLS` can restrict live trading to a comma-separated symbol allowlist.
- `KIS_LIVE_DAILY_LOSS_LIMIT_PCT=5.0` pauses new entries when daily asset drawdown reaches 5%.
- `KIS_LIVE_MIN_TOTAL_EQUITY_KRW=5000000` pauses new entries when total evaluated assets fall to 5 million KRW or lower.
- `KIS_LIVE_COMMON_STOCK_ONLY=true` blocks ETFs, ETNs, leveraged products, covered-call products, and other non-common-stock symbols by checking the OpenDART listed company universe.

Useful broker-gateway controls:

- `POST /live/arm` arms live order submission.
- `POST /live/disarm` immediately disarms live order submission.
- `POST /live/risk-check` refreshes live balance-derived protection state.
- `GET /snapshot` shows whether the gateway is in prod-only mode and whether live trading is armed.

The gateway allows live cancel requests without arming when `RVSE_CNCL_DVSN_CD=02`, so an
operator can still try to reduce risk while new exposure is blocked.

## Current Status

The repository now includes:

- canonical message contracts for broker, market data, disclosure, risk, intent, and replay flows
- SQLAlchemy models for the v1 ledger, projection, policy, and reconciliation tables
- service skeletons for broker gateway, trading core, market intelligence, replay, and shadow live
- a KIS HTTP adapter covering token, approval, hashkey, order, cancel/replace, balance, possible order, and daily fill queries
- JetStream-backed event publishing for canonical broker events
- OpenDART corp-code download, disclosure list fetch, and disclosure parsing with OpenAI structured output plus fallback rules
- Alembic migration scaffolding for bootstrapping the MariaDB schema and a verified local MariaDB runtime
- an operator API with dashboard, replay, kill switch, strategy, account, and reconciliation endpoints
- a Vue operator console that reads the ops API and surfaces the main operational views
- raw event persistence into MariaDB and MinIO plus replay package generation
- dockerized local infrastructure for MariaDB, Redis, NATS JetStream, MinIO, Prometheus, Loki, and Grafana

Current verified capabilities:

- KIS REST token issuance
- KIS websocket approval issuance
- read-only KIS balance query
- OpenDART corp-code download and disclosure list fetch
- raw event storage and canonical event publish path
- replay package assembly

Remaining work is focused on production depth rather than repo bootstrapping:

- live websocket consumers and reconnect supervision
- richer OMS reconciliation and open-order recovery
- strategy-specific candidate generation beyond sample pipelines
- production hardening for alerts, dashboards, and runbook automation
