# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

 - `永遠用繁體中文回答問題`
tw-market-tracker 台股市場追蹤系統。台灣股市三大法人持股比重追蹤與分析，提供每日自動化 ETL 資料擷取、REST API 服務、以及互動式網頁分析介面。

## Commands

```bash
# Docker Compose (Production)
docker-compose up -d              # Start all services
docker-compose logs -f etl-worker # View ETL logs
docker-compose logs -f api        # View API logs
docker-compose down               # Stop all services

# Manual ETL execution
docker-compose exec etl-worker python -m src.etl.run_all
docker-compose exec etl-worker python -m src.etl.run_broker

# Database access
docker-compose exec db psql -U stocker -d tw_stocker

# Local development
pip install -r requirements-etl.txt   # For ETL work
pip install -r requirements-api.txt   # For API work
playwright install chromium            # For broker scraping
```

## Architecture

### Docker Services

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose                            │
├─────────────┬─────────────────┬─────────────────────────────┤
│ PostgreSQL  │   ETL Worker    │      FastAPI                │
│   :5432     │  (cron + Playwright)  │     :8000            │
│             │                 │                             │
│  - stocks   │  每日 08:30 抓取:│  GET /api/v1/stocks        │
│  - flows    │  - 三大法人      │  GET /api/v1/prices        │
│  - holdings │  - 外資持股      │  GET /api/v1/rankings      │
│  - prices   │  - 股價成交量    │  GET /api/v1/brokers       │
│  - brokers  │  - 券商分點      │                            │
└─────────────┴─────────────────┴─────────────────────────────┘
```

### Directory Structure

```
tw-market-tracker/
├── docker/
│   ├── Dockerfile.etl          # ETL worker (Python + Playwright + cron)
│   ├── Dockerfile.api          # FastAPI server
│   ├── init.sql                # Database schema
│   └── crontab                 # Cron schedule
├── src/
│   ├── common/
│   │   ├── config.py           # Environment config
│   │   ├── database.py         # SQLAlchemy engine
│   │   ├── models.py           # ORM models
│   │   └── utils.py            # Shared utilities
│   ├── etl/
│   │   ├── run_all.py          # Main ETL orchestrator
│   │   ├── run_broker.py       # Broker ETL
│   │   ├── fetchers/           # Data fetchers
│   │   ├── processors/         # Holdings/ratio computation
│   │   └── loaders/            # Database upsert
│   └── api/
│       ├── main.py             # FastAPI entry
│       ├── dependencies.py     # DB session dependency
│       ├── routes/             # API endpoints
│       └── schemas/            # Pydantic models
├── docker-compose.yml
├── requirements-etl.txt
└── requirements-api.txt
```

### Data Pipeline Flow

```
TWSE/TPEX APIs
    ↓
fetchers/twse_flows.py + tpex_flows.py     → institutional flows
fetchers/twse_foreign.py + tpex_foreign.py → foreign holdings
fetchers/twse_prices.py + tpex_prices.py   → stock prices (NEW)
fetchers/broker.py                          → broker trades (Playwright)
    ↓
processors/holdings.py   → estimate trust/dealer shares
processors/ratios.py     → calculate ratio changes [5,20,60,120]d
    ↓
loaders/db_loader.py     → PostgreSQL upsert
    ↓
FastAPI REST API
```

### Key Modules

| File | Purpose |
|------|---------|
| `src/etl/run_all.py` | Main ETL orchestrator - fetches institutional + price data |
| `src/etl/run_broker.py` | Broker branch trading data via Playwright |
| `src/etl/fetchers/*.py` | Individual data source fetchers |
| `src/etl/loaders/db_loader.py` | PostgreSQL upsert operations |
| `src/api/main.py` | FastAPI application entry point |

### Database Tables

| Table | Purpose |
|-------|---------|
| `stocks` | Stock master data (code, name, market, total_shares) |
| `institutional_flows` | Daily buy/sell by foreign, trust, dealer |
| `foreign_holdings` | Official foreign ownership ratio |
| `stock_prices` | OHLCV price data |
| `institutional_ratios` | Computed holdings ratios + change metrics |
| `broker_trades` | Broker branch trading details |
| `institutional_baselines` | Baseline correction points |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/stocks` | List stocks with filtering |
| GET | `/api/v1/stocks/{code}` | Stock details |
| GET | `/api/v1/stocks/{code}/institutional` | Institutional history |
| GET | `/api/v1/stocks/{code}/prices` | Price history |
| GET | `/api/v1/institutional/flows` | Institutional flows by date |
| GET | `/api/v1/institutional/holdings` | Foreign holdings by date |
| GET | `/api/v1/institutional/ratios` | Institutional ratios by date |
| GET | `/api/v1/prices/latest` | Latest prices |
| GET | `/api/v1/prices/date/{date}` | Prices for specific date |
| GET | `/api/v1/rankings/{window}` | Top movers (5/20/60/120 days) |
| GET | `/api/v1/brokers/trades` | Broker trades |
| GET | `/api/v1/brokers/ranking` | Broker ranking by volume |
| GET | `/api/v1/brokers/{name}/history` | Broker trading history |

## Holdings Estimation Model

Foreign holdings use official `foreign_ratio` directly from TWSE/TPEX.

Trust/dealer holdings use baseline correction:
```python
# If baseline exists for (date, code):
trust_shares_est = baseline_trust + cumsum(trust_net since baseline)
dealer_shares_est = baseline_dealer + cumsum(dealer_net since baseline)

# Fallback (no baseline):
trust_shares_est = cumsum(trust_net)
dealer_shares_est = cumsum(dealer_net)
```

## Data Sources

| Source | Endpoint | Encoding |
|--------|----------|----------|
| TWSE T86 | `/exchangeReport/MI_INDEX` | CP950 (Big5) |
| TWSE Foreign | `/fund/MI_QFIIS` | CP950 |
| TWSE Prices | `/openapi/v1/exchangeReport/STOCK_DAY_ALL` | UTF-8 JSON |
| TPEX Flows | `/web/stock/3itrade/3itrade_hedge.php` | UTF-8 |
| TPEX Foreign | `/web/stock/exright/QFII.php` | UTF-8 |
| TPEX Prices | `/openapi/v1/tpex_mainboard_quotes` | UTF-8 JSON |

## Cron Schedule (Container Time = UTC)

```
30 0 * * 1-5  # 08:30 Taipei - Main ETL (institutional + prices)
0  1 * * 1-5  # 09:00 Taipei - Broker ETL
```

## Environment Variables

```bash
POSTGRES_USER=stocker
POSTGRES_PASSWORD=stocker_password
POSTGRES_DB=tw_stocker
DATABASE_URL=postgresql://stocker:stocker_password@db:5432/tw_stocker
```
