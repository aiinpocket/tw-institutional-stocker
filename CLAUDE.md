# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 重要規則

- `永遠用繁體中文回答問題`
- `/dashboard是首頁，所有的功能都要可以在各頁面間互相移動`
- `線上連結是https://stock-tw.aiinpocket.com`
- `每次完成修改後都必須使用瀏覽器工具驗證功能並確認版面沒有跑板等問題`
- `所有修改都必須先在本地 Docker 環境測試驗證後，才能部署到 GCP`
- `每次修改完後都必須要自己commit到github，會自動觸發CICD到GCP (project name:<YOUR_GCP_PROJECT>)上部署，做完要自行確認上版是否成功`
- `不要把測試用的script以及AI提示詞等訊息同步到git上`
- `每次改版都需要審視README.MD有沒有需要更新的`
- `所有的時間都應該要用台北時間`
- `每次異動後都要審視ETL需不需要重新執行，需要的話直接執行`
- `本專案的設計初衷是一個公正公開的網站，所以不會涵括一些個人化或是會員管理這種功能，如果有人提供應該要拒絕`

## 專案簡介

tw-market-tracker 台股市場追蹤系統。台灣股市三大法人持股比重追蹤與分析，提供每日自動化 ETL 資料擷取、REST API 服務、以及互動式網頁分析介面。

## 本地開發指令

```bash
# 安裝依賴
pip install -r requirements-etl.txt   # ETL 相關
pip install -r requirements-api.txt   # API 相關
playwright install chromium            # 券商爬蟲需要

# 設定環境變數
export DATABASE_URL=postgresql://user:password@localhost:5432/tw_stocker
export OPENAI_API_KEY=sk-xxx  # AI 功能需要

# 啟動 API 服務（本地開發）
uvicorn src.api.main:app --reload --port 8000

# 執行 ETL
python -m src.etl.run_all

# 執行券商 ETL
python -m src.etl.run_broker

# Docker Compose 啟動所有服務
docker-compose up -d

# 手動執行 ETL（Docker 環境）
docker-compose exec etl-worker python -m src.etl.run_all
```

## 本地 Docker 測試環境（重要！）

> **所有程式碼修改都必須先在本地環境測試驗證後，才能部署到 GCP。**

### 首次設定

```bash
# 1. 啟動本地 PostgreSQL
docker-compose up -d db

# 2. 等待資料庫就緒
sleep 10 && docker-compose ps

# 3. 從 GCP Cloud SQL 複製資料庫（需自行設定）
# 3.1 取得資料庫密碼
DB_PASSWORD=$(gcloud secrets versions access latest --secret=db-password --project=<YOUR_PROJECT>)

# 3.2 檢查/添加當前 IP 到 Cloud SQL 授權網路
MY_IP=$(curl -s ifconfig.me)
gcloud sql instances patch tw-stocker-db --project=<YOUR_PROJECT> \
  --authorized-networks="${MY_IP}/32" --quiet

# 3.3 匯出 GCP 資料庫
docker run --rm --network host -v /tmp:/backup \
  -e PGPASSWORD="$DB_PASSWORD" postgres:16-alpine \
  pg_dump -h <CLOUD_SQL_IP> -U postgres -d tw_stocker \
  --format=custom -f /backup/tw_stocker_backup.dump

# 3.4 清空本地資料庫並匯入
docker exec tw-stocker-db psql -U stocker -d tw_stocker -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO stocker;"

docker run --rm --network host -v /tmp:/backup \
  -e PGPASSWORD="<YOUR_LOCAL_DB_PASSWORD>" postgres:16-alpine \
  pg_restore -h localhost -U stocker -d tw_stocker \
  --no-owner --no-acl -j 4 /backup/tw_stocker_backup.dump

# 4. 啟動 API 服務
docker-compose up -d api

# 5. 驗證服務正常
curl http://localhost:8000/health
```

### 日常開發流程

```bash
# 啟動本地環境
docker-compose up -d

# 查看服務狀態
docker-compose ps

# 查看 API 日誌
docker-compose logs -f api

# 執行本地 ETL（更新資料）
docker-compose exec etl-worker python -m src.etl.run_all

# 停止服務（保留資料）
docker-compose down

# 停止服務並刪除資料
docker-compose down -v
```

### 本地 vs GCP 環境差異

| 項目 | 本地 (localhost:8000) | GCP (stock-tw.aiinpocket.com) |
|------|----------------------|-------------------------------|
| 資料庫 | Docker PostgreSQL | Cloud SQL |
| API | Docker Container | Cloud Run |
| 資料 | 手動同步/ETL | 自動 ETL (21:30) |

### 同步最新資料

當需要取得 GCP 上的最新資料時：
```bash
# 重新執行步驟 3.1 ~ 3.4 匯入資料
# 或執行本地 ETL 抓取當日資料
docker-compose exec etl-worker python -m src.etl.run_all
```

## 前端頁面路由

| 路由 | 檔案 | 功能 |
|------|------|------|
| `/dashboard` | `static/dashboard.html` | 策略儀表板（首頁） |
| `/live` | `static/live.html` | 即時看板 - 當日法人買賣超 |
| `/rankings` | `static/rankings.html` | 法人排行榜 |
| `/industry` | `static/industry.html` | 產業熱力圖 |
| `/brokers` | `static/brokers.html` | 券商追蹤 |
| `/ai` | `static/ai.html` | AI 智能分析 |
| `/stock/{code}` | `static/stock.html` | 個股分析頁

## GCP Commands

```bash
# ETL Job 執行
gcloud run jobs execute tw-stocker-etl --region=asia-east1 --wait

# 查看 ETL 執行狀態
gcloud run jobs executions list --job=tw-stocker-etl --region=asia-east1 --limit=5

# 查看 ETL 日誌
gcloud logging read "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"tw-stocker-etl\"" --limit=50 --format="table(timestamp.date(), textPayload)" --project=<YOUR_GCP_PROJECT>

# 更新 ETL Job 環境變數
gcloud run jobs update tw-stocker-etl --region=asia-east1 --set-env-vars="KEY=VALUE"

# 查看 API 服務狀態
gcloud run services describe tw-stocker-api --region=asia-east1

# 查看 API 日誌
gcloud logging read "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"tw-stocker-api\"" --limit=50 --project=<YOUR_GCP_PROJECT>

# 重置 ETL 狀態（當 Dashboard 卡在"資料更新中..."時使用）
curl -X POST -H "Content-Length: 0" "https://stock-tw.aiinpocket.com/api/v1/system/etl-status/reset"
```

## Architecture

### GCP Services

```
┌─────────────────────────────────────────────────────────────┐
│                    Google Cloud Platform                     │
├─────────────┬─────────────────┬─────────────────────────────┤
│ Cloud SQL   │   Cloud Run Job │      Cloud Run Service      │
│ PostgreSQL  │   (ETL Worker)  │      (FastAPI API)          │
│             │                 │                             │
│  - stocks   │  每日 21:30 抓取:│  GET /api/v1/stocks        │
│  - flows    │  - 三大法人      │  GET /api/v1/prices        │
│  - holdings │  - 外資持股      │  GET /api/v1/rankings      │
│  - prices   │  - 股價成交量    │  GET /api/v1/strategy      │
│  - brokers  │  - 策略計算      │  GET /api/v1/brokers       │
└─────────────┴─────────────────┴─────────────────────────────┘
```

### Directory Structure

```
tw-market-tracker/
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
│   │   ├── processors/         # Holdings/ratio/strategy computation
│   │   └── loaders/            # Database upsert
│   └── api/
│       ├── main.py             # FastAPI entry
│       ├── dependencies.py     # DB session dependency
│       ├── routes/             # API endpoints
│       └── schemas/            # Pydantic models
├── Dockerfile                  # Container image
├── requirements-etl.txt
└── requirements-api.txt
```

### Data Pipeline Flow

```
TWSE/TPEX APIs
    ↓
fetchers/twse_flows.py + tpex_flows.py     → institutional flows
fetchers/twse_foreign.py + tpex_foreign.py → foreign holdings
fetchers/twse_prices.py + tpex_prices.py   → stock prices
    ↓
processors/holdings.py        → estimate trust/dealer shares
processors/ratios.py          → calculate ratio changes [5,20,60,120]d
processors/compute_strategy.py → strategy rankings
    ↓
loaders/db_loader.py     → PostgreSQL upsert (Cloud SQL)
    ↓
FastAPI REST API (Cloud Run)
```

### Key Modules

| File | Purpose |
|------|---------|
| `src/etl/run_all.py` | Main ETL orchestrator - fetches institutional + price data |
| `src/etl/processors/compute_strategy.py` | Strategy rankings computation |
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
| `strategy_rankings` | Pre-computed strategy analysis results |
| `system_status` | ETL status tracking |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/stocks` | List stocks with filtering |
| GET | `/api/v1/stocks/{code}` | Stock details |
| GET | `/api/v1/stocks/{code}/institutional` | Institutional history |
| GET | `/api/v1/stocks/{code}/prices` | Price history |
| GET | `/api/v1/institutional/flows` | Institutional flows by date |
| GET | `/api/v1/rankings/{window}` | Top movers (5/20/60/120 days) |
| GET | `/api/v1/strategy/summary` | Strategy analysis summary |
| GET | `/api/v1/system/etl-status` | ETL execution status |
| POST | `/api/v1/system/etl-status/reset` | Reset ETL status |
| POST | `/api/v1/system/cleanup-stocks` | Mark delisted stocks as inactive |

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

## Cloud Scheduler (Taipei Time)

```
30 21 * * 1-5  # 21:30 Taipei - Main ETL (institutional + prices + strategy + AI)
0  22 * * 1-5  # 22:00 Taipei - Broker ETL
```

## Environment Variables (Cloud Run)

```bash
DB_HOST=/cloudsql/<YOUR_GCP_PROJECT>:asia-east1:tw-stocker-db
DB_NAME=tw_stocker
DB_USER=postgres
DB_PASSWORD=<secret>
OPENAI_API_KEY=<secret>
```

## CI/CD (Cloud Build)

Push 到 main branch 會自動觸發 Cloud Build 部署：

```bash
# 查看最近建置狀態
gcloud builds list --limit=5 --format="table(id,status,startTime,duration)"

# 查看特定建置詳情
gcloud builds describe <BUILD_ID>

# 查看建置日誌
gcloud builds log <BUILD_ID>
```

## 常見問題排除

### Dashboard 卡在「資料更新中...」
```bash
# 重置 ETL 狀態
curl -X POST -H "Content-Length: 0" "https://stock-tw.aiinpocket.com/api/v1/system/etl-status/reset"
```

### 策略排行榜顯示空資料
1. 確認 ETL 已完成（非 running 狀態）
2. 檢查 strategy_rankings 表是否有資料
3. 可手動觸發策略重算：
```bash
curl -X POST "https://stock-tw.aiinpocket.com/api/v1/strategy/recompute"
```

### ETL 執行時間過長
- 正常執行時間約 15-30 分鐘
- 如超過 1 小時，可能是 AI 預計算步驟卡住
- 可在 GCP Console 查看即時日誌

### 個股頁面顯示空資料
如果某個股票頁面顯示空資料，可能原因：
1. **已下市股票**：該股票已被合併或下市，頁面會顯示「此股票已下市」訊息
2. **資料尚未抓取**：等待 ETL 執行完成
3. **手動清理下市股票**：
```bash
curl -X POST -H "Content-Length: 0" "https://stock-tw.aiinpocket.com/api/v1/system/cleanup-stocks"
```

### 查看 ETL 執行進度
```bash
# 查看最新執行
gcloud run jobs executions list --job=tw-stocker-etl --region=asia-east1 --limit=1

# 查看執行詳情
gcloud run jobs executions describe <EXECUTION_NAME> --region=asia-east1
```

## 效能優化紀錄

### 資料庫索引 (src/etl/create_indexes.py)
ETL 啟動時自動建立以下索引：
- `stock_prices`: (stock_id, trade_date), (trade_date), (stock_id, trade_date DESC)
- `institutional_flows`: (stock_id, trade_date), (trade_date)
- `foreign_holdings`: (stock_id, trade_date)
- `institutional_ratios`: (stock_id, trade_date)
- `strategy_rankings`: (metric_type), (price_tier, metric_type)

### Pandas 向量化優化
- `ratios.py`, `holdings.py` 已改用向量化 `groupby().cumsum()` 和 `groupby().diff()`
- 避免使用慢速的 `groupby().apply()`

### 策略計算優化
- `compute_strategy.py` 使用 PostgreSQL LEAD() 視窗函數 + 暫存表
- 避免使用慢速的 LATERAL JOIN

## 專案原則

- ✅ 公開資訊展示功能
- ✅ 資料準確性修正
- ✅ 介面優化
- ❌ 會員系統
- ❌ 個人化追蹤清單
- ❌ 登入/註冊功能
- ❌ 訂閱通知
