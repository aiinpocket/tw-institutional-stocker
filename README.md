# tw-market-tracker 台股市場追蹤

台灣股市三大法人（外資、投信、自營商）持股比重追蹤與分析系統。提供每日自動化 ETL 資料擷取、REST API 服務、以及互動式網頁分析介面。

## 線上服務

**https://stock-tw.aiinpocket.com**

| 頁面 | 功能 |
|------|------|
| [/dashboard](https://stock-tw.aiinpocket.com/dashboard) | 策略儀表板 - 法人連續買超排行 |
| [/live](https://stock-tw.aiinpocket.com/live) | 即時看板 - 當日法人買賣超總覽 |
| [/rankings](https://stock-tw.aiinpocket.com/rankings) | 法人排行榜 - 多策略篩選 |
| [/industry](https://stock-tw.aiinpocket.com/industry) | 產業熱力圖 - 資金流向分析 |
| [/brokers](https://stock-tw.aiinpocket.com/brokers) | 券商追蹤 - 主力分點進出 |
| [/ai](https://stock-tw.aiinpocket.com/ai) | AI 智能分析 - GPT 市場摘要與選股 |
| [/stock/{code}](https://stock-tw.aiinpocket.com/stock/2330) | 個股分析 - 技術面與籌碼面 |

## 自動化部署

本專案採用 **完全自動化** 的 CI/CD 與資料更新流程：

```
GitHub (push to main)
        │
        ▼
Cloud Build ──────► Cloud Run (API)
        │                  │
        │                  ▼
        │           https://stock-tw.aiinpocket.com
        │
Cloud Scheduler (21:30 Mon-Fri)
        │
        ▼
Cloud Run Jobs (ETL) ──► Cloud SQL (PostgreSQL)
```

| 自動化項目 | 說明 | 排程 |
|-----------|------|------|
| CI/CD 部署 | push to `main` 自動觸發 Cloud Build | 即時 |
| ETL 資料更新 | Cloud Scheduler 觸發 ETL Job | 週一至週五 21:30 (台北) |
| API 擴展 | Cloud Run 自動擴展 0-2 instances | 依流量 |

> **無需人工介入**：程式碼推送後自動部署，每日收盤後自動更新資料。

## 功能特色

### 資料擷取
- **三大法人買賣超**：每日從 TWSE/TPEX 擷取外資、投信、自營商買賣超資料
- **外資持股比例**：官方外資持股統計（MI_QFIIS / QFII）
- **股價資料**：每日 OHLCV 收盤資料
- **券商分點**：主力券商進出明細（透過 Playwright 爬蟲）

### 分析功能
- **技術指標**：MA、RSI、MACD、KD、支撐壓力
- **法人動向**：5/20/60/120 日持股變化追蹤
- **策略排行**：外資連買、投信認養、三大法人同步、乖離過大
- **AI 智能分析**：GPT-4o-mini 提供市場摘要、智能選股、個股分析

### 系統架構
- **後端**：FastAPI REST API
- **資料庫**：PostgreSQL (Cloud SQL)
- **部署**：GCP Cloud Run + Cloud Build
- **排程**：Cloud Scheduler + Cloud Run Jobs
- **AI**：OpenAI GPT-4o-mini

## 快速開始

### 本地開發

```bash
# 複製專案
git clone https://github.com/aiinpocket/tw-institutional-stocker.git
cd tw-institutional-stocker

# 安裝依賴
pip install -r requirements-etl.txt   # ETL 相關
pip install -r requirements-api.txt   # API 相關
playwright install chromium            # 券商爬蟲需要

# 設定環境變數
export DATABASE_URL=postgresql://user:password@localhost:5432/tw_stocker
export OPENAI_API_KEY=sk-xxx  # AI 功能需要

# 執行 ETL
python -m src.etl.run_all

# 啟動 API 服務
uvicorn src.api.main:app --reload
```

### Docker Compose 部署

```bash
# 啟動所有服務
docker-compose up -d

# 查看日誌
docker-compose logs -f api
docker-compose logs -f etl-worker

# 手動執行 ETL
docker-compose exec etl-worker python -m src.etl.run_all
```

## API 端點

### 股票資料
| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/stocks` | 股票列表（支援篩選） |
| GET | `/api/v1/stocks/{code}` | 股票詳情 |
| GET | `/api/v1/stocks/{code}/institutional` | 法人歷史資料 |
| GET | `/api/v1/stocks/{code}/prices` | 股價歷史 |

### 價格與排行
| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/prices/latest` | 最新股價 |
| GET | `/api/v1/rankings/{window}` | 法人持股變化排行（5/20/60/120日） |
| GET | `/api/v1/institutional/flows` | 法人買賣超明細 |

### 分析功能
| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/analysis/{code}` | 股票技術分析 |
| GET | `/api/v1/industry/summary` | 產業資金流向 |
| GET | `/api/v1/brokers/trades` | 券商分點交易 |

### AI 智能分析
| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/ai/market-summary` | AI 市場摘要 |
| GET | `/api/v1/ai/recommendations` | AI 智能選股 |
| GET | `/api/v1/ai/stock/{code}` | AI 個股分析 |
| GET | `/api/v1/ai/compare?codes=2330,2317` | AI 股票比較 |

### 策略排行
| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/strategy/consecutive-buying` | 外資連續買超 |
| GET | `/api/v1/strategy/trust-adoption` | 投信認養股 |
| GET | `/api/v1/strategy/three-way-sync` | 三大法人同步 |
| GET | `/api/v1/strategy/deviation` | 乖離過大股 |

## 資料來源

| 來源 | 端點 | 資料類型 |
|------|------|----------|
| TWSE 證交所 | `/exchangeReport/MI_INDEX` | 上市三大法人買賣超 |
| TWSE 證交所 | `/fund/MI_QFIIS` | 上市外資持股 |
| TWSE 證交所 | `/openapi/v1/exchangeReport/STOCK_DAY_ALL` | 上市股價 |
| TPEX 櫃買中心 | `/web/stock/3itrade/3itrade_hedge.php` | 上櫃三大法人買賣超 |
| TPEX 櫃買中心 | `/web/stock/exright/QFII.php` | 上櫃外資持股 |
| TPEX 櫃買中心 | `/openapi/v1/tpex_mainboard_quotes` | 上櫃股價 |

## 目錄結構

```
tw-institutional-stocker/
├── src/
│   ├── common/           # 共用模組（設定、資料庫、模型）
│   ├── etl/              # ETL 資料處理
│   │   ├── fetchers/     # 資料擷取器
│   │   ├── processors/   # 資料處理器
│   │   └── loaders/      # 資料載入器
│   └── api/              # FastAPI 應用
│       ├── routes/       # API 路由
│       ├── schemas/      # Pydantic 模型
│       └── static/       # 靜態網頁
├── docker/               # Docker 設定檔
├── gcp/                  # GCP 部署設定
├── cloudbuild.yaml       # Cloud Build CI/CD 設定
├── requirements-*.txt    # Python 依賴
└── docker-compose.yml    # Docker Compose 設定
```

## GCP 部署

詳細部署說明請參考 [gcp/README.md](gcp/README.md)。

### 環境變數 / Secrets

| 變數 | 說明 | 儲存方式 |
|------|------|----------|
| `DB_HOST` | Cloud SQL 連線路徑 | 環境變數 |
| `DB_NAME` | 資料庫名稱 | 環境變數 |
| `DB_USER` | 資料庫使用者 | 環境變數 |
| `DB_PASSWORD` | 資料庫密碼 | Secret Manager |
| `OPENAI_API_KEY` | OpenAI API 金鑰 | Secret Manager |

### GCP 資源清單

| 資源類型 | 名稱 | 說明 |
|---------|------|------|
| Cloud Run Service | `tw-stocker-api` | API 服務 |
| Cloud Run Job | `tw-stocker-etl` | ETL 排程任務 |
| Cloud SQL | `tw-stocker-db` | PostgreSQL 資料庫 |
| Cloud Scheduler | `tw-stocker-etl-schedule` | ETL 排程觸發 |
| Cloud Build Trigger | `tw-institutional-stocker` | CI/CD 自動部署 |
| Secret Manager | `db-password`, `openai-api-key` | 機密資料 |

## 授權條款

本專案採用 [Apache License 2.0](LICENSE) 授權。

## 致謝

本專案基於 [voidful/tw-institutional-stocker](https://github.com/voidful/tw-institutional-stocker) 原始專案進行開發與擴充。

原始專案由 [voidful](https://github.com/voidful) 開發，提供了三大法人持股追蹤的核心概念與基礎架構，本專案在此基礎上新增了：
- PostgreSQL 資料庫支援
- FastAPI REST API 服務
- 互動式網頁分析介面
- 券商分點資料擷取
- 技術指標分析功能
- AI 智能分析（GPT-4o-mini）
- GCP Cloud Run 雲端部署
- 完全自動化 CI/CD 與 ETL 排程

感謝原作者 voidful 的貢獻！

## 免責聲明

本系統僅供學術研究與個人投資參考使用，不構成任何投資建議。投資人應自行判斷並承擔投資風險。資料來源為公開資訊，但不保證資料的即時性與正確性。
