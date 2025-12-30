# 台股法人持股追蹤系統

台灣股市三大法人（外資、投信、自營商）持股比重追蹤與分析系統。提供每日自動化 ETL 資料擷取、REST API 服務、以及互動式網頁分析介面。

## 功能特色

### 資料擷取
- **三大法人買賣超**：每日從 TWSE/TPEX 擷取外資、投信、自營商買賣超資料
- **外資持股比例**：官方外資持股統計（MI_QFIIS / QFII）
- **股價資料**：每日 OHLCV 收盤資料
- **券商分點**：主力券商進出明細（透過 Playwright 爬蟲）

### 技術分析
- **均線分析**：MA5、MA10、MA20、MA60、MA120
- **技術指標**：RSI、MACD、KD
- **支撐壓力**：自動計算關鍵價位
- **法人動向**：5/20/60/120 日持股變化追蹤

### 系統架構
- **後端**：FastAPI REST API
- **資料庫**：PostgreSQL
- **部署**：GCP Cloud Run + Cloud SQL
- **排程**：Cloud Run Jobs（每日自動執行 ETL）

## 系統架構圖

```
┌─────────────────────────────────────────────────────────────┐
│                    GCP Cloud Run                             │
├─────────────┬─────────────────┬─────────────────────────────┤
│  Cloud SQL  │   ETL Job       │      FastAPI API            │
│ (PostgreSQL)│  (每日排程)      │     (REST 服務)             │
│             │                 │                             │
│  - stocks   │  每日 08:30 抓取:│  GET /api/v1/stocks        │
│  - flows    │  - 三大法人      │  GET /api/v1/prices        │
│  - holdings │  - 外資持股      │  GET /api/v1/rankings      │
│  - prices   │  - 股價成交量    │  GET /api/v1/analysis      │
│  - brokers  │  - 券商分點      │  GET /stock/{code}         │
└─────────────┴─────────────────┴─────────────────────────────┘
```

## 快速開始

### 環境需求
- Python 3.11+
- PostgreSQL 14+
- Docker & Docker Compose（可選）

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

| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/health` | 健康檢查 |
| GET | `/api/v1/stocks` | 股票列表（支援篩選） |
| GET | `/api/v1/stocks/{code}` | 股票詳情 |
| GET | `/api/v1/stocks/{code}/institutional` | 法人歷史資料 |
| GET | `/api/v1/stocks/{code}/prices` | 股價歷史 |
| GET | `/api/v1/prices/latest` | 最新股價 |
| GET | `/api/v1/rankings/{window}` | 法人持股變化排行（5/20/60/120日） |
| GET | `/api/v1/analysis/{code}` | 股票技術分析 |
| GET | `/api/v1/analysis/{code}/brokers` | 券商分點資料 |
| GET | `/stock/{code}` | 股票分析網頁 |

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
├── requirements-*.txt    # Python 依賴
└── docker-compose.yml    # Docker Compose 設定
```

## 持股估算模型

### 外資持股
直接使用官方 `foreign_ratio` 資料。

### 投信/自營商持股
採用基準點校正模型：

```python
# 若有基準點資料：
trust_shares_est = baseline_trust + cumsum(trust_net since baseline)
dealer_shares_est = baseline_dealer + cumsum(dealer_net since baseline)

# 無基準點時（退化模型）：
trust_shares_est = cumsum(trust_net)
dealer_shares_est = cumsum(dealer_net)
```

基準點資料格式（`data/inst_baseline.csv`）：
```csv
date,code,trust_shares_base,dealer_shares_base
2025-01-31,2330,100000000,20000000
2025-01-31,0050,50000000,0
```

## GCP 部署

詳細部署說明請參考 [gcp/README.md](gcp/README.md)。

### 環境變數設定

| 變數 | 說明 |
|------|------|
| `DB_HOST` | Cloud SQL 連線路徑 |
| `DB_NAME` | 資料庫名稱 |
| `DB_USER` | 資料庫使用者 |
| `DB_PASSWORD` | 資料庫密碼（建議使用 Secret Manager） |

## 授權條款

本專案採用 [Apache License 2.0](LICENSE) 授權。

## 致謝

本專案基於 [tw_institutional_stocker](https://github.com/aiinpocket/tw-institutional-stocker) 原始專案進行開發與擴充。

原始專案提供了三大法人持股追蹤的核心概念與基礎架構，本專案在此基礎上新增了：
- PostgreSQL 資料庫支援
- FastAPI REST API 服務
- 互動式網頁分析介面
- 券商分點資料擷取
- 技術指標分析功能
- GCP Cloud Run 雲端部署

感謝原作者的貢獻！

## 免責聲明

本系統僅供學術研究與個人投資參考使用，不構成任何投資建議。投資人應自行判斷並承擔投資風險。資料來源為公開資訊，但不保證資料的即時性與正確性。
