# GCP 部署指南

## 架構總覽

```
Cloud Scheduler ──▶ Cloud Run Jobs (ETL)
                           │
                           ▼
Cloud Run (API) ────▶ Cloud SQL (PostgreSQL)
```

## 預估成本 (~$7-10/月)

| 服務 | 規格 | 月費 |
|------|------|------|
| Cloud SQL | db-f1-micro (614MB RAM) | ~$7-9 |
| Cloud Run API | 256MB, scale to 0 | ~$0 |
| Cloud Run Jobs | ETL 每日一次 | ~$0.30 |
| Cloud Scheduler | 1 個排程 | ~$0.10 |
| **總計** | | **~$7-10** |

## 前置需求

1. GCP 帳號並啟用 Billing
2. 安裝 [gcloud CLI](https://cloud.google.com/sdk/docs/install)
3. 建立 GCP Project

## 快速部署

```bash
# 1. 設定環境變數
export PROJECT_ID=your-project-id
export REGION=asia-east1  # 台灣區域

# 2. 登入 GCP
gcloud auth login
gcloud config set project $PROJECT_ID

# 3. 執行部署腳本
chmod +x gcp/deploy.sh
./gcp/deploy.sh
```

## 手動部署步驟

### Step 1: 啟用必要的 API

```bash
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  sqladmin.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com
```

### Step 2: 建立 Cloud SQL (PostgreSQL)

```bash
# 建立 Cloud SQL 實例 (db-f1-micro 最便宜)
gcloud sql instances create tw-stocker-db \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region=$REGION \
  --storage-size=10GB \
  --storage-type=HDD \
  --no-assign-ip \
  --network=default

# 設定密碼
gcloud sql users set-password postgres \
  --instance=tw-stocker-db \
  --password=YOUR_SECURE_PASSWORD

# 建立資料庫
gcloud sql databases create tw_stocker --instance=tw-stocker-db
```

### Step 3: 建立 Secret (資料庫密碼)

```bash
echo -n "YOUR_SECURE_PASSWORD" | \
  gcloud secrets create db-password --data-file=-

# 授權 Cloud Run 存取
gcloud secrets add-iam-policy-binding db-password \
  --member="serviceAccount:$PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Step 4: 部署 Cloud Run API

```bash
# 建置並部署
gcloud run deploy tw-stocker-api \
  --source=. \
  --dockerfile=gcp/Dockerfile.api \
  --region=$REGION \
  --platform=managed \
  --allow-unauthenticated \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --add-cloudsql-instances=$PROJECT_ID:$REGION:tw-stocker-db \
  --set-env-vars="DB_HOST=/cloudsql/$PROJECT_ID:$REGION:tw-stocker-db" \
  --set-env-vars="DB_NAME=tw_stocker" \
  --set-env-vars="DB_USER=postgres" \
  --set-secrets="DB_PASSWORD=db-password:latest"
```

### Step 5: 部署 Cloud Run Jobs (ETL)

```bash
# 建立 ETL Job
gcloud run jobs create tw-stocker-etl \
  --source=. \
  --dockerfile=gcp/Dockerfile.etl \
  --region=$REGION \
  --memory=1Gi \
  --cpu=1 \
  --task-timeout=30m \
  --max-retries=1 \
  --add-cloudsql-instances=$PROJECT_ID:$REGION:tw-stocker-db \
  --set-env-vars="DB_HOST=/cloudsql/$PROJECT_ID:$REGION:tw-stocker-db" \
  --set-env-vars="DB_NAME=tw_stocker" \
  --set-env-vars="DB_USER=postgres" \
  --set-secrets="DB_PASSWORD=db-password:latest"
```

### Step 6: 設定 Cloud Scheduler

```bash
# 建立排程 (每日 21:30 台北時間)
gcloud scheduler jobs create http tw-stocker-etl-schedule \
  --location=$REGION \
  --schedule="30 21 * * 1-5" \
  --time-zone="Asia/Taipei" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/tw-stocker-etl:run" \
  --http-method=POST \
  --oauth-service-account-email=$PROJECT_ID@appspot.gserviceaccount.com
```

## 資料庫初始化

首次部署後，需要執行資料庫 migration：

```bash
# 手動執行一次 ETL job 來初始化
gcloud run jobs execute tw-stocker-etl --region=$REGION --wait
```

## 監控與日誌

```bash
# 查看 API 日誌
gcloud run services logs read tw-stocker-api --region=$REGION

# 查看 ETL 執行日誌
gcloud run jobs executions list --job=tw-stocker-etl --region=$REGION
gcloud run jobs logs read tw-stocker-etl --region=$REGION
```

## 備份策略

Cloud SQL 自動每日備份，保留 7 天。如需手動備份：

```bash
gcloud sql backups create --instance=tw-stocker-db
```

## 清理資源

```bash
# 刪除所有資源
gcloud run services delete tw-stocker-api --region=$REGION
gcloud run jobs delete tw-stocker-etl --region=$REGION
gcloud scheduler jobs delete tw-stocker-etl-schedule --location=$REGION
gcloud sql instances delete tw-stocker-db
gcloud secrets delete db-password
```
