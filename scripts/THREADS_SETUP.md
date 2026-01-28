# Threads API 設定指南

本文件說明如何設定 Threads API 以啟用每日市場總結自動發文功能。

## 前置需求

1. Meta Developer 帳號
2. 要用來發文的 Threads 帳號

## 設定步驟

### Step 1: 建立 Meta App

1. 前往 [Meta for Developers](https://developers.facebook.com/)
2. 建立新應用程式，選擇「其他」→「消費者」
3. 在應用程式設定中，新增「Threads API」產品
4. 記下 **App ID** 和 **App Secret**

### Step 2: 設定 OAuth

1. 在應用程式設定中，前往「Threads API」→「設定」
2. 新增 OAuth Redirect URI: `https://stock-tw.aiinpocket.com/oauth/callback`
3. 確保已啟用以下權限：
   - `threads_basic`
   - `threads_content_publish`

### Step 3: 取得授權碼

1. 在瀏覽器中訪問以下 URL（替換 YOUR_APP_ID）：

```
https://threads.net/oauth/authorize?client_id=YOUR_APP_ID&redirect_uri=https://stock-tw.aiinpocket.com/oauth/callback&scope=threads_basic,threads_content_publish&response_type=code
```

2. 使用你的 Threads 帳號授權
3. 授權成功後，你會被重導向到 callback 頁面，複製頁面上顯示的授權碼

### Step 4: 換取 Access Token

在本地執行以下 Python 腳本（不要提交到 git）：

```python
# scripts/setup_threads_token.py (不要提交到 git)
import requests
from datetime import datetime, timedelta

# 填入你的資訊
APP_ID = "YOUR_APP_ID"
APP_SECRET = "YOUR_APP_SECRET"
AUTH_CODE = "YOUR_AUTHORIZATION_CODE"
REDIRECT_URI = "https://stock-tw.aiinpocket.com/oauth/callback"

# Step 1: 換取短期 token
resp = requests.post("https://graph.threads.net/oauth/access_token", data={
    "client_id": APP_ID,
    "client_secret": APP_SECRET,
    "grant_type": "authorization_code",
    "redirect_uri": REDIRECT_URI,
    "code": AUTH_CODE
})
data = resp.json()
print(f"Short-lived token response: {data}")
short_token = data["access_token"]
user_id = data["user_id"]

# Step 2: 換取長期 token
resp = requests.get("https://graph.threads.net/access_token", params={
    "grant_type": "th_exchange_token",
    "client_secret": APP_SECRET,
    "access_token": short_token
})
data = resp.json()
print(f"Long-lived token response: {data}")
long_token = data["access_token"]
expires_in = data.get("expires_in", 5184000)
expires_at = datetime.now() + timedelta(seconds=expires_in)

print(f"\n=== 設定資訊 ===")
print(f"User ID: {user_id}")
print(f"Long-lived Token: {long_token}")
print(f"Expires at: {expires_at}")

# 產生 SQL
print(f"\n=== 執行以下 SQL 儲存 token ===")
print(f"""
INSERT INTO social_tokens (platform, user_id, access_token, expires_at, scopes)
VALUES ('threads', '{user_id}', '{long_token}', '{expires_at.isoformat()}', 'threads_basic,threads_content_publish')
ON CONFLICT (platform, user_id) DO UPDATE SET
    access_token = EXCLUDED.access_token,
    expires_at = EXCLUDED.expires_at,
    updated_at = CURRENT_TIMESTAMP;
""")
```

### Step 5: 儲存 Token 到資料庫

執行上面腳本產生的 SQL，將 token 存入 GCP Cloud SQL：

```bash
# 連接到 Cloud SQL
gcloud sql connect tw-stocker-db --user=postgres --database=tw_stocker

# 貼上並執行 SQL
```

### Step 6: 設定 GCP Secrets

```bash
# 建立 Threads App Secret
gcloud secrets create threads-app-secret --project=tw-stocker-20241201
echo -n "YOUR_APP_SECRET" | gcloud secrets versions add threads-app-secret --data-file=-
```

### Step 7: 更新 ETL Job 環境變數

```bash
gcloud run jobs update tw-stocker-etl --region=asia-east1 \
  --set-env-vars="THREADS_APP_ID=YOUR_APP_ID,THREADS_USER_ID=YOUR_USER_ID,THREADS_PUBLISH_ENABLED=true" \
  --set-secrets="THREADS_APP_SECRET=threads-app-secret:latest"
```

或者更新 `cloudbuild.yaml` 後重新部署。

## 驗證

1. 手動執行 ETL：
```bash
gcloud run jobs execute tw-stocker-etl --region=asia-east1 --wait
```

2. 檢查日誌是否有 "Market summary published to Threads" 訊息

3. 確認 Threads 帳號有新貼文

## 疑難排解

### Token 過期
系統會在 token 過期前 7 天自動刷新。如果刷新失敗，需要重新執行 Step 3-5。

### 發文失敗
檢查 ETL 日誌中的錯誤訊息。常見原因：
- Token 無效或過期
- 權限不足
- API 限流（每小時 200 次）

### 關閉功能
設定 `THREADS_PUBLISH_ENABLED=false` 即可暫時關閉發文功能。
