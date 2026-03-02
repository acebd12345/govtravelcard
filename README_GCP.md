# GCP 部署指南 - GovTravel

本文件說明如何將 GovTravel 專案部署到 Google Cloud Platform，實現全自動化的資料管線。

---

## 系統架構

```
┌──────────────┐   HTTP    ┌──────────────────┐
│  使用者瀏覽器  │ ──────▶  │   Cloud Run       │
│  (Admin UI)  │ ◀──────  │   govtravel-admin │
└──────────────┘          │   /admin /health  │
                          └────────┬─────────┘
                                   │ 讀取
                                   ▼
                          ┌──────────────────┐
                          │  Cloud Storage    │
                          │  (Parquet 檔案)   │
                          └──────▲───────────┘
                                 │ 寫入
              ┌──────────────────┴─────────────────────┐
              │                                        │
   ┌──────────┴─────────┐              ┌───────────────┴──────┐
   │  Cloud Scheduler    │  HTTP POST   │  Cloud Functions     │
   │  每日 02:00 觸發     │ ───────────▶ │  scheduled-pipeline  │
   └────────────────────┘              └───────────┬──────────┘
                                                   │
                                        ┌──────────┴──────────┐
                                        │   Cloud Tasks        │
                                        │   (平行派工佇列)      │
                                        └──────────┬──────────┘
                                                   │ 觸發多個 Worker
                                        ┌──────────▼──────────┐
                                        │  Worker Functions    │
                                        │  (每區一個 Worker)    │
                                        └─────────────────────┘
```

### 運作模式

| 模式 | 說明 | 觸發方式 |
|:---|:---|:---|
| `dispatch` | Master Job：拆分任務到 Cloud Tasks 佇列 | Cloud Scheduler → Cloud Function |
| `scrape` | Worker：執行單一行政區的爬蟲 + AI 處理 | Cloud Tasks → Cloud Function |
| `merge` | 合併所有碎片為 `final_data.parquet` | Cloud Scheduler → Cloud Function |

---

## 部署前準備

### 必備條件

- Google Cloud 帳號，已建立 Project
- Google Maps API Key (Google Cloud Console)
- Gemini API Key (Google AI Studio)
- 本機已安裝 `gcloud` CLI

### 安裝 gcloud CLI

```bash
# macOS
brew install google-cloud-cli

# Windows
# 下載安裝: https://cloud.google.com/sdk/docs/install

# 登入
gcloud auth login
gcloud config set project YOUR-PROJECT-ID
```

---

## 一鍵部署

```bash
bash deploy_to_gcp.sh YOUR-PROJECT-ID asia-east1
```

腳本會自動執行以下步驟：

| 步驟 | 說明 |
|:---|:---|
| 1 | 設定 GCP Project |
| 2 | 建立 Cloud Storage Bucket (`govtravel-{PROJECT_ID}-data`) |
| 3 | 建立 Cloud Tasks 佇列 (`data-pipeline-queue`) |
| 4 | 建立 Service Account (`govtravel-app`) |
| 5 | 授予 IAM 權限 (Storage, Logging, Tasks, Scheduler) |
| 6 | 產生 Service Account 金鑰 |
| 7 | 上傳 API Key 到 Secret Manager |
| 8 | 建立 Cloud Scheduler Job (每日 02:00) |
| 9 | 部署 Cloud Function (`scheduled-pipeline`) |
| 10 | 部署 Cloud Run (`govtravel-admin`) |

---

## 預設排程

| 名稱 | 時間 | 模式 | 內容 |
|:---|:---|:---|:---|
| `govtravel-daily-pipeline` | 每日 02:00 (台北時間) | dispatch | 台北市全區，自動派工至 Cloud Tasks |
| `merge-daily-job` | 每日 04:00 (台北時間) | merge | 合併所有碎片檔案 |

### 自訂排程

```bash
python schedule_manager.py init      # 初始化預設排程配置
python schedule_manager.py custom    # 互動式建立新排程
python schedule_manager.py list      # 列出已儲存的排程
```

或透過 Admin Dashboard (`/admin`) 在網頁上管理排程。

---

## 環境變數

部署腳本會自動設定以下環境變數：

| 變數 | 說明 | 設定方式 |
|:---|:---|:---|
| `GCP_PROJECT` | GCP Project ID | deploy 腳本自動設定 |
| `GCP_REGION` | 部署區域 | deploy 腳本自動設定 |
| `BUCKET_NAME` | GCS Bucket 名稱 | deploy 腳本自動設定 |
| `GOOGLE_API_KEY` | Maps + Gemini API Key | Secret Manager |
| `SERVICE_ACCOUNT_EMAIL` | Service Account 信箱 | deploy 腳本自動設定 |
| `FUNCTION_URL` | Cloud Function URL (選填) | 手動設定，用於 dispatch 模式的自我呼叫 |

### FUNCTION_URL 說明

`cloud_scheduler_handler.py` 在 dispatch 模式中會呼叫自身 URL 來建立 Cloud Tasks。預設使用 `request.url`，但某些情況下 (如透過 proxy 或 load balancer) 可能需要手動設定：

```bash
gcloud functions deploy scheduled-pipeline \
  --update-env-vars="FUNCTION_URL=https://REGION-PROJECT.cloudfunctions.net/scheduled-pipeline"
```

---

## 部署後驗證

```bash
# 1. 檢查服務狀態
gcloud functions list --region=asia-east1
gcloud run services list --region=asia-east1
gcloud scheduler jobs list --location=asia-east1

# 2. 測試 Cloud Run 健康端點
ADMIN_URL=$(gcloud run services describe govtravel-admin --region=asia-east1 --format='value(status.url)')
curl $ADMIN_URL/health

# 3. 手動觸發一次爬蟲測試
gcloud scheduler jobs run govtravel-daily-pipeline --location=asia-east1

# 4. 查看執行日誌
gcloud functions logs read scheduled-pipeline --limit=50 --region=asia-east1
```

---

## 新增的 GCP 檔案

| 檔案 | 說明 |
|:---|:---|
| `cloud_scheduler_handler.py` | Cloud Function 入口 (支援 dispatch/scrape/merge 三種模式) |
| `schedule_manager.py` | 排程配置 CLI 工具 |
| `deploy_to_gcp.sh` | 一鍵部署 Bash 腳本 |
| `Dockerfile` | Cloud Run Docker 映像 (Python 3.10 + FastAPI) |
| `.dockerignore` | Docker build 排除清單 (金鑰、env、parquet 等) |
| `cloudfunctions_requirements.txt` | Cloud Functions 精簡依賴 |
| `.env.gcp` | GCP 環境變數範本 |
| `main.py` | FastAPI Admin Dashboard + 排程 CRUD API |
| `templates/admin.html` | 排程管理 Web UI |

---

## 資源位置

| 資源 | 位置 |
|:---|:---|
| 爬蟲資料 | `gs://govtravel-{PROJECT_ID}-data/` |
| 碎片檔案 | `gs://govtravel-{PROJECT_ID}-data/fragments/` |
| 最終資料 | `gs://govtravel-{PROJECT_ID}-data/final_data.parquet` |
| Geocoding 快取 | `gs://govtravel-{PROJECT_ID}-data/geocoding_cache.parquet` |
| Admin UI | `https://govtravel-admin-XXXXX.run.app/admin` |
| Cloud Function | `https://{REGION}-{PROJECT_ID}.cloudfunctions.net/scheduled-pipeline` |
| 日誌 | Cloud Logging Console |

---

## 費用估算

| 服務 | 免費額度 | 預期月用量 | 預期費用 |
|:---|:---|:---|:---|
| Cloud Scheduler | 3 個免費 Job | 2 個 Job | **$0** |
| Cloud Functions | 200 萬次呼叫 | ~30 次 | **$0** |
| Cloud Run | 180 萬 vCPU-秒 | ~36,000 秒 | **$0** |
| Cloud Storage | 5 GB | ~1 GB | **$0** |
| Cloud Tasks | 100 萬次 | ~360 次 | **$0** |
| Cloud Logging | 50 GB | ~0.1 GB | **$0** |
| **合計** | | | **$0** |

在免費額度範圍內，預期月費用為零。

---

## 常見問題

### Permission denied

```bash
gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
  --member=serviceAccount:govtravel-app@YOUR-PROJECT-ID.iam.gserviceaccount.com \
  --role=roles/editor
```

### Cloud Function 超時

預設 timeout 為 3600 秒 (1 小時)。若仍不夠：

```bash
gcloud functions deploy scheduled-pipeline \
  --timeout=3600 --memory=2048MB --region=asia-east1
```

建議改用 dispatch 模式拆分任務，每個 Worker 只處理一個行政區。

### Cloud Tasks 佇列不存在

```bash
gcloud tasks queues create data-pipeline-queue --location=asia-east1
```

### 本地測試 Cloud Function

```bash
pip install functions-framework
functions-framework --target=scheduled_pipeline --port=8081
# 另一個 terminal:
curl -X POST http://localhost:8081 \
  -H "Content-Type: application/json" \
  -d '{"mode":"scrape","config":{"city":"001","industries":["0009"],"districts":["111"]}}'
```

### 查看所有日誌

```bash
# Cloud Function 日誌
gcloud functions logs read scheduled-pipeline --limit=100 --region=asia-east1

# Cloud Run 日誌
gcloud run services logs read govtravel-admin --limit=100 --region=asia-east1

# 或使用 Cloud Logging Console:
# https://console.cloud.google.com/logs
```

---

## 安全性注意事項

- `gcp-sa-key.json`、`.env.gcp`、`service_account.json` 已加入 `.gitignore`，不會被提交到版本控制。
- `.dockerignore` 排除所有金鑰和環境變數檔案，不會被打包進 Docker 映像。
- API Key 透過 Secret Manager 管理，不寫入程式碼或環境變數檔案。
- Admin Dashboard 的排程列表使用安全的 DOM 操作 (textContent)，防止 XSS 攻擊。

