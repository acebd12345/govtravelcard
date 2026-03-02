# GovTravel Cloud Project

整合 Google Maps API、Google Gemini AI 與 Google Cloud Storage 的旅遊資訊爬蟲、清洗、增強與地圖展示專案。

---

## 專案結構

```
travelcardbackend/
├── data_pipeline_gemini.py      # 主管線：爬蟲 → 清洗 → Geocoding → Gemini AI 增強
├── pipeline_config.py           # 設定檔：城市/行政區/行業代碼、同義詞、價格級距
├── merge_data.py                # 合併工具：將 outputs/ 批次檔整合為 final_data.parquet
├── run_parallel.py              # 平行執行工具：多進程爬蟲 (跨平台, Windows/Linux/Mac)
├── sheet_sync.py                # Google Sheet 雙向同步 (匯出/匯入)
├── main.py                      # FastAPI Web 應用 (Admin Dashboard + 排程 API)
├── templates/
│   └── admin.html               # 排程管理介面
├── cloud_scheduler_handler.py   # Cloud Scheduler HTTP 入口 (dispatch/scrape/merge)
├── schedule_manager.py          # 排程配置管理工具
├── requirements.txt             # Python 依賴 (Cloud Run / 本地開發)
├── cloudfunctions_requirements.txt  # Cloud Functions 依賴 (精簡版)
├── Dockerfile                   # Docker 映像 (Cloud Run 用)
├── .dockerignore                # Docker 排除清單
├── .gitignore                   # Git 排除清單
├── deploy_to_gcp.sh             # 一鍵 GCP 部署腳本
├── .env.gcp                     # GCP 環境變數範本
└── README_GCP.md                # GCP 部署指南
```

---

## 快速開始 (本地開發)

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 設定環境變數

複製 `.env.example` 為 `.env` 並填入：

```ini
BUCKET_NAME=your-gcs-bucket-name
GOOGLE_API_KEY=your-google-maps-and-gemini-api-key
SHEET_URL=your-google-sheet-url          # 若需同步功能
```

將 Service Account 金鑰檔案命名為 `service_account.json` 放在專案根目錄。

### 3. 執行資料管線

單次執行特定區域與行業別：

```bash
# 爬取台北市(001) 士林區(111) 的旅宿業(0009)
python data_pipeline_gemini.py --city 001 --zip 111 --industry 0009

# 多區域用逗號分隔
python data_pipeline_gemini.py --city 001 --zip 111,103,106 --industry 0009
```

平行執行多個區域 (自動控制並發數量)：

```bash
python run_parallel.py
```

`TARGET_INDUSTRY` 可透過環境變數覆寫：

```bash
TARGET_INDUSTRY=0009 python run_parallel.py
```

### 4. 合併資料

所有爬蟲完成後，整合批次檔案：

```bash
python merge_data.py
```

產出 `outputs/final_data.parquet` 並上傳至 GCS。

### 5. Google Sheet 同步

```bash
# 互動式選單
python sheet_sync.py

# 指令模式
python sheet_sync.py export final   # 匯出最終資料到 Sheet
python sheet_sync.py import final   # 從 Sheet 匯回資料
python sheet_sync.py export geo     # 匯出 Geocoding 快取
python sheet_sync.py export all     # 匯出所有資料集
```

### 6. 啟動 Admin Dashboard (本地)

```bash
uvicorn main:app --host 0.0.0.0 --port 8080
```

瀏覽 `http://localhost:8080/admin` 管理排程。

---

## 資料管線流程

```
1. 爬蟲 (Scraper)
   travel.nccc.com.tw → 原始店家資料 (名稱、地址、電話、行業)

2. 清洗 (Cleaner)
   地址正規化、電話格式統一、去重

3. Geocoding
   Google Maps API → 經緯度 (含 GCS 快取)

4. 隱藏標籤 (Hidden Tags)
   rapidfuzz 模糊比對 → 同義詞標籤 (如 "星巴克" → "starbucks")

5. Gemini AI 增強
   Gemini API + Google Search → 評論、星級、價格區間

6. 輸出
   outputs/final_{city}_{zip}_{ind}.parquet → GCS 上傳
```

---

## 資料欄位說明

`final_data.parquet` 輸出欄位：

| 欄位 | 說明 | 範例 |
|:---|:---|:---|
| `id` | 唯一識別碼 | `001_111_0009_00001` |
| `name` | 店家名稱 | `台北老爺大酒店` |
| `ind` | 行業別 | `旅宿業` |
| `city` | 縣市 | `台北市` |
| `district` | 行政區 | `士林區` |
| `address` | 完整地址 (不含樓層) | `中山北路二段37-1號` |
| `floor` | 樓層資訊 | `2F` |
| `lat` | 緯度 | `25.0654` |
| `lng` | 經度 | `121.5244` |
| `phone` | 電話 | `(02)25422266` |
| `review_summary` | AI 生成評論 (80-100 字) | `位於中山北路，交通便利...` |
| `rating` | 星級評分 | `4.5/5` |
| `price_level` | 價格區間 | `3000-5000` 或 `中價位` |
| `hidden_tags` | 搜尋標籤 (逗號分隔) | `starbucks,louisa` |

---

## 價格級距設定

在 `pipeline_config.py` 的 `PRICE_THRESHOLDS` 中設定：

```python
PRICE_THRESHOLDS = {
    "旅宿業": [1500, 3000, 5000, 8000],
    # Level 1 ($)    : < 1500
    # Level 2 ($$)   : 1500 - 3000
    # Level 3 ($$$)  : 3000 - 5000
    # Level 4 ($$$$) : 5000 - 8000
    # Level 5 ($$$$$): > 8000
    "default": [200, 500, 1000, 2000]
}
```

---

## 支援的城市與行政區

| 城市代碼 | 城市 | 行政區 (12 區) |
|:---|:---|:---|
| `001` | 台北市 | 士林、大同、大安、中山、中正、內湖、文山、北投、松山、信義、南港、萬華 |

行業別代碼見 `pipeline_config.py` 中 `INDUSTRY_CODES`，包含旅宿、餐飲、旅行、交通、觀光遊樂等 15 個類別。

---

## GCP 雲端部署

本專案支援一鍵部署到 Google Cloud Platform，詳見 **[README_GCP.md](README_GCP.md)**。

```bash
bash deploy_to_gcp.sh YOUR-PROJECT-ID asia-east1
```

部署後系統會自動：
- Cloud Scheduler 每日凌晨 2:00 觸發爬蟲
- Cloud Functions 平行處理各行政區 (Master-Worker 架構)
- 凌晨 4:00 自動合併所有資料碎片
- Cloud Run 提供 Admin Dashboard

---

## 開發者注意事項

- **跨平台相容**：`run_parallel.py` 使用 `subprocess.Popen.poll()` 取代 Unix-only 的 `os.wait()`，Windows/Linux/Mac 均可運行。
- **暫存路徑**：所有暫存檔使用 `tempfile.gettempdir()` 而非硬編碼 `/tmp`，確保跨平台相容。
- **地址修正**：Geocoding 時自動補全「縣市」與「行政區」以提高準確度。
- **並行安全**：使用 file lock + atomic write，多進程同時執行不會衝突。
- **安全性**：`gcp-sa-key.json`、`.env.gcp`、`service_account.json` 均已加入 `.gitignore`，不會被提交。
- **Gemini SDK**：使用 `google-genai` (新版 SDK)，非舊版 `google-generativeai`。
- **Sheets 認證**：使用 `google.oauth2.service_account.Credentials`，非已棄用的 `oauth2client`。
