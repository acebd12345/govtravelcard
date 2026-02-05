# GovTravel Cloud Project

這是一個整合 Google Maps API、Google Gemini AI 與 Google Cloud Storage 的旅遊資訊爬蟲、清洗、增強與地圖展示專案。

## 專案結構

*   **核心管線**:
    *   `data_pipeline_gemini.py`: 主程式。負責爬蟲、資料清洗、地址經緯度轉換 (Geocoding)，並呼叫 Gemini AI 進行資料增強 (評論、評分、隱藏標籤等)。
    *   `pipeline_config.py`: 設定檔。包含城市代碼、行政區代碼、行業別代碼以及同義詞對照表 (Synonyms)。
    *   `merge_data.py`: 合併工具。將分散在 `outputs/` 資料夾中的批次檔案合併為 `raw_data.parquet` 與 `final_data.parquet`，並上傳至 GCS。
    *   `run_parallel.py`: 平行執行工具。可同時啟動多個爬蟲進程以加速資料收集。

*   **資料同步與管理**:
    *   `sheet_sync.py`: Google Sheet 雙向同步工具。可將資料匯出至 Sheet 供人工校對，或將校對後的資料匯回系統。

*   **前端展示**:
    *   `app.py`: FastAPI 後端伺服器，提供搜尋 API 與地圖網頁服務。
    *   `index.html`: 前端地圖介面，視覺化展示店家位置、評分與資訊。

## 快速開始

### 1. 安裝相依套件
```bash
pip3 install -r requirements.txt
```

### 2. 設定環境變數
請複製 `.env.example` 為 `.env` 並填入以下資訊：
```ini
BUCKET_NAME=您的GCS儲存桶名稱
GOOGLE_API_KEY=您的GoogleMaps與Gemini金鑰
SHEET_URL=您的GoogleSheet網址 (若需同步功能)
```
並將您的 Service Account 金鑰檔案命名為 `service_account.json` 放在專案根目錄。

### 3. 執行資料爬蟲 (Data Pipeline)
單次執行特定區域與行業別：
```bash
# 範例：爬取台北市(001) 士林區(111) 的 旅宿業(0009)
python3 data_pipeline_gemini.py --city 001 --zip 111 --industry 0009
```
程式會自動將結果存入 `outputs/` 目錄。

若要平行執行多個區域：
```bash
python3 run_parallel.py
```

### 4. 合併資料
當爬蟲完成後，執行合併程式將所有批次檔案整合：
```bash
python3 merge_data.py
```
這會產生 `final_data.parquet` 並嘗試上傳至 GCS。

### 5. 啟動地圖網頁
```bash
python3 app.py
```
開啟瀏覽器訪問 `http://localhost:8080` 即可看到地圖。

---

## 資料欄位說明

最終產出的 `final_data.parquet` 包含以下欄位：

| 欄位名 | 說明 |
| :--- | :--- |
| `id` | 唯一識別碼 (格式: 城市_地區_行業_流水號) |
| `name` | 店家名稱 |
| `ind` | 行業別 (如: 旅宿業、餐飲業) |
| `city` | 縣市 |
| `district` | 行政區 |
| `address` | 完整地址 |
| `floor` | 樓層資訊 |
| `lat` | 緯度 |
| `lng` | 經度 |
| `phone` | 電話號碼 |
| `review_summary` | AI 生成的評論摘要 |
| `rating` | 星級評分 (如: 4.5/5) |
| `price_level` | 價格區間 |
| `hidden_tags` | 隱藏標籤 (用於搜尋關鍵字匹配) |

---

## Google Sheet 同步功能

您可以使用 `sheet_sync.py` 來管理資料：

```bash
# 啟動互動式選單
python3 sheet_sync.py

# 或使用指令模式
python3 sheet_sync.py export final  # 匯出最終資料到 Sheet
python3 sheet_sync.py import final  # 從 Sheet 匯回資料
python3 sheet_sync.py export geo    # 匯出經緯度快取 (用於修正座標)
```

## 如何調整價格級距 (Price Levels)

本專案支援根據不同行業別設定不同的價格篩選標準。設定檔位於 `pipeline_config.py` 中的 `PRICE_THRESHOLDS`。

範例：
```python
PRICE_THRESHOLDS = {
    "旅宿業": [1500, 3000, 5000, 8000],
    # 代表:
    # Level 1 ($)    : < 1500
    # Level 2 ($$)   : 1500 - 3000
    # Level 3 ($$$)  : 3000 - 5000
    # Level 4 ($$$$) : 5000 - 8000
    # Level 5 ($$$$$): > 8000
}
```
修改後請重新啟動 `app.py` 即可生效。

## 開發者注意事項

*   **地址修正**：`data_pipeline_gemini.py` 在查詢經緯度時會自動補全「縣市」與「行政區」以提高準確度。
*   **欄位調整**：若需修改行業別定義或同義詞，請編輯 `pipeline_config.py`。
*   **平行處理**：`data_pipeline_gemini.py` 支援 atomic write，可安全地多進程同時寫入不同檔案，不會發生衝突。
