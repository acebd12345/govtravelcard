import os
import io
import json
import glob
import pandas as pd
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage

# 載入 .env 檔案中的環境變數
load_dotenv()

# ================= 設定 =================
# 請將您的 Service Account 金鑰檔案命名為 service_account.json 並放在同目錄
KEY_FILE = "service_account.json"

# 自動設定 GOOGLE_APPLICATION_CREDENTIALS 以供 GCS 使用
if os.path.exists(KEY_FILE):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(KEY_FILE)

# 請填入您的 Google Sheet 網址或是 ID
SHEET_URL = os.getenv("SHEET_URL", "https://docs.google.com/spreadsheets/d/1Qx1uv17r7GxQWc6KYTWflqdfDk7HoR0hs1Nk3toB97M/edit?gid=0#gid=0") 

BUCKET_NAME = os.getenv("BUCKET_NAME")

# 定義檔案與工作表 (Worksheet) 的對應
TARGET_CONFIG = {
    "final": {
        "blob": "final_data.parquet",
        "sheet_name": "Final Data"
    },
    "geo": {
        "blob": "geocoding_cache.parquet",
        "sheet_name": "Geocoding Cache"
    },
    "raw": {
        "blob": "raw_data.parquet",
        "sheet_name": "Raw Data"
    }
}

# 全域變數，由 Main 設定
CURRENT_KEY = "final" 

# =======================================

def get_gspread_client():
    if not os.path.exists(KEY_FILE):
        print(f"[ERROR] 找不到 {KEY_FILE}。請確認您已將金鑰檔案放在目錄下。")
        return None
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)
    client = gspread.authorize(creds)
    return client

def get_worksheet(client, sheet_name, create_if_missing=False):
    """取得指定名稱的工作表，若不存在可選擇建立"""
    try:
        spreadsheet = client.open_by_url(SHEET_URL)
    except Exception as e:
        print(f"[ERROR] 無法開啟試算表: {e}")
        return None

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except gspread.WorksheetNotFound:
        if create_if_missing:
            print(f"[INFO] 工作表 '{sheet_name}' 不存在，正在建立...")
            # 建立新工作表 (預設 1000 列 20 欄)
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            return worksheet
        else:
            print(f"[ERROR] 找不到工作表 '{sheet_name}'。")
            return None

def get_dataframe(blob_name):
    """取得資料 (優先讀取本地，若無則從 GCS 下載)"""
    # 1. 嘗試讀取本地檔案
    if os.path.exists(blob_name):
        print(f"[INFO] Reading local file: {blob_name}")
        return pd.read_parquet(blob_name)
    
    # 2. 嘗試讀取 outputs/ 目錄下的檔案 (針對 final_data.parquet 等)
    output_path = os.path.join("outputs", blob_name)
    if os.path.exists(output_path):
        print(f"[INFO] Reading local file from outputs: {output_path}")
        return pd.read_parquet(output_path)

    # 3. 嘗試從 GCS 讀取
    if BUCKET_NAME:
        print(f"[INFO] Reading {blob_name} from bucket {BUCKET_NAME}...")
        try:
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                print(f"[WARN] 檔案 {blob_name} 不存在於 GCS。")
                return pd.DataFrame()

            data_bytes = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(data_bytes))
            return df
        except Exception as e:
            print(f"[WARN] GCS 讀取失敗: {e}")
            return pd.DataFrame()
    
    print(f"[WARN] 找不到檔案 {blob_name} 且未設定 BUCKET_NAME。")
    return pd.DataFrame()

def save_to_gcs(df, blob_name):
    """將 DataFrame 轉為 Parquet 並上傳 GCS"""
    print(f"[INFO] Saving to {blob_name}...")
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    # 轉為 parquet bytes
    with io.BytesIO() as bio:
        df.to_parquet(bio, index=False)
        bio.seek(0)
        blob.upload_from_file(bio)
    
    print("[INFO] Upload complete.")

def write_to_sheet(df, sheet_name):
    """將 DataFrame 寫入指定的 Google Sheet 工作表"""
    client = get_gspread_client()
    if not client: return

    if not SHEET_URL:
        print("[ERROR] 請在程式碼中設定 SHEET_URL")
        return

    # 取得或建立工作表
    sheet = get_worksheet(client, sheet_name, create_if_missing=True)
    if not sheet: return

    if df.empty:
        print(f"[WARN] 資料為空，不執行寫入 '{sheet_name}'。")
        return

    # 處理 NaN，Sheet 不接受 NaN
    df = df.fillna("")
    
    # 轉為 List of Lists
    # JSON compatible types only (Pandas timestamps -> str)
    df = df.astype(object) 
    # 特別處理 Timestamp
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].astype(str)

    data = [df.columns.values.tolist()] + df.values.tolist()
    
    print(f"[INFO] Writing {len(df)} rows to sheet '{sheet_name}'...")
    try:
        sheet.clear()
        sheet.update(data)
        print(f"[SUCCESS] Successfully exported to tab '{sheet_name}'.")
    except Exception as e:
        print(f"[ERROR] Failed to write to sheet '{sheet_name}': {e}")

def sync_to_sheet():
    """系統 -> Sheet"""
    config = TARGET_CONFIG[CURRENT_KEY]
    blob_name = config["blob"]
    sheet_name = config["sheet_name"]

    # 下載資料
    df = get_dataframe(blob_name)
    print(f"[INFO] Data loaded. Rows: {len(df)}")
    
    write_to_sheet(df, sheet_name)

def sync_outputs_folder():
    """將 outputs/ 資料夾下的所有 Parquet 檔同步到 Sheet"""
    output_dir = "outputs"
    if not os.path.exists(output_dir):
        print(f"[WARN] 目錄 {output_dir} 不存在。")
        return

    files = glob.glob(os.path.join(output_dir, "*.parquet"))
    files.sort() # 排序，讓上傳順序固定
    
    print(f"[INFO] Found {len(files)} files in {output_dir}")
    
    for f in files:
        # 檔名範例: final_001_111_0009.parquet
        filename = os.path.basename(f)
        sheet_name = filename.replace(".parquet", "")
        
        # 限制 Sheet 名稱長度 (Google Sheet 上限 100 字元，這裡取前 50 保險)
        if len(sheet_name) > 50:
            sheet_name = sheet_name[:50]
            
        print(f"\n>>> Processing {filename} -> '{sheet_name}'")
        try:
            df = pd.read_parquet(f)
            write_to_sheet(df, sheet_name)
        except Exception as e:
            print(f"[ERROR] Failed to process {filename}: {e}")

def sync_from_sheet():
    """Sheet -> 系統"""
    config = TARGET_CONFIG[CURRENT_KEY]
    blob_name = config["blob"]
    sheet_name = config["sheet_name"]

    client = get_gspread_client()
    if not client: return

    if not SHEET_URL:
        print("[ERROR] 請在程式碼中設定 SHEET_URL")
        return

    sheet = get_worksheet(client, sheet_name, create_if_missing=False)
    if not sheet: return

    print(f"[INFO] Reading from sheet '{sheet_name}'...")
    all_values = sheet.get_all_records()
    
    if not all_values:
        print("[WARN] Sheet is empty.")
        return

    new_df = pd.DataFrame(all_values)
    print(f"[INFO] Read {len(new_df)} rows from Sheet.")
    
    # 簡單的型態處理
    if 'lat' in new_df.columns:
        new_df['lat'] = pd.to_numeric(new_df['lat'], errors='coerce')
    if 'lng' in new_df.columns:
        new_df['lng'] = pd.to_numeric(new_df['lng'], errors='coerce')

    # Ensure phone/id is string
    for col in ['phone', 'id']:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str)
        
    save_to_gcs(new_df, blob_name)
    print(f"[SUCCESS] Imported from '{sheet_name}' to GCS '{blob_name}'.")

import sys

if __name__ == "__main__":
    action = None

    # 解析參數: python sheet_sync.py [export|import] [final|geo|enrich|raw|all]
    if len(sys.argv) > 1:
        arg1 = sys.argv[1].lower()
        if arg1 in ["export", "1"]: action = "export"
        elif arg1 in ["import", "2"]: action = "import"
        
        if len(sys.argv) > 2:
            arg2 = sys.argv[2].lower()
            if arg2 in TARGET_CONFIG or arg2 == "all": CURRENT_KEY = arg2
    
    # 互動模式
    if not action:
        print("=== Google Sheet Sync Tool (Multi-Tab Support) ===")
        print("請選擇要操作的檔案:")
        print("1. Final Data -> Sheet: 'Final Data'")
        print("2. Geocoding Cache -> Sheet: 'Geocoding Cache'")
        print("3. Raw Data -> Sheet: 'Raw Data'")
        print("4. Sync All (Process All Targets)")
        print("5. Sync Outputs Folder (Upload individual split files)")
        
        t_choice = input("請選擇檔案 (1-5): ").strip()
        if t_choice == "2": CURRENT_KEY = "geo"
        elif t_choice == "3": CURRENT_KEY = "raw"
        elif t_choice == "4": CURRENT_KEY = "all"
        elif t_choice == "5": CURRENT_KEY = "outputs"
        else: CURRENT_KEY = "final"
        
        if CURRENT_KEY == "all":
            print(f"\n目前選擇: All Main Targets")
        elif CURRENT_KEY == "outputs":
            print(f"\n目前選擇: All files in outputs/")
        else:
            target_info = TARGET_CONFIG[CURRENT_KEY]
            print(f"\n目前選擇: {target_info['blob']} <-> Sheet: {target_info['sheet_name']}")

        print("--------------------------------")
        print("1. 匯出: GCS -> Google Sheet")
        print("2. 匯入: Google Sheet -> GCS")
        
        a_choice = input("請選擇動作 (1/2): ").strip()
        if a_choice == "1": action = "export"
        elif a_choice == "2": action = "import"

    print(f"[INFO] Action: {action}, Target: {CURRENT_KEY}")

    if action == "export":
        if CURRENT_KEY == "all":
            for key in TARGET_CONFIG:
                print(f"\n>>> Starting Batch Export: {key} <<<")
                CURRENT_KEY = key
                sync_to_sheet()
        elif CURRENT_KEY == "outputs":
            sync_outputs_folder()
        else:
            sync_to_sheet()
    elif action == "import":
        if CURRENT_KEY == "all":
            for key in TARGET_CONFIG:
                print(f"\n>>> Starting Batch Import: {key} <<<")
                CURRENT_KEY = key
                sync_from_sheet()
        else:
            sync_from_sheet()
    else:
        print("無效操作或已取消")
