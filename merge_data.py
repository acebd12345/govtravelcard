import os
import glob
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from pipeline_config import CITIES, ZIP_CODES, INDUSTRY_CODES

load_dotenv()

OUTPUT_DIR = "outputs"
FINAL_BLOB_NAME = "final_data.parquet"
RAW_BLOB_NAME = "raw_data.parquet"
BUCKET_NAME = os.getenv("BUCKET_NAME")

# 設定 Service Account 金鑰 (如果存在)
KEY_FILE = "service_account.json"
if os.path.exists(KEY_FILE):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(KEY_FILE)
    print(f"[INFO] Set GOOGLE_APPLICATION_CREDENTIALS to {KEY_FILE}")

def merge_and_upload():
    if not os.path.exists(OUTPUT_DIR):
        print(f"Error: {OUTPUT_DIR} does not exist.")
        return

    # 1. Merge Raw Data
    raw_files = glob.glob(os.path.join(OUTPUT_DIR, "raw_*.parquet"))
    if raw_files:
        print(f"[INFO] Found {len(raw_files)} raw data files.")
        raw_dfs = []
        for f in raw_files:
            try:
                df = pd.read_parquet(f)
                
                # 從檔名解析 Meta Data
                filename = os.path.basename(f)
                parts = filename.replace("raw_", "").replace(".parquet", "").split("_")
                
                if len(parts) >= 3:
                    city_code, zip_code, ind_code = parts[0], parts[1], parts[2]
                    df['city'] = CITIES.get(city_code, city_code)
                    df['district'] = ZIP_CODES.get(zip_code, zip_code)
                    df['industry'] = INDUSTRY_CODES.get(ind_code, ind_code)
                
                raw_dfs.append(df)
            except Exception as e:
                print(f"  [ERROR] Failed to read raw file {f}: {e}")
                
        if raw_dfs:
            merged_raw = pd.concat(raw_dfs, ignore_index=True)
            print(f"[INFO] Merged raw data shape: {merged_raw.shape}")
            merged_raw.to_parquet(RAW_BLOB_NAME, index=False)
            print(f"[INFO] Saved merged raw data to {RAW_BLOB_NAME}")
    else:
        print("[WARN] No raw data files found.")

    # 2. Merge Final Data
    final_files = glob.glob(os.path.join(OUTPUT_DIR, "final_*.parquet"))
    if final_files:
        print(f"[INFO] Found {len(final_files)} final data files.")
        final_dfs = []
        for f in final_files:
            try:
                df = pd.read_parquet(f)
                
                # 從檔名解析 Meta Data
                # 檔名格式: final_{city_code}_{zip_code}_{ind_code}.parquet
                filename = os.path.basename(f)
                parts = filename.replace("final_", "").replace(".parquet", "").split("_")
                
                if len(parts) >= 3:
                    city_code, zip_code, ind_code = parts[0], parts[1], parts[2]
                    
                    city_name = CITIES.get(city_code, city_code)
                    zip_name = ZIP_CODES.get(zip_code, zip_code)
                    ind_name = INDUSTRY_CODES.get(ind_code, ind_code)
                    
                    # 補上欄位 (若已存在則覆蓋，確保一致性)
                    df['city'] = city_name
                    df['district'] = zip_name  # 行政區
                    # df['industry'] = ind_name  # 行業別 (User requested to remove this)
                
                print(f"  -> Loading {filename}: {len(df)} records")
                final_dfs.append(df)
            except Exception as e:
                print(f"  [ERROR] Failed to read {f}: {e}")

        if not final_dfs:
            print("[WARN] No valid final data loaded.")
            return

        merged_final = pd.concat(final_dfs, ignore_index=True)
        print(f"[INFO] Merged raw count (before dedup): {len(merged_final)}")
        
        # 修正：不能使用 'id' 去重，因為不同批次的 id 會重複 (都是從 0 開始)
        # 改用 'name' 和 'phone' 組合去重，或者 'name' 和 'address'
        dedup_cols = []
        if 'name' in merged_final.columns and 'phone' in merged_final.columns:
            dedup_cols = ['name', 'phone']
        elif 'name' in merged_final.columns and 'address' in merged_final.columns:
            dedup_cols = ['name', 'address']
            
        if dedup_cols:
            before_dedup = len(merged_final)
            # 移除電話或地址為空的重複項可能會有風險，這裡假設資料已有基本品質
            merged_final.drop_duplicates(subset=dedup_cols, keep='first', inplace=True)
            after_dedup = len(merged_final)
            print(f"[INFO] Dedup by {dedup_cols}: {before_dedup} -> {after_dedup} (Removed {before_dedup - after_dedup})")
        
        # 重新產生全域唯一的 ID (如果需要)
        # merged_final['id'] = range(len(merged_final))

        # 顯示各地區統計 (幫助除錯)
        if 'district' in merged_final.columns:
            print("\n[INFO] Records per district:")
            print(merged_final['district'].value_counts())
            print("")

        merged_final.to_parquet(FINAL_BLOB_NAME, index=False)
        print(f"[INFO] Saved merged final data to {FINAL_BLOB_NAME} ({len(merged_final)} records)")
    else:
        print("[WARN] No final data files found.")

    # 3. Upload to GCS
    if BUCKET_NAME:
        try:
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)

            if os.path.exists(FINAL_BLOB_NAME):
                blob = bucket.blob(FINAL_BLOB_NAME)
                blob.upload_from_filename(FINAL_BLOB_NAME)
                print(f"[INFO] Uploaded to gs://{BUCKET_NAME}/{FINAL_BLOB_NAME}")
            
            if os.path.exists(RAW_BLOB_NAME):
                blob = bucket.blob(RAW_BLOB_NAME)
                blob.upload_from_filename(RAW_BLOB_NAME)
                print(f"[INFO] Uploaded to gs://{BUCKET_NAME}/{RAW_BLOB_NAME}")
                
        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")
    else:
        print("[WARN] BUCKET_NAME not set, skipping upload.")

if __name__ == "__main__":
    merge_and_upload()
