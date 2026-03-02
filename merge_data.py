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

def merge_and_upload(output_dir=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR
        
    if not os.path.exists(output_dir):
        # 若目錄不存在，嘗試建立它（對 Cloud Functions 來說很重要）
        try:
            os.makedirs(output_dir)
        except OSError:
            print(f"Error: {output_dir} does not exist and cannot be created.")
            return {"status": "failed", "error": f"{output_dir} does not exist"}

    # [NEW] 從 GCS 下載所有 Fragments 到本地 output_dir
    if BUCKET_NAME:
        print(f"[INFO] Downloading fragments from gs://{BUCKET_NAME}/fragments/ ...")
        try:
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blobs = list(bucket.list_blobs(prefix="fragments/"))
            
            downloaded_count = 0
            for blob in blobs:
                if blob.name.endswith(".parquet"):
                    # 下載到 output_dir/final_xxx.parquet
                    # 注意：blob.name 包含 'fragments/' 前綴，需要去掉或保留
                    filename = os.path.basename(blob.name)
                    local_path = os.path.join(output_dir, filename)
                    blob.download_to_filename(local_path)
                    downloaded_count += 1
            
            print(f"[INFO] Downloaded {downloaded_count} fragments.")
        except Exception as e:
            print(f"[ERROR] Failed to download fragments: {e}")
            return {"status": "failed", "error": str(e)}

    # 1. Merge Raw Data
    raw_files = glob.glob(os.path.join(output_dir, "raw_*.parquet"))
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
            raw_output_path = os.path.join(output_dir, RAW_BLOB_NAME)
            merged_raw.to_parquet(raw_output_path, index=False)
            print(f"[INFO] Saved merged raw data to {raw_output_path}")
    else:
        print("[WARN] No raw data files found.")

    # 2. Merge Final Data
    # 這裡會讀取剛剛從 GCS 下載下來的所有 fragments
    final_files = glob.glob(os.path.join(output_dir, "final_*.parquet"))
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
            return {"status": "skipped", "message": "No valid final data loaded"}

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

        # Ensure numeric columns are actually numeric to avoid mixed type errors in PyArrow
        for col in ['lat', 'lng']:
            if col in merged_final.columns:
                merged_final[col] = pd.to_numeric(merged_final[col], errors='coerce')

        final_output_path = os.path.join(output_dir, FINAL_BLOB_NAME)
        merged_final.to_parquet(final_output_path, index=False)
        print(f"[INFO] Saved merged final data to {final_output_path} ({len(merged_final)} records)")
    else:
        print("[WARN] No final data files found.")

    # 3. Upload to GCS
    if BUCKET_NAME:
        try:
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)

            final_output_path = os.path.join(output_dir, FINAL_BLOB_NAME)
            if os.path.exists(final_output_path):
                blob = bucket.blob(FINAL_BLOB_NAME)
                blob.upload_from_filename(final_output_path)
                print(f"[INFO] Uploaded to gs://{BUCKET_NAME}/{FINAL_BLOB_NAME}")
            
            raw_output_path = os.path.join(output_dir, RAW_BLOB_NAME)
            if os.path.exists(raw_output_path):
                blob = bucket.blob(RAW_BLOB_NAME)
                blob.upload_from_filename(raw_output_path)
                print(f"[INFO] Uploaded to gs://{BUCKET_NAME}/{RAW_BLOB_NAME}")
                
        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")
            return {"status": "failed", "error": str(e)}
    else:
        print("[WARN] BUCKET_NAME not set, skipping upload.")
        return {"status": "skipped", "message": "Bucket name not set"}

    return {"status": "success", "message": "Merge and upload completed"}

if __name__ == "__main__":
    merge_and_upload()
