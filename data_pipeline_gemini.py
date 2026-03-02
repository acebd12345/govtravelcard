import os
import time
import io
import re
import argparse
import unicodedata
import tempfile
import requests
import urllib3
import pandas as pd
import googlemaps
import logging

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed

# Local imports
from pipeline_config import CITIES, ZIP_CODES, INDUSTRY_CODES, SYNONYMS_MAP

# ================= 環境配置 =================
load_dotenv()

# 設定 Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 環境變數
BUCKET_NAME = os.getenv("BUCKET_NAME")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# 檔案名稱設定
CACHE_BLOB_NAME = "geocoding_cache.parquet"
# Verify this model name is available in your Gemini API plan
GEMINI_MODEL_NAME = "gemini-3-pro-preview"
FRAGMENTS_DIR = "fragments"  # GCS 上的暫存目錄

# 設定 Gemini SDK Client
client = None
if GOOGLE_API_KEY:
    client = genai.Client(api_key=GOOGLE_API_KEY)

# 自動設定 Service Account Credentials
if os.path.exists("service_account.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("service_account.json")

# ================= 並行寫入輔助函數 =================

def acquire_lock(lock_path, timeout=30, poll=0.2):
    """Attempt to create a lock file exclusively."""
    import errno
    start = time.time()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.fsync(fd)
            return fd
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            if time.time() - start > timeout:
                return None
            time.sleep(poll)

def release_lock(fd, lock_path):
    """Release the lock file."""
    try:
        os.close(fd)
    except Exception:
        pass
    try:
        os.remove(lock_path)
    except Exception:
        pass

def atomic_write_parquet(path, df, tmp_suffix=None, timeout=30):
    """Write DataFrame to a temporary file and atomically replace target path."""
    if tmp_suffix is None:
        tmp_suffix = f".tmp.{os.getpid()}"
    tmp_path = path + tmp_suffix
    lock_path = path + ".lock"

    fd = acquire_lock(lock_path, timeout=timeout)
    if fd is None:
        print(f"[WARN] Could not acquire lock for {path} after {timeout}s. Skipping write.")
        return False

    try:
        with io.BytesIO() as bio:
            df.to_parquet(bio, index=False)
            bio.seek(0)
            with open(tmp_path, 'wb') as f:
                f.write(bio.read())
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        print(f"[ERROR] atomic write failed for {path}: {e}")
        try:
            if os.path.exists(tmp_path): os.remove(tmp_path)
        except Exception:
            pass
        return False
    finally:
        release_lock(fd, lock_path)

def _upload_geocoding_cache_safe(blob, new_cache_rows, tmp_cache_path, max_retries=5):
    """Upload new geocoding results to GCS with optimistic locking.

    Uses GCS object generation to detect concurrent writes. If another
    process updated the cache between our read and write, we re-download
    the latest version, merge again, and retry.
    """
    new_df = pd.DataFrame(new_cache_rows)

    for attempt in range(max_retries):
        try:
            # 1. Download the latest cache and record its generation
            if blob.exists():
                blob.reload()
                generation = blob.generation
                cache_df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
            else:
                generation = 0
                cache_df = pd.DataFrame(columns=['full_address_key', 'lat', 'lng'])

            # 2. Merge our new results into the latest cache
            merged = pd.concat([cache_df, new_df], ignore_index=True)
            merged.drop_duplicates(subset=['full_address_key'], keep='last', inplace=True)

            # 3. Write to local temp file
            merged.to_parquet(tmp_cache_path, index=False)

            # 4. Upload with generation match (optimistic lock)
            blob.upload_from_filename(
                tmp_cache_path,
                if_generation_match=generation
            )
            print(f"[INFO] Geocoding cache updated ({len(merged)} entries)")
            return True

        except PreconditionFailed:
            print(f"[WARN] Geocoding cache conflict (attempt {attempt + 1}/{max_retries}), retrying...")
            time.sleep(0.5 * (attempt + 1))
        except Exception as e:
            print(f"[WARN] Failed to upload geocoding cache: {e}")
            return False

    print("[WARN] Geocoding cache update failed after max retries, results saved locally only")
    return False

# ================= 1. 爬蟲模組 =================

def run_scraper_batch(city_code, city_name, zip_code, zip_name, ind_code, ind_name, max_limit=None):
    print(f"[INFO] Scraping {city_name} {zip_name} - {ind_name} ({ind_code})...")
    url = "https://travel.nccc.com.tw/NASApp/NTC/servlet/com.du.mvc.EntryServlet"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    all_data = []
    page = 1
    empty_count = 0

    try:
        while True:
            if max_limit and len(all_data) >= max_limit: break

            request_val = f"NULL_NULL_NULL_{city_code}_{zip_code}_NULL_{ind_code}_NULL_NULL_0_0_2_2000_0"
            payload = {"Action": "RetailerList", "Type": "GetFull", "WebMode": "", "Request": request_val, "Page": str(page)}

            try:
                # verify=False: government website (travel.nccc.com.tw) has SSL certificate issues
                resp = requests.post(url, data=payload, headers=headers, timeout=20, verify=False)
                resp.raise_for_status()
                resp.encoding = "big5"
                if "查無資料" in resp.text or "查無特店資訊" in resp.text: break

                dfs = pd.read_html(io.StringIO(resp.text))
                target_table = None
                for t in dfs:
                    if t.shape[1] >= 5 and "特店名稱" in str(t.iloc[0, 0]):
                        target_table = t
                        break

                if target_table is None:
                    empty_count += 1
                    if empty_count >= 3: break
                else:
                    empty_count = 0
                    target_table.columns = target_table.iloc[0]
                    target_table = target_table[1:]
                    for _, row in target_table.iterrows():
                        if str(row.iloc[0]).strip() == "特店名稱": continue
                        all_data.append({
                            "縣市": city_name,
                            "行政區": zip_name,
                            "特店名稱": str(row.iloc[0]).strip(),
                            "行業別": str(row.iloc[1]).strip(),
                            "電話": str(row.iloc[2]).strip(),
                            "地址": str(row.iloc[3]).strip()
                        })
                    if page % 5 == 0:
                        print(f"[INFO] Page {page}, Collected {len(all_data)} items so far...")
            
            except Exception as e:
                print(f"[ERROR] Page {page}: {e}")

            page += 1
            time.sleep(0.3)
            
    except Exception as e:
        print(f"[ERROR] Scraper Batch Error: {e}")

    return pd.DataFrame(all_data)

# ================= 2. 清洗與處理模組 =================

def run_cleaner(df):
    if df.empty: return df

    def clean_text(text):
        if pd.isna(text): return ""
        text = unicodedata.normalize('NFKC', str(text))
        text = text.replace(" ", "").replace("　", "").replace("臺", "台")
        replacements = {'一段': '1段', '二段': '2段', '三段': '3段', '至': '-', '之': '-'}
        for k, v in replacements.items(): text = text.replace(k, v)
        text = re.sub(r'^.{2,3}[縣市]', '', text)
        text = re.sub(r'^.{2,4}[鄉鎮市區]', '', text)
        text = re.sub(r'^.{2,4}[村里]', '', text)
        return text

    def clean_phone(text):
        if pd.isna(text): return ""
        text = unicodedata.normalize('NFKC', str(text))
        text = text.replace(" ", "").replace("　", "")
        if '#' in text:
            parts = text.split('#', 1)
            main = re.sub(r'\D', '', parts[0])
            ext = parts[1]
            return f"{main}#{ext}"
        else:
            return re.sub(r'\D', '', text)

    for col in ['特店名稱', '地址']:
        df[col] = df[col].apply(clean_text)
    
    df['電話'] = df['電話'].apply(clean_phone)

    # Dedup (注：f-string 內不能包含反斜線，改用字符串拼接)
    df['dedup_key'] = df.apply(lambda row: re.sub(r'\D', '', row['電話']) + '_' + row['地址'], axis=1)
    df.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
    return df.drop(columns=['dedup_key'])

def run_geocoder_with_cache(df, tmp_cache_path=None):
    print("[INFO] Geocoding with cache...")
    if df.empty: return df
    
    # 若未指定暫存檔路徑，使用預設 (含 PID 以防萬一)
    if not tmp_cache_path:
        tmp_cache_path = os.path.join(tempfile.gettempdir(), f"cache_{os.getpid()}.parquet")

    def make_full_address(row):
        addr = str(row.get('地址', ''))
        city = str(row.get('縣市', ''))
        dist = str(row.get('行政區', ''))
        
        full = addr
        # 補回行政區 (若地址開頭沒有)
        if dist and not full.startswith(dist):
            full = dist + full
        # 補回縣市 (若地址開頭沒有)
        if city and not full.startswith(city):
            full = city + full
        return full

    df['full_address_key'] = df.apply(make_full_address, axis=1)

    # Setup GCS for cache
    cache_df = pd.DataFrame(columns=['full_address_key', 'lat', 'lng'])
    client_storage = storage.Client()
    bucket = client_storage.bucket(BUCKET_NAME)
    blob = bucket.blob(CACHE_BLOB_NAME)

    if blob.exists():
        try:
            cache_df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        except Exception:
            pass

    cache_df.drop_duplicates(subset=['full_address_key'], inplace=True)
    df = pd.merge(df, cache_df, on='full_address_key', how='left')
    
    mask_missing = df['lat'].isna() | df['lng'].isna()

    if mask_missing.sum() > 0 and GOOGLE_API_KEY:
        gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
        indices = df[mask_missing].index
        new_cache_rows = []

        print(f"[INFO] Resolving {len(indices)} addresses via API...")
        for i, idx in enumerate(indices):
            if (i + 1) % 10 == 0:
                print(f"[INFO] Geocoding progress: {i+1}/{len(indices)}")
            
            addr = df.at[idx, 'full_address_key']
            try:
                res = gmaps.geocode(addr)
                if res:
                    loc = res[0]['geometry']['location']
                    df.at[idx, 'lat'] = loc['lat']
                    df.at[idx, 'lng'] = loc['lng']
                    new_cache_rows.append({'full_address_key': addr, 'lat': loc['lat'], 'lng': loc['lng']})
            except Exception:
                pass
            time.sleep(0.05)

        if new_cache_rows:
            _upload_geocoding_cache_safe(blob, new_cache_rows, tmp_cache_path)

    return df.drop(columns=['full_address_key'])

def add_hidden_tags(df):
    if df.empty: return df
    
    # Try import rapidfuzz for fuzzy matching
    try:
        from rapidfuzz import fuzz
    except ImportError:
        fuzz = None

    def _find_tags(name):
        if pd.isna(name) or not name: return ""
        name_proc = str(name).lower()
        tags = set()
        for k, v in SYNONYMS_MAP.items():
            v_proc = str(v).lower()
            # Token matching
            for token in v_proc.split():
                if token in name_proc:
                    tags.add(k)
                    break
            else:
                # Fuzzy fallback
                if fuzz and fuzz.partial_ratio(name_proc, v_proc) >= 80:
                    tags.add(k)
        return ",".join(sorted(tags))

    df['hidden_tags'] = df['特店名稱'].apply(_find_tags)
    return df

# ================= 3. Gemini AI 處理模組 =================

def get_prompt_content(ind_code, csv_text):
    if ind_code == "0009": # 旅宿業
        instructions = """
11. 星級數請註明滿星是幾星 (例: 4.5/5)。
12. 價格區間以平日住一晚最低價參考，直接給我數字就好。
13. 評論請搜尋網路資料，寫一篇 80-100 字評論。
        """
    elif ind_code == "0008": # 餐飲業
        instructions = """
11. 星級數請註明滿星是幾星 (例: 4.5/5)。
12. 價格區間以人均消費參考，直接給我數字就好。
13. 評論請搜尋網路資料，寫一篇 80-100 字的食記摘要。
        """
    else: # 通用
        instructions = """
11. 星級數請註明滿星是幾星 (例: 4.5/5)。
12. 價格區間請標示消費等級 (例如: 平價, 中價位, 高價位)。
13. 評論請搜尋網路資料，寫一篇 80-100 字的店家特色摘要。
        """

    return f"""
請將輸入資料整理、**去重**並擴充為 CSV 格式，包含以下欄位(請保持順序)：
ID|店名|縣市|行政區|地址(不含樓層)|樓層|緯度|經度|電話|評論|星級數|價格區間

**重要規則：**
1. **使用直線符號 `|` 作為分隔符號 (不要用逗號)。**
2. **直接輸出 CSV 內容，不要有任何開頭語或結尾語。**
3. **不要使用 Markdown 代碼區塊 (不要 ```csv ... ```)。**
4. 第一行必須是標題列。
5. ID 請對應輸入資料的 temp_id。
6. 地址要修正，確保縣市、地區、路名都有資訊且正確 (使用 Google Search)。
7. 電話格式統一，前2碼加括號，其餘連接號捨棄。
8. 地址中的段數改為繁體中文，巷弄及樓層用阿拉伯數字。
9. 經緯度請分為 緯度, 經度 兩個欄位 (數值)。
10. **若網路上查無確切資訊，該欄位請直接留空。**
14. **若輸入資料中有重複的店家，請自動合併。**
{instructions}

# 輸入資料
{csv_text}
"""

def run_gemini_processor(df, city_code, zip_code, ind_code):
    df = df.reset_index(drop=True)
    CHUNK_SIZE = 30
    results = []
    total_records = len(df)
    
    print(f"[INFO] AI Processing: {total_records} records.")

    if not client:
        print("[ERROR] Gemini Client not initialized.")
        return []

    for i in range(0, total_records, CHUNK_SIZE):
        chunk = df.iloc[i: i + CHUNK_SIZE].copy()
        if chunk.empty: continue

        chunk['temp_id'] = chunk.index.map(lambda x: f"{city_code}_{zip_code}_{ind_code}_{x:05d}")
        csv_text = chunk[['temp_id', '縣市', '行政區', '特店名稱', '行業別', '電話', '地址', 'lat', 'lng', 'hidden_tags']].to_csv(index=False)
        prompt_content = get_prompt_content(ind_code, csv_text)

        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL_NAME,
                    contents=prompt_content,
                    config=types.GenerateContentConfig(
                        tools=[types.Tool(google_search=types.GoogleSearch())]
                    )
                )
                
                content = response.text or ""
                # 清理 Markdown
                content = content.replace("```csv", "").replace("```", "").strip()
                
                # 尋找 CSV 起始點
                match = re.search(r'(ID\s*\|.*)', content, re.DOTALL)
                if match:
                    content = match.group(1)

                df_chunk_res = pd.read_csv(io.StringIO(content),
                                         names=['id', 'name', 'city', 'district', 'address', 'floor', 'lat', 'lng', 'phone', 'review_summary', 'rating', 'price_level'],
                                         header=0,
                                         sep='|')

                if not df_chunk_res.empty:
                    print(f"[SUCCESS] Batch {i} processed {len(df_chunk_res)} records.")
                    
                    # 補回 hidden_tags (從 chunk 對應)
                    if 'hidden_tags' in chunk.columns:
                        df_chunk_res['hidden_tags'] = df_chunk_res['id'].map(chunk.set_index('temp_id')['hidden_tags'])

                    results.extend(df_chunk_res.to_dict('records'))
                    break
                else:
                    print(f"[WARN] Batch {i} Attempt {attempt+1}: Empty CSV returned.")
            
            except Exception as e:
                print(f"[ERROR] Batch {i} Attempt {attempt+1} Error: {e}")
                time.sleep(2)
            
            time.sleep(2) # Retry delay
        
        time.sleep(2) # Chunk delay

    return results

# ================= Main Execution =================

def main():
    parser = argparse.ArgumentParser(description="Gemini Data Pipeline")
    parser.add_argument("--city", help="City code (e.g., 001)")
    parser.add_argument("--zip", help="Zip codes separated by comma (e.g., 111,103)")
    parser.add_argument("--industry", help="Industry codes separated by comma (e.g., 0009)")
    parser.add_argument("--output_dir", default="outputs", help="Directory to save partial results")
    parser.add_argument("--use_raw", action="store_true", help="Use existing raw parquet file")
    args = parser.parse_args()

    if not GOOGLE_API_KEY:
        print("Error: GOOGLE_API_KEY is missing.")
        return

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    target_cities = [args.city] if args.city else ["001"]
    target_zips = args.zip.split(",") if args.zip else list(ZIP_CODES.keys())
    target_industries = args.industry.split(",") if args.industry else ["0009"]

    print(f"[CONFIG] City: {target_cities}, Zips: {target_zips}, Industries: {target_industries}")
    print("[INFO] Starting Pipeline...")

    for city_code, city_name in CITIES.items():
        if city_code not in target_cities: continue

        for zip_code, zip_name in ZIP_CODES.items():
            if zip_code not in target_zips: continue

            for ind_code, ind_name in INDUSTRY_CODES.items():
                if ind_code not in target_industries: continue

                file_suffix = f"{city_code}_{zip_code}_{ind_code}"
                raw_file_path = os.path.join(args.output_dir, f"raw_{file_suffix}.parquet")
                final_file_path = os.path.join(args.output_dir, f"final_{file_suffix}.parquet")

                if os.path.exists(final_file_path):
                    print(f"[SKIP] {zip_name} - {ind_name} already exists.")
                    continue

                # 1. Scrape
                df_batch = run_scraper_batch(city_code, city_name, zip_code, zip_name, ind_code, ind_name)

                if df_batch.empty:
                    print(f"[INFO] No data for {zip_name} - {ind_name}.")
                    continue

                # 2. Clean & Geocode & Tag
                df_batch = run_cleaner(df_batch)
                
                # 使用 City/Zip/Ind 作為 Cache 暫存檔名，避免平行衝突
                cache_path = os.path.join(tempfile.gettempdir(), f"cache_{city_code}_{zip_code}_{ind_code}.parquet")
                df_batch = run_geocoder_with_cache(df_batch, tmp_cache_path=cache_path)

                df_batch = add_hidden_tags(df_batch)

                # Save Raw
                if not df_batch.empty:
                    atomic_write_parquet(raw_file_path, df_batch)
                    print(f"[INFO] Saved raw records to {raw_file_path}")

                # 3. Gemini Processing
                if args.use_raw:
                    try:
                        df_for_ai = pd.read_parquet(raw_file_path)
                    except Exception:
                        df_for_ai = pd.DataFrame()
                else:
                    df_for_ai = df_batch

                if df_for_ai.empty:
                    print("[INFO] No data for AI processing.")
                    continue

                batch_results = run_gemini_processor(df_for_ai, city_code, zip_code, ind_code)

                if batch_results:
                    final_df = pd.DataFrame(batch_results)
                    # Data Type Formatting
                    for col in ['id', 'phone', 'name']:
                        if col in final_df.columns: final_df[col] = final_df[col].astype(str)

                    final_df['ind'] = ind_name

                    for col in ['lat', 'lng']:
                        if col in final_df.columns: final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

                    # Fill NaN for string columns only
                    str_cols = ['id', 'name', 'ind', 'city', 'district', 'address', 'floor', 'phone', 'review_summary', 'rating', 'price_level', 'hidden_tags']
                    for col in str_cols:
                        if col in final_df.columns:
                            final_df[col] = final_df[col].fillna("").astype(str)

                    # Reorder columns as requested
                    target_order = ['id', 'name', 'ind', 'city', 'district', 'address', 'floor', 'lat', 'lng', 'phone', 'review_summary', 'rating', 'price_level']
                    if 'hidden_tags' in final_df.columns:
                        target_order.append('hidden_tags')
                    
                    # Ensure we only select columns that exist
                    final_cols = [c for c in target_order if c in final_df.columns]
                    final_df = final_df[final_cols]
                    
                    if atomic_write_parquet(final_file_path, final_df):
                        print(f"[INFO] Saved final records to {final_file_path}")
                        
                        # [NEW] 立即上傳 Fragment 到 GCS，作為合併前的暫存
                        if BUCKET_NAME:
                            try:
                                blob_name = f"{FRAGMENTS_DIR}/final_{file_suffix}.parquet"
                                client_storage = storage.Client()
                                bucket = client_storage.bucket(BUCKET_NAME)
                                blob = bucket.blob(blob_name)
                                blob.upload_from_filename(final_file_path)
                                print(f"[UPLOAD] Fragment uploaded to gs://{BUCKET_NAME}/{blob_name}")
                            except Exception as e:
                                print(f"[ERROR] Failed to upload fragment: {e}")
                else:
                    print("[INFO] No results from Gemini.")

    print("[INFO] Batch processing completed.")

def run_pipeline_for_config(config):
    """
    適用於 Cloud Scheduler 的管線運行函數
    
    Args:
        config (dict): 排程配置，格式如下：
        {
            "city": "001",                  # 城市代碼（必須）
            "industries": ["0009"],         # 行業代碼清單（必須）
            "districts": None,              # 特定行政區代碼清單（可選）
            "output_dir": "outputs",        # 輸出目錄（可選）
            "use_raw": False                # 是否使用現有 raw 檔案（可選）
        }
    
    Returns:
        dict: 執行結果，格式如下：
        {
            "status": "success|failed",
            "config": config,
            "message": "描述訊息",
            "timestamp": "ISO時間戳"
        }
    """
    from datetime import datetime
    
    try:
        # 取得配置參數
        city = config.get("city", "001")
        industries = config.get("industries", ["0009"])
        districts = config.get("districts", None)
        output_dir = config.get("output_dir", "outputs")
        use_raw = config.get("use_raw", False)
        
        # 驗證必要的環境
        if not GOOGLE_API_KEY:
            raise Exception("GOOGLE_API_KEY is missing")
        
        # 建立輸出目錄
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        logger.info(f"[CONFIG] City: {city}, Industries: {industries}, Output: {output_dir}")
        logger.info("[INFO] Starting pipeline from Cloud Scheduler...")
        
        # 驗證城市代碼
        if city not in CITIES:
            raise ValueError(f"Invalid city code: {city}")
        
        # 驗證行業代碼
        invalid_industries = [ind for ind in industries if ind not in INDUSTRY_CODES]
        if invalid_industries:
            logger.warning(f"[WARN] Invalid industry codes: {invalid_industries}, will skip them")
            industries = [ind for ind in industries if ind in INDUSTRY_CODES]
        
        if not industries:
            raise ValueError("No valid industries specified")
        
        city_name = CITIES.get(city, city)
        success_count = 0
        total_count = 0
        
        # 決定要處理的行政區
        target_zips = ZIP_CODES
        if districts:
            # 過濾只保留指定的行政區
            target_zips = {k: v for k, v in ZIP_CODES.items() if k in districts}
            if not target_zips:
                 logger.warning(f"[WARN] No valid districts found in {districts}, using all.")
                 target_zips = ZIP_CODES

        # 執行爬蟲 - 遍歷所有行政區和行業
        for zip_code, zip_name in target_zips.items():
            for ind_code in industries:
                if ind_code not in INDUSTRY_CODES:
                    continue
                    
                ind_name = INDUSTRY_CODES.get(ind_code, ind_code)
                total_count += 1
                
                try:
                    file_suffix = f"{city}_{zip_code}_{ind_code}"
                    raw_file_path = os.path.join(output_dir, f"raw_{file_suffix}.parquet")
                    final_file_path = os.path.join(output_dir, f"final_{file_suffix}.parquet")
                    
                    # 跳過已存在的檔案
                    if os.path.exists(final_file_path):
                        logger.info(f"[SKIP] {zip_name} - {ind_name} already exists.")
                        success_count += 1
                        continue
                    
                    # 1. Scrape
                    logger.info(f"[SCRAPE] {city_name} - {zip_name} - {ind_name}")
                    df_batch = run_scraper_batch(city, city_name, zip_code, zip_name, ind_code, ind_name)
                    
                    if df_batch.empty:
                        logger.info(f"[INFO] No data for {zip_name} - {ind_name}.")
                        success_count += 1
                        continue
                    
                    # 2. Clean & Geocode & Tag
                    df_batch = run_cleaner(df_batch)
                    cache_path = os.path.join(tempfile.gettempdir(), f"cache_{city}_{zip_code}_{ind_code}.parquet")
                    df_batch = run_geocoder_with_cache(df_batch, tmp_cache_path=cache_path)
                    df_batch = add_hidden_tags(df_batch)
                    
                    # Save Raw
                    if not df_batch.empty:
                        atomic_write_parquet(raw_file_path, df_batch)
                        logger.info(f"[INFO] Saved raw records to {raw_file_path}")
                    
                    # 3. Gemini Processing
                    df_for_ai = df_batch if not use_raw else pd.read_parquet(raw_file_path)
                    
                    if df_for_ai.empty:
                        logger.info("[INFO] No data for AI processing.")
                        success_count += 1
                        continue
                    
                    batch_results = run_gemini_processor(df_for_ai, city, zip_code, ind_code)
                    
                    if batch_results:
                        final_df = pd.DataFrame(batch_results)
                        
                        # 資料型別轉換和格式化
                        for col in ['id', 'phone', 'name']:
                            if col in final_df.columns:
                                final_df[col] = final_df[col].astype(str)

                        final_df['ind'] = ind_name
                        
                        for col in ['lat', 'lng']:
                            if col in final_df.columns:
                                final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
                        
                        # 填充 NaN 值
                        str_cols = ['id', 'name', 'ind', 'city', 'district', 'address', 'floor', 'phone', 'review_summary', 'rating', 'price_level', 'hidden_tags']
                        for col in str_cols:
                            if col in final_df.columns:
                                final_df[col] = final_df[col].fillna("").astype(str)
                        
                        # 重新排列欄位
                        target_order = ['id', 'name', 'ind', 'city', 'district', 'address', 'floor', 'lat', 'lng', 'phone', 'review_summary', 'rating', 'price_level']
                        if 'hidden_tags' in final_df.columns:
                            target_order.append('hidden_tags')
                        
                        final_cols = [c for c in target_order if c in final_df.columns]
                        final_df = final_df[final_cols]
                        
                        if atomic_write_parquet(final_file_path, final_df):
                            logger.info(f"[SUCCESS] {city_name} - {zip_name} - {ind_name}: {len(final_df)} records")
                            success_count += 1
                    else:
                        logger.warning("[WARN] No results from Gemini.")
                        success_count += 1
                
                except Exception as e:
                    logger.error(f"[ERROR] Failed to process {city_name} - {zip_name} - {ind_name}: {str(e)}")
        
        # 返回結果
        result = {
            "status": "success",
            "config": config,
            "processed_count": success_count,
            "total_count": total_count,
            "message": f"Processed {success_count}/{total_count} city-district-industry combinations",
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"[COMPLETED] Pipeline execution finished: {result['message']}")
        return result
        
    except Exception as e:
        logger.error(f"[FATAL ERROR] Pipeline failed: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "config": config,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    main()
