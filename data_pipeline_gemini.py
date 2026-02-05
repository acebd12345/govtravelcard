import os
import time
import io
import re
import argparse
import unicodedata
import requests
import urllib3
import pandas as pd
import googlemaps

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.cloud import storage

# Local imports
from pipeline_config import CITIES, ZIP_CODES, INDUSTRY_CODES, SYNONYMS_MAP

# ================= 環境配置 =================
load_dotenv()
# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 環境變數
BUCKET_NAME = os.getenv("BUCKET_NAME")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# 檔案名稱設定
CACHE_BLOB_NAME = "geocoding_cache.parquet"
TMP_CACHE = f"/tmp/cache_{os.getpid()}.parquet" # 使用 PID 避免多進程衝突
GEMINI_MODEL_NAME = "gemini-3-pro-preview"

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
                resp = requests.post(url, data=payload, headers=headers, timeout=20, verify=False)
                resp.raise_for_status()
                resp.encoding = "big5"
                if "查無資料" in resp.text: break

                dfs = pd.read_html(resp.text)
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

    # Dedup
    df['dedup_key'] = df.apply(lambda row: f"{re.sub(r'\D', '', row['電話'])}_{row['地址']}", axis=1)
    df.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
    return df.drop(columns=['dedup_key'])

def run_geocoder_with_cache(df):
    print("[INFO] Geocoding with cache...")
    if df.empty: return df

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
            new_cache_df = pd.DataFrame(new_cache_rows)
            updated_cache = pd.concat([cache_df, new_cache_df], ignore_index=True)
            updated_cache.drop_duplicates(subset=['full_address_key'], inplace=True)
            try:
                updated_cache.to_parquet(TMP_CACHE, index=False)
                blob.upload_from_filename(TMP_CACHE)
            except Exception as e:
                print(f"[WARN] Failed to upload geocoding cache: {e}")

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
                df_batch = run_geocoder_with_cache(df_batch)
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
                    for col in ['id', 'phone', '特店名稱', '行業別', 'name']:
                         if col in final_df.columns: final_df[col] = final_df[col].astype(str)
                    
                    # User requested to remove '特店名稱' duplication and rename '行業別' to 'ind'
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
                else:
                    print("[INFO] No results from Gemini.")

    print("[INFO] Batch processing completed.")

if __name__ == "__main__":
    main()
