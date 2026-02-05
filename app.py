import os
import io
import pandas as pd
from dotenv import load_dotenv
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from google.cloud import storage
from pipeline_config import PRICE_THRESHOLDS

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
DATA_FILE = "final_data.parquet"

# Auto-configure credentials
if os.path.exists("service_account.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("service_account.json")

global_store = {"df": None}

def load_local_data():
    if os.path.exists(DATA_FILE):
        print(f"Loading local file: {DATA_FILE}")
        return pd.read_parquet(DATA_FILE)
    return pd.DataFrame()

@asynccontextmanager
async def lifespan(app: FastAPI):
    df = pd.DataFrame()
    
    # 1. Try GCS
    if BUCKET_NAME:
        print(f"Attempting to load from GCS: {BUCKET_NAME}/{DATA_FILE}...")
        try:
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(DATA_FILE)
            data = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(data))
            print("GCS load successful.")
        except Exception as e:
            print(f"GCS load failed: {e}")
            df = pd.DataFrame()

    # 2. Fallback to Local
    if df.empty:
        print("Falling back to local file...")
        try:
            df = load_local_data()
        except Exception as e:
            print(f"Local load failed: {e}")

    try:
        if not df.empty:
            # 建立搜尋索引
            df['search_index'] = (
                df['name'].fillna('') + " " +
                df['address'].fillna('') + " " +
                df['hidden_tags'].fillna('')
            ).astype(str).str.lower()
            
            # 處理評分 (將 "4.5/5" 轉為 4.5)
            def parse_rating(r):
                try:
                    return float(str(r).split('/')[0])
                except:
                    return 0.0
            
            if 'rating' in df.columns:
                df['rating_val'] = df['rating'].apply(parse_rating)
            else:
                df['rating_val'] = 0.0

            # Convert numeric columns from string if needed
            for col in ['lat', 'lng', 'price_level']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            global_store["df"] = df
            print(f"Loaded {len(df)} records.")
    except Exception as e:
        print(f"Load failed: {e}")
        global_store["df"] = pd.DataFrame()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists("index.html"):
        with open("index.html", encoding="utf-8") as f:
            return f.read()
    return "No index.html found"

@app.get("/api/stores")
def get_stores(
    min_lat: float, max_lat: float, min_lng: float, max_lng: float,
    category: str = None, keyword: str = None,
    min_rating: Optional[float] = None,
    price_level: Optional[int] = None
):
    df = global_store["df"]
    if df is None or df.empty: return {"count": 0, "data": []}

    # 1. 範圍
    mask = (df['lat'] >= min_lat) & (df['lat'] <= max_lat) & \
           (df['lng'] >= min_lng) & (df['lng'] <= max_lng)
    res = df[mask]

    # 2. 關鍵字
    if keyword:
        res = res[res['search_index'].str.contains(keyword.lower(), na=False)]

    # 3. 類別
    if category and category != "All":
        res = res[res['ind'] == category]

    # 4. 評分
    if min_rating and min_rating > 0:
        res = res[res['rating_val'] >= min_rating]

    # 5. 價位 (根據不同行業別套用不同級距)
    if price_level is not None:
        final_mask = pd.Series([False] * len(res), index=res.index)
        
        # 若資料中有 ind 欄位，則依行業別分組處理
        if 'ind' in res.columns:
            for ind_name, group in res.groupby('ind'):
                thresholds = PRICE_THRESHOLDS.get(ind_name, PRICE_THRESHOLDS['default'])
                # 確保至少有 4 個切分點 (5 個等級)
                while len(thresholds) < 4:
                    thresholds.append(999999) # 補上無限大以防錯誤
                
                p_col = group['price_level'].fillna(-1)
                sub_mask = pd.Series([False] * len(group), index=group.index)
                
                if price_level == 1:
                    sub_mask = (p_col < thresholds[0]) & (p_col > 0)
                    sub_mask |= (p_col == 1)
                elif price_level == 2:
                    sub_mask = (p_col >= thresholds[0]) & (p_col < thresholds[1])
                    sub_mask |= (p_col == 2)
                elif price_level == 3:
                    sub_mask = (p_col >= thresholds[1]) & (p_col < thresholds[2])
                    sub_mask |= (p_col == 3)
                elif price_level == 4:
                    sub_mask = (p_col >= thresholds[2]) & (p_col < thresholds[3])
                    sub_mask |= (p_col == 4)
                elif price_level >= 5:
                    sub_mask = (p_col >= thresholds[3])
                    sub_mask |= (p_col == 5) # 只匹配等級 5，避免匹配到真實價格 (如 3800 >= 5)
                
                final_mask.update(sub_mask)
            res = res[final_mask]
        else:
            # 若無 ind 欄位，使用預設級距
            thresholds = PRICE_THRESHOLDS['default']
            p_col = res['price_level'].fillna(-1)
            if price_level == 1:
                res = res[(p_col < thresholds[0]) & (p_col > 0) | (p_col == 1)]
            elif price_level == 2:
                res = res[((p_col >= thresholds[0]) & (p_col < thresholds[1])) | (p_col == 2)]
            elif price_level == 3:
                res = res[((p_col >= thresholds[1]) & (p_col < thresholds[2])) | (p_col == 3)]
            elif price_level == 4:
                res = res[((p_col >= thresholds[2]) & (p_col < thresholds[3])) | (p_col == 4)]
            elif price_level >= 5:
                res = res[(p_col >= thresholds[3]) | (p_col >= 5)]

    limit_res = res.head(500)
    
    cols = ['name', 'ind', 'phone', 'address', 'lat', 'lng',
            'rating', 'price_level', 'hidden_tags', 'review_summary']
    
    valid_cols = [c for c in cols if c in limit_res.columns]
    data = limit_res[valid_cols].fillna("").to_dict(orient='records')
    
    return {"count": len(data), "data": data}

@app.get("/api/categories")
def get_cats():
    df = global_store["df"]
    if df is None or df.empty: return ["All"]
    return ["All"] + sorted(df['ind'].dropna().unique().tolist())

@app.get("/api/config")
def get_config():
    return {"price_thresholds": PRICE_THRESHOLDS}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
