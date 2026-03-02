from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os
import json
import logging
from google.cloud import scheduler_v1
from google.protobuf.duration_pb2 import Duration
from pipeline_config import CITIES, ZIP_CODES, INDUSTRY_CODES

app = FastAPI()

# 設定 Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境變數
PROJECT_ID = os.getenv("GCP_PROJECT", "your-project-id")
REGION = os.getenv("GCP_REGION", "asia-east1")
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL", "")

# 建立 templates 目錄 (如果還沒有)
if not os.path.exists("templates"):
    os.makedirs("templates")
templates = Jinja2Templates(directory="templates")

# Cloud Scheduler Client
try:
    scheduler_client = scheduler_v1.CloudSchedulerClient()
except Exception as e:
    logger.warning(f"Cloud Scheduler Client init failed (running locally?): {e}")
    scheduler_client = None

@app.get("/health")
def health_check():
    return {"status": "ok", "project": PROJECT_ID, "region": REGION}

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "cities": CITIES,
        "districts": ZIP_CODES,
        "industries": INDUSTRY_CODES,
        "project_id": PROJECT_ID
    })

@app.get("/api/jobs")
async def list_jobs():
    if not scheduler_client:
        return {"error": "Cloud Scheduler Client not initialized"}
    
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    jobs = []
    try:
        for job in scheduler_client.list_jobs(parent=parent):
            # [Filter] 只顯示與 GovTravel 相關的排程
            # 條件 1: 名稱包含 'govtravel' 或 'taipei' (預設名稱)
            # 條件 2: 或是我們自己建立的 (通常 body 會有 mode/config)
            
            job_name = job.name.split('/')[-1]
            
            # Parse config from body
            config = {}
            mode = "unknown"
            is_our_job = False
            
            try:
                body_content = job.http_target.body.decode('utf-8')
                payload = json.loads(body_content)
                if "mode" in payload or "config" in payload:
                    mode = payload.get("mode", "unknown")
                    config = payload.get("config", {})
                    is_our_job = True
            except Exception:
                pass

            # 過濾邏輯
            if not is_our_job and "govtravel" not in job_name and "taipei" not in job_name and "merge" not in job_name:
                continue

            jobs.append({
                "name": job_name,
                "schedule": job.schedule,
                "state": str(job.state),
                "mode": mode,
                "config": config,
                "description": job.description
            })
        return {"jobs": jobs}
    except Exception as e:
        logger.error(f"List jobs failed: {e}")
        return {"error": str(e)}

@app.post("/api/jobs")
async def create_job(request: Request):
    if not scheduler_client:
        return {"error": "Cloud Scheduler Client not initialized"}

    data = await request.json()
    job_id = data.get("name")
    schedule = data.get("schedule", "0 2 * * *") # 預設凌晨 2 點
    description = data.get("description", "")
    mode = data.get("mode", "dispatch")
    config = data.get("config", {})

    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    job_name = f"{parent}/jobs/{job_id}"
    
    # 這裡假設你的 Cloud Function URL 是標準格式
    # 如果部署腳本有變，這裡也要改
    # Assumes standard Cloud Functions URL format; update if using Cloud Run or custom domain
    target_uri = f"https://{REGION}-{PROJECT_ID}.cloudfunctions.net/scheduled-pipeline"

    payload = {
        "mode": mode,
        "config": config
    }
    
    job = {
        "name": job_name,
        "http_target": {
            "uri": target_uri,
            "http_method": scheduler_v1.HttpMethod.POST,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode("utf-8"),
            "oidc_token": {
                "service_account_email": SERVICE_ACCOUNT_EMAIL,
                "audience": target_uri
            }
        },
        "schedule": schedule,
        "time_zone": "Asia/Taipei",
        "description": description
    }

    try:
        response = scheduler_client.create_job(request={"parent": parent, "job": job})
        return {"status": "created", "job": response.name}
    except Exception as e:
        logger.error(f"Create job failed: {e}")
        return {"error": str(e)}

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    if not scheduler_client:
        return {"error": "Cloud Scheduler Client not initialized"}

    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{job_id}"
    try:
        scheduler_client.delete_job(name=name)
        return {"status": "deleted", "job": name}
    except Exception as e:
        return {"error": str(e)}

# Cloud Functions entry point wrapper (保留原本的)
def scheduled_pipeline(request):
    from cloud_scheduler_handler import scheduled_pipeline as _handler
    return _handler(request)
