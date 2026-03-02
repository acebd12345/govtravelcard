"""
Google Cloud Scheduler Handler
適用於 Cloud Scheduler 定期觸發資料管線
"""

import os
import json
import tempfile
import functions_framework
from datetime import datetime
import logging
from google.cloud import storage
from data_pipeline_gemini import run_pipeline_for_config
from merge_data import merge_and_upload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def scheduled_pipeline(request):
    """
    HTTP 入口點供 Cloud Scheduler 呼叫
    接收排程參數並執行全套資料管線
    
    支援兩種模式：
    1. 執行爬蟲任務 (mode="scrape") - 預設
    2. 執行合併任務 (mode="merge")
    
    Request JSON 格式：
    {
        "mode": "scrape" | "merge",
        "config": { ... }
    }
    """
    try:
        request_json = request.get_json(silent=True) or {}
        mode = request_json.get("mode", "scrape") # 預設為爬蟲模式
        
        logger.info(f"Starting scheduled job [Mode: {mode}] at {datetime.now()}")
        
        # ==================== 合併模式 ====================
        if mode == "merge":
            logger.info("Starting merge job...")
            try:
                # 使用 /tmp 作為暫存區來下載 fragments 和合併
                merge_result = merge_and_upload(output_dir=tempfile.gettempdir())
                logger.info("Merge job completed successfully")
                return {
                    "status": "success",
                    "mode": "merge",
                    "merge_result": merge_result,
                    "timestamp": datetime.now().isoformat()
                }, 200
            except Exception as e:
                logger.error(f"Merge job failed: {str(e)}")
                return {
                    "status": "failed",
                    "mode": "merge",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, 500

        # ==================== 派工模式 (Dispatch) ====================
        elif mode == "dispatch":
            # 這是 Master Job 的入口
            # 接收一個大範圍的 config (例如整個台北市)
            # 自動拆分成多個小範圍的 config (例如每個行政區一個)，然後觸發 Worker (Scrape Mode)
            
            config = request_json.get("config", {})
            logger.info(f"Starting dispatch job with config: {config}")
            
            # 1. 取得目標城市與行政區
            city = config.get("city", "001")
            industries = config.get("industries", ["0009"])
            districts = config.get("districts") # 若為 None 代表全部
            
            from pipeline_config import ZIP_CODES
            
            # 決定要處理的行政區列表
            target_zips = []
            if districts:
                target_zips = [z for z in districts if z in ZIP_CODES]
            else:
                target_zips = list(ZIP_CODES.keys())
                
            logger.info(f"Target Zips: {target_zips}")
            
            # 2. 透過 Google Cloud Tasks 發送任務
            # 或是簡單一點：直接用 HTTP 呼叫自己 (非同步)
            # 為了簡化部署，這裡我們假設 Cloud Scheduler 直接呼叫 Cloud Function
            # 但 Cloud Function 呼叫 Cloud Function 需要認證
            
            # 這裡我們採用「簡單迴圈呼叫」方式 (僅適用於小規模)
            # 若要大規模平行，建議前端 Scheduler 直接設定好，或使用 Cloud Tasks
            #
            # 鑑於用戶希望「簡單」，我們這裡做一個折衷：
            # 如果 districts 只有 1 個，直接執行 (Worker Mode)
            # 如果 districts 多於 1 個，我們回傳一個「建議的拆分設定」，
            # 讓用戶知道原來可以這樣拆。
            #
            # 但等等，用戶要的是「全自動」。
            # 所以正確做法是：這裡應該是用戶設定的一個入口，
            # 程式內部自己 loop 執行？不行，這樣會 timeout。
            #
            # 最佳解：使用 Google Cloud Tasks。
            # 但這需要啟用新服務。
            #
            # 替代方案：
            # 改回原本的「直接執行」，但在 run_pipeline_for_config 內部做最佳化？
            # 不行，單一 Function 有 540s (或 3600s) 限制。
            
            # 讓我們重新審視用戶需求：「就像地端一樣簡單」。
            # 地端 run_parallel 是開多個 process。
            # 雲端對應的就是開多個 Cloud Function。
            #
            # 我們來實作一個簡單的 Cloud Tasks Dispatcher。
            # 需要: pip install google-cloud-tasks
            
            try:
                from google.cloud import tasks_v2
                
                # 需設定環境變數
                PROJECT_ID = os.getenv("GCP_PROJECT")
                REGION = os.getenv("GCP_REGION", "asia-east1")
                QUEUE_NAME = "scraper-queue" # 需先建立: gcloud tasks queues create scraper-queue
                FUNCTION_URL = os.getenv("FUNCTION_URL", request.url)
                
                client = tasks_v2.CloudTasksClient()
                parent = client.queue_path(PROJECT_ID, REGION, QUEUE_NAME)
                
                dispatched_count = 0
                
                # 策略：每個行政區 x 每個行業 = 一個任務 (最細粒度)
                for zip_code in target_zips:
                    # 為了避免任務過多，我們可以每個行政區打包所有行業
                    payload = {
                        "mode": "scrape",
                        "config": {
                            "city": city,
                            "industries": industries,
                            "districts": [zip_code],  # 鎖定單一行政區
                            "output_dir": tempfile.gettempdir()
                        }
                    }

                    task = {
                        "http_request": {
                            "http_method": tasks_v2.HttpMethod.POST,
                            "url": FUNCTION_URL,
                            "headers": {"Content-Type": "application/json"},
                            "body": json.dumps(payload).encode(),
                            "oidc_token": {
                                "service_account_email": os.getenv("SERVICE_ACCOUNT_EMAIL")
                            }
                        }
                    }

                    response = client.create_task(request={"parent": parent, "task": task})
                    logger.info(f"Dispatched task for {zip_code}: {response.name}")
                    dispatched_count += 1
                
                return {
                    "status": "success",
                    "mode": "dispatch",
                    "message": f"Dispatched {dispatched_count} tasks to Cloud Tasks queue '{QUEUE_NAME}'",
                    "timestamp": datetime.now().isoformat()
                }, 200

            except ImportError:
                return {
                    "status": "failed",
                    "error": "google-cloud-tasks library not found. Please add it to requirements.txt",
                }, 500
            except Exception as e:
                logger.error(f"Dispatch failed: {e}")
                # Fallback: 如果沒有 Cloud Tasks，就直接跑 (可能會 Timeout)
                logger.warning("Falling back to sequential execution...")
                config["output_dir"] = tempfile.gettempdir()
                pipeline_result = run_pipeline_for_config(config)
                return {
                    "status": "fallback_success",
                    "mode": "scrape_fallback",
                    "pipeline": pipeline_result
                }, 200

        # ==================== 爬蟲模式 (Worker) ====================
        else:
            # 這是被 Dispatcher 呼叫，或是單獨執行的 Worker
            config = request_json.get("config", {})
            logger.info(f"Starting scrape worker with config: {config}")
            
            # Force output directory to system temp
            config["output_dir"] = tempfile.gettempdir()
            
            # 1. 執行爬蟲與 AI 處理 (單一行政區)
            pipeline_result = run_pipeline_for_config(config)
            logger.info(f"Worker result: {pipeline_result}")
            
            response = {
                "status": pipeline_result.get("status", "unknown"),
                "mode": "scrape",
                "timestamp": datetime.now().isoformat(),
                "pipeline": pipeline_result
            }
            return response, 200
        
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, 500
