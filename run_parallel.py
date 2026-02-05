import subprocess
import time
import os

# 配置
PYTHON_EXEC = "python3"
SCRIPT_NAME = "data_pipeline_gemini.py"
MAX_CONCURRENT_JOBS = 3  # 同時執行的最大數量 (根據電腦效能調整)

# 任務列表
# 這裡定義你要跑的所有組合
# 範例：針對不同 Zip Code 進行平行處理
TARGET_ZIPS = ["111", "103", "106", "104", "100", "114", "116", "112", "105", "110", "115", "108"]
TARGET_INDUSTRY = "0009" # 旅宿業

def run_jobs():
    processes = []
    
    for zip_code in TARGET_ZIPS:
        cmd = [PYTHON_EXEC, SCRIPT_NAME, "--zip", zip_code, "--industry", TARGET_INDUSTRY]
        print(f"[JOB START] Zip: {zip_code}")
        
        # 啟動子進程
        p = subprocess.Popen(cmd)
        processes.append(p)
        
        # 控制並發數量
        if len(processes) >= MAX_CONCURRENT_JOBS:
            # 等待任一個結束
            finished_pid = os.wait()
            print(f"[JOB FINISH] Process {finished_pid} finished.")
            
            # 清理 processes 列表 (移除已結束的)
            processes = [p for p in processes if p.poll() is None]

    # 等待剩餘的任務完成
    for p in processes:
        p.wait()

    print("[ALL JOBS COMPLETED]")

if __name__ == "__main__":
    run_jobs()
