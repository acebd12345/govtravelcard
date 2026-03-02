"""
排程管理模組
適用於 Cloud Scheduler 自動化執行
"""

import os
import json
import logging
from typing import Dict, List, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScheduleFrequency(Enum):
    """排程頻率"""
    HOURLY = "0 * * * *"           # 每小時
    DAILY = "0 2 * * *"             # 每天凌晨 2 點
    DAILY_6AM = "0 6 * * *"         # 每天早上 6 點
    DAILY_2PM = "0 14 * * *"        # 每天下午 2 點
    TWICE_DAILY = "0 2,14 * * *"    # 每天凌晨和下午
    WEEKLY = "0 2 * * 0"            # 每週一凌晨 2 點
    MONTHLY = "0 2 1 * *"           # 每月 1 號凌晨 2 點

@dataclass
class PipelineConfig:
    """資料管線配置"""
    city: str              # 城市代碼 (如: "001" 台北市)
    industries: List[str]  # 行業別清單
    districts: List[str] = None   # 特定行政區 (若為空則全部)
    batch_size: int = 30   # 一批次處理多少筆記錄
    retry_count: int = 3   # 失敗重試次數
    
    def to_dict(self) -> Dict:
        return {
            "city": self.city,
            "industries": self.industries,
            "districts": self.districts,
            "batch_size": self.batch_size,
            "retry_count": self.retry_count
        }

@dataclass
class SchedulerConfig:
    """排程配置"""
    name: str              # 工作名稱
    frequency: ScheduleFrequency  # 執行頻率
    timezone: str = "Asia/Taipei"  # 時區
    description: str = ""  # 描述
    pipeline_configs: List[PipelineConfig] = None
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "frequency": self.frequency.value,
            "timezone": self.timezone,
            "description": self.description,
            "pipeline_configs": [cfg.to_dict() for cfg in (self.pipeline_configs or [])]
        }

# ===== 預設排程配置 =====

# 台北市完整排程 (Master Job)
# 注意：這個單一排程會觸發 "dispatch" 模式，自動平行化到所有行政區
TAIPEI_FULL_DAILY = SchedulerConfig(
    name="taipei-full-daily",
    frequency=ScheduleFrequency.DAILY,
    description="台北市全區全行業每日爬蟲 (自動平行派工)",
    pipeline_configs=[
        PipelineConfig(
            city="001",  # 台北市
            industries=["0009", "0008", "0017", "0007", "0018"], # 包含所有主要行業
            districts=None # None 代表全部行政區 (由 Master 自動派工)
        )
    ]
)

# 合併任務排程 (每天凌晨 4 點執行)
MERGE_DAILY_JOB = SchedulerConfig(
    name="merge-daily-job",
    frequency=ScheduleFrequency.DAILY,
    description="每日合併任務 - 彙整所有分散的爬蟲資料",
    pipeline_configs=[]
)

# 所有預設排程
PREDEFINED_SCHEDULES = {
    "taipei-full": TAIPEI_FULL_DAILY,
    "merge-job": MERGE_DAILY_JOB,
}

class ScheduleManager:
    """排程管理器"""
    
    def __init__(self, config_dir: str = "./scheduler_configs"):
        self.config_dir = config_dir
        os.makedirs(config_dir, exist_ok=True)
    
    def save_schedule(self, schedule: SchedulerConfig) -> str:
        """儲存排程配置"""
        config_path = os.path.join(self.config_dir, f"{schedule.name}.json")
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(schedule.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info(f"Saved schedule: {config_path}")
        return config_path
    
    def load_schedule(self, name: str) -> SchedulerConfig:
        """載入排程配置"""
        config_path = os.path.join(self.config_dir, f"{name}.json")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Schedule config not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        pipeline_configs = [
            PipelineConfig(**cfg) for cfg in data.get("pipeline_configs", [])
        ]
        
        return SchedulerConfig(
            name=data["name"],
            frequency=ScheduleFrequency(data["frequency"]),
            timezone=data.get("timezone", "Asia/Taipei"),
            description=data.get("description", ""),
            pipeline_configs=pipeline_configs
        )
    
    def list_schedules(self) -> List[str]:
        """列出所有排程"""
        files = [f.replace(".json", "") for f in os.listdir(self.config_dir) 
                 if f.endswith(".json")]
        return sorted(files)
    
    def generate_cloud_scheduler_job(self, schedule: SchedulerConfig,
                                     function_url: str) -> Dict[str, Any]:
        """產生 Cloud Scheduler Job 配置"""
        
        if "merge" in schedule.name:
            # 合併模式
            payload = {"mode": "merge"}
            schedule_time = "0 4 * * *" # 強制凌晨 4 點
        else:
            # 派工模式 (Dispatch)
            # 一般排程現在預設使用 dispatch 模式來自動平行化
            payload = {
                "mode": "dispatch",
                "config": schedule.pipeline_configs[0].to_dict() if schedule.pipeline_configs else {}
            }
            schedule_time = schedule.frequency.value

        return {
            "name": schedule.name,
            "schedule": schedule_time,
            "timezone": schedule.timezone,
            "httpTarget": {
                "uri": function_url,
                "httpMethod": "POST",
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(payload).encode('utf-8').hex()
            },
            "description": schedule.description
        }

# ===== CLI 工具 =====

def initialize_default_schedules():
    """初始化預設排程"""
    manager = ScheduleManager()
    for name, schedule in PREDEFINED_SCHEDULES.items():
        manager.save_schedule(schedule)
        print(f"✅ Created schedule: {name}")

def create_custom_schedule():
    """交互式建立自訂排程"""
    print("\n=== 建立自訂排程 ===")
    name = input("排程名稱 (如: my-schedule): ").strip()
    description = input("排程描述: ").strip()
    
    freq_input = input("執行頻率 (1=每小時, 2=每天, 3=每週, 4=每月) [2]: ") or "2"
    freqs = {
        "1": ScheduleFrequency.HOURLY,
        "2": ScheduleFrequency.DAILY,
        "3": ScheduleFrequency.WEEKLY,
        "4": ScheduleFrequency.MONTHLY,
    }
    frequency = freqs.get(freq_input, ScheduleFrequency.DAILY)
    
    timezone = input("時區 (預設: Asia/Taipei) [Asia/Taipei]: ").strip() or "Asia/Taipei"
    
    configs = []
    while True:
        city = input("\n城市代碼 (如: 001 台北市): ").strip()
        industries = input("行業別代碼 (多個用逗號分隔，如: 0009,0008): ").strip().split(",")
        
        config = PipelineConfig(
            city=city,
            industries=[i.strip() for i in industries]
        )
        configs.append(config)
        
        if input("\n新增另一個城市? (y/n) [n]: ").lower() != "y":
            break
    
    schedule = SchedulerConfig(
        name=name,
        frequency=frequency,
        timezone=timezone,
        description=description,
        pipeline_configs=configs
    )
    
    manager = ScheduleManager()
    manager.save_schedule(schedule)
    print(f"\n✅ 排程已建立: {name}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "init":
            initialize_default_schedules()
        elif sys.argv[1] == "custom":
            create_custom_schedule()
        elif sys.argv[1] == "list":
            manager = ScheduleManager()
            schedules = manager.list_schedules()
            print("已儲存的排程:")
            for s in schedules:
                print(f"  - {s}")
    else:
        print("Usage: python schedule_manager.py [init|custom|list]")
