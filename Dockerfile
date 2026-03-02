# 使用輕量級 Python 3.10 映像檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統依賴 (例如: curl 用於健康檢查)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝 Python 依賴
# 這裡使用 requirements.txt (包含 FastAPI, uvicorn 等) 而不是 cloudfunctions_requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製專案程式碼
COPY . .

# 設定環境變數 (避免生成 .pyc 檔案，並讓 stdout/stderr 立即輸出)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 開放連接埠 (Cloud Run 預設使用 8080)
EXPOSE 8080

# 啟動命令：使用 uvicorn 執行 FastAPI 應用 (main:app)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
