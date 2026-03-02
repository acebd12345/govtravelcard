#!/bin/bash

# GCP 部署腳本
# 使用方式: bash deploy_to_gcp.sh <project-id> <region>

set -e

PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"asia-east1"}
BUCKET_NAME="govtravel-${PROJECT_ID}-data"
QUEUE_NAME="data-pipeline-queue"

echo "=========================================="
echo "GovTravel 部署到 Google Cloud Platform"
echo "=========================================="
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Bucket: $BUCKET_NAME"
echo ""

# 1. 設定 GCP Project
echo "[Step 1] 設定 GCP Project..."
gcloud config set project $PROJECT_ID
gcloud auth configure-docker

# 2. 建立 Cloud Storage Bucket
echo "[Step 2] 建立 Cloud Storage Bucket..."
if gsutil ls -b gs://$BUCKET_NAME > /dev/null 2>&1; then
    echo "Bucket $BUCKET_NAME 已存在"
else
    gsutil mb -l $REGION gs://$BUCKET_NAME
    echo "已建立 Bucket: $BUCKET_NAME"
fi

# 3. 設定 Cloud Tasks 佇列
echo "[Step 3] 建立 Cloud Tasks 佇列..."
gcloud tasks queues create $QUEUE_NAME \
    --location=$REGION \
    --quiet 2>/dev/null || echo "Queue $QUEUE_NAME 已存在"

# 4. 建立服務帳號
echo "[Step 4] 建立服務帳號..."
SERVICE_ACCOUNT="govtravel-app"
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com > /dev/null 2>&1; then
    gcloud iam service-accounts create $SERVICE_ACCOUNT \
        --display-name="GovTravel Application Service Account"
fi

# 5. 授予所需權限
echo "[Step 5] 授予 IAM 權限..."
ROLES=(
    "roles/storage.objectAdmin"
    "roles/logging.logWriter"
    "roles/cloudtasks.enqueuer"
    "roles/cloudscheduler.admin"
)

for ROLE in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member=serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
        --role=$ROLE \
        --quiet
done

# 6. 建立 Service Account 金鑰
echo "[Step 6] 建立 Service Account 金鑰..."
SA_KEY_FILE="gcp-sa-key.json"
if [ ! -f "$SA_KEY_FILE" ]; then
    gcloud iam service-accounts keys create $SA_KEY_FILE \
        --iam-account=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com
    echo "已建立金鑰: $SA_KEY_FILE"
else
    echo "金鑰文件已存在: $SA_KEY_FILE"
fi

# 7. 上傳 API 金鑰到 Secret Manager
echo "[Step 7] 設定 Secret Manager..."
read -p "請輸入您的 Google API Key (Maps & Gemini): " API_KEY
if [ -n "$API_KEY" ]; then
    echo -n "$API_KEY" | gcloud secrets create google-api-key \
        --replication-policy="user-managed" \
        --locations=$REGION \
        --data-file=- \
        2>/dev/null || echo "Secret 已存在"

    # Grant Secret Accessor role to Service Account
    echo "Granting Secret Accessor role to Service Account..."
    gcloud secrets add-iam-policy-binding google-api-key \
        --member=serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
        --role=roles/secretmanager.secretAccessor \
        --quiet
fi

# 8. 建立 Cloud Scheduler Job
echo "[Step 8] 設定 Cloud Scheduler..."
SCHEDULER_JOB="govtravel-daily-pipeline"
CLOUD_FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/scheduled-pipeline"

gcloud scheduler jobs create http $SCHEDULER_JOB \
    --location=$REGION \
    --schedule="0 2 * * *" \
    --timezone="Asia/Taipei" \
    --uri=$CLOUD_FUNCTION_URL \
    --http-method=POST \
    --message-body='{"config":{"city":"001"}}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
    --oidc-token-audience=$CLOUD_FUNCTION_URL \
    --quiet 2>/dev/null || echo "Scheduler job 已存在"

# 9. 部署 Cloud Function
echo "[Step 9] 部署 Cloud Function..."
gcloud functions deploy scheduled-pipeline \
    --gen2 \
    --source=. \
    --entry-point=scheduled_pipeline \
    --runtime=python311 \
    --region=$REGION \
    --memory=2048MB \
    --timeout=3600s \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars="GCP_PROJECT=$PROJECT_ID,GCP_REGION=$REGION,BUCKET_NAME=$BUCKET_NAME,SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
    --set-secrets="GOOGLE_API_KEY=google-api-key:latest"

# 10. 部署 Cloud Run (FastAPI Admin App)
echo "[Step 10] 部署 Cloud Run (Admin UI)..."
gcloud run deploy govtravel-admin \
    --source=. \
    --region=$REGION \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars="GCP_PROJECT=$PROJECT_ID,GCP_REGION=$REGION,BUCKET_NAME=$BUCKET_NAME,SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
    --set-secrets="GOOGLE_API_KEY=google-api-key:latest"

# 11. 輸出部署資訊
echo ""
echo "=========================================="
echo "✅ 部署完成！"
echo "=========================================="
echo ""
echo "📌 重要資訊:"
echo "Service Account: $SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"
echo "Storage Bucket: gs://$BUCKET_NAME"
echo "Cloud Tasks Queue: $QUEUE_NAME"
echo "Cloud Scheduler Job: $SCHEDULER_JOB"
echo "Cloud Function (Pipeline): $CLOUD_FUNCTION_URL"
echo ""
echo "🌐 管理介面 (Cloud Run):"
echo "   請前往 GCP Console 查看 Cloud Run 'govtravel-admin' 的 URL"
echo "   或執行: gcloud run services describe govtravel-admin --region=$REGION --format='value(status.url)'"
echo ""
echo "📊 查看日誌:"
echo "  - Cloud Function: gcloud functions logs read scheduled-pipeline --limit 100"
echo "  - Cloud Run: gcloud run services logs read govtravel-admin --limit 100"
echo ""
echo "⏰ Scheduler 設定:"
echo "  - 頻率: 每天凌晨 2 點 (UTC+8)"
echo "  - 時區: Asia/Taipei"
echo ""
