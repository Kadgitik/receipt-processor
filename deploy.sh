#!/usr/bin/env bash
# deploy.sh
# Полный деплой Cloud Functions Gen2 для receipt_data_processor (Python 3.11)

set -euo pipefail

# ----------------------------
# ПАРАМЕТРЫ ДЕПЛОЯ (редактируйте)
# ----------------------------
PROJECT_ID="${PROJECT_ID:-datascience-417611}"
REGION="${REGION:-europe-central2}"
SERVICE_NAME="${SERVICE_NAME:-receipt-data-processor}"
ENTRYPOINT="${ENTRYPOINT:-receipt_data_processor}"
RUNTIME="${RUNTIME:-python311}"
DATASET="${DATASET:-vlad}"
LOCATION="${LOCATION:-europe-central2}"

# ВАЖНО: передайте реальный ключ как переменную окружения при запуске:
# GEMINI_API_KEY=xxxxx ./deploy.sh
GEMINI_API_KEY="${GEMINI_API_KEY:-AIzaSyBJTQ1yNa9fZASgskN4IBXVgy-V8J931Mw}"

if [[ -z "${GEMINI_API_KEY}" ]]; then
  echo "ERROR: GEMINI_API_KEY не задан. Экспортируйте переменную окружения и повторите запуск."
  exit 1
fi

echo "Project:       ${PROJECT_ID}"
echo "Region:        ${REGION}"
echo "Service name:  ${SERVICE_NAME}"
echo "Runtime:       ${RUNTIME}"
echo "Dataset:       ${DATASET}"
echo "Location:      ${LOCATION}"

gcloud config set project "${PROJECT_ID}"

# Опционально: включение нужных API (раскомментируйте при первом деплое)
# gcloud services enable \
#   cloudfunctions.googleapis.com \
#   run.googleapis.com \
#   artifactregistry.googleapis.com \
#   bigquery.googleapis.com \
#   aiplatform.googleapis.com

# Деплой Cloud Functions (Gen 2, HTTP)
gcloud functions deploy "${SERVICE_NAME}" \
  --gen2 \
  --runtime "${RUNTIME}" \
  --region "${REGION}" \
  --source "." \
  --entry-point "${ENTRYPOINT}" \
  --trigger-http \
  --allow-unauthenticated \
  --memory "8192MiB" \
  --timeout "3600s" \
  --min-instances "1" \
  --max-instances "5" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},DATASET=${DATASET},LOCATION=${LOCATION},GEMINI_API_KEY=${GEMINI_API_KEY}"

# Вывод URL
gcloud functions describe "${SERVICE_NAME}" --region "${REGION}" --format="value(serviceConfig.uri)"
