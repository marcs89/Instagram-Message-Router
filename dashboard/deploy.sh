#!/bin/bash
# Deploy Instagram Dashboard to Google Cloud Run

PROJECT_ID="root-slate-454410-u0"
SERVICE_NAME="instagram-dashboard"
REGION="europe-west1"
SA_EMAIL="streamlit-dashboard@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Deploying Instagram Dashboard to Cloud Run..."

gcloud run deploy $SERVICE_NAME \
  --source=. \
  --region=$REGION \
  --project=$PROJECT_ID \
  --service-account=$SA_EMAIL \
  --set-secrets="/root/.streamlit/secrets.toml=dashboard-secrets:latest" \
  --allow-unauthenticated \
  --port=8080 \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=3 \
  --timeout=300 \
  --quiet

echo ""
echo "====================================="
echo "Deployment complete!"
echo "====================================="
echo ""
echo "Dashboard URL: https://${SERVICE_NAME}-309571657642.${REGION}.run.app"
