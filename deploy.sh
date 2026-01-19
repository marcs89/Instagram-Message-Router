#!/bin/bash
# Deploy Instagram Message Router to Google Cloud Functions

# Konfiguration
PROJECT_ID="root-slate-454410-u0"
FUNCTION_NAME="instagram-webhook"
REGION="europe-west1"

# Environment Variables für die Function
VERIFY_TOKEN="lilimaus_webhook_2024_secure"
APP_SECRET="fb7c65de8d132aeb367c11a15970b3e3"

echo "Deploying Instagram Webhook to Cloud Functions..."

gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=python312 \
  --region=$REGION \
  --source=. \
  --entry-point=webhook \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="WEBHOOK_VERIFY_TOKEN=$VERIFY_TOKEN,META_APP_SECRET=$APP_SECRET" \
  --project=$PROJECT_ID

echo ""
echo "====================================="
echo "Deployment complete!"
echo "====================================="
echo ""
echo "Deine Webhook URL ist:"
echo "https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo ""
echo "Trage diese URL in Meta ein:"
echo "- Callback-URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo "- Verifizierungstoken: $VERIFY_TOKEN"
