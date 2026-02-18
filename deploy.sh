#!/bin/bash
# Deploy Instagram Message Router to Google Cloud Functions

# Konfiguration
PROJECT_ID="root-slate-454410-u0"
FUNCTION_NAME="instagram-webhook"
REGION="europe-west1"

# Secrets werden NICHT im Script gespeichert!
# Vor dem Deploy als Environment-Variablen setzen:
if [ -z "$WEBHOOK_VERIFY_TOKEN" ] || [ -z "$META_APP_SECRET" ]; then
    echo "❌ Fehlende Environment-Variablen!"
    echo ""
    echo "Vor dem Deploy ausführen:"
    echo "  export WEBHOOK_VERIFY_TOKEN=\$(gcloud secrets versions access latest --secret=webhook-verify-token --project=$PROJECT_ID)"
    echo "  export META_APP_SECRET=\$(gcloud secrets versions access latest --secret=meta-app-secret --project=$PROJECT_ID)"
    echo ""
    echo "Dann erneut: ./deploy.sh"
    exit 1
fi

echo "Deploying Instagram Webhook to Cloud Functions..."

gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=python312 \
  --region=$REGION \
  --source=. \
  --entry-point=webhook \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="WEBHOOK_VERIFY_TOKEN=$WEBHOOK_VERIFY_TOKEN,META_APP_SECRET=$META_APP_SECRET" \
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
echo "- Verifizierungstoken: (dein WEBHOOK_VERIFY_TOKEN)"
