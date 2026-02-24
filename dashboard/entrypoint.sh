#!/bin/bash
# Write secrets.toml from mounted Secret Manager secret
if [ -f /secrets/secrets.toml ]; then
    mkdir -p /app/.streamlit
    cp /secrets/secrets.toml /app/.streamlit/secrets.toml
fi

exec streamlit run app.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --browser.gatherUsageStats=false
