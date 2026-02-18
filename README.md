# Instagram Message Router

Tool zum automatischen Kategorisieren und Routen von Instagram DMs und Ad-Kommentaren.

## Features

- Empfängt Instagram DMs via Webhook (Cloud Run / Cloud Function)
- Automatische Kategorisierung (Kundenservice, Kooperation, Feedback)
- Ad-Kommentare synchronisieren und beantworten
- KI-Antwortvorschläge (Google Gemini)
- Streamlit-Dashboard mit Login, Filtern, Chat-Ansicht
- Blacklist-Verwaltung (persistent in BigQuery)

## Architektur

```
Instagram-Message-Router/
├── main.py                           # Webhook Handler (Cloud Run)
├── refresh_tokens.py                 # Token-Refresh Script (manuell)
├── requirements.txt                  # Dependencies: Webhook
├── Dockerfile                        # Cloud Run Container
├── deploy.sh                         # Deploy-Script (liest Secrets aus Env)
├── env_template.txt                  # Env-Template (KEINE echten Werte!)
├── .env                              # Lokale Secrets (NICHT committen!)
├── .gitignore
├── dashboard/
│   ├── app.py                        # Streamlit Dashboard
│   ├── requirements.txt              # Dependencies: Dashboard
│   └── .streamlit/
│       ├── config.toml               # Streamlit Config
│       └── secrets.toml              # Streamlit Secrets (NICHT committen!)
└── cloud_functions/
    └── token_refresh/
        ├── main.py                   # Cloud Function: Token Refresh
        └── requirements.txt          # Dependencies: Token Refresh
```

## Komponenten

### 1. Webhook (`main.py`)
- Empfängt POST-Events von Meta (Instagram DMs + Ad-Kommentare)
- Signaturprüfung (fail-closed: ohne APP_SECRET wird alles abgelehnt)
- Auto-Tagging nach Keywords
- Idempotente Speicherung in BigQuery (kein Duplikat bei Webhook-Retries)
- Deployed als Cloud Run Service oder Cloud Function

### 2. Dashboard (`dashboard/app.py`)
- Streamlit-App mit Multi-User Login
- Inbox: Chat-Verlauf mit Antwort-Funktion
- Ad-Kommentare: Sync von Instagram, Antworten, Sentiment-Analyse
- KI-Vorschläge via Google Gemini
- Alle DB-Queries parameterisiert (kein SQL-Injection-Risiko)

### 3. Token Refresh (`refresh_tokens.py` + `cloud_functions/token_refresh/`)
- Erneuert Instagram Access Token via Meta API
- Speichert neuen Token in Google Secret Manager
- Kann manuell oder via Cloud Scheduler ausgeführt werden

## Setup

### Voraussetzungen
- Google Cloud Projekt mit BigQuery + Secret Manager
- Meta Developer App mit Instagram Messaging Permissions
- Python 3.12+

### 1. Secrets konfigurieren

Alle Secrets werden über **Environment-Variablen** (lokal via `.env`) oder **Google Secret Manager** (Produktion) bereitgestellt.

Kopiere `env_template.txt` nach `.env` und fülle die Werte aus:
```bash
cp env_template.txt .env
# Dann .env editieren und echte Werte eintragen
```

### 2. Webhook deployen
```bash
# Secrets aus Secret Manager laden
export WEBHOOK_VERIFY_TOKEN=$(gcloud secrets versions access latest --secret=webhook-verify-token)
export META_APP_SECRET=$(gcloud secrets versions access latest --secret=meta-app-secret)

# Deploy
./deploy.sh
```

### 3. Dashboard starten
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

## BigQuery Tabellen

- `instagram_messages.messages` - DM-Nachrichten
- `instagram_messages.ad_comments` - Ad-Kommentare
- `instagram_messages.blacklist` - Blockierte User

## Sicherheit

- Webhook-Signaturprüfung ist **fail-closed** (ohne `META_APP_SECRET` werden alle Anfragen abgelehnt)
- Keine Secrets im Code oder in Templates
- Alle BigQuery-Queries nutzen parameterisierte Parameter (kein String-Escaping)
- Sensitive Daten werden nicht in Logs geschrieben
