# Instagram Message Router

Tool zum automatischen Kategorisieren und Routen von Instagram DMs und Ad-Kommentaren.

## Features

- Empfängt Instagram DMs via Webhook (Cloud Run)
- Automatische Kategorisierung (Kundenservice, Kooperation, Feedback)
- Ad-Kommentare synchronisieren, beantworten und als erledigt markieren
- KI-Antwortvorschläge (Google Gemini)
- Streamlit-Dashboard mit Multi-User Login, Filtern, Chat-Ansicht
- Automatische Username-Auflösung via Instagram API
- Bulk-Aktionen (mehrere Chats als erledigt markieren, Blacklist)
- Blacklist-Verwaltung (persistent in BigQuery)
- Story-Reaktionen ohne Text werden gefiltert

## Architektur

```
Instagram-Message-Router/
├── main.py                           # Webhook Handler (Cloud Function)
├── refresh_tokens.py                 # Token-Refresh Script (manuell)
├── requirements.txt                  # Dependencies: Webhook
├── Dockerfile                        # Cloud Function Container
├── deploy.sh                         # Deploy-Script: Webhook
├── env_template.txt                  # Env-Template (KEINE echten Werte!)
├── .env                              # Lokale Secrets (NICHT committen!)
├── ANLEITUNG.md                      # Anwender-Dokumentation
├── .gitignore
├── dashboard/
│   ├── app.py                        # Streamlit Dashboard
│   ├── Dockerfile                    # Cloud Run Container
│   ├── entrypoint.sh                 # Startup: Secrets laden + Streamlit starten
│   ├── deploy.sh                     # Deploy-Script: Dashboard (Cloud Run)
│   ├── .dockerignore                 # Schützt secrets.toml vor Docker-Build
│   ├── requirements.txt              # Dependencies: Dashboard
│   └── .streamlit/
│       ├── config.toml               # Streamlit Config (Theme)
│       └── secrets.toml              # Lokale Secrets (NICHT committen!)
└── cloud_functions/
    └── token_refresh/
        ├── main.py                   # Cloud Function: Token Refresh
        └── requirements.txt          # Dependencies: Token Refresh
```

## Komponenten

### 1. Webhook (`main.py`)
- Empfängt POST-Events von Meta (Instagram DMs)
- Signaturprüfung ist **fail-closed** (ohne `META_APP_SECRET` werden alle Anfragen abgelehnt)
- Auto-Tagging nach Keywords (Kundenservice, Kooperation, Feedback, Retoure, etc.)
- Idempotente Speicherung in BigQuery (kein Duplikat bei Webhook-Retries)
- Verarbeitet ein- und ausgehende Nachrichten
- Story-Reaktionen ohne Text werden übersprungen
- Deployed als Cloud Run Service (`instagram-webhook`, europe-west1)

### 2. Dashboard (`dashboard/app.py`)
- Streamlit-App mit Multi-User Login (individuelle Passwörter pro Team-Mitglied)
- **Inbox:** Chat-Liste mit Filtern (Alle/Unbeantwortet, Tags), Paging, Bulk-Aktionen
- **Chat-Ansicht:** Vollständiger Verlauf, KI-Vorschläge, direkt antworten
- **Ad-Kommentare:** Sync von Instagram, Antworten, Sentiment-Analyse, Timestamps
- **Header-Stats:** Offene Chats + offene Kommentare als Badges neben Logo
- Automatische Username-Auflösung (bis zu 10 pro Seitenaufruf, persistent in DB)
- Alle DB-Queries parameterisiert (kein SQL-Injection-Risiko)
- Deployed als Cloud Run Service (`instagram-dashboard`, europe-west1)
- Service Account `streamlit-dashboard@...` für BigQuery + Secret Manager Zugriff
- Secrets werden aus Google Secret Manager gemountet (`dashboard-secrets`)

### 3. Token Refresh (`cloud_functions/token_refresh/`)
- Erneuert Instagram Access Token via Meta API (Token läuft nach 60 Tagen ab)
- Speichert neuen Token in Google Secret Manager
- Deployed als Cloud Function (`instagram-token-refresh`)
- **Automatisch via Cloud Scheduler** (`instagram-token-refresh-monthly`, am 1. jeden Monats um 03:00)

## Setup

### Voraussetzungen
- Google Cloud Projekt mit BigQuery + Secret Manager
- Meta Developer App mit Instagram Messaging Permissions
- Python 3.12+

### 1. Secrets konfigurieren

Alle Secrets werden über **Environment-Variablen** (lokal via `.env`) oder **Google Secret Manager** (Produktion) bereitgestellt.

```bash
cp env_template.txt .env
# Dann .env editieren und echte Werte eintragen
```

Für das Dashboard: `dashboard/.streamlit/secrets.toml` mit API-Keys und User-Passwörtern befüllen.

### 2. Webhook deployen
```bash
export WEBHOOK_VERIFY_TOKEN=$(gcloud secrets versions access latest --secret=webhook-verify-token)
export META_APP_SECRET=$(gcloud secrets versions access latest --secret=meta-app-secret)
./deploy.sh
```

### 3. Dashboard starten (lokal)
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

### 4. Dashboard deployen (Cloud Run)

Das Dashboard läuft permanent auf Google Cloud Run. Re-Deployment nach Code-Änderungen:

```bash
cd dashboard
./deploy.sh
```

**Produktions-URL:** https://instagram-dashboard-309571657642.europe-west1.run.app

Die Dashboard-Secrets (User-Passwörter, API Keys) liegen im Google Secret Manager unter `dashboard-secrets`. Bei Passwort-Änderungen:

```bash
# secrets.toml lokal bearbeiten, dann hochladen:
gcloud secrets versions add dashboard-secrets --data-file=.streamlit/secrets.toml --project=root-slate-454410-u0
# Danach Re-Deploy damit die neue Version geladen wird:
./deploy.sh
```

## BigQuery Tabellen

- `instagram_messages.messages` - DM-Nachrichten (ein- und ausgehend)
- `instagram_messages.ad_comments` - Ad-Kommentare mit Sentiment und Replies
- `instagram_messages.blacklist` - Blockierte User (user_id + username)

## Team-Mitglieder / Login

Definiert in `app.py` unter `TEAM_MEMBERS`. Individuelle Passwörter werden in `secrets.toml` als `USER_XX = "passwort"` hinterlegt:

| Kürzel | Name  | Secret-Key |
|--------|-------|------------|
| MS     | Marc  | `USER_MS`  |
| AS     | Anni  | `USER_AS`  |
| SM     | Sina  | `USER_SM`  |
| JD     | Jessy | `USER_JD`  |
| SG     | Sinem | `USER_SG`  |
| KB     | Kea   | `USER_KB`  |

Fallback: `APP_PASSWORD` funktioniert für alle (loggt als MS ein).

## Sicherheit

- Webhook-Signaturprüfung ist **fail-closed** (ohne `META_APP_SECRET` werden alle Anfragen abgelehnt)
- Keine Secrets im Code oder in Templates
- Alle BigQuery-Queries nutzen parameterisierte Parameter
- Sensitive Daten werden nicht in Logs geschrieben
