# Instagram Message Router

Tool zum automatischen Kategorisieren und Routen von Instagram DMs an die richtigen Mitarbeiter.

## Features

- 📥 Empfängt Instagram DMs via Webhook
- 🏷️ Automatische Kategorisierung (Größenberatung, Kooperation, Support, etc.)
- 👥 Zuweisung an Mitarbeiter nach Kategorie
- 📊 Dashboard mit Filtern und Status-Tracking
- ⚡ Schnellantworten via Templates

## Setup-Status

- [ ] Meta Developer App erstellt
- [ ] Instagram Business Account verknüpft
- [ ] App Review eingereicht
- [ ] App Review genehmigt
- [ ] Backend deployed
- [ ] Dashboard live

---

## Phase 1: Meta App Setup (JETZT)

### Schritt 1: Meta Developer Account

1. Gehe zu: https://developers.facebook.com/
2. Klicke "Erste Schritte" oder "Log In" (mit eurem Business Facebook Account)
3. Akzeptiere die Nutzungsbedingungen

### Schritt 2: Neue App erstellen

1. Klicke "Meine Apps" → "App erstellen"
2. Wähle **"Anderer"** als Use Case (oder "Business" wenn verfügbar)
3. Wähle App-Typ: **"Business"**
4. App-Details:
   - **App-Name:** `[Firmenname] Instagram Router` (z.B. "ACME Instagram Router")
   - **Kontakt-E-Mail:** Eure Business-E-Mail
   - **Business Portfolio:** Euer Meta Business Account auswählen
5. Klicke "App erstellen"

### Schritt 3: Instagram Messaging aktivieren

1. In der App-Übersicht: Suche nach **"Instagram"** in den Produkten
2. Klicke bei **"Instagram Basic Display"** NICHT auf Einrichten
3. Suche stattdessen nach **"Messenger"** → "Einrichten"
4. Unter "Instagram" → "Instagram-Nachrichten-API" aktivieren

### Schritt 4: Instagram Business Account verbinden

1. Gehe zu "Instagram" → "Instagram-Konten"
2. Klicke "Konto hinzufügen"
3. Verbinde euren Instagram Business Account
4. **Wichtig:** Der Instagram Account muss:
   - Ein Business oder Creator Account sein (kein privater)
   - Mit einer Facebook Page verknüpft sein

### Schritt 5: Berechtigungen konfigurieren

Unter "App-Einstellungen" → "Berechtigungen" benötigt ihr:

| Permission | Zweck |
|------------|-------|
| `instagram_basic` | Basis-Zugriff |
| `instagram_manage_messages` | Nachrichten lesen/senden |
| `pages_messaging` | Messenger-Zugriff |
| `pages_manage_metadata` | Webhook-Subscriptions |

### Schritt 6: App Review einreichen

1. Gehe zu "App-Prüfung" → "Berechtigungen und Features"
2. Für jede Permission:
   - Klicke "Anfordern"
   - Beschreibe den Use Case (siehe unten)
   - Lade ggf. Screenshots hoch
3. Reiche die App zur Prüfung ein

**Use Case Beschreibung (Copy-Paste Vorlage):**

```
Wir sind ein E-Commerce Unternehmen und erhalten täglich Kundenanfragen 
via Instagram Direct Messages. Diese Nachrichten umfassen:
- Produktfragen (Größenberatung, Verfügbarkeit)
- Kundenservice-Anfragen
- Feedback und Bewertungen

Unser Tool soll:
1. Eingehende Nachrichten automatisch kategorisieren
2. Nachrichten an den zuständigen Mitarbeiter weiterleiten
3. Antworten über ein zentrales Dashboard ermöglichen

Dies verbessert unsere Antwortzeiten und Kundenzufriedenheit.
```

---

## Phase 2: Backend (nach App-Erstellung)

Wird im nächsten Schritt angelegt:
- `webhook.py` - Cloud Function für eingehende Nachrichten
- `categorizer.py` - Kategorisierungslogik
- `bigquery_client.py` - Datenbank-Verbindung

---

## Phase 3: Dashboard (nach Backend)

- Streamlit-basiertes Dashboard
- Filter nach Kategorie, Status, Mitarbeiter
- Schnellantwort-Templates

---

## Dateien

```
Instagram-Message-Router/
├── README.md              # Diese Datei
├── env_template.txt       # Umgebungsvariablen Template
├── requirements.txt       # Python Dependencies
├── webhook.py            # Cloud Function Entry Point
├── categorizer.py        # Nachricht → Kategorie
├── bigquery_client.py    # DB Queries
├── message_sender.py     # Antworten senden
└── dashboard/
    └── app.py            # Streamlit Dashboard
```
