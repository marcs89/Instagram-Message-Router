# LILIMAUS Instagram Inbox – Anleitung

## Zugang

Öffne die App im Browser: **http://localhost:8501** (lokal) oder die Streamlit Cloud URL (Produktion).

Melde dich mit deinem persönlichen Passwort an. Falls du noch kein eigenes hast, nutze das Team-Passwort.

---

## Übersicht

Nach dem Login siehst du oben:
- **Lilimaus-Logo** links
- **Offene Chats / Offene Kommentare** als Badges in der Mitte
- **Dein Name + Logout** rechts

Darunter zwei Tabs: **Inbox** und **Ad-Kommentare**.

---

## Inbox (DMs)

### Chat-Liste (links)

- **Filter:** Oben über der Liste kannst du zwischen "Alle" und "Unbeantwortet" wechseln und nach Tags filtern.
- **Grünes ✅:** Chat ist beantwortet / erledigt.
- **Rotes 🔴:** Neue unbeantwortete Nachricht.
- **Paging:** Bei vielen Chats kannst du mit ◀ / ▶ blättern.

### Chat-Ansicht (rechts)

Klicke auf einen Chat, um den Verlauf zu sehen:
- **Eigene Nachrichten** werden rechts (dunkel) angezeigt.
- **Kundennachrichten** werden links (hell) angezeigt.
- Oben: **🔄** zum Aktualisieren, **🚫** zum Blockieren des Users.

### Antworten

1. Schreibe deine Antwort in das Textfeld unten.
2. Klicke **💾 Speichern** um die Antwort nur in der DB zu hinterlegen.
3. Klicke **📤 Senden** um die Antwort direkt an Instagram zu schicken.
4. **✨ KI-Vorschlag:** Generiert automatisch einen Antwort-Entwurf.

### Auswahl-Modus (Bulk-Aktionen)

1. Aktiviere den **☑️ Auswahl-Modus** Toggle.
2. Wähle einzelne Chats per Checkbox, oder nutze "Seite auswählen" / "Alle auswählen".
3. Klicke **✅ Als erledigt markieren** oder **🚫 Blacklist**.

### Ältere Nachrichten laden

Am Ende eines Chats gibt es den Button **📥 Ältere Nachrichten von Instagram laden**. Dieser holt die letzten ~20 Nachrichten direkt von Instagram nach (nützlich wenn Nachrichten vor dem Webhook-Start gesendet wurden).

---

## Ad-Kommentare

### Kommentare synchronisieren

Klicke **🔄 Sync Instagram** um neue Kommentare von euren Werbeanzeigen zu laden. Das kann 30-60 Sekunden dauern.

### Stats-Leiste

Zeigt auf einen Blick: Gesamt, Offen, Beantwortet, Negativ, Fragen.

### Filter

- **Alle:** Zeigt alle Kommentare.
- **Unbearbeitet:** Nur Kommentare die noch nicht beantwortet oder als erledigt markiert sind.

### Kommentar-Status

- **⚠️ Offen:** Noch nicht bearbeitet.
- **✅ Bereits beantwortet:** Wurde auf Instagram beantwortet (automatisch erkannt).
- **✓ Erledigt:** Manuell als erledigt markiert.

### Auf Kommentar antworten

1. Klicke **💬 Antworten** neben dem Kommentar.
2. Ein KI-Vorschlag wird automatisch generiert.
3. Passe den Text an und klicke **📤 Senden** (postet direkt auf Instagram) oder **💾 Speichern** (nur in DB).
4. **🔄 Neu** generiert einen neuen KI-Vorschlag.

### Weitere Aktionen

- **✓ Erledigt:** Markiert den Kommentar als bearbeitet (ohne zu antworten).
- **👁️:** Blendet den Kommentar im Dashboard aus (wird auf Instagram NICHT gelöscht).

---

## Blockierte User

Blockierte User werden aus der Chat-Liste und der "Offen"-Zählung ausgeblendet. Du findest die Liste am Ende der Chat-Liste unter **🚫 Blockierte User**. Klicke **✓** um einen User wieder zu entsperren.

---

## Tipps

- **Username wird nicht angezeigt?** Beim ersten Laden einer Seite werden automatisch bis zu 10 fehlende Usernames aufgelöst. Lade die Seite einfach nochmal.
- **Chat zeigt nur eigene Nachrichten?** Klicke auf den Chat und dann **📥 Ältere Nachrichten von Instagram laden**.
- **Kommentare fehlen?** Klicke **🔄 Sync Instagram** im Ad-Kommentare-Tab.
- **Stats nicht aktuell?** Warte 30 Sekunden (Cache) oder klicke 🔄 zum Aktualisieren.
