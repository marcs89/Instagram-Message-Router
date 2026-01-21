"""
Instagram Message Router - Webhook Handler
Cloud Function zum Empfangen von Instagram DMs
"""

import os
import json
import hashlib
import hmac
from datetime import datetime
import functions_framework
from flask import Request

# Wird später für BigQuery Import verwendet
# from google.cloud import bigquery

# Environment Variables
VERIFY_TOKEN = os.environ.get('WEBHOOK_VERIFY_TOKEN', 'lilimaus_webhook_2024_secure')
APP_SECRET = os.environ.get('META_APP_SECRET', '')


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verifiziert dass die Anfrage wirklich von Meta kommt"""
    if not APP_SECRET:
        print("WARNING: APP_SECRET nicht gesetzt, Signatur-Check übersprungen")
        return True
    
    expected_signature = hmac.new(
        APP_SECRET.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(f"sha256={expected_signature}", signature)


def auto_tag_message(message_text: str) -> dict:
    """
    Vergibt automatisch Tags basierend auf Keywords.
    Tags: Kundenservice, Feedback, Kooperationen
    """
    text_lower = message_text.lower() if message_text else ""
    
    # Keywords für jeden Tag
    tag_keywords = {
        "Kooperationen": [
            "zusammenarbeit", "kooperation", "influencer", "pr", "collab",
            "partnership", "werbung", "promotion", "creator", "ugc",
            "ambassador", "botschafter"
        ],
        "Feedback": [
            "toll", "super", "danke", "liebe", "perfekt", "amazing",
            "love", "great", "awesome", "wunderschön", "begeistert",
            "empfehlen", "zufrieden", "glücklich", "happy",
            "❤️", "🔥", "😍", "👍", "💕", "🥰", "😊"
        ]
    }
    
    detected_tags = []
    
    # Prüfe auf Kooperationen und Feedback
    for tag, keywords in tag_keywords.items():
        for keyword in keywords:
            if keyword in text_lower:
                detected_tags.append(tag)
                break
    
    # Wenn weder Kooperation noch Feedback -> Kundenservice
    if not detected_tags:
        detected_tags = ["Kundenservice"]
    
    # Priorität: Kooperationen = normal, Kundenservice = normal, Feedback = low
    priority = "normal"
    
    return {
        "tags": ",".join(detected_tags),
        "priority": priority
    }


def process_message(messaging_event: dict) -> dict:
    """Verarbeitet ein einzelnes Messaging Event"""
    
    sender_id = messaging_event.get("sender", {}).get("id", "unknown")
    recipient_id = messaging_event.get("recipient", {}).get("id", "unknown")
    timestamp = messaging_event.get("timestamp", 0)
    
    # Nachrichteninhalt extrahieren
    message = messaging_event.get("message", {})
    message_id = message.get("mid", "")
    message_text = message.get("text", "")
    
    # Attachments (Bilder, etc.)
    attachments = message.get("attachments", [])
    has_attachments = len(attachments) > 0
    attachment_types = [att.get("type", "unknown") for att in attachments]
    
    # Story Mention/Reply erkennen
    is_story_reply = "story" in message.get("reply_to", {})
    
    # Auto-Tagging
    tagging = auto_tag_message(message_text)
    
    # Wenn Story Reply, Tag auf Feedback setzen
    if is_story_reply:
        tagging["tags"] = "Feedback"
        tagging["priority"] = "low"
    
    processed = {
        "message_id": message_id,
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "timestamp": timestamp,
        "received_at": datetime.utcnow().isoformat(),
        "message_text": message_text,
        "has_attachments": has_attachments,
        "attachment_types": attachment_types,
        "is_story_reply": is_story_reply,
        "tags": tagging["tags"],
        "priority": tagging["priority"],
        "status": "new"
    }
    
    return processed


def analyze_comment_sentiment(text: str) -> dict:
    """Analysiert das Sentiment eines Kommentars (Keyword-basiert für Webhook)"""
    text_lower = text.lower() if text else ""
    
    negative_keywords = ["schlecht", "enttäuscht", "schrecklich", "betrug", "fake", 
                        "abzocke", "nie wieder", "warnung", "finger weg", "miserabel",
                        "scam", "terrible", "awful", "worst", "hate"]
    question_keywords = ["?", "wann", "wie", "verfügbar", "größe", "preis", 
                        "kostet", "lieferung", "farbe", "where", "when", "how"]
    
    if any(kw in text_lower for kw in negative_keywords):
        return {"sentiment": "negative", "score": 0.8, "is_question": False, "contains_complaint": True}
    elif any(kw in text_lower for kw in question_keywords):
        return {"sentiment": "question", "score": 0.7, "is_question": True, "contains_complaint": False}
    else:
        return {"sentiment": "positive", "score": 0.6, "is_question": False, "contains_complaint": False}


def process_comment(change: dict, entry: dict) -> dict:
    """Verarbeitet einen Ad/Post Kommentar (Webhook)"""
    value = change.get("value", {})
    
    comment_id = value.get("comment_id", "") or value.get("id", "")
    post_id = value.get("post_id", "") or value.get("media_id", "") or entry.get("id", "")
    
    # Kommentar-Details
    comment_text = value.get("message", "") or value.get("text", "")
    parent_id = value.get("parent_id", "")
    
    # Commenter Info
    from_data = value.get("from", {})
    commenter_id = from_data.get("id", "unknown")
    commenter_name = from_data.get("name", "") or from_data.get("username", "Unbekannt")
    
    # Zeitstempel
    created_time = value.get("created_time", "") or value.get("timestamp", "")
    if not created_time:
        created_time = datetime.utcnow().isoformat()
    
    # Sentiment analysieren
    sentiment = analyze_comment_sentiment(comment_text)
    
    # Post-Info aus dem Webhook (falls verfügbar)
    media = value.get("media", {})
    post_shortcode = media.get("shortcode", "")
    
    return {
        "comment_id": comment_id,
        "post_id": post_id,
        "post_shortcode": post_shortcode,
        "post_type": "ad",  # Webhook-Kommentare kommen von Ads
        "ad_id": "",
        "ad_name": "",  # Kann später via Dashboard ergänzt werden
        "commenter_id": commenter_id,
        "commenter_name": commenter_name,
        "comment_text": comment_text,
        "parent_comment_id": parent_id,
        "created_at": created_time,
        "received_at": datetime.utcnow().isoformat(),
        "sentiment": sentiment["sentiment"],
        "sentiment_score": sentiment["score"],
        "is_question": sentiment["is_question"],
        "contains_complaint": sentiment["contains_complaint"],
        "status": "new",
        "priority": "high" if sentiment["sentiment"] == "negative" else "normal",
        "has_our_reply": False,
        "is_done": False
    }


def save_comment_to_bigquery(comment_data: dict):
    """Speichert einen Kommentar in BigQuery (Webhook)"""
    from google.cloud import bigquery
    
    try:
        client = bigquery.Client()
        table_id = "root-slate-454410-u0.instagram_messages.ad_comments"
        
        def escape(s):
            if not s: return ""
            if not isinstance(s, str): return str(s)
            return s.replace("'", "''").replace("\\", "\\\\")
        
        # Prüfe erst ob Kommentar schon existiert
        check_query = f"""
        SELECT comment_id FROM `{table_id}`
        WHERE comment_id = '{escape(comment_data.get("comment_id"))}'
        """
        check_result = client.query(check_query).to_dataframe()
        
        if not check_result.empty:
            print(f"[BigQuery] Comment {comment_data.get('comment_id')} already exists, skipping")
            return
        
        # Timestamp vorbereiten
        created_at = comment_data.get("created_at", "")
        if created_at:
            # Versuche ISO Format zu parsen, sonst aktuelles Datum
            try:
                if "T" not in created_at:
                    created_at = datetime.utcnow().isoformat()
            except:
                created_at = datetime.utcnow().isoformat()
        else:
            created_at = datetime.utcnow().isoformat()
        
        query = f"""
        INSERT INTO `{table_id}`
        (comment_id, post_id, post_shortcode, post_type, ad_id, ad_name, 
         commenter_id, commenter_name, comment_text, parent_comment_id, 
         created_at, received_at, sentiment, sentiment_score, is_question, 
         contains_complaint, status, is_hidden, is_deleted, priority,
         has_our_reply, is_done)
        VALUES (
            '{escape(comment_data.get("comment_id"))}',
            '{escape(comment_data.get("post_id"))}',
            '{escape(comment_data.get("post_shortcode", ""))}',
            '{escape(comment_data.get("post_type", "ad"))}',
            '{escape(comment_data.get("ad_id", ""))}',
            '{escape(comment_data.get("ad_name", ""))}',
            '{escape(comment_data.get("commenter_id"))}',
            '{escape(comment_data.get("commenter_name"))}',
            '{escape(comment_data.get("comment_text"))}',
            '{escape(comment_data.get("parent_comment_id", ""))}',
            TIMESTAMP('{created_at}'),
            TIMESTAMP('{comment_data.get("received_at")}'),
            '{escape(comment_data.get("sentiment", "positive"))}',
            {comment_data.get("sentiment_score", 0.5)},
            {str(comment_data.get("is_question", False)).upper()},
            {str(comment_data.get("contains_complaint", False)).upper()},
            'new',
            FALSE,
            FALSE,
            '{escape(comment_data.get("priority", "normal"))}',
            FALSE,
            FALSE
        )
        """
        
        job = client.query(query)
        job.result()
        print(f"[BigQuery] Saved comment {comment_data.get('comment_id')}")
        
    except Exception as e:
        print(f"[BigQuery] Error saving comment: {e}")


def save_to_bigquery(message_data: dict):
    """
    Speichert die Nachricht in BigQuery via INSERT Statement (sofort updatebar).
    """
    from google.cloud import bigquery
    
    try:
        client = bigquery.Client()
        table_id = "root-slate-454410-u0.instagram_messages.messages"
        
        # Safe string escaping
        def escape(s):
            if not s: return ""
            if not isinstance(s, str): return str(s)
            return s.replace("'", "''").replace("\\", "\\\\")
            
        # Werte vorbereiten
        msg_id = escape(message_data.get("message_id"))
        sender_id = escape(message_data.get("sender_id"))
        recipient_id = escape(message_data.get("recipient_id"))
        text = escape(message_data.get("message_text"))
        tags = escape(message_data.get("tags", "Kundenservice"))
        prio = escape(message_data.get("priority", "normal"))
        
        ts = int(message_data.get("timestamp", 0) or 0)
        received = message_data.get("received_at")
        
        query = f"""
        INSERT INTO `{table_id}`
        (message_id, sender_id, recipient_id, timestamp, received_at, 
         message_text, has_attachments, attachment_types, is_story_reply, 
         tags, priority, status)
        VALUES (
            '{msg_id}',
            '{sender_id}',
            '{recipient_id}',
            {ts},
            '{received}',
            '{text}',
            {str(message_data.get("has_attachments", False)).upper()},
            '{json.dumps(message_data.get("attachment_types", []))}',
            {str(message_data.get("is_story_reply", False)).upper()},
            '{tags}',
            '{prio}',
            'new'
        )
        """
        
        job = client.query(query)
        job.result() # Warten auf Fertigstellung
        print(f"[BigQuery] Saved message {msg_id}")
            
    except Exception as e:
        print(f"[BigQuery] Error: {e}")


@functions_framework.http
def webhook(request: Request):
    """
    Hauptendpunkt für Instagram/Messenger Webhooks.
    
    GET  -> Webhook Verification (von Meta beim Setup aufgerufen)
    POST -> Eingehende Nachrichten
    """
    
    # ===== GET: Webhook Verification =====
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        
        print(f"[Verification] mode={mode}, token={token}")
        
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print("[Verification] SUCCESS - Token matches")
            return challenge, 200
        else:
            print(f"[Verification] FAILED - Expected token: {VERIFY_TOKEN}")
            return "Verification failed", 403
    
    # ===== POST: Incoming Messages =====
    if request.method == "POST":
        # Signatur verifizieren
        signature = request.headers.get("X-Hub-Signature-256", "")
        if not verify_signature(request.data, signature):
            print("[Security] Invalid signature!")
            return "Invalid signature", 403
        
        # Payload parsen
        try:
            payload = request.get_json()
        except Exception as e:
            print(f"[Error] JSON parsing failed: {e}")
            return "Invalid JSON", 400
        
        print(f"[Webhook] Received: {json.dumps(payload, indent=2)}")
        
        # Object Type prüfen (instagram oder page)
        object_type = payload.get("object", "")
        
        if object_type not in ["instagram", "page"]:
            print(f"[Webhook] Ignored object type: {object_type}")
            return "OK", 200
        
        # Entries verarbeiten
        entries = payload.get("entry", [])
        processed_messages = []
        processed_comments = []
        
        for entry in entries:
            # ===== MESSAGING (DMs) =====
            messaging_events = entry.get("messaging", []) or entry.get("messages", [])
            
            for event in messaging_events:
                try:
                    processed = process_message(event)
                    processed_messages.append(processed)
                    
                    # In BigQuery speichern
                    save_to_bigquery(processed)
                    
                    # Log für Debugging
                    print(f"[Processed DM] Tags: {processed['tags']} | "
                          f"Priority: {processed['priority']} | "
                          f"Text: {processed['message_text'][:50]}...")
                    
                except Exception as e:
                    print(f"[Error] Processing DM failed: {e}")
            
            # ===== FEED/COMMENTS (Ad-Kommentare) =====
            changes = entry.get("changes", [])
            
            for change in changes:
                field = change.get("field", "")
                
                # Nur Kommentare verarbeiten
                if field == "comments" or field == "feed":
                    try:
                        # Prüfen ob es ein Kommentar-Event ist
                        value = change.get("value", {})
                        item = value.get("item", "")
                        verb = value.get("verb", "")
                        
                        # Nur neue Kommentare (nicht edits/deletes)
                        if item == "comment" and verb in ["add", "created"]:
                            processed = process_comment(change, entry)
                            processed_comments.append(processed)
                            
                            # In BigQuery speichern
                            save_comment_to_bigquery(processed)
                            
                            # Alert bei negativem Sentiment
                            if processed["sentiment"] == "negative":
                                print(f"[ALERT] Negative comment detected! "
                                      f"Text: {processed['comment_text'][:50]}...")
                            else:
                                print(f"[Processed Comment] {processed['sentiment']} | "
                                      f"Text: {processed['comment_text'][:50]}...")
                    
                    except Exception as e:
                        print(f"[Error] Processing comment failed: {e}")
        
        return json.dumps({
            "status": "received",
            "processed_messages": len(processed_messages),
            "processed_comments": len(processed_comments)
        }), 200
    
    return "Method not allowed", 405


# Flask App für Cloud Run
from flask import Flask
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    from flask import request as flask_request
    return webhook(flask_request)

# Health check endpoint
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

# Lokaler Test
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test-Kategorisierung
        test_messages = [
            "Welche Größe soll ich bei 175cm nehmen?",
            "Ist das Produkt wieder verfügbar?",
            "Hey, ich bin Influencerin und würde gerne zusammenarbeiten",
            "Mein Paket ist kaputt angekommen!",
            "Ihr seid toll! ❤️",
            "Hallo, eine Frage...",
        ]
        
        for msg in test_messages:
            result = auto_tag_message(msg)
            print(f"'{msg[:40]}...' -> {result}")
    else:
        # Run Flask server
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
