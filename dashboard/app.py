"""
Instagram Message Router - Dashboard (Simplified)
Streamlit App zum Verwalten von Instagram DMs
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import json
import os
import requests
from pathlib import Path
from dotenv import load_dotenv
from google import genai

# Load .env
possible_env_paths = [
    Path(__file__).parent.parent / ".env",
    Path(__file__).parent / ".env",
    Path.cwd() / ".env",
    Path.cwd().parent / ".env",
]
for env_path in possible_env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        break

if not os.getenv("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = "AIzaSyBCZxnGUJwFJ6j3f4brhV8XrLLUYV9vjHg"

# Google Secret Manager for Token Management
GCP_PROJECT_ID = "root-slate-454410-u0"

@st.cache_data(ttl=300)  # 5 min cache
def get_secret_from_gcp(secret_name: str) -> str:
    """Load secret from Google Secret Manager"""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        # Fallback to None if Secret Manager fails
        return None

# Instagram API Functions
def get_page_access_token():
    """Get Page Access Token - tries Secret Manager first, then st.secrets"""
    # 1. Try Google Secret Manager
    token = get_secret_from_gcp("page-access-token")
    if token:
        return token
    # 2. Fallback to Streamlit secrets
    return st.secrets.get("PAGE_ACCESS_TOKEN", os.getenv("PAGE_ACCESS_TOKEN", ""))

def get_instagram_access_token():
    """Get Instagram Access Token - tries Secret Manager first, then st.secrets"""
    # 1. Try Google Secret Manager
    token = get_secret_from_gcp("instagram-access-token")
    if token:
        return token
    # 2. Fallback to Streamlit secrets
    return st.secrets.get("INSTAGRAM_ACCESS_TOKEN", os.getenv("INSTAGRAM_ACCESS_TOKEN", ""))

def get_instagram_user_info(user_id: str) -> dict:
    """Fetch Instagram user info (username, name) via Instagram Graph API"""
    # Skip API call for demo/test users
    if user_id.startswith("demo_") or user_id.startswith("test_"):
        return {"username": user_id, "name": "", "error": None}
    
    # Use Instagram Token (IGAAT...) for user lookups
    token = get_instagram_access_token()
    if not token:
        return {"username": "", "name": "", "error": "No Instagram token"}
    
    try:
        # Instagram Graph API endpoint
        url = f"https://graph.instagram.com/v21.0/{user_id}"
        params = {
            "fields": "username,name",
            "access_token": token
        }
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        if response.status_code == 200 and "error" not in data:
            return {
                "username": data.get("username", ""),
                "name": data.get("name", ""),
                "error": None
            }
        
        # Return error info for debugging
        error_msg = data.get("error", {}).get("message", "Unknown error")
        return {"username": "", "name": "", "error": error_msg}
        
    except Exception as e:
        return {"username": "", "name": "", "error": str(e)}

@st.cache_data(ttl=3600)  # 1 hour cache
def get_cached_user_info(user_id: str) -> dict:
    """Cached version of user info lookup via Instagram API"""
    return get_instagram_user_info(user_id)

def save_sender_name_to_db(sender_id: str, sender_name: str):
    """Speichert den Sender-Namen in BigQuery für alle Nachrichten dieses Senders"""
    if not sender_name or not sender_id:
        return
    try:
        client = bigquery.Client()
        query = """
        UPDATE `root-slate-454410-u0.instagram_messages.messages`
        SET sender_name = @sender_name
        WHERE sender_id = @sender_id AND (sender_name IS NULL OR sender_name = '')
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sender_name", "STRING", sender_name),
                bigquery.ScalarQueryParameter("sender_id", "STRING", sender_id),
            ]
        )
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error saving sender name: {e}")

def get_instagram_account_id():
    """Get Instagram Business Account ID from secrets or env"""
    return st.secrets.get("INSTAGRAM_ACCOUNT_ID", os.getenv("INSTAGRAM_ACCOUNT_ID", ""))

def send_instagram_message(recipient_id: str, message_text: str) -> tuple[bool, str]:
    """Send a message via Instagram Graph API (using Instagram Token!)"""
    # Use Instagram Token (IGAAT...) for sending DMs
    token = get_instagram_access_token()
    if not token:
        return False, "Kein Instagram Access Token konfiguriert"
    
    try:
        # Instagram Graph API endpoint (not Facebook!)
        url = f"https://graph.instagram.com/v21.0/me/messages"
        
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": message_text}
        }
        
        params = {"access_token": token}
        
        response = requests.post(url, json=payload, params=params, timeout=10)
        
        if response.status_code == 200:
            return True, "Nachricht gesendet"
        else:
            error_data = response.json()
            error_msg = error_data.get("error", {}).get("message", "Unbekannter Fehler")
            error_code = error_data.get("error", {}).get("code", "")
            
            # Debug info
            print(f"Instagram API Error: {error_code} - {error_msg}")
            print(f"Recipient ID: {recipient_id}")
            
            return False, f"API Fehler: ({error_code}) {error_msg}"
    except Exception as e:
        return False, f"Fehler: {str(e)}"


def load_instagram_conversations(limit: int = 100) -> list:
    """Lädt alle Instagram Conversations von der API"""
    token = get_instagram_access_token()
    if not token:
        return []
    
    try:
        url = "https://graph.instagram.com/v21.0/me/conversations"
        params = {
            "platform": "instagram",
            "fields": "participants,id,updated_time",
            "limit": limit,
            "access_token": token
        }
        
        all_conversations = []
        while url and len(all_conversations) < limit:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                all_conversations.extend(data.get("data", []))
                # Pagination
                url = data.get("paging", {}).get("next")
                params = {}  # URL hat bereits alle Parameter
            else:
                break
        
        return all_conversations
    except Exception as e:
        print(f"Error loading conversations: {e}")
        return []


def load_conversation_messages(conversation_id: str, limit: int = 50) -> list:
    """Lädt Nachrichten einer Conversation von der Instagram API"""
    token = get_instagram_access_token()
    if not token:
        return []
    
    try:
        url = f"https://graph.instagram.com/v21.0/{conversation_id}"
        params = {
            # Lade auch story und attachments um Reaktionen zu erkennen
            "fields": "messages{id,message,created_time,from,story,attachments}",
            "access_token": token
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data.get("messages", {}).get("data", [])
        return []
    except Exception as e:
        print(f"Error loading messages for {conversation_id}: {e}")
        return []


def load_message_content(message_id: str) -> str:
    """Lädt den Inhalt einer Nachricht (Attachments, Story-Replies) von Instagram"""
    token = get_instagram_access_token()
    if not token:
        return "⚠️ Kein Token"
    
    try:
        url = f"https://graph.instagram.com/v21.0/{message_id}"
        params = {
            "fields": "id,message,attachments,story",
            "access_token": token
        }
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            # Prüfe auf Text
            message = data.get("message", "")
            
            # Prüfe auf Story-Reply
            story = data.get("story", {})
            if story:
                reply_to = story.get("reply_to", {})
                story_link = reply_to.get("link", "")
                
                if message and story_link:
                    # Story-Reply MIT Text - beides anzeigen
                    return f"📸 \"{message}\" [Story ansehen]({story_link})"
                elif message:
                    # Nur Text (Story abgelaufen)
                    return f"📸 \"{message}\" (Story abgelaufen)"
                elif story_link:
                    # Nur Story-Link (Quick-Reaction ohne Text)
                    return f"📸 [Story-Reaktion ansehen]({story_link})"
                else:
                    return "📸 Story-Antwort (Story abgelaufen)"
            
            # Normaler Text ohne Story
            if message:
                return message
            
            # Prüfe auf Attachments
            attachments = data.get("attachments", {}).get("data", [])
            if attachments:
                att = attachments[0]  # Erstes Attachment
                
                # Bild
                if "image_data" in att:
                    img_url = att["image_data"].get("url", "")
                    if img_url:
                        return f"🖼️ [Bild anzeigen]({img_url})"
                    return "🖼️ Bild gesendet"
                
                # Video
                if "video_data" in att:
                    video_url = att["video_data"].get("url", "")
                    if video_url:
                        return f"🎬 [Video anzeigen]({video_url})"
                    return "🎬 Video gesendet"
                
                # Audio
                if "audio_data" in att:
                    return "🎤 Sprachnachricht"
                
                # Sonstiges
                return "📎 Datei gesendet"
            
            return "📨 Medien-Nachricht (Details nicht verfügbar)"
        else:
            error_data = response.json().get("error", {})
            error_msg = error_data.get("message", "")
            
            # Bekannte Fehler mit freundlicher Meldung
            if "Unsupported get request" in error_msg:
                return "📨 Ältere Nachricht (nicht mehr abrufbar)"
            
            return "⚠️ Nicht abrufbar"
            
    except Exception as e:
        return "⚠️ Laden fehlgeschlagen"


def sync_conversation_history(sender_id: str, conversation_id: str = None) -> tuple[int, str]:
    """Synchronisiert die Nachrichtenhistorie eines Users von Instagram zu BigQuery
    Returns: (count, message)
    """
    client = get_bq_client()
    token = get_instagram_access_token()
    if not token:
        return 0, "Kein Instagram Token"
    
    # Finde Conversation ID falls nicht gegeben
    if not conversation_id:
        conversations = load_instagram_conversations(limit=200)
        for conv in conversations:
            participants = conv.get("participants", {}).get("data", [])
            for p in participants:
                if p.get("id") == sender_id:
                    conversation_id = conv.get("id")
                    break
            if conversation_id:
                break
    
    if not conversation_id:
        return 0, "Conversation nicht gefunden"
    
    # Lade Nachrichten
    messages = load_conversation_messages(conversation_id, limit=100)
    if not messages:
        return 0, "Keine Nachrichten gefunden"
    
    # Eigene IG ID
    own_ig_id = get_instagram_account_id() or get_own_instagram_id()
    
    new_count = 0
    skipped_reactions = 0
    for msg in messages:
        msg_id = msg.get("id", "")
        msg_text = msg.get("message", "")
        created_time = msg.get("created_time", "")
        from_id = msg.get("from", {}).get("id", "")
        from_username = msg.get("from", {}).get("username", "")
        has_story = bool(msg.get("story", {}))
        has_attachments = bool(msg.get("attachments", {}).get("data", []))
        
        if not msg_id:
            continue
        
        # Überspringe reine Reaktionen (kein Text, kein Attachment, nur Story-Reaktion)
        if not msg_text.strip() and has_story and not has_attachments:
            skipped_reactions += 1
            continue
        
        # Prüfe ob schon existiert
        check_query = f"""
        SELECT message_id FROM `root-slate-454410-u0.instagram_messages.messages`
        WHERE message_id = '{msg_id}'
        """
        try:
            existing = client.query(check_query).to_dataframe()
            if not existing.empty:
                continue  # Bereits vorhanden
        except:
            pass
        
        # Bestimme sender/recipient und direction basierend auf from_id
        if from_id == own_ig_id:
            # Unsere eigene Nachricht (ausgehend)
            actual_sender_id = own_ig_id
            actual_recipient_id = sender_id
            direction = "outgoing"
        else:
            # Kundennachricht (eingehend)
            actual_sender_id = sender_id
            actual_recipient_id = own_ig_id
            direction = "incoming"
        
        # In BigQuery speichern
        insert_query = f"""
        INSERT INTO `root-slate-454410-u0.instagram_messages.messages`
        (message_id, sender_id, sender_name, recipient_id, timestamp, received_at, 
         message_text, has_attachments, attachment_types, is_story_reply, 
         categories, primary_category, priority, status, tags, direction)
        VALUES (
            '{msg_id}',
            '{actual_sender_id}',
            '{from_username.replace("'", "''")}',
            '{actual_recipient_id}',
            UNIX_SECONDS(TIMESTAMP('{created_time}')),
            TIMESTAMP('{created_time}'),
            '{msg_text.replace("'", "''")}',
            FALSE,
            '[]',
            FALSE,
            '[]',
            'historie',
            'normal',
            'synced',
            '',
            '{direction}'
        )
        """
        try:
            client.query(insert_query).result()
            new_count += 1
        except Exception as e:
            print(f"Error inserting message {msg_id}: {e}")
    
    return new_count, f"{new_count} Nachrichten synchronisiert"


# === INSTAGRAM POSTS & COMMENTS API ===
def get_ad_account_id():
    """Get Ad Account ID from secrets or env"""
    return st.secrets.get("AD_ACCOUNT_ID", os.getenv("AD_ACCOUNT_ID", "1266832358443930"))

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_ad_media_ids() -> tuple:
    """Lädt Instagram Media IDs direkt von den Ad Creatives (für Dark Posts)
    Returns: (dict of media_id -> ad_info, debug_info)
    """
    token = get_page_access_token()
    if not token:
        return ({}, "Kein Token")
    
    ad_media = {}  # media_id -> {"ad_name": ..., "shortcode": ...}
    ad_account_id = get_ad_account_id()
    debug_info = []
    
    try:
        # Lade Ads (mit Pagination)
        # Wir laden alle Ads, aber filtern Kommentare nach Datum (ab 2026)
        ads_url = f"https://graph.facebook.com/v21.0/act_{ad_account_id}/ads"
        params = {
            "fields": "id,name,status,creative",
            "limit": 500,
            "access_token": token
        }
        
        all_ads = []
        page_count = 0
        while ads_url and page_count < 4:  # Max 4 Seiten = 2000 Ads
            response = requests.get(ads_url, params=params, timeout=30)
            if response.status_code != 200:
                error = response.json().get("error", {}).get("message", "Unknown error")
                return ({}, f"Ads API Error: {error}")
            
            result = response.json()
            all_ads.extend(result.get("data", []))
            
            # Pagination - nächste Seite
            ads_url = result.get("paging", {}).get("next")
            params = {}  # URL enthält bereits alle Parameter
            page_count += 1
        
        ads_data = all_ads
        debug_info.append(f"Ads: {len(ads_data)}")
        
        # Sammle Creative IDs mit Ad-Namen
        creative_to_ad = {}  # creative_id -> ad_name
        for ad in ads_data:
            creative_id = ad.get("creative", {}).get("id")
            if creative_id:
                creative_to_ad[creative_id] = ad.get("name", "Unbekannte Ad")
        
        debug_info.append(f"Creatives: {len(creative_to_ad)}")
        
        # Batch-Request für Creatives - hole effective_instagram_media_id
        creative_ids = list(creative_to_ad.keys())
        for i in range(0, len(creative_ids), 50):
            batch_ids = creative_ids[i:i+50]
            ids_param = ",".join(batch_ids)
            
            creative_url = f"https://graph.facebook.com/v21.0/"
            creative_params = {
                "ids": ids_param,
                "fields": "id,effective_instagram_media_id,instagram_permalink_url",
                "access_token": token
            }
            
            creative_response = requests.get(creative_url, params=creative_params, timeout=30)
            if creative_response.status_code == 200:
                creatives_data = creative_response.json()
                for creative_id, creative_info in creatives_data.items():
                    media_id = creative_info.get("effective_instagram_media_id")
                    permalink = creative_info.get("instagram_permalink_url", "")
                    
                    if media_id:
                        # Extrahiere Shortcode aus URL
                        shortcode = ""
                        if "/p/" in permalink:
                            shortcode = permalink.split("/p/")[1].split("/")[0].split("?")[0]
                        elif "/reel/" in permalink:
                            shortcode = permalink.split("/reel/")[1].split("/")[0].split("?")[0]
                        
                        ad_media[media_id] = {
                            "ad_name": creative_to_ad.get(creative_id, ""),
                            "shortcode": shortcode,
                            "permalink": permalink
                        }
        
        debug_info.append(f"Media IDs: {len(ad_media)}")
        
    except Exception as e:
        debug_info.append(f"Error: {str(e)}")
    
    return (ad_media, " | ".join(debug_info))

def load_instagram_posts(limit: int = 20) -> list:
    """Lädt Instagram Posts mit Kommentar-Anzahl"""
    token = get_page_access_token()
    ig_account_id = get_instagram_account_id()
    
    if not token or not ig_account_id:
        return []
    
    try:
        url = f"https://graph.facebook.com/v21.0/{ig_account_id}/media"
        params = {
            "fields": "id,caption,comments_count,shortcode,timestamp,permalink,media_type",
            "limit": limit,
            "access_token": token
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json().get("data", [])
    except Exception as e:
        print(f"Error loading posts: {e}")
    
    return []

def load_post_comments(media_id: str, limit: int = 50, since_date: str = "2026-01-01") -> list:
    """Lädt Kommentare eines Posts inkl. Replies (nur ab since_date)"""
    token = get_page_access_token()
    if not token:
        return []
    
    try:
        # Unix timestamp für since_date berechnen
        from datetime import datetime
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        since_timestamp = int(since_dt.timestamp())
        
        url = f"https://graph.facebook.com/v21.0/{media_id}/comments"
        params = {
            "fields": "id,text,timestamp,username,from,replies{id,text,timestamp,username,from}",
            "limit": limit,
            "since": since_timestamp,  # Nur Kommentare ab diesem Datum
            "access_token": token
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json().get("data", [])
    except Exception as e:
        print(f"Error loading comments for {media_id}: {e}")
    
    return []

def reply_to_comment(comment_id: str, message: str) -> tuple[bool, str]:
    """Antwortet auf einen Instagram Kommentar"""
    token = get_page_access_token()
    if not token:
        return False, "Kein Page Access Token konfiguriert"
    
    try:
        url = f"https://graph.facebook.com/v21.0/{comment_id}/replies"
        params = {
            "message": message,
            "access_token": token
        }
        
        response = requests.post(url, params=params, timeout=10)
        if response.status_code == 200:
            return True, "Antwort gesendet"
        else:
            error_data = response.json()
            error_msg = error_data.get("error", {}).get("message", "Unbekannter Fehler")
            return False, f"API Fehler: {error_msg}"
    except Exception as e:
        return False, f"Fehler: {str(e)}"

def like_comment(comment_id: str) -> tuple[bool, str]:
    """Liked einen Instagram Kommentar via Graph API"""
    token = get_page_access_token()
    
    if not token:
        return False, "Kein Page Access Token konfiguriert"
    
    try:
        # Instagram Graph API: POST /{comment-id}/likes
        url = f"https://graph.facebook.com/v21.0/{comment_id}/likes"
        params = {
            "access_token": token
        }
        
        response = requests.post(url, params=params, timeout=10)
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                return True, "Kommentar geliked"
            else:
                return False, "Like nicht erfolgreich"
        else:
            error_data = response.json()
            error_msg = error_data.get("error", {}).get("message", "Unbekannter Fehler")
            return False, f"API Fehler: {error_msg}"
    except Exception as e:
        return False, f"Fehler: {str(e)}"

def analyze_sentiment(text: str) -> str:
    """Einfache Sentiment-Analyse"""
    text_lower = text.lower()
    
    # Negative Keywords
    negative_words = ["schlecht", "enttäuscht", "ärger", "problem", "kaputt", "defekt", 
                      "reklamation", "beschwerde", "mangelhaft", "nie wieder", "unverschämt",
                      "betrug", "abzocke", "schrott", "müll", "furchtbar", "horrible"]
    
    # Question indicators
    question_words = ["?", "wann", "wie", "wo", "warum", "weshalb", "kann man", "gibt es",
                      "habt ihr", "könnt ihr", "verfügbar", "lieferzeit", "größe"]
    
    # Check for negative
    for word in negative_words:
        if word in text_lower:
            return "negative"
    
    # Check for question
    for word in question_words:
        if word in text_lower:
            return "question"
    
    return "positive"

def ensure_comments_table_schema():
    """Stellt sicher dass die BigQuery Tabelle alle nötigen Spalten hat"""
    client = get_bq_client()
    
    # Prüfe/Erstelle Spalten
    alter_queries = [
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS post_shortcode STRING",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS post_type STRING",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS responded_by STRING",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS has_our_reply BOOL",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS our_reply_text STRING",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS is_done BOOL",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS replies_json STRING",
        "ALTER TABLE `root-slate-454410-u0.instagram_messages.ad_comments` ADD COLUMN IF NOT EXISTS is_liked BOOL",
    ]
    
    for query in alter_queries:
        try:
            client.query(query).result()
        except Exception as e:
            # Spalte existiert bereits oder anderer Fehler - ignorieren
            pass

def sync_instagram_comments():
    """Synchronisiert Ad-Kommentare direkt von den Ad Media IDs
    Returns: (new_count, synced_count, debug_info)
    """
    client = get_bq_client()
    debug_messages = []
    
    # Schema sicherstellen
    ensure_comments_table_schema()
    
    # 1. Lade Ad Media IDs direkt (funktioniert auch für Dark Posts)
    ad_media, ads_debug = load_ad_media_ids()
    debug_messages.append(ads_debug)
    
    if not ad_media:
        debug_messages.append("Keine Ad Media IDs gefunden - Sync abgebrochen")
        return 0, 0, " | ".join(debug_messages)
    
    synced_count = 0
    new_count = 0
    ads_with_comments = 0
    
    # 2. Für jede Ad Media ID: Kommentare laden
    for media_id, ad_info in ad_media.items():
        # Lade Kommentare für diese Ad
        comments = load_post_comments(media_id, limit=100)
        
        if not comments:
            continue
        
        ads_with_comments += 1
        shortcode = ad_info.get("shortcode", "")
        ad_name = ad_info.get("ad_name", "")[:200]
        post_type = "ad"
        
        # Eigene Instagram Account ID für Reply-Erkennung
        own_ig_id = get_instagram_account_id()
        
        for comment in comments:
            comment_id = comment.get("id", "")
            comment_text = comment.get("text", "")
            username = comment.get("username", "") or comment.get("from", {}).get("username", "")
            commenter_id = comment.get("from", {}).get("id", "")
            timestamp = comment.get("timestamp", "")
            
            if not comment_id or not comment_text:
                continue
            
            # Prüfe ob wir bereits geantwortet haben (in den Replies)
            has_our_reply = False
            our_reply_text = ""
            replies = comment.get("replies", {}).get("data", [])
            
            # Alle Replies als JSON speichern
            replies_list = []
            for reply in replies:
                reply_from_id = reply.get("from", {}).get("id", "")
                reply_username = reply.get("username", "") or reply.get("from", {}).get("username", "")
                reply_text = reply.get("text", "")
                reply_timestamp = reply.get("timestamp", "")
                
                replies_list.append({
                    "id": reply.get("id", ""),
                    "username": reply_username,
                    "text": reply_text,
                    "timestamp": reply_timestamp,
                    "is_own": reply_from_id == own_ig_id
                })
                
                # Prüfe ob Reply von uns ist (eigene IG Account ID)
                if reply_from_id == own_ig_id and not has_our_reply:
                    has_our_reply = True
                    our_reply_text = reply_text[:500]
            
            replies_json = json.dumps(replies_list) if replies_list else ""
            
            # Prüfe ob Kommentar bereits existiert
            check_query = f"""
            SELECT comment_id FROM `root-slate-454410-u0.instagram_messages.ad_comments`
            WHERE comment_id = '{comment_id}'
            """
            try:
                existing = client.query(check_query).to_dataframe()
                if not existing.empty:
                    # Update Replies (immer aktualisieren für neue Antworten)
                    update_query = f"""
                    UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                    SET has_our_reply = {has_our_reply}, 
                        our_reply_text = '{our_reply_text.replace("'", "''")}',
                        replies_json = '{replies_json.replace("'", "''")}'
                    WHERE comment_id = '{comment_id}'
                    """
                    try:
                        client.query(update_query).result()
                    except:
                        pass
                    synced_count += 1
                    continue
            except:
                pass
            
            # Sentiment analysieren
            sentiment = analyze_sentiment(comment_text)
            priority = "high" if sentiment == "negative" else ("medium" if sentiment == "question" else "normal")
            
            # In BigQuery speichern
            insert_query = f"""
            INSERT INTO `root-slate-454410-u0.instagram_messages.ad_comments`
            (comment_id, post_id, post_shortcode, post_type, ad_name, commenter_id, commenter_name, 
             comment_text, created_at, sentiment, status, is_hidden, is_deleted, priority,
             has_our_reply, our_reply_text, is_done, replies_json)
            VALUES (
                '{comment_id}',
                '{media_id}',
                '{shortcode}',
                '{post_type}',
                '{ad_name.replace("'", "''")}',
                '{commenter_id}',
                '{username.replace("'", "''")}',
                '{comment_text.replace("'", "''")}',
                TIMESTAMP('{timestamp}'),
                '{sentiment}',
                'new',
                FALSE,
                FALSE,
                '{priority}',
                {has_our_reply},
                '{our_reply_text.replace("'", "''")}',
                FALSE,
                '{replies_json.replace("'", "''")}'
            )
            """
            try:
                client.query(insert_query).result()
                new_count += 1
            except Exception as e:
                print(f"Error inserting comment {comment_id}: {e}")
        
        synced_count += len(comments)
    
    debug_messages.append(f"Ads mit Kommentaren: {ads_with_comments}")
    debug_messages.append(f"Neue Kommentare: {new_count}")
    
    return new_count, synced_count, " | ".join(debug_messages)

# Page Config
st.set_page_config(
    page_title="LILIMAUS Inbox",
    page_icon="📬",
    layout="wide"
)

# Team-Mitglieder (Kürzel -> Name) - Muss vor Login definiert sein
TEAM_MEMBERS = {
    "AS": "Anni",
    "MS": "Marc",
    "SM": "Sina",
    "JD": "Jessy",
    "SG": "Sinem"
}

# === LOGIN ===
def get_user_passwords():
    """Holt User-Passwörter aus Secrets"""
    passwords = {}
    for kuerzel in TEAM_MEMBERS.keys():
        # Format in Secrets: USER_AS = "passwort"
        pwd = st.secrets.get(f"USER_{kuerzel}", "")
        if pwd:
            passwords[kuerzel] = pwd
    return passwords

def check_password():
    """Multi-user password protection"""
    
    # Check if already authenticated
    if st.session_state.get("authenticated", False) and st.session_state.get("user_kuerzel"):
        return True
    
    st.markdown("""
    <div style="text-align: center; padding: 50px 0;">
        <img src="https://lilimaus.de/cdn/shop/files/Lilimaus_Logo_241212.png?v=1743081255" height="60">
        <h2 style="margin-top: 20px;">Instagram Inbox</h2>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        password = st.text_input("Passwort", type="password", key="pwd_input")
        
        if st.button("Login", type="primary", use_container_width=True):
            user_passwords = get_user_passwords()
            
            # Prüfe ob Passwort zu einem User gehört
            authenticated_user = None
            for kuerzel, pwd in user_passwords.items():
                if password == pwd:
                    authenticated_user = kuerzel
                    break
            
            # Fallback: Altes gemeinsames Passwort (für Übergang)
            if not authenticated_user and password == st.secrets.get("APP_PASSWORD", ""):
                authenticated_user = "MS"  # Default User
            
            if authenticated_user:
                st.session_state["authenticated"] = True
                st.session_state["user_kuerzel"] = authenticated_user
                st.session_state["user_name"] = TEAM_MEMBERS.get(authenticated_user, authenticated_user)
                st.rerun()
            else:
                st.error("Falsches Passwort")
    
    return False

# Check login before showing app
if not check_password():
    st.stop()

# LILIMAUS Branding
COLORS = {
    "lililight": "#FFFFFF",
    "lilidark": "#000000",
    "lilisoft": "#F4E5D8",
    "lilisoft_dark": "#EBD4C1",
    "text": "#000000",
    "text_light": "#666666",
}

# Custom CSS
st.markdown(f"""
<style>
    .stApp {{
        background-color: {COLORS['lililight']};
    }}
    h1, h2, h3 {{
        color: {COLORS['lilidark']} !important;
    }}
    [data-testid="stSidebar"] {{
        background-color: {COLORS['lilisoft']} !important;
    }}
    .chat-container {{
        max-height: 500px;
        overflow-y: auto;
        padding: 20px;
        background: #FAFAFA;
        border-radius: 12px;
        margin-bottom: 20px;
        border: 1px solid #EEEEEE;
    }}
    .message-incoming {{
        background: {COLORS['lililight']};
        padding: 14px 18px;
        border-radius: 16px 16px 16px 4px;
        margin: 10px 0;
        max-width: 80%;
        border: 1px solid #E5E5E5;
    }}
    .message-outgoing {{
        background: {COLORS['lilisoft']};
        padding: 14px 18px;
        border-radius: 16px 16px 4px 16px;
        margin: 10px 0;
        max-width: 80%;
        margin-left: auto;
        border: 1px solid {COLORS['lilisoft_dark']};
    }}
    .message-time {{
        font-size: 11px;
        color: {COLORS['text_light']};
        margin-top: 6px;
    }}
    .tag {{
        display: inline-block;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 12px;
        margin: 2px;
        background: {COLORS['lilisoft']};
        border: 1px solid {COLORS['lilisoft_dark']};
    }}
    .stButton > button {{
        border-radius: 8px !important;
    }}
</style>
""", unsafe_allow_html=True)

# BigQuery Client
@st.cache_resource
def get_bq_client():
    from google.oauth2 import service_account
    
    # Try to use Streamlit secrets (for cloud deployment)
    try:
        if "GCP_SERVICE_ACCOUNT_JSON" in st.secrets:
            # JSON string format
            creds_dict = json.loads(st.secrets["GCP_SERVICE_ACCOUNT_JSON"])
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            return bigquery.Client(credentials=credentials, project="root-slate-454410-u0")
        elif "gcp_service_account" in st.secrets:
            # TOML section format
            creds_dict = {
                "type": st.secrets["gcp_service_account"]["type"],
                "project_id": st.secrets["gcp_service_account"]["project_id"],
                "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
                "private_key": st.secrets["gcp_service_account"]["private_key"],
                "client_email": st.secrets["gcp_service_account"]["client_email"],
                "client_id": st.secrets["gcp_service_account"]["client_id"],
                "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
                "token_uri": st.secrets["gcp_service_account"]["token_uri"],
                "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
                "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
            }
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            return bigquery.Client(credentials=credentials, project="root-slate-454410-u0")
    except Exception as e:
        st.error(f"BigQuery Auth Error: {e}")
    
    # Fallback to default credentials (local development)
    return bigquery.Client(project="root-slate-454410-u0")

# Standard Tags
DEFAULT_TAGS = ["Kundenservice", "Kooperationen", "Feedback"]

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_all_tags():
    """Holt alle verwendeten Tags (Standard + Custom) - cached"""
    client = get_bq_client()
    query = """
    SELECT DISTINCT tags
    FROM `root-slate-454410-u0.instagram_messages.messages`
    WHERE tags IS NOT NULL AND tags != ''
    """
    try:
        df = client.query(query).to_dataframe()
        all_tags = set(DEFAULT_TAGS)
        for tags_str in df['tags']:
            if tags_str:
                for tag in tags_str.split(','):
                    tag = tag.strip()
                    if tag:
                        all_tags.add(tag)
        return sorted(list(all_tags))
    except:
        return DEFAULT_TAGS


# === BLACKLIST FUNCTIONS (persistent in BigQuery) ===
@st.cache_data(ttl=60)
def load_blacklist() -> set:
    """Lädt die Blacklist aus BigQuery"""
    client = get_bq_client()
    try:
        df = client.query("""
            SELECT user_id FROM `root-slate-454410-u0.instagram_messages.blacklist`
        """).to_dataframe()
        return set(df['user_id'].tolist())
    except:
        return set()


def add_to_blacklist(user_id: str, username: str = "", blocked_by: str = ""):
    """Fügt einen User zur Blacklist hinzu"""
    client = get_bq_client()
    escaped_id = user_id.replace("'", "''")
    escaped_name = username.replace("'", "''") if username else ""
    escaped_by = blocked_by.replace("'", "''") if blocked_by else ""
    
    try:
        client.query(f"""
            INSERT INTO `root-slate-454410-u0.instagram_messages.blacklist`
            (user_id, username, blocked_by)
            VALUES ('{escaped_id}', '{escaped_name}', '{escaped_by}')
        """).result()
        load_blacklist.clear()  # Cache leeren
        return True
    except:
        return False


def remove_from_blacklist(user_id: str):
    """Entfernt einen User von der Blacklist"""
    client = get_bq_client()
    escaped_id = user_id.replace("'", "''")
    
    try:
        client.query(f"""
            DELETE FROM `root-slate-454410-u0.instagram_messages.blacklist`
            WHERE user_id = '{escaped_id}'
        """).result()
        load_blacklist.clear()  # Cache leeren
        return True
    except:
        return False


# AI System Prompt
AI_SYSTEM_PROMPT = """
Du antwortest auf Instagram DMs für LILIMAUS (Baby- und Kinderzimmer-Ausstattung).

WICHTIG - Das ist ein CHAT, keine E-Mail!
- Schreibe wie in einem echten Chat mit einer Freundin
- KEINE förmliche Anrede
- KEINE Unterschrift
- Kurz, knackig, freundlich
- Emojis sparsam (😊 🤍 ✨)
- Duzen ist selbstverständlich

Beispiele:
- "Hey! Klar, bei 80cm würde ich Größe 86 nehmen 😊"
- "Oh nein, das tut mir leid! Schick mir mal ein Foto, dann klären wir das sofort 🤍"

Schreibe eine kurze Chat-Antwort (max 2-3 Sätze).
"""

def generate_ai_reply(message_text: str, sender_name: str, history: str = ""):
    """Generiert einen Antwortvorschlag mittels Google Gemini"""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "Hey! Danke für deine Nachricht 😊"
    
    try:
        client = genai.Client(api_key=api_key)
        
        full_prompt = f"""{AI_SYSTEM_PROMPT}

Kunde: {sender_name}
Letzte Nachrichten:
{history}

Aktuelle Nachricht:
"{message_text}"
"""
        
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=full_prompt
        )
        return response.text.strip()
    except Exception as e:
        return f"Hey! Danke für deine Nachricht 😊"


def get_own_instagram_id():
    """Get our own Instagram Account ID to filter out"""
    return st.secrets.get("INSTAGRAM_ACCOUNT_ID", os.getenv("INSTAGRAM_ACCOUNT_ID", "17841462069085392"))

@st.cache_data(ttl=15)  # Cache for 15 seconds
def load_conversations(filter_type: str = "all", filter_tags_str: str = ""):
    """Lädt Konversationen mit Filtern (cached)"""
    client = get_bq_client()
    
    own_id = get_own_instagram_id()
    
    # Base query - get conversations with latest activity
    # A conversation is identified by the customer's ID (not our own)
    # Check if the latest INCOMING message is unanswered
    query = f"""
    WITH all_conversations AS (
        -- Get all unique customer IDs (either as sender or recipient)
        SELECT DISTINCT
            CASE 
                WHEN sender_id = '{own_id}' THEN recipient_id
                ELSE sender_id
            END as customer_id
        FROM `root-slate-454410-u0.instagram_messages.messages`
        WHERE sender_id != '{own_id}' OR recipient_id != '{own_id}'
    ),
    conversation_messages AS (
        -- Get all messages for each conversation
        SELECT 
            CASE 
                WHEN sender_id = '{own_id}' THEN recipient_id
                ELSE sender_id
            END as customer_id,
            sender_name,
            message_text,
            tags,
            response_text,
            received_at,
            direction
        FROM `root-slate-454410-u0.instagram_messages.messages`
        WHERE (sender_id != '{own_id}' OR recipient_id != '{own_id}')
          AND sender_id NOT LIKE 'demo_%'
          AND sender_id NOT LIKE 'test_%'
    ),
    latest_activity AS (
        -- Get the latest message (any direction) per customer
        SELECT 
            customer_id,
            MAX(received_at) as last_activity_at
        FROM conversation_messages
        GROUP BY customer_id
    ),
    latest_incoming AS (
        -- Get the latest INCOMING message per customer (to check if answered)
        SELECT 
            customer_id,
            sender_name,
            message_text,
            tags,
            response_text,
            received_at,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY received_at DESC) as rn
        FROM conversation_messages
        WHERE direction = 'incoming' OR direction IS NULL
    ),
    conversation_stats AS (
        SELECT 
            customer_id,
            COUNT(*) as message_count
        FROM conversation_messages
        GROUP BY customer_id
    )
    SELECT 
        ac.customer_id as sender_id,
        COALESCE(li.sender_name, '') as sender_name,
        COALESCE(cs.message_count, 0) as message_count,
        la.last_activity_at as last_message_at,
        CASE WHEN li.response_text IS NULL OR li.response_text = '' THEN 1 ELSE 0 END as has_unanswered,
        COALESCE(li.tags, '') as tags,
        COALESCE(li.message_text, '') as last_message
    FROM all_conversations ac
    JOIN latest_activity la ON ac.customer_id = la.customer_id
    LEFT JOIN latest_incoming li ON ac.customer_id = li.customer_id AND li.rn = 1
    LEFT JOIN conversation_stats cs ON ac.customer_id = cs.customer_id
    WHERE ac.customer_id IS NOT NULL
      AND ac.customer_id != ''
      AND ac.customer_id NOT LIKE 'demo_%'
      AND ac.customer_id NOT LIKE 'test_%'
    """
    
    # Apply filters
    if filter_type == "unbeantwortet":
        query += " AND (lm.response_text IS NULL OR lm.response_text = '')"
    
    if filter_tags_str:
        filter_tags = filter_tags_str.split(",")
        tag_conditions = []
        for tag in filter_tags:
            tag_conditions.append(f"lm.tags LIKE '%{tag}%'")
        query += f" AND ({' OR '.join(tag_conditions)})"
    
    query += """
    ORDER BY 
        has_unanswered DESC,
        last_message_at DESC
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Fehler: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=15)  # Cache for 15 seconds
def load_chat_history(sender_id: str):
    """Lädt den Chat-Verlauf für einen Sender (eingehend + ausgehend)"""
    client = get_bq_client()
    # Lade sowohl eingehende (sender_id = kunde) als auch ausgehende (recipient_id = kunde) Nachrichten
    query = f"""
    SELECT *
    FROM `root-slate-454410-u0.instagram_messages.messages`
    WHERE sender_id = '{sender_id}' 
       OR recipient_id = '{sender_id}'
    ORDER BY received_at ASC
    """
    try:
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame()


def update_message(message_id: str, updates: dict):
    """Aktualisiert eine Nachricht und leert den Cache"""
    client = get_bq_client()
    
    set_clauses = []
    for key, value in updates.items():
        if value is None:
            set_clauses.append(f"{key} = NULL")
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            set_clauses.append(f"{key} = '{escaped}'")
        else:
            set_clauses.append(f"{key} = {value}")
    
    query = f"""
    UPDATE `root-slate-454410-u0.instagram_messages.messages`
    SET {", ".join(set_clauses)}
    WHERE message_id = '{message_id.replace("'", "''")}'
    """
    
    try:
        client.query(query).result()
        # Clear cache after update
        load_conversations.clear()
        load_chat_history.clear()
        return True
    except Exception as e:
        st.error(f"Fehler: {e}")
        return False


def bulk_mark_chats_as_read(sender_ids: list):
    """Markiert mehrere Chats als gelesen (neueste Nachricht jedes Chats)"""
    if not sender_ids:
        return
    
    client = get_bq_client()
    user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
    now = datetime.utcnow().isoformat()
    
    # Für jeden Sender die neueste Nachricht als beantwortet markieren
    sender_list = ", ".join([f"'{s.replace(chr(39), chr(39)+chr(39))}'" for s in sender_ids])
    
    query = f"""
    UPDATE `root-slate-454410-u0.instagram_messages.messages` m
    SET 
        response_text = '[Als erledigt markiert]',
        responded_at = '{now}',
        responded_by = '{user_kuerzel}'
    WHERE message_id IN (
        SELECT message_id FROM (
            SELECT message_id, 
                   ROW_NUMBER() OVER (PARTITION BY sender_id ORDER BY timestamp DESC) as rn
            FROM `root-slate-454410-u0.instagram_messages.messages`
            WHERE sender_id IN ({sender_list})
              AND direction = 'incoming'
        )
        WHERE rn = 1
    )
    AND (response_text IS NULL OR response_text = '')
    """
    
    try:
        client.query(query).result()
        load_conversations.clear()
        load_chat_history.clear()
    except Exception as e:
        st.error(f"Fehler beim Bulk-Update: {e}")


def render_chat_view(sender_id: str, auto_refresh_chat: bool = False):
    """Rendert die Chat-Ansicht"""
    messages = load_chat_history(sender_id)
    
    if messages.empty:
        st.info("Keine Nachrichten")
        return
    
    # Username: DB zuerst, dann API falls nötig (und dann in DB speichern!)
    db_name = messages.iloc[0].get('sender_name', '') or ''
    if db_name:
        sender_name = db_name
    else:
        user_info = get_cached_user_info(sender_id)
        api_username = user_info.get('username', '') or ''
        sender_name = api_username or f"Kunde #{sender_id[-6:]}"
        # Wenn wir einen Namen von der API haben, in DB speichern (nur einmal nötig!)
        if api_username:
            save_sender_name_to_db(sender_id, api_username)
    last_msg = messages.iloc[-1]
    
    # Header with refresh and blacklist buttons
    col_header, col_refresh, col_blacklist = st.columns([4, 1, 1])
    with col_header:
        st.subheader(f"💬 {sender_name}")
    with col_refresh:
        if st.button("🔄", key=f"refresh_chat_{sender_id}", help="Chat aktualisieren"):
            load_chat_history.clear()
            st.rerun()
    with col_blacklist:
        if st.button("🚫", key=f"blacklist_{sender_id}", help="User blockieren (aus Liste ausblenden)"):
            user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
            add_to_blacklist(sender_id, sender_name, user_kuerzel)
            st.session_state.selected_chat = None
            st.success(f"User ausgeblendet")
            st.rerun()
    
    # Tags anzeigen & bearbeiten (kompakt, ohne Expander)
    current_tags_str = last_msg.get('tags', '') or ''
    current_tags = [t.strip() for t in current_tags_str.split(',') if t.strip()]
    all_tags = get_all_tags()
    
    # Tags inline bearbeiten
    col_tags, col_save = st.columns([4, 1])
    with col_tags:
        selected_tags = st.multiselect(
            "🏷️ Tags",
            options=all_tags,
            default=[t for t in current_tags if t in all_tags],
            key=f"tags_{sender_id}",
            label_visibility="collapsed"
        )
    with col_save:
        if st.button("💾", key=f"save_tags_{sender_id}", help="Tags speichern"):
            tags_str = ",".join(selected_tags)
            for _, msg in messages.iterrows():
                update_message(msg['message_id'], {"tags": tags_str})
            st.rerun()
    
    # === ANTWORT-BOX (oben) ===
    last_msg_text = last_msg.get('message_text', '')
    reply_key = f"reply_{sender_id}"
    
    # KI-Button
    if st.button("✨ KI-Vorschlag", key=f"ai_{sender_id}"):
        with st.spinner("Schreibt..."):
            history = ""
            for _, m in messages.tail(3).iterrows():
                history += f"Kunde: {m.get('message_text', '')}\n"
                if m.get('response_text'):
                    history += f"Wir: {m.get('response_text')}\n"
            suggestion = generate_ai_reply(last_msg_text, sender_name, history)
            st.session_state[reply_key] = suggestion
            st.rerun()
    
    # Text Area
    if reply_key not in st.session_state:
        st.session_state[reply_key] = ""
    
    reply_text = st.text_area(
        "Antwort schreiben...",
        height=80,
        key=reply_key,
        label_visibility="collapsed"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📤 Senden", type="primary", key=f"send_{sender_id}"):
            if reply_text:
                success, msg = send_instagram_message(sender_id, reply_text)
                if success:
                    user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
                    update_message(last_msg['message_id'], {
                        "response_text": reply_text,
                        "responded_at": datetime.utcnow().isoformat(),
                        "responded_by": user_kuerzel
                    })
                    st.success(f"✅ Gesendet ({user_kuerzel})")
                    if reply_key in st.session_state:
                        del st.session_state[reply_key]
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")
    with col2:
        if st.button("✅ Als beantwortet markieren", key=f"done_{sender_id}"):
            user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
            update_message(last_msg['message_id'], {
                "response_text": "[Als erledigt markiert]",
                "responded_at": datetime.utcnow().isoformat(),
                "responded_by": user_kuerzel
            })
            st.success(f"✅ Markiert ({user_kuerzel})")
            st.rerun()
    
    st.divider()
    
    # === CHAT-VERLAUF (neueste zuerst) ===
    # Pagination State
    page_key = f"chat_page_{sender_id}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1
    
    messages_per_page = 10
    total_messages = len(messages)
    current_page = st.session_state[page_key]
    
    # Nachrichten umkehren (neueste zuerst)
    messages_reversed = messages.iloc[::-1]
    
    # Nur die anzuzeigenden Nachrichten
    start_idx = 0
    end_idx = current_page * messages_per_page
    messages_to_show = messages_reversed.iloc[start_idx:end_idx]
    
    # Nachrichten anzeigen (neueste oben)
    for _, msg in messages_to_show.iterrows():
        received_at = msg.get('received_at', '')
        try:
            # Konvertiere zu deutscher Zeit (Europe/Berlin)
            utc_time = pd.to_datetime(received_at)
            if utc_time.tzinfo is None:
                utc_time = utc_time.tz_localize('UTC')
            german_time = utc_time.tz_convert('Europe/Berlin')
            time_str = german_time.strftime('%d.%m. %H:%M')
        except:
            time_str = ""
        
        message_text = msg.get('message_text', '') or ''
        direction = msg.get('direction', 'incoming') or 'incoming'
        
        # Prüfe ob es eine Antwort über das Tool gibt (response_text)
        response = msg.get('response_text', '')
        if response:
            responded_by = msg.get('responded_by', '')
            user_badge = f"<span style='background:#eee;padding:2px 6px;border-radius:3px;font-size:11px;margin-right:5px;'>{responded_by}</span>" if responded_by else ""
            st.markdown(f"""
            <div class="message-outgoing">
                <div>{response}</div>
                <div class="message-time">{user_badge}✓ Gesendet</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Ausgehende Nachricht (von uns gesendet - direkt in Instagram oder via Sync)
        if direction == 'outgoing':
            if message_text.strip():
                st.markdown(f"""
                <div class="message-outgoing">
                    <div>{message_text}</div>
                    <div class="message-time">✓ {time_str}</div>
                </div>
                """, unsafe_allow_html=True)
            # Leere ausgehende Nachrichten werden übersprungen
            # (Reine Reaktionen oder alte nicht-abrufbare Medien)
        
        # Eingehende Nachricht (vom Kunden)
        elif direction == 'incoming':
            if message_text.strip():
                st.markdown(f"""
                <div class="message-incoming">
                    <div>{message_text}</div>
                    <div class="message-time">{time_str}</div>
                </div>
                """, unsafe_allow_html=True)
            # Leere Nachrichten ohne Text werden übersprungen
            # (Reine Reaktionen oder alte nicht-abrufbare Medien)
    
    # "Mehr laden" Button unten (falls es ältere Nachrichten in der DB gibt)
    if end_idx < total_messages:
        remaining = total_messages - end_idx
        if st.button(f"📜 Mehr anzeigen ({remaining} weitere in DB)", key=f"load_more_{sender_id}"):
            st.session_state[page_key] += 1
            st.rerun()
    
    # Historie von Instagram laden (unter dem Chat)
    st.divider()
    if st.button("📥 Ältere Nachrichten von Instagram laden", key=f"load_ig_history_{sender_id}"):
        with st.spinner("Lade von Instagram..."):
            count, msg = sync_conversation_history(sender_id)
            if count > 0:
                st.success(f"✅ {msg}")
                load_chat_history.clear()
                st.rerun()
            else:
                st.info(msg)
    


def main():
    # Header
    # Kompakter Header mit eingeloggtem User
    col_logo, col_spacer, col_user = st.columns([2, 4, 2])
    with col_logo:
        st.markdown("""
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://lilimaus.de/cdn/shop/files/Lilimaus_Logo_241212.png?v=1743081255" height="30">
        </div>
        """, unsafe_allow_html=True)
    with col_user:
        # Zeige eingeloggten User
        user_name = st.session_state.get("user_name", "User")
        user_kuerzel = st.session_state.get("user_kuerzel", "XX")
        
        col_name, col_logout = st.columns([3, 1])
        with col_name:
            st.markdown(f"**{user_name}** ({user_kuerzel})")
        with col_logout:
            if st.button("🚪", help="Logout", key="logout_btn"):
                st.session_state["authenticated"] = False
                st.session_state["user_kuerzel"] = None
                st.session_state["user_name"] = None
                st.rerun()
    
    # Tabs
    tab1, tab2 = st.tabs(["📬 Inbox", "📢 Ad-Kommentare"])
    
    # ===== TAB 1: Inbox =====
    with tab1:
        # Sidebar für Filter & Übersicht
        with st.sidebar:
            # Übersicht oben
            st.subheader("Offen")
            
            # Cached stats function
            @st.cache_data(ttl=30)
            def get_sidebar_stats():
                client = get_bq_client()
                own_id = get_own_instagram_id()
                stats = {"chats": 0, "comments": 0}
                try:
                    # Count CONVERSATIONS where the LATEST message is unanswered
                    chat_stats = client.query(f"""
                    WITH latest_per_sender AS (
                        SELECT 
                            sender_id,
                            response_text,
                            ROW_NUMBER() OVER (PARTITION BY sender_id ORDER BY received_at DESC) as rn
                        FROM `root-slate-454410-u0.instagram_messages.messages`
                        WHERE sender_id != '{own_id}'
                          AND sender_id NOT LIKE 'demo_%'
                          AND sender_id NOT LIKE 'test_%'
                    )
                    SELECT COUNT(*) as offen
                    FROM latest_per_sender
                    WHERE rn = 1 AND (response_text IS NULL OR response_text = '')
                    """).to_dataframe().iloc[0]
                    stats["chats"] = int(chat_stats['offen'])
                except:
                    pass
                try:
                    comment_stats = client.query("""
                    SELECT COUNTIF((response_text IS NULL OR response_text = '') AND (is_liked IS NULL OR is_liked = FALSE)) as offen
                    FROM `root-slate-454410-u0.instagram_messages.ad_comments`
                    WHERE is_deleted = FALSE
                    """).to_dataframe().iloc[0]
                    stats["comments"] = int(comment_stats['offen'])
                except:
                    pass
                return stats
            
            sidebar_stats = get_sidebar_stats()
            st.markdown(f"**Chats:** {sidebar_stats['chats']}")
            st.markdown(f"**Ad-Kommentare:** {sidebar_stats['comments']}")
            
            # Filter
            st.subheader("🔍 Filter")
            
            # Filter: Alle / Unbeantwortet
            filter_type = st.radio(
                "Anzeigen",
                ["Alle", "Unbeantwortet"],
                key="filter_type",
                horizontal=True
            )
            
            st.divider()
            
            # Filter: Tags (Mehrfachauswahl)
            all_tags = get_all_tags()
            filter_tags = st.multiselect(
                "Nach Tags filtern",
                options=all_tags,
                key="filter_tags"
            )
            
            # Blacklist-Verwaltung (persistent aus DB)
            blacklist = load_blacklist()
            
            if blacklist:
                st.divider()
                with st.expander(f"🚫 Blockierte User ({len(blacklist)})"):
                    for blocked_id in list(blacklist):
                        blocked_name = f"User #{blocked_id[-6:]}"
                        
                        col_name, col_unblock = st.columns([3, 1])
                        with col_name:
                            st.write(blocked_name)
                        with col_unblock:
                            if st.button("✓", key=f"unblock_{blocked_id}", help="Entsperren"):
                                remove_from_blacklist(blocked_id)
                                st.rerun()
        
        # Main Content
        col_inbox, col_chat = st.columns([1, 2])
        
        with col_inbox:
            col_title, col_refresh = st.columns([3, 1])
            with col_title:
                st.subheader("Chats")
            with col_refresh:
                if st.button("🔄", help="Aktualisieren"):
                    load_conversations.clear()
                    load_chat_history.clear()
                    st.rerun()
            
            # Konversationen laden (filter_tags als comma-separated string für caching)
            conversations = load_conversations(
                filter_type="unbeantwortet" if filter_type == "Unbeantwortet" else "all",
                filter_tags_str=",".join(filter_tags) if filter_tags else ""
            )
            
            # Blacklist aus DB laden
            blacklist = load_blacklist()
            
            # Selected Chats für Bulk-Actions
            if 'selected_chats' not in st.session_state:
                st.session_state.selected_chats = set()
            
            # Blacklist anwenden
            if not conversations.empty and blacklist:
                conversations = conversations[~conversations['sender_id'].isin(blacklist)]
            
            # Paging
            CHATS_PER_PAGE = 15
            if 'chat_page' not in st.session_state:
                st.session_state.chat_page = 0
            
            total_chats = len(conversations) if not conversations.empty else 0
            total_pages = max(1, (total_chats + CHATS_PER_PAGE - 1) // CHATS_PER_PAGE)
            current_page = min(st.session_state.chat_page, total_pages - 1)
            
            if conversations.empty:
                st.info("Keine Chats gefunden")
            else:
                # Paging Info
                start_idx = current_page * CHATS_PER_PAGE
                end_idx = min(start_idx + CHATS_PER_PAGE, total_chats)
                
                # === BULK SELECTION MODE ===
                selection_mode = st.toggle("☑️ Auswahl-Modus", key="bulk_select_mode", help="Mehrere Chats markieren")
                
                # Bulk Actions (nur wenn Auswahl-Modus aktiv)
                if selection_mode and st.session_state.selected_chats:
                    selected_count = len(st.session_state.selected_chats)
                    st.markdown(f"**{selected_count} ausgewählt**")
                    
                    bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
                    with bulk_col1:
                        if st.button("✅ Gelesen", key="bulk_read", help="Als gelesen markieren"):
                            bulk_mark_chats_as_read(list(st.session_state.selected_chats))
                            st.session_state.selected_chats = set()
                            load_conversations.clear()
                            st.success(f"{selected_count} Chats als gelesen markiert")
                            st.rerun()
                    with bulk_col2:
                        if st.button("🚫 Blacklist", key="bulk_blacklist", help="Auf Blacklist setzen"):
                            user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
                            for user_id in st.session_state.selected_chats:
                                add_to_blacklist(user_id, blocked_by=user_kuerzel)
                            st.session_state.selected_chats = set()
                            st.success(f"{selected_count} User auf Blacklist")
                            st.rerun()
                    with bulk_col3:
                        if st.button("❌ Abbrechen", key="bulk_cancel"):
                            st.session_state.selected_chats = set()
                            st.rerun()
                    
                    st.divider()
                
                st.caption(f"Zeige {start_idx + 1}-{end_idx} von {total_chats}")
                
                # Paging Buttons oben
                if total_pages > 1:
                    pg_col1, pg_col2, pg_col3 = st.columns([1, 2, 1])
                    with pg_col1:
                        if st.button("◀", key="prev_page", disabled=current_page == 0):
                            st.session_state.chat_page = current_page - 1
                            st.rerun()
                    with pg_col2:
                        st.markdown(f"<center>Seite {current_page + 1}/{total_pages}</center>", unsafe_allow_html=True)
                    with pg_col3:
                        if st.button("▶", key="next_page", disabled=current_page >= total_pages - 1):
                            st.session_state.chat_page = current_page + 1
                            st.rerun()
                
                # Konversationen dieser Seite anzeigen
                page_conversations = conversations.iloc[start_idx:end_idx]
                
                for _, conv in page_conversations.iterrows():
                    sender_id = conv['sender_id']
                    db_name = conv.get('sender_name', '') or ''
                    
                    # In der Liste: KEINE API-Calls! Nur DB-Name oder formatierte ID
                    # API wird nur beim Öffnen des Chats aufgerufen
                    sender_name = db_name or f"Kunde #{sender_id[-6:]}"
                    has_unanswered = conv.get('has_unanswered', 0)
                    last_message = conv.get('last_message', '')[:50] + "..." if conv.get('last_message') else ""
                    tags = conv.get('tags', '') or ''
                    
                    # Button Style
                    icon = "🔴" if has_unanswered else "✅"
                    
                    # Tags anzeigen
                    tags_display = ""
                    if tags:
                        tags_list = [t.strip() for t in tags.split(',') if t.strip()][:2]
                        tags_display = " · ".join(tags_list)
                    
                    # === SELECTION MODE: Checkbox + Info ===
                    if selection_mode:
                        chat_col1, chat_col2 = st.columns([1, 8])
                        with chat_col1:
                            is_selected = sender_id in st.session_state.selected_chats
                            if st.checkbox("", value=is_selected, key=f"sel_{sender_id}", label_visibility="collapsed"):
                                st.session_state.selected_chats.add(sender_id)
                            else:
                                st.session_state.selected_chats.discard(sender_id)
                        with chat_col2:
                            btn_label = f"{icon} **{sender_name}**"
                            if tags_display:
                                btn_label += f" · 🏷️ {tags_display}"
                            if st.button(btn_label, key=f"conv_{sender_id}", use_container_width=True):
                                st.session_state.selected_chat = sender_id
                                st.rerun()
                    else:
                        # === NORMAL MODE: Just button ===
                        btn_label = f"{icon} **{sender_name}**"
                        if tags_display:
                            btn_label += f"\n🏷️ {tags_display}"
                        
                        if st.button(btn_label, key=f"conv_{sender_id}", use_container_width=True):
                            st.session_state.selected_chat = sender_id
                            st.rerun()
        
        with col_chat:
            if st.session_state.get('selected_chat'):
                render_chat_view(st.session_state.selected_chat)
            else:
                st.info("👈 Wähle einen Chat aus")
    
    # ===== TAB 2: Ad-Kommentare =====
    with tab2:
        # Header mit Sync-Button
        col_title, col_sync = st.columns([3, 1])
        with col_title:
            st.subheader("📢 Kommentare")
        with col_sync:
            if st.button("🔄 Sync Instagram", key="sync_comments"):
                with st.spinner("Lade Kommentare von Instagram... (kann 30-60 Sek. dauern)"):
                    try:
                        new_count, total_count, debug_info = sync_instagram_comments()
                        st.session_state['sync_result'] = {
                            'new': new_count,
                            'total': total_count,
                            'debug': debug_info
                        }
                        # Cache leeren damit neue Daten geladen werden
                        load_ad_media_ids.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Fehler beim Sync: {e}")
        
        # Sync-Ergebnis anzeigen (nach rerun)
        if 'sync_result' in st.session_state:
            result = st.session_state['sync_result']
            if result['new'] > 0:
                st.success(f"✅ {result['new']} neue Kommentare geladen!")
            else:
                st.info(f"Keine neuen Kommentare. ({result['total']} geprüft)")
            with st.expander("🔍 Debug-Info"):
                st.code(result['debug'])
            del st.session_state['sync_result']
        
        # Funktion für KI-Antwort
        def generate_comment_reply(comment_text: str, sentiment: str, commenter_name: str) -> str:
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                return "Danke für deinen Kommentar! 🤍"
            try:
                client = genai.Client(api_key=api_key)
                prompt = f"""Du antwortest auf einen öffentlichen Kommentar unter einer LILIMAUS Werbeanzeige.

WICHTIG - Öffentlicher Kommentar, keine DM!
- Kurz und freundlich (1-2 Sätze)
- Professionell aber herzlich
- Bei Fragen: Kurze Antwort oder auf DM verweisen
- Bei Kritik: Verständnisvoll, Lösung anbieten
- Bei Lob: Herzlich bedanken
- Emojis sparsam (1-2 max)

Kommentar von {commenter_name}:
"{comment_text}"

Sentiment: {sentiment}
"""
                response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                return response.text.strip()
            except:
                return "Danke für deinen Kommentar! Bei Fragen schreib uns gerne eine DM 🤍"
        
        # Stats
        client = get_bq_client()
        try:
            stats = client.query("""
            SELECT 
                COUNT(*) as total,
                COUNTIF(
                    (has_our_reply IS NULL OR has_our_reply = FALSE) 
                    AND (is_done IS NULL OR is_done = FALSE)
                    AND (response_text IS NULL OR response_text = '')
                ) as offen,
                COUNTIF(sentiment = 'negative') as negative,
                COUNTIF(sentiment = 'question') as questions,
                COUNTIF(has_our_reply = TRUE) as bereits_beantwortet
            FROM `root-slate-454410-u0.instagram_messages.ad_comments`
            WHERE is_deleted = FALSE AND post_type = 'ad'
            """).to_dataframe().iloc[0]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("📊 Gesamt", int(stats.get('total', 0) or 0))
            col2.metric("⚠️ Offen", int(stats.get('offen', 0) or 0))
            col3.metric("✅ Beantwortet", int(stats.get('bereits_beantwortet', 0) or 0))
            col4.metric("🔴 Negativ", int(stats.get('negative', 0) or 0))
            col5.metric("🟡 Fragen", int(stats.get('questions', 0) or 0))
        except:
            pass
        
        st.divider()
        
        # Filter (nur Status, kein Typ mehr nötig)
        comment_filter = st.radio(
            "Anzeigen",
            ["Alle", "Unbearbeitet"],
            horizontal=True,
            key="comment_filter"
        )
        
        # Kommentare laden (nur Ads)
        try:
            # Filter anwenden - nur Ad-Kommentare
            where_clause = "is_deleted = FALSE AND post_type = 'ad'"
            if comment_filter == "Unbearbeitet":
                # Offen = keine eigene Reply UND nicht als erledigt markiert UND nicht manuell beantwortet
                where_clause += """ AND (has_our_reply IS NULL OR has_our_reply = FALSE) 
                                   AND (is_done IS NULL OR is_done = FALSE)
                                   AND (response_text IS NULL OR response_text = '')"""
            
            comments = client.query(f"""
            SELECT * FROM `root-slate-454410-u0.instagram_messages.ad_comments`
            WHERE {where_clause}
            ORDER BY 
                CASE WHEN (response_text IS NULL AND (is_liked IS NULL OR is_liked = FALSE)) THEN 0 ELSE 1 END,
                CASE sentiment WHEN 'negative' THEN 0 WHEN 'question' THEN 1 ELSE 2 END,
                created_at DESC
            LIMIT 50
            """).to_dataframe()
            
            if not comments.empty:
                for idx, comment in comments.iterrows():
                    sentiment = comment.get('sentiment', 'neutral') or 'neutral'
                    
                    # Safe boolean checks for NA/NaN values
                    response_text = comment.get('response_text')
                    has_manual_response = pd.notna(response_text) and response_text != ''
                    
                    # Bereits auf Instagram beantwortet?
                    has_our_reply_val = comment.get('has_our_reply')
                    has_our_reply = pd.notna(has_our_reply_val) and has_our_reply_val == True
                    our_reply_text = comment.get('our_reply_text', '') or ''
                    
                    # Als erledigt markiert?
                    is_done_val = comment.get('is_done')
                    is_done = pd.notna(is_done_val) and is_done_val == True
                    
                    # Kommentar ist "bearbeitet" wenn: eigene Reply ODER manuell beantwortet ODER erledigt
                    is_processed = has_our_reply or has_manual_response or is_done
                    
                    # Icon basierend auf Sentiment
                    sentiment_icon = "🔴" if sentiment == 'negative' else ("🟡" if sentiment == 'question' else "🟢")
                    
                    with st.container():
                        # Status-Zeile
                        status_parts = []
                        if has_our_reply:
                            status_parts.append("<span style='background: #D4EDDA; padding: 2px 8px; border-radius: 4px; font-size: 12px;'>✅ Bereits beantwortet</span>")
                        elif is_done:
                            status_parts.append("<span style='background: #E2E3E5; padding: 2px 8px; border-radius: 4px; font-size: 12px;'>✓ Erledigt</span>")
                        elif not is_processed:
                            status_parts.append("<span style='background: #FFF3CD; padding: 2px 8px; border-radius: 4px; font-size: 12px;'>⚠️ Offen</span>")
                        
                        if status_parts:
                            st.markdown(" ".join(status_parts), unsafe_allow_html=True)
                        
                        col1, col2, col3, col4, col5 = st.columns([4, 1, 1, 1, 1])
                        
                        # Prüfe ob bereits geliked
                        is_liked = pd.notna(comment.get('is_liked')) and comment.get('is_liked') == True
                        
                        with col1:
                            st.markdown(f"**{sentiment_icon} {comment.get('commenter_name', 'Unbekannt')}**")
                            st.write(comment.get('comment_text', ''))
                            
                            # Zeige ALLE Replies
                            replies_json_str = comment.get('replies_json', '') or ''
                            if replies_json_str:
                                try:
                                    replies_list = json.loads(replies_json_str)
                                    if replies_list:
                                        for reply in replies_list:
                                            reply_user = reply.get('username', 'Unbekannt')
                                            reply_text = reply.get('text', '')
                                            is_own = reply.get('is_own', False)
                                            
                                            if is_own:
                                                st.caption(f"↳ **{reply_user}** (ihr): {reply_text}")
                                            else:
                                                st.caption(f"↳ {reply_user}: {reply_text}")
                                except:
                                    # Fallback auf altes Format
                                    if has_our_reply and our_reply_text:
                                        st.caption(f"↳ Eure Antwort (Instagram): {our_reply_text}")
                            elif has_our_reply and our_reply_text:
                                st.caption(f"↳ Eure Antwort (Instagram): {our_reply_text}")
                            elif has_manual_response:
                                st.caption(f"↳ Eure Antwort: {response_text}")
                            
                            # Ad-Name
                            ad_name = comment.get('ad_name', '') or ''
                            if ad_name:
                                short_caption = ad_name[:60] + "..." if len(ad_name) > 60 else ad_name
                                st.caption(f"📢 {short_caption}")
                        
                        with col2:
                            # Antwort-Button (nur wenn noch nicht beantwortet)
                            if not has_our_reply and not has_manual_response:
                                if st.button("💬 Antworten", key=f"reply_c_{idx}"):
                                    st.session_state.selected_comment_id = comment['comment_id']
                                    st.rerun()
                            else:
                                st.write("✅")
                        
                        with col3:
                            # Erledigt-Button (nur wenn noch nicht erledigt)
                            if not is_processed:
                                if st.button("✓ Erledigt", key=f"done_c_{idx}"):
                                    escaped_id = comment['comment_id'].replace("'", "''")
                                    client.query(f"""
                                    UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                                    SET is_done = TRUE
                                    WHERE comment_id = '{escaped_id}'
                                    """).result()
                                    st.rerun()
                        
                        with col4:
                            # Like-Button (deaktiviert - API Permission fehlt noch)
                            st.button("🤍", key=f"like_c_{idx}", disabled=True, help="Like-Funktion noch nicht verfügbar (API Permission ausstehend)")
                        
                        with col5:
                            # Ausblenden (nur im Dashboard, nicht bei Meta!)
                            if st.button("👁️", key=f"hide_c_{idx}", help="Nur im Dashboard ausblenden"):
                                escaped_id = comment['comment_id'].replace("'", "''")
                                client.query(f"""
                                UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                                SET is_deleted = TRUE
                                WHERE comment_id = '{escaped_id}'
                                """).result()
                                st.rerun()
                        
                        # === INLINE ANTWORT-DIALOG (direkt unter diesem Kommentar) ===
                        current_comment_id = comment['comment_id']
                        if st.session_state.get('selected_comment_id') == current_comment_id:
                            st.markdown("""
                            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                        padding: 2px; border-radius: 10px; margin: 0.5rem 0;">
                                <div style="background: #1a1a2e; border-radius: 8px; padding: 1rem;">
                            """, unsafe_allow_html=True)
                            
                            # KI Vorschlag generieren
                            reply_key = f"comment_reply_{current_comment_id}"
                            comment_sentiment = comment.get('sentiment', 'neutral')
                            if reply_key not in st.session_state:
                                with st.spinner("✨ KI generiert Antwort..."):
                                    st.session_state[reply_key] = generate_comment_reply(
                                        comment.get('comment_text', ''),
                                        comment_sentiment,
                                        comment.get('commenter_name', 'Nutzer')
                                    )
                            
                            reply_text = st.text_area("💬 Antwort schreiben:", height=80, key=reply_key)
                            
                            btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
                            
                            with btn_col1:
                                if st.button("📤 Senden", type="primary", key=f"send_{idx}"):
                                    success, msg = reply_to_comment(current_comment_id, reply_text)
                                    if success:
                                        escaped_reply = reply_text.replace("'", "''")
                                        escaped_id = current_comment_id.replace("'", "''")
                                        user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
                                        client.query(f"""
                                        UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                                        SET response_text = '{escaped_reply}', 
                                            responded_at = CURRENT_TIMESTAMP(),
                                            responded_by = '{user_kuerzel}',
                                            has_our_reply = TRUE,
                                            our_reply_text = '{escaped_reply}'
                                        WHERE comment_id = '{escaped_id}'
                                        """).result()
                                        st.success(f"✅ Gesendet!")
                                        st.session_state.selected_comment_id = None
                                        if reply_key in st.session_state:
                                            del st.session_state[reply_key]
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {msg}")
                            
                            with btn_col2:
                                if st.button("💾 Speichern", key=f"save_{idx}"):
                                    escaped_reply = reply_text.replace("'", "''")
                                    escaped_id = current_comment_id.replace("'", "''")
                                    user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
                                    client.query(f"""
                                    UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                                    SET response_text = '{escaped_reply}', 
                                        responded_at = CURRENT_TIMESTAMP(),
                                        responded_by = '{user_kuerzel}'
                                    WHERE comment_id = '{escaped_id}'
                                    """).result()
                                    st.success("✅ Gespeichert")
                                    st.session_state.selected_comment_id = None
                                    if reply_key in st.session_state:
                                        del st.session_state[reply_key]
                                    st.rerun()
                            
                            with btn_col3:
                                if st.button("🔄 Neu", key=f"regen_{idx}"):
                                    if reply_key in st.session_state:
                                        del st.session_state[reply_key]
                                    st.rerun()
                            
                            with btn_col4:
                                if st.button("❌ Abbrechen", key=f"cancel_{idx}"):
                                    st.session_state.selected_comment_id = None
                                    if reply_key in st.session_state:
                                        del st.session_state[reply_key]
                                    st.rerun()
                            
                            st.markdown("</div></div>", unsafe_allow_html=True)
                        
                        st.divider()
            else:
                st.info("Keine Kommentare gefunden. Klicke auf '🔄 Sync Instagram' um Kommentare zu laden.")
                
        except Exception as e:
            st.error(f"Fehler beim Laden: {e}")


if __name__ == "__main__":
    main()
