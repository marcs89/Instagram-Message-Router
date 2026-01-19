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

# Instagram API Functions
def get_page_access_token():
    """Get Page Access Token from secrets or env"""
    return st.secrets.get("PAGE_ACCESS_TOKEN", os.getenv("PAGE_ACCESS_TOKEN", ""))

def get_instagram_user_info(user_id: str) -> dict:
    """Fetch Instagram user info (username, name) via Graph API"""
    # Skip API call for demo users
    if user_id.startswith("demo_"):
        return {"username": user_id, "name": ""}
    
    token = get_page_access_token()
    if not token:
        return {"username": "", "name": ""}
    
    try:
        url = f"https://graph.facebook.com/v21.0/{user_id}"
        params = {
            "fields": "username,name",
            "access_token": token
        }
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            # Check if it's an error response
            if "error" not in data:
                return {
                    "username": data.get("username", ""),
                    "name": data.get("name", "")
                }
    except Exception as e:
        pass
    
    # Return empty - will fall back to sender_name from DB or ID
    return {"username": "", "name": ""}

@st.cache_data(ttl=3600)
def get_cached_user_info(user_id: str) -> dict:
    """Cached version of user info lookup - disabled until App Review"""
    # API calls fail without App Review, skip them for now
    # return get_instagram_user_info(user_id)
    return {"username": "", "name": ""}

def get_instagram_account_id():
    """Get Instagram Business Account ID from secrets or env"""
    return st.secrets.get("INSTAGRAM_ACCOUNT_ID", os.getenv("INSTAGRAM_ACCOUNT_ID", ""))

def send_instagram_message(recipient_id: str, message_text: str) -> tuple[bool, str]:
    """Send a message via Instagram Graph API"""
    token = get_page_access_token()
    if not token:
        return False, "Kein Page Access Token konfiguriert"
    
    try:
        # Instagram uses the Page ID or Instagram Account ID for sending
        # The recipient_id is the Instagram-scoped User ID (IGSID)
        url = f"https://graph.facebook.com/v21.0/me/messages"
        
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

# Page Config
st.set_page_config(
    page_title="LILIMAUS Inbox",
    page_icon="📬",
    layout="wide"
)

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

# Team-Mitglieder (Kürzel -> Name)
TEAM_MEMBERS = {
    "AS": "Anni",
    "MS": "Marc",
    "SM": "Sina",
    "JD": "Jessy",
    "SG": "Sinem"
}

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

@st.cache_data(ttl=30)  # Cache for 30 seconds
def load_conversations(filter_type: str = "all", filter_tags_str: str = ""):
    """Lädt Konversationen mit Filtern (cached)"""
    client = get_bq_client()
    
    own_id = get_own_instagram_id()
    
    # Filter out our own account and demo users
    where_clauses = [
        f"sender_id != '{own_id}'",
        "sender_id NOT LIKE 'demo_%'"
    ]
    
    if filter_type == "unbeantwortet":
        where_clauses.append("(response_text IS NULL OR response_text = '')")
    
    if filter_tags_str:
        filter_tags = filter_tags_str.split(",")
        tag_conditions = []
        for tag in filter_tags:
            tag_conditions.append(f"tags LIKE '%{tag}%'")
        where_clauses.append(f"({' OR '.join(tag_conditions)})")
    
    where_sql = " AND ".join(where_clauses)
    
    query = f"""
    SELECT 
        sender_id,
        sender_name,
        COUNT(*) as message_count,
        MAX(received_at) as last_message_at,
        MAX(CASE WHEN response_text IS NULL OR response_text = '' THEN 1 ELSE 0 END) as has_unanswered,
        ARRAY_AGG(tags IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[OFFSET(0)] as tags,
        ARRAY_AGG(message_text ORDER BY received_at DESC LIMIT 1)[OFFSET(0)] as last_message
    FROM `root-slate-454410-u0.instagram_messages.messages`
    WHERE {where_sql}
    GROUP BY sender_id, sender_name
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
    """Lädt den Chat-Verlauf für einen Sender (cached)"""
    client = get_bq_client()
    query = f"""
    SELECT *
    FROM `root-slate-454410-u0.instagram_messages.messages`
    WHERE sender_id = '{sender_id}'
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


def render_chat_view(sender_id: str, auto_refresh_chat: bool = False):
    """Rendert die Chat-Ansicht"""
    messages = load_chat_history(sender_id)
    
    if messages.empty:
        st.info("Keine Nachrichten")
        return
    
    # Get username from Instagram API
    user_info = get_cached_user_info(sender_id)
    sender_name = user_info.get('username', '') or messages.iloc[0].get('sender_name', '') or 'Unbekannt'
    last_msg = messages.iloc[-1]
    
    # Header with optional auto-refresh for chat messages only
    col_header, col_refresh = st.columns([4, 1])
    with col_header:
        st.subheader(f"💬 {sender_name}")
    with col_refresh:
        if st.button("🔄", key=f"refresh_chat_{sender_id}", help="Chat aktualisieren"):
            load_chat_history.clear()
            st.rerun()
    
    # Tags anzeigen & bearbeiten
    current_tags_str = last_msg.get('tags', '') or ''
    current_tags = [t.strip() for t in current_tags_str.split(',') if t.strip()]
    
    # Tags Display
    if current_tags:
        tags_html = " ".join([f'<span class="tag">{t}</span>' for t in current_tags])
        st.markdown(tags_html, unsafe_allow_html=True)
    
    # Tags bearbeiten
    with st.expander("🏷️ Tags bearbeiten"):
        all_tags = get_all_tags()
        
        selected_tags = st.multiselect(
            "Tags auswählen",
            options=all_tags,
            default=[t for t in current_tags if t in all_tags],
            key=f"tags_{sender_id}"
        )
        
        new_tag = st.text_input("Neuen Tag erstellen", key=f"new_tag_{sender_id}")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Tags speichern", key=f"save_tags_{sender_id}"):
                final_tags = selected_tags.copy()
                if new_tag and new_tag.strip():
                    final_tags.append(new_tag.strip())
                
                tags_str = ",".join(final_tags)
                for _, msg in messages.iterrows():
                    update_message(msg['message_id'], {"tags": tags_str})
                st.success("✅ Tags gespeichert!")
                st.rerun()
    
    st.divider()
    
    # Chat-Verlauf (neueste zuerst)
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
    
    # "Mehr laden" Button (falls es mehr gibt)
    if end_idx < total_messages:
        remaining = total_messages - end_idx
        if st.button(f"📜 Ältere Nachrichten laden ({remaining} weitere)", key=f"load_more_{sender_id}"):
            st.session_state[page_key] += 1
            st.rerun()
    
    # Nachrichten anzeigen (neueste oben)
    for _, msg in messages_to_show.iterrows():
        received_at = msg.get('received_at', '')
        try:
            time_str = pd.to_datetime(received_at).strftime('%d.%m. %H:%M')
        except:
            time_str = ""
        
        # Antwort zuerst (da umgekehrte Reihenfolge)
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
        
        # Eingehende Nachricht
        message_text = msg.get('message_text', '')
        st.markdown(f"""
        <div class="message-incoming">
            <div>{message_text}</div>
            <div class="message-time">{time_str}</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Antwort-Box
    st.subheader("💬 Antworten")
    
    last_msg_text = last_msg.get('message_text', '')
    reply_key = f"reply_{sender_id}"
    
    # KI-Button
    if st.button("✨ KI-Vorschlag", key=f"ai_{sender_id}"):
        with st.spinner("Schreibt..."):
            # Kontext aus letzten Nachrichten
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
        "Nachricht",
        height=100,
        key=reply_key
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📤 Senden", type="primary", key=f"send_{sender_id}"):
            if reply_text:
                # Sende an Instagram
                success, msg = send_instagram_message(sender_id, reply_text)
                
                if success:
                    # Speichere in DB mit User-Kürzel
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
            # Leere Antwort setzen um als "beantwortet" zu markieren
            user_kuerzel = st.session_state.get('user_kuerzel', 'XX')
            update_message(last_msg['message_id'], {
                "response_text": "[Extern beantwortet]",
                "responded_at": datetime.utcnow().isoformat(),
                "responded_by": user_kuerzel
            })
            st.success(f"✅ Markiert ({user_kuerzel})")
            st.rerun()


def main():
    # Header
    # Kompakter Header mit eingeloggtem User
    col_logo, col_spacer, col_user = st.columns([2, 4, 2])
    with col_logo:
        st.markdown("""
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://lilimaus.de/cdn/shop/files/Lilimaus_Logo_241212.png?v=1743081255" height="30">
            <span style="color: #666; font-size: 14px;">Inbox</span>
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
    tab1, tab2, tab3 = st.tabs(["📬 Inbox", "📢 Ad-Kommentare", "🧪 Test"])
    
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
                    # Count CONVERSATIONS with unanswered messages, not individual messages
                    chat_stats = client.query(f"""
                    SELECT COUNT(DISTINCT sender_id) as offen
                    FROM `root-slate-454410-u0.instagram_messages.messages`
                    WHERE sender_id != '{own_id}'
                      AND sender_id NOT LIKE 'demo_%'
                      AND (response_text IS NULL OR response_text = '')
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
            
            if conversations.empty:
                st.info("Keine Chats gefunden")
            else:
                for _, conv in conversations.iterrows():
                    sender_id = conv['sender_id']
                    # Try to get username - API only works for testers without App Review
                    user_info = get_cached_user_info(sender_id)
                    username = user_info.get('username', '')
                    db_name = conv.get('sender_name', '')
                    # Use: API username > DB name > shortened ID
                    sender_name = username or db_name or f"User {sender_id[:10]}..."
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
        st.subheader("📢 Ad-Kommentare")
        
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
                COUNTIF((response_text IS NULL OR response_text = '') AND (is_liked IS NULL OR is_liked = FALSE)) as offen,
                COUNTIF(sentiment = 'negative') as negative,
                COUNTIF(sentiment = 'question') as questions,
                COUNTIF(sentiment = 'positive') as positive
            FROM `root-slate-454410-u0.instagram_messages.ad_comments`
            WHERE is_deleted = FALSE
            """).to_dataframe().iloc[0]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("📊 Gesamt", int(stats.get('total', 0) or 0))
            col2.metric("⚠️ Offen", int(stats.get('offen', 0) or 0))
            col3.metric("🔴 Negativ", int(stats.get('negative', 0) or 0))
            col4.metric("🟡 Fragen", int(stats.get('questions', 0) or 0))
            col5.metric("🟢 Positiv", int(stats.get('positive', 0) or 0))
        except:
            pass
        
        st.divider()
        
        # Filter
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            comment_filter = st.radio(
                "Anzeigen",
                ["Alle", "Unbearbeitet"],
                horizontal=True,
                key="comment_filter"
            )
        
        st.divider()
        
        # Antwort-Dialog wenn Kommentar ausgewählt
        if st.session_state.get('selected_comment_id'):
            selected_id = st.session_state.selected_comment_id
            try:
                comment_df = client.query(f"""
                SELECT * FROM `root-slate-454410-u0.instagram_messages.ad_comments`
                WHERE comment_id = '{selected_id}'
                """).to_dataframe()
                
                if not comment_df.empty:
                    comment = comment_df.iloc[0]
                    
                    st.markdown("### 💬 Antwort schreiben")
                    
                    sentiment = comment.get('sentiment', 'neutral')
                    icon = "🔴" if sentiment == 'negative' else ("🟡" if sentiment == 'question' else "🟢")
                    
                    st.markdown(f"**{icon} {comment.get('commenter_name', 'Unbekannt')}:** {comment.get('comment_text', '')}")
                    st.caption(f"Ad: {comment.get('ad_name', 'Unbekannt') or 'Unbekannt'}")
                    
                    # KI Vorschlag
                    reply_key = f"comment_reply_{selected_id}"
                    if reply_key not in st.session_state:
                        with st.spinner("✨ KI generiert Antwort..."):
                            st.session_state[reply_key] = generate_comment_reply(
                                comment.get('comment_text', ''),
                                sentiment,
                                comment.get('commenter_name', 'Nutzer')
                            )
                    
                    reply_text = st.text_area("Antwort", height=100, key=reply_key)
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("📤 Speichern", type="primary", key="save_comment"):
                            escaped_reply = reply_text.replace("'", "''")
                            escaped_id = selected_id.replace("'", "''")
                            client.query(f"""
                            UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                            SET response_text = '{escaped_reply}', responded_at = CURRENT_TIMESTAMP()
                            WHERE comment_id = '{escaped_id}'
                            """).result()
                            st.success("✅ Gespeichert!")
                            st.session_state.selected_comment_id = None
                            if reply_key in st.session_state:
                                del st.session_state[reply_key]
                            st.rerun()
                    
                    with col2:
                        if st.button("🔄 Neu generieren", key="regen_comment"):
                            if reply_key in st.session_state:
                                del st.session_state[reply_key]
                            st.rerun()
                    
                    with col3:
                        if st.button("❌ Abbrechen", key="cancel_comment"):
                            st.session_state.selected_comment_id = None
                            if reply_key in st.session_state:
                                del st.session_state[reply_key]
                            st.rerun()
                    
                    st.divider()
            except Exception as e:
                st.error(f"Fehler: {e}")
        
        # Kommentare laden
        try:
            # Filter anwenden
            where_clause = "is_deleted = FALSE"
            if comment_filter == "Unbearbeitet":
                where_clause += " AND (response_text IS NULL OR response_text = '') AND (is_liked IS NULL OR is_liked = FALSE)"
            
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
                    sentiment = comment.get('sentiment', 'neutral')
                    has_response = bool(comment.get('response_text'))
                    is_liked = bool(comment.get('is_liked'))
                    is_processed = has_response or is_liked
                    
                    # Icon basierend auf Sentiment
                    sentiment_icon = "🔴" if sentiment == 'negative' else ("🟡" if sentiment == 'question' else "🟢")
                    
                    # Unbearbeitet-Indikator
                    status_icon = "✅" if is_processed else "⚪"
                    
                    with st.container():
                        # Warnung für unbearbeitete
                        if not is_processed:
                            st.markdown(f"<div style='background: #FFF3CD; padding: 2px 8px; border-radius: 4px; display: inline-block; font-size: 12px; margin-bottom: 5px;'>⚠️ Unbearbeitet</div>", unsafe_allow_html=True)
                        
                        col1, col2, col3 = st.columns([4, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{sentiment_icon} {comment.get('commenter_name', 'Unbekannt')}**")
                            st.write(comment.get('comment_text', ''))
                            
                            if has_response:
                                st.caption(f"↳ Eure Antwort: {comment.get('response_text')}")
                            elif is_liked:
                                st.caption("❤️ Geliked")
                            
                            st.caption(f"Ad: {comment.get('ad_name', '') or 'Unbekannt'}")
                        
                        with col2:
                            # Like Button
                            if is_liked:
                                st.write("❤️")
                            else:
                                if st.button("🤍", key=f"like_c_{idx}", help="Kommentar liken"):
                                    escaped_id = comment['comment_id'].replace("'", "''")
                                    client.query(f"""
                                    UPDATE `root-slate-454410-u0.instagram_messages.ad_comments`
                                    SET is_liked = TRUE
                                    WHERE comment_id = '{escaped_id}'
                                    """).result()
                                    st.rerun()
                        
                        with col3:
                            if not has_response and not is_liked:
                                if st.button("💬", key=f"reply_c_{idx}", help="Antworten"):
                                    st.session_state.selected_comment_id = comment['comment_id']
                                    st.rerun()
                            elif has_response:
                                st.write("✅")
                        
                        st.divider()
            else:
                st.info("Keine Kommentare gefunden")
                
        except Exception as e:
            st.error(f"Fehler beim Laden: {e}")
        
        # Test-Kommentar erstellen
        with st.expander("🧪 Test-Kommentar erstellen"):
            test_comment = st.text_input("Kommentar", "Ist das auch in Blau verfügbar?", key="tc_text")
            test_sentiment = st.selectbox("Sentiment", ["question", "positive", "negative"], key="tc_sent")
            test_ad = st.text_input("Ad-Name", "Betthimmel Kampagne", key="tc_ad")
            
            if st.button("Erstellen", key="create_tc"):
                comment_id = f"test_c_{datetime.now().timestamp()}"
                client.query(f"""
                INSERT INTO `root-slate-454410-u0.instagram_messages.ad_comments`
                (comment_id, post_id, ad_name, commenter_id, commenter_name, comment_text, 
                 created_at, sentiment, status, is_hidden, is_deleted, priority)
                VALUES (
                    '{comment_id}', 'test_post', '{test_ad.replace("'", "''")}',
                    'test_user', 'Test Nutzer', '{test_comment.replace("'", "''")}',
                    CURRENT_TIMESTAMP(), '{test_sentiment}', 'new', FALSE, FALSE,
                    '{"high" if test_sentiment == "negative" else "normal"}'
                )
                """).result()
                st.success("✅ Erstellt!")
                st.rerun()
    
    # ===== TAB 3: Test =====
    with tab3:
        st.subheader("🧪 Test-Nachricht erstellen")
        
        test_text = st.text_input("Nachrichtentext", "Hallo, welche Größe passt bei 80cm?")
        test_sender = st.text_input("Absender", "Test User")
        test_tags = st.multiselect("Tags", options=get_all_tags(), key="test_tags")
        
        if st.button("Erstellen"):
            client = get_bq_client()
            msg_id = f"test_{datetime.now().timestamp()}"
            sender_id = f"test_sender_{hash(test_sender) % 10000}"
            tags_str = ",".join(test_tags) if test_tags else ""
            
            query = f"""
            INSERT INTO `root-slate-454410-u0.instagram_messages.messages`
            (message_id, sender_id, sender_name, recipient_id, timestamp, received_at, 
             message_text, has_attachments, attachment_types, is_story_reply, 
             categories, primary_category, priority, status, tags)
            VALUES (
                '{msg_id}',
                '{sender_id}',
                '{test_sender.replace("'", "''")}',
                'lilimaus_page',
                {int(datetime.now().timestamp())},
                CURRENT_TIMESTAMP(),
                '{test_text.replace("'", "''")}',
                FALSE,
                '[]',
                FALSE,
                '[]',
                'unkategorisiert',
                'normal',
                'new',
                '{tags_str}'
            )
            """
            try:
                client.query(query).result()
                st.success("✅ Test-Nachricht erstellt!")
                st.rerun()
            except Exception as e:
                st.error(f"Fehler: {e}")


if __name__ == "__main__":
    main()
