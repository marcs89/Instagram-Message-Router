#!/usr/bin/env python3
"""
Einmaliges Script zum Laden aller Instagram Chat-Historie ab 01.01.2026
Ausführen: python3 load_history.py
"""

import requests
import json
from datetime import datetime
from google.cloud import bigquery
import os
import time

# Tokens und Config
IG_TOKEN = "IGAATZBlIUQt8VBZAGFEaTkxdkNaS2h4NDZABdnN0MDk4aHQ4MUs0QWZAZAVmx6Mm95aVNQVnJyQ3o5d3VPRElxWkZAJUEhwT1BRLUh6VWhLNEIxODY1NjRtbUs5ZA1dueHIzZAFBUVjFDWU1WRzI1WU41RENpU0VNXzlkeVVjZAlFjWkpXSQZDZD"
OWN_IG_ID = "17841462069085392"  # lilimaus_de
SINCE_DATE = datetime(2026, 1, 1)

# BigQuery Client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "dashboard/gcp-credentials.json"
client = bigquery.Client()

def load_all_conversations():
    """Lädt alle Conversations"""
    print("Lade Conversations von Instagram...")
    url = "https://graph.instagram.com/v21.0/me/conversations"
    params = {
        "platform": "instagram",
        "fields": "participants,id,updated_time",
        "limit": 100,
        "access_token": IG_TOKEN
    }
    
    all_convs = []
    while url and len(all_convs) < 200:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            all_convs.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
            params = {}
        else:
            print(f"Error: {response.text}")
            break
    return all_convs

def load_messages(conversation_id):
    """Lädt Nachrichten einer Conversation"""
    url = f"https://graph.instagram.com/v21.0/{conversation_id}"
    params = {
        "fields": "messages{id,message,created_time,from}",
        "access_token": IG_TOKEN
    }
    
    response = requests.get(url, params=params, timeout=30)
    if response.status_code == 200:
        data = response.json()
        return data.get("messages", {}).get("data", [])
    return []

def save_message(msg, sender_id, sender_username):
    """Speichert eine Nachricht in BigQuery"""
    msg_id = msg.get("id", "")
    msg_text = msg.get("message", "") or ""
    created_time = msg.get("created_time", "")
    from_id = msg.get("from", {}).get("id", "")
    from_username = msg.get("from", {}).get("username", "")
    
    if not msg_id:
        return False
    
    # Parse created_time und prüfe ob nach SINCE_DATE
    try:
        msg_date = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
        if msg_date.replace(tzinfo=None) < SINCE_DATE:
            return False  # Nachricht zu alt
    except:
        pass
    
    # Prüfe ob schon existiert
    check_query = f"""
    SELECT message_id FROM `root-slate-454410-u0.instagram_messages.messages`
    WHERE message_id = '{msg_id}'
    """
    try:
        existing = client.query(check_query).result()
        if list(existing):
            return False  # Schon vorhanden
    except:
        pass
    
    # Bestimme sender/recipient
    if from_id == OWN_IG_ID:
        actual_sender_id = OWN_IG_ID
        actual_recipient_id = sender_id
        actual_sender_name = "lilimaus_de"
    else:
        actual_sender_id = sender_id
        actual_recipient_id = OWN_IG_ID
        actual_sender_name = from_username or sender_username
    
    # Escape single quotes
    msg_text_escaped = msg_text.replace("'", "''")
    sender_name_escaped = actual_sender_name.replace("'", "''")
    
    # Insert
    insert_query = f"""
    INSERT INTO `root-slate-454410-u0.instagram_messages.messages`
    (message_id, sender_id, sender_name, recipient_id, timestamp, received_at, 
     message_text, has_attachments, attachment_types, is_story_reply, 
     categories, primary_category, priority, status, tags)
    VALUES (
        '{msg_id}',
        '{actual_sender_id}',
        '{sender_name_escaped}',
        '{actual_recipient_id}',
        UNIX_SECONDS(TIMESTAMP('{created_time}')),
        TIMESTAMP('{created_time}'),
        '{msg_text_escaped}',
        FALSE,
        '[]',
        FALSE,
        '[]',
        'historie',
        'normal',
        'synced',
        ''
    )
    """
    try:
        client.query(insert_query).result()
        return True
    except Exception as e:
        print(f"  Error inserting: {e}")
        return False


def main():
    print("=" * 50)
    print("Instagram Chat-Historie Import")
    print(f"Lade Nachrichten ab: {SINCE_DATE.strftime('%d.%m.%Y')}")
    print("=" * 50)
    
    # Lade alle Conversations
    conversations = load_all_conversations()
    print(f"\nGefunden: {len(conversations)} Conversations\n")
    
    total_new = 0
    for i, conv in enumerate(conversations):
        conv_id = conv.get("id", "")
        participants = conv.get("participants", {}).get("data", [])
        
        # Finde den anderen Teilnehmer (nicht lilimaus)
        other_user = None
        for p in participants:
            if p.get("id") != OWN_IG_ID:
                other_user = p
                break
        
        if not other_user:
            continue
        
        sender_id = other_user.get("id", "")
        sender_username = other_user.get("username", "")
        
        print(f"[{i+1}/{len(conversations)}] @{sender_username or sender_id}...", end=" ", flush=True)
        
        # Lade Nachrichten
        messages = load_messages(conv_id)
        
        new_count = 0
        for msg in messages:
            if save_message(msg, sender_id, sender_username):
                new_count += 1
        
        print(f"{new_count} neue Nachrichten")
        total_new += new_count
        
        # Rate limiting
        time.sleep(0.2)
    
    print("\n" + "=" * 50)
    print(f"FERTIG: {total_new} neue Nachrichten gespeichert")
    print("=" * 50)


if __name__ == "__main__":
    main()
