#!/usr/bin/env python3
"""
Instagram Token Refresh Script
Erneuert den Instagram Access Token und speichert ihn in Google Secret Manager.

Kann manuell oder via Cloud Scheduler ausgeführt werden.
Empfehlung: Monatlich ausführen (Token läuft nach 60 Tagen ab).

Usage:
  python3 refresh_tokens.py
"""

import requests
from google.cloud import secretmanager

GCP_PROJECT_ID = "root-slate-454410-u0"
SECRET_NAME = "instagram-access-token"


def get_current_token() -> str:
    """Hole aktuellen Token aus Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def refresh_instagram_token(current_token: str) -> dict:
    """Erneuere Instagram Token via Meta API"""
    url = "https://graph.instagram.com/refresh_access_token"
    params = {
        "grant_type": "ig_refresh_token",
        "access_token": current_token
    }
    
    response = requests.get(url, params=params, timeout=30)
    return response.json()


def store_new_token(new_token: str):
    """Speichere neuen Token als neue Version in Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_NAME}"
    
    # Füge neue Version hinzu
    client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": new_token.encode("UTF-8")},
        }
    )
    print(f"✅ Neuer Token in Secret Manager gespeichert")


def main():
    print("🔄 Instagram Token Refresh gestartet...")
    
    # 1. Aktuellen Token holen
    print("📥 Hole aktuellen Token aus Secret Manager...")
    current_token = get_current_token()
    print(f"   Token gefunden (endet mit: ...{current_token[-10:]})")
    
    # 2. Token erneuern
    print("🔄 Erneuere Token via Meta API...")
    result = refresh_instagram_token(current_token)
    
    if "access_token" in result:
        new_token = result["access_token"]
        expires_in = result.get("expires_in", 0)
        expires_days = expires_in // 86400
        
        print(f"   ✅ Neuer Token erhalten (gültig für {expires_days} Tage)")
        
        # 3. Neuen Token speichern
        store_new_token(new_token)
        
        print(f"\n✅ Token erfolgreich erneuert!")
        print(f"   Neuer Token endet mit: ...{new_token[-10:]}")
        print(f"   Gültig bis: ca. {expires_days} Tage")
        
    else:
        error = result.get("error", {}).get("message", str(result))
        print(f"❌ Fehler beim Erneuern: {error}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
