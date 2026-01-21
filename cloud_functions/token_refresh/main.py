"""
Cloud Function: Instagram Token Refresh
Wird monatlich via Cloud Scheduler aufgerufen.
"""

import functions_framework
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
    client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": new_token.encode("UTF-8")},
        }
    )


@functions_framework.http
def refresh_token(request):
    """HTTP Cloud Function für Token Refresh"""
    try:
        # 1. Aktuellen Token holen
        current_token = get_current_token()
        
        # 2. Token erneuern
        result = refresh_instagram_token(current_token)
        
        if "access_token" in result:
            new_token = result["access_token"]
            expires_in = result.get("expires_in", 0)
            expires_days = expires_in // 86400
            
            # 3. Neuen Token speichern
            store_new_token(new_token)
            
            return {
                "success": True,
                "message": f"Token erneuert, gültig für {expires_days} Tage"
            }, 200
        else:
            error = result.get("error", {}).get("message", str(result))
            return {
                "success": False,
                "error": error
            }, 500
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }, 500
