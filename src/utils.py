# src/utils.py
import os, time, requests
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()                       # reads .env

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
CLIENT_ID     = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
SCOPE         = "https://api.ebay.com/oauth/api_scope"

_cache = {"token": None, "exp": 0}

def get_app_token():
    """Return a valid eBay application token (cached + auto-refresh)."""
    if time.time() < _cache["exp"] - 60:
        return _cache["token"]

    body    = urlencode({"grant_type": "client_credentials", "scope": SCOPE})
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(
        EBAY_OAUTH_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data=body,
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()

    _cache["token"] = j["access_token"]
    _cache["exp"]   = time.time() + int(j["expires_in"])
    return _cache["token"]
