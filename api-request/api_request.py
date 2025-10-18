"""
api_request.py
---------------
Fetches historical crypto prices from the CoinGecko API.
Used by Airflow DAG to ingest daily data into Postgres.

Example endpoint:
https://api.coingecko.com/api/v3/coins/ethereum/history?date=30-12-2024&localization=false
"""

import os
import requests
from datetime import datetime

# ---------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------
COINGECKO_API_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "CG-ec5vyXgT4pDKCLBQ1GPGFseb")

# Headers obligatorios para autenticación
HEADERS = {"x-cg-demo-api-key": COINGECKO_API_KEY}


# ---------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------
def fetch_coin_history(coin_id: str, date_str: str) -> dict:
    """
    Fetch historical price and metadata for a given coin and date from CoinGecko API.

    Parameters
    ----------
    coin_id : str
        CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
    date_str : str
        Date string in format 'DD-MM-YYYY' (CoinGecko requirement)

    Returns
    -------
    dict
        JSON response with market_data, community_data, etc.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP request fails or returns non-200 status.
    """
    url = f"{COINGECKO_API_BASE}/api/v3/coins/{coin_id}/history"
    params = {"date": date_str, "localization": "false"}

    print(f"🔗 Fetching CoinGecko data for {coin_id} on {date_str} ...")

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)
        response.raise_for_status()
        print(f"✅ API call succeeded for {coin_id}")
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data for {coin_id}: {e}")
        raise


# ---------------------------------------------------------------------
# Prueba local (opcional)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Si ejecutas el script directamente: python api_request.py
    # probará Ethereum con la fecha de ayer
    yesterday = (datetime.now().date()).strftime("%d-%m-%Y")
    data = fetch_coin_history("ethereum", yesterday)
    print("💰 Current price USD:", data["market_data"]["current_price"]["usd"])

