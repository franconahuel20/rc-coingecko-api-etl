import pandas as pd

def transform_coin_history(data: dict, coin_id: str):
    """
    Transform CoinGecko /history JSON into a clean one-row DataFrame.

    Parameters
    ----------
    data : dict
        Full JSON returned by CoinGecko /api/v3/coins/{id}/history
    coin_id : str
        The coin ID (e.g. 'bitcoin', 'ethereum')

    Returns
    -------
    pd.DataFrame
        DataFrame with normalized fields:
        [coin_id, date, price_usd, market_cap_usd, total_volume_usd, symbol, name]
    """
    if not data or "market_data" not in data:
        raise ValueError(f"Invalid API response for {coin_id}: missing 'market_data'")

    # Extract safe values
    market_data = data.get("market_data", {})
    price_usd = market_data.get("current_price", {}).get("usd")
    market_cap_usd = market_data.get("market_cap", {}).get("usd")
    total_volume_usd = market_data.get("total_volume", {}).get("usd")

    # Some metadata
    name = data.get("name", "")
    symbol = data.get("symbol", "")
    inserted_at = pd.Timestamp.now()

    df = pd.DataFrame([{
        "coin_id": coin_id,
        "symbol": symbol,
        "name": name,
        "price_usd": price_usd,
        "market_cap_usd": market_cap_usd,
        "total_volume_usd": total_volume_usd,
        "inserted_at": inserted_at,
        "payload": data,   # optional: keep raw JSON
    }])

    print(f"✅ Transformed historical data for {coin_id} (price_usd={price_usd})")
    return df


# ---------------------------------------------------------------------
# Test rápido (solo para desarrollo local)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    from api_request import fetch_coin_history
    from datetime import datetime

    cg_date = datetime.now().strftime("%d-%m-%Y")
    data = fetch_coin_history("ethereum", cg_date)
    df = transform_coin_history(data, "ethereum")
    print(df.head())
