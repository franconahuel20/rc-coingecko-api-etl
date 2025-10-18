import os
import json
import psycopg2
from datetime import datetime, date
from typing import List, Dict, Any

# Importa la función que ya adaptaste en api_request.py
from api_request import fetch_coin_history

# ---------------------------------------------------------------------
# Config DB (permite override por variables de entorno)
# ---------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "db"),
    "user": os.getenv("DB_USER", "db_user"),
    "password": os.getenv("DB_PASSWORD", "db_password"),
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def connect_to_db():
    """Create a connection to PostgreSQL"""
    print("🔌 Connecting to PostgreSQL database...")
    return psycopg2.connect(**DB_CONFIG)

def ensure_tables(cursor):
    """Create crypto tables if they don't exist"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coin_daily_prices (
          coin_id   TEXT NOT NULL,
          at_date   DATE NOT NULL,
          price_usd DOUBLE PRECISION NOT NULL,
          payload   JSONB NOT NULL,
          PRIMARY KEY (coin_id, at_date)
        );

        CREATE INDEX IF NOT EXISTS ix_coin_daily_prices_month
          ON coin_daily_prices (coin_id, date_trunc('month', at_date));

        CREATE TABLE IF NOT EXISTS coin_monthly_agg (
          coin_id    TEXT NOT NULL,
          year_month DATE NOT NULL,
          max_price  DOUBLE PRECISION NOT NULL,
          min_price  DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (coin_id, year_month)
        );
    """)
    print("✅ Tables ensured (created if not exists)")

def to_cg_date(d: date) -> str:
    """CoinGecko /history exige DD-MM-YYYY"""
    return d.strftime("%d-%m-%Y")

def parse_execution_date() -> date:
    """
    Toma la fecha desde variables que Airflow inyecta:
    - AIRFLOW_CTX_EXECUTION_DATE (ISO)
    - EXECUTION_DATE (YYYY-MM-DD)
    Si no están, usa hoy.
    """
    ctx = os.getenv("AIRFLOW_CTX_EXECUTION_DATE") or os.getenv("EXECUTION_DATE")
    if ctx:
        try:
            # Airflow suele pasar ISO con tiempo -> '2025-01-01T00:00:00+00:00' o '2025-01-01'
            return datetime.fromisoformat(ctx).date()
        except Exception:
            # fallback a YYYY-MM-DD
            return datetime.strptime(ctx[:10], "%Y-%m-%d").date()
    return date.today()

def get_coin_ids() -> List[str]:
    """
    Lee COINGECKO_COIN_IDS como JSON (ej. '["ethereum","bitcoin"]').
    Si no existe, usa ["ethereum"].
    """
    raw = os.getenv("COINGECKO_COIN_IDS", '["ethereum"]')
    try:
        return json.loads(raw)
    except Exception:
        # Permitir formato simple separado por comas: "ethereum,bitcoin"
        return [c.strip() for c in raw.split(",") if c.strip()]

def upsert_daily_price(cursor, row: Dict[str, Any]):
    """
    UPSERT en coin_daily_prices.
    row = {
      "coin_id": str,
      "at_date": date,
      "price_usd": float,
      "payload": dict
    }
    """
    cursor.execute(
        """
        INSERT INTO coin_daily_prices (coin_id, at_date, price_usd, payload)
        VALUES (%(coin_id)s, %(at_date)s, %(price_usd)s, %(payload)s::jsonb)
        ON CONFLICT (coin_id, at_date) DO UPDATE
          SET price_usd = EXCLUDED.price_usd,
              payload   = EXCLUDED.payload;
        """,
        {
            "coin_id": row["coin_id"],
            "at_date": row["at_date"].isoformat(),
            "price_usd": row["price_usd"],
            "payload": json.dumps(row["payload"]),
        },
    )

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    exec_date = parse_execution_date()
    cg_date = to_cg_date(exec_date)
    coin_ids = get_coin_ids()

    print(f"🚀 Ingesting CoinGecko /history for {coin_ids} on {cg_date} (exec_date={exec_date})")

    # 1) Fetch + transform mínimo para cada coin
    rows_to_upsert: List[Dict[str, Any]] = []
    for coin in coin_ids:
        try:
            payload = fetch_coin_history(coin_id=coin, date_str=cg_date)
            # Extraer price_usd con guardas
            price_usd = None
            try:
                price_usd = float(payload["market_data"]["current_price"]["usd"])
            except Exception:
                print(f"⚠️  No USD price for {coin} on {cg_date}; skipping.")
                continue

            rows_to_upsert.append(
                {
                    "coin_id": coin,
                    "at_date": exec_date,
                    "price_usd": price_usd,
                    "payload": payload,
                }
            )
            print(f"✅ {coin}: price_usd={price_usd}")

        except Exception as e:
            print(f"❌ Error fetching {coin} on {cg_date}: {e}")

    if not rows_to_upsert:
        print("ℹ️ Nothing to upsert.")
        return

    # 2) Insert/Upsert en Postgres
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            ensure_tables(cur)
            for row in rows_to_upsert:
                upsert_daily_price(cur, row)
        conn.commit()

    print("🎉 Data successfully upserted into coin_daily_prices!")

if __name__ == "__main__":
    main()