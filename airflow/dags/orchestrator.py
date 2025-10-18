# airflow/dags/orchestrator.py

from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowException
from docker.types import Mount

# ----------------------------------------------------------------------
# Helpers para Variables (compat SDK vs models) y ENV
# ----------------------------------------------------------------------
def _sdk_var_get_or_default(key: str, default_str: str | None):
    """Usa airflow.sdk.Variable si existe; si falta o no está seteada, retorna default_str."""
    try:
        from airflow.sdk import Variable as SDKVariable  # Airflow ≥ 3
        try:
            val = SDKVariable.get(key)  # no admite default_var
            return val if val is not None else default_str
        except Exception:
            return default_str
    except Exception:
        # Fallback a Airflow 2.x
        from airflow.models import Variable as ModelsVariable  # deprecado pero válido
        return ModelsVariable.get(key, default_var=default_str)

def get_coin_ids() -> list[str]:
    """
    Prioridad:
      1) ENV COINGECKO_COIN_IDS (JSON o CSV)
      2) Variable Airflow 'coingecko_coin_ids' (string JSON)
      3) ['ethereum'] por defecto
    """
    env_val = os.environ.get("COINGECKO_COIN_IDS")
    if env_val:
        # admite JSON o CSV
        try:
            return json.loads(env_val)
        except Exception:
            return [s.strip() for s in env_val.split(",") if s.strip()]

    var_str = _sdk_var_get_or_default("coingecko_coin_ids", default_str='["ethereum"]')
    try:
        return json.loads(var_str)
    except Exception:
        return ["ethereum"]

# ----------------------------------------------------------------------
# Entorno requerido
# ----------------------------------------------------------------------
HOST_PROJECT_PATH = os.environ.get("HOST_PROJECT_PATH")
if not HOST_PROJECT_PATH:
    raise ValueError("HOST_PROJECT_PATH environment variable is not set!")

COINGECKO_API_BASE = os.environ.get("COINGECKO_API_BASE", "https://api.coingecko.com")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY")  # puede ser None si usas conexión HTTP

# ----------------------------------------------------------------------
# Tareas
# ----------------------------------------------------------------------
def ensure_tables(**_):
    ddl = """
    CREATE TABLE IF NOT EXISTS coin_daily_prices (
      coin_id   TEXT NOT NULL,
      at_date   DATE NOT NULL,
      price_usd DOUBLE PRECISION,
      payload   JSONB NOT NULL,
      PRIMARY KEY (coin_id, at_date)
    );

    CREATE INDEX IF NOT EXISTS ix_coin_daily_prices_at_date
      ON coin_daily_prices (at_date);

    CREATE TABLE IF NOT EXISTS coin_monthly_agg (
      coin_id    TEXT NOT NULL,
      year_month DATE NOT NULL,
      max_price  DOUBLE PRECISION,
      min_price  DOUBLE PRECISION,
      PRIMARY KEY (coin_id, year_month)
    );
    """
    PostgresHook(postgres_conn_id="postgres_default").run(ddl)

def fetch_and_upsert(execution_date: str, **_):
    """
    Para cada coin_id, llama /history del día execution_date y upsertea en coin_daily_prices.
    Incluye manejo de rate limit (429) y fallback a requests si el HttpHook omitiera params.
    """
    from time import sleep
    import requests

    pg = PostgresHook(postgres_conn_id="postgres_default")
    coin_ids = get_coin_ids()

    dt = datetime.fromisoformat(execution_date).date()   # 'YYYY-MM-DD'
    cg_date = dt.strftime("%d-%m-%Y")                    # dd-mm-YYYY para CoinGecko

    # Intentar conexión HTTP 'COINGECKO' (headers en extra)
    try:
        http = HttpHook(method="GET", http_conn_id="COINGECKO")
    except Exception:
        http = None

    upsert_sql = """
    INSERT INTO coin_daily_prices (coin_id, at_date, price_usd, payload)
    VALUES (%(coin_id)s, %(at_date)s, %(price_usd)s, %(payload)s::jsonb)
    ON CONFLICT (coin_id, at_date) DO UPDATE
      SET price_usd = EXCLUDED.price_usd,
          payload   = EXCLUDED.payload;
    """

    # Fallback headers si no hay conexión HTTP
    fallback_headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else None

    def _call_api(coin: str) -> dict:
        """Llama CoinGecko /history con reintentos tolerando 429 y 422 por params ausentes."""
        max_tries = 5
        backoff = 2.0
        use_http = http is not None
        for attempt in range(1, max_tries + 1):
            try:
                if use_http:
                    # Para GET en este provider, pasa SIEMPRE los params en data=
                    resp = http.run(
                        endpoint=f"/api/v3/coins/{coin}/history",
                        data={"date": cg_date, "localization": "false"},
                        extra_options={"timeout": 60},
                    )
                    if resp.status_code == 422 and "Missing parameter date" in resp.text:
                        # Alguna variante del provider ignoró params -> forzamos fallback
                        use_http = False
                        continue
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After")
                        wait = float(ra) if ra else backoff ** attempt
                        sleep(wait)
                        continue
                    # HttpHook ya lanza para >=400 en run_and_check, pero por si acaso:
                    resp.raise_for_status()
                    return resp.json()
                else:
                    url = f"{COINGECKO_API_BASE}/api/v3/coins/{coin}/history"
                    r = requests.get(
                        url,
                        params={"date": cg_date, "localization": "false"},
                        headers=fallback_headers,
                        timeout=60,
                    )
                    if r.status_code == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra else backoff ** attempt
                        sleep(wait)
                        continue
                    r.raise_for_status()
                    return r.json()
            except Exception as e:
                msg = str(e)
                # Reintenta en 429 o errores transitorios
                if ("429" in msg or "Too Many Requests" in msg) and attempt < max_tries:
                    sleep(backoff ** attempt)
                    continue
                # Primer 422 desde HttpHook: ya hicimos fallback, reintenta
                if use_http and "422" in msg and attempt < max_tries:
                    use_http = False
                    continue
                # Propaga el error si agotamos reintentos o es otro tipo
                if attempt >= max_tries:
                    raise
                sleep(backoff ** attempt)
        raise AirflowException(f"Failed to fetch {coin} for {cg_date} after retries")

    for idx, coin in enumerate(coin_ids):
        data = _call_api(coin)

        # Extraer precio USD (si falta, salta el día)
        try:
            price_usd = float(data["market_data"]["current_price"]["usd"])
        except Exception:
            continue

        pg.run(
            upsert_sql,
            parameters={
                "coin_id": coin,
                "at_date": dt.isoformat(),
                "price_usd": price_usd,
                "payload": json.dumps(data),
            },
        )

        # Pausa corta entre coins para evitar rate limit
        if idx < len(coin_ids) - 1:
            sleep(1.5)

def refresh_monthly_agg(**_):
    sql = """
    INSERT INTO coin_monthly_agg (coin_id, year_month, max_price, min_price)
    SELECT
      coin_id,
      date_trunc('month', at_date)::date as year_month,
      MAX(price_usd) as max_price,
      MIN(price_usd) as min_price
    FROM coin_daily_prices
    GROUP BY coin_id, date_trunc('month', at_date)
    ON CONFLICT (coin_id, year_month) DO UPDATE
      SET max_price = EXCLUDED.max_price,
          min_price = EXCLUDED.min_price;
    """
    PostgresHook(postgres_conn_id="postgres_default").run(sql)

# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    dag_id="crypto-coingecko-dbt-orchestrator",
    default_args=default_args,
    description="Ingesta diaria CoinGecko + modelos dbt",
    start_date=datetime(2024, 12, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["crypto", "coingecko", "dbt"],
)

with dag:
    t0 = PythonOperator(
        task_id="ensure_tables",
        python_callable=ensure_tables,
    )

    t1 = PythonOperator(
        task_id="ingest_coingecko_day",
        python_callable=fetch_and_upsert,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    t2 = PythonOperator(
        task_id="refresh_monthly_agg",
        python_callable=refresh_monthly_agg,
    )

    t3 = DockerOperator(
        task_id="dbt_run_models",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_PATH}/dbt/my_project",
                target="/usr/app",
                type="bind",
                read_only=False,
            ),
            Mount(
                source=f"{HOST_PROJECT_PATH}/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
                read_only=True,
            ),
        ],
        network_mode=os.environ.get("AIRFLOW_DOCKER_NETWORK", "coingecko-project"),
        docker_url="unix:///var/run/docker.sock",
        environment={"DBT_PROFILES_DIR": "/root/.dbt"},
        auto_remove="success",   # provider docker actual espera: 'never' | 'success' | 'force'
        do_xcom_push=False,
        mount_tmp_dir=False,
    )

    t0 >> t1 >> t2 >> t3