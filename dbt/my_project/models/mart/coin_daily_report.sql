{{ config(
    materialized='table'
) }}

-- ======================================================
-- Model: coin_daily_report
-- Aplana coin_daily_prices + payload JSON de CoinGecko
-- para analytics y dashboards (Superset).
-- ======================================================

with src as (
  select
    coin_id,
    at_date::date                             as at_date,
    price_usd::numeric                        as price_usd,

    -- Metadatos principales
    (payload ->> 'name')                      as name,
    (payload ->> 'symbol')                    as symbol,
    (payload -> 'image' ->> 'thumb')          as image_thumb_url,
    (payload -> 'image' ->> 'small')          as image_small_url,

    -- market_data -> current_price / market_cap / total_volume (USD)
    ((payload -> 'market_data' -> 'current_price' ->> 'usd'))::numeric      as current_price_usd,
    ((payload -> 'market_data' -> 'market_cap'     ->> 'usd'))::numeric      as market_cap_usd,
    ((payload -> 'market_data' -> 'total_volume'   ->> 'usd'))::numeric      as total_volume_usd,

    -- Derivadas útiles
    date_trunc('month', at_date)::date        as year_month,
    to_char(at_date, 'YYYY-MM-DD')            as at_date_str
  from {{ source('crypto','coin_daily_prices') }}
)

select
  coin_id,
  symbol,
  name,
  at_date,
  year_month,
  -- Precio base que guardamos + redundante “current_price_usd” del payload
  round(price_usd, 2)                         as price_usd,
  round(current_price_usd, 2)                 as current_price_usd,
  round(market_cap_usd, 2)                    as market_cap_usd,
  round(total_volume_usd, 2)                  as total_volume_usd,
  image_thumb_url,
  image_small_url
from src