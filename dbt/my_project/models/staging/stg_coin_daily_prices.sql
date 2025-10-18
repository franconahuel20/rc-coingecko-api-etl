{{ config(
    materialized='view'
) }}

-- Staging de precios diarios desde CoinGecko
-- Fuente: coin_daily_prices (ingesta diaria con payload JSONB completo)

with src as (
    select *
    from {{ source('crypto','coin_daily_prices') }}
)

select
    coin_id,
    at_date::date                                              as at_date,
    price_usd::numeric                                         as price_usd,

    -- Metadatos útiles desde el payload (opcionales para exploración)
    (payload ->> 'name')                                       as name,
    (payload ->> 'symbol')                                     as symbol,
    (payload -> 'image' ->> 'thumb')                           as image_thumb_url,
    (payload -> 'image' ->> 'small')                           as image_small_url,

    -- Derivados frecuentes
    ((payload -> 'market_data' -> 'current_price' ->> 'usd'))::numeric  as current_price_usd,
    ((payload -> 'market_data' -> 'market_cap'     ->> 'usd'))::numeric  as market_cap_usd,
    ((payload -> 'market_data' -> 'total_volume'   ->> 'usd'))::numeric  as total_volume_usd,

    payload
from src