{{ config(
    materialized='table'
) }}

-- ================================================
-- Model: daily_prices
-- Description: Creates a clean, analytics-friendly
-- table of daily cryptocurrency prices from CoinGecko
-- ================================================

select
    coin_id,
    at_date as date,
    round(avg(price_usd)::numeric, 2) as avg_price_usd,
    round(min(price_usd)::numeric, 2) as min_price_usd,
    round(max(price_usd)::numeric, 2) as max_price_usd,
    count(*) as records_count
from {{ ref('stg_coin_daily_prices') }}
group by
    coin_id,
    at_date
order by
    coin_id,
    at_date
