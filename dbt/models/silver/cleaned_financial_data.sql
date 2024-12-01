select
    ticker,
    cast(price as decimal(10,2)) as price,
    volume,
    cast(date as date) as trade_date
from {{ ref('financial_data') }}
where price > 0
