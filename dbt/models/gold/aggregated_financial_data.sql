select
    trade_date,
    avg(price) as avg_price,
    sum(volume) as total_volume
from {{ ref('cleaned_financial_data') }}
group by trade_date
