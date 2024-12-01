
  
    

  create  table "airflow"."public_gold"."aggregated_financial_data__dbt_tmp"
  
  
    as
  
  (
    -- Modelo da camada Gold
with daily_aggregates as (
    select
        date as transaction_date,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        count(ticker) as total_transactions
    from public_silver.cleaned_financial_data
    group by date
)

select
    *
from daily_aggregates
order by transaction_date
  );
  