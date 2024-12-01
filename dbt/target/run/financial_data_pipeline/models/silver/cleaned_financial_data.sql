
      
  
    

  create  table "airflow"."public_silver"."cleaned_financial_data"
  
  
    as
  
  (
    -- Modelo da camada Silver
with raw_data as (
    select
        *,
        cast(price as numeric(10, 2)) as price_numeric,
        cast(date as date) as transaction_date
    from public.bronze_financial_data
),
cleaned_data as (
    select
        ticker,
        price_numeric as price,
        transaction_date as date
    from raw_data
    where price_numeric > 0  -- Exclui valores de preço inválidos
)

select
    *
from cleaned_data
  );
  
  