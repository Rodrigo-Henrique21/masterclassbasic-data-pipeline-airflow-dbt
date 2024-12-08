
  
    

  create  table "airflow"."public_bronze"."financial_data__dbt_tmp"
  
  
    as
  
  (
    -- Modelo Bronze
select
    *
from public.bronze_financial_data
  );
  