select
    *
from {{ source('bronze', 'financial_data') }}