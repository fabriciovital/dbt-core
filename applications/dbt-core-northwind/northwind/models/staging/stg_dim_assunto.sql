-- models/staging/stg_customers.sql

select
    id,
    assunto,
    last_update,
    month_key
from
    {{ ref('raw_dim_assunto') }}