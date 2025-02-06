-- models/reporting/report_quantidade_assuntos.sql

{{ config(
    schema='gold',
    materialized='table'
) }}

with quantidade_assuntos as (
    select 
    assunto,
    count(id) as total
    from 
        {{ ref('stg_dim_assunto') }}
    group by 1
)
select *
from quantidade_assuntos