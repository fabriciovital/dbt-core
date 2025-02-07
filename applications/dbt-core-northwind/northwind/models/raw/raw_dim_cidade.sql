-- models/raw/raw_dim_cidade.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_cidade') }}  -- Acessando a tabela de produção