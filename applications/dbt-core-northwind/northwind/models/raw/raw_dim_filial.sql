-- models/raw/raw_dim_filial.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_filial') }}  -- Acessando a tabela de produção