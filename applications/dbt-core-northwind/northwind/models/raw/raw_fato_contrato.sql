-- models/raw/raw_dim_contrato.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_contrato') }}  -- Acessando a tabela de produção