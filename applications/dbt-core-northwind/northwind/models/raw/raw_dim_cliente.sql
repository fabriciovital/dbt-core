-- models/raw/raw_dim_cliente.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_cliente') }}  -- Acessando a tabela de produção