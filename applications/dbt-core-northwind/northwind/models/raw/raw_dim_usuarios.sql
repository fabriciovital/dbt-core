-- models/raw/raw_dim_usuarios.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_usuarios') }}  -- Acessando a tabela de produção