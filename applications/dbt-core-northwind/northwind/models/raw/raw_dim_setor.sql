-- models/raw/raw_dim_setor.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_setor') }}  -- Acessando a tabela de produção