-- models/raw/raw_dim_colaboradores.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_colaboradores') }}  -- Acessando a tabela de produção