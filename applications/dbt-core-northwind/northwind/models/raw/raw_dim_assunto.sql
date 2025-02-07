-- models/raw/raw_dim_assunto.sql

SELECT *
FROM {{ source('bronze', 'bronze_dim_assunto') }}  -- Acessando a tabela de produção