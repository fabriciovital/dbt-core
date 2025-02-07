-- models/raw/raw_ordem_servico_aberto.sql

SELECT *
FROM {{ source('bronze', 'bronze_ordem_servico_aberto') }}  -- Acessando a tabela de produção