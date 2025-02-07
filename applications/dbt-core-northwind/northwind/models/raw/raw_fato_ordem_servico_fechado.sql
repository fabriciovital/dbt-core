-- models/raw/raw_ordem_servico_fechado.sql

SELECT *
FROM {{ source('bronze', 'bronze_ordem_servico_fechado') }}  -- Acessando a tabela de produção