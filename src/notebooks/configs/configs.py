lake_path = {
    "landing": "s3a://landing/isp_performance/",
    "bronze": "s3a://bronze/isp_performance/",
    "silver": "s3a://silver/isp_performance/",
    "gold": "s3a://gold/isp_performance/",
}

prefix_layer_name = {"0": "landing_", "1": "bronze_", "2": "silver_", "3": "gold_"}

# ************************
# Start Landing Parquet from API Situacionais
# ************************
tables_landing = {
    "1": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/filial",
    "2": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/colaboradores",
    "3": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/assunto",
    "4": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/setor",
    "5": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/usuarios",
    "6": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/ordem-servico/aberto",
    "7": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/ordem-servico/fechado",
}

# ************************
# Start Bronze Tables Situacionais
# ************************
tables_api_isp_performance = {
    "1": "dim_filial",
    "2": "dim_colaboradores",
    "3": "dim_assunto",
    "4": "dim_setor",
    "5": "dim_usuarios",
    "6": "ordem_servico_aberto",
    "7": "ordem_servico_fechado",
}

# ************************
# Start Silver Tables Situacionais
# ************************
tables_silver = {
    # Dimensao Filial
    "dim_filial": f"""
SELECT 
    id,
    fantasia,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_filial`
    """,
    # Dimensao Colaboradores
    "dim_colaboradores": f"""
SELECT 
     id,
     funcionario,
     last_update,
     month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_colaboradores`
    """,
    # Dimensao Assunto
    "dim_assunto": f"""
SELECT 
    id,
    assunto,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_assunto`
    """,
        # Dimensao Setor
    "dim_setor": f"""
SELECT 
    id,
    setor,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_setor`
    """,
        # Dimensao Usuarios
    "dim_usuarios": f"""
SELECT 
    id,
    upper(login) login,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_usuarios`
    """,
        # Ordem Serviço
    "ordem_servico": f"""
SELECT
    mensagem_resposta,
    data_hora_analise,
    data_hora_encaminhado,
    data_hora_assumido,
    data_hora_execucao,
    id_contrato_kit,
    preview,
    data_agenda_final,
    id,
    tipo,
    id_filial,
    id_wfl_tarefa,
    status_sla,
    CAST(data_abertura AS TIMESTAMP) AS data_abertura,
    YEAR(data_abertura) AS ano_abertura,
    DATE_FORMAT(data_abertura, 'yyyy-MM') AS ano_mes_abertura,
    MONTH(data_abertura) AS mes_abertura,
    QUARTER(data_abertura) AS trimestre_abertura,
    WEEKOFYEAR(data_abertura) AS semana_do_ano_abertura,
    FLOOR((DAY(data_abertura) - 1) / 7) + 1 AS semana_do_mes_abertura,
    DAYOFWEEK(data_abertura) AS dia_da_semana_abertura,
    DAY(data_abertura) AS dia_do_mes_abertura,
    HOUR(data_abertura) AS hora_abertura,
    CASE 
        WHEN HOUR(data_abertura) < 6 THEN 'Madrugada'
        WHEN HOUR(data_abertura) < 12 THEN 'Manhã'
        WHEN HOUR(data_abertura) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_abertura,
    melhor_horario_agenda,
    liberado,
    status id_status,
    CASE 
        WHEN status = 'RAG' THEN 'Reagendada'
        WHEN status = 'EN' THEN 'Encaminhada'
        WHEN status = 'AS' THEN 'Assumida'
        WHEN status = 'DS' THEN 'Deslocamento'
        WHEN status = 'AG' THEN 'Agendada'
        WHEN status = 'A' THEN 'Aberta'
        WHEN status = 'EX' THEN 'Em Execução'
        ELSE 'Fechada'
    END AS status,
    id_cliente,
    id_assunto,
    setor as id_setor,
    id_cidade,
    id_tecnico,
    prioridade,
    mensagem,
    protocolo,
    endereco,
    complemento,
    id_condominio,
    bloco,
    apartamento,
    latitude,
    bairro,
    longitude,
    referencia,
    impresso,
    data_inicio,
    data_agenda,
    data_final,
    CAST(data_fechamento AS TIMESTAMP) AS data_fechamento,
    YEAR(data_fechamento) AS ano_fechamento,
    DATE_FORMAT(data_fechamento, 'yyyy-MM') AS ano_mes_fechamento,
    MONTH(data_fechamento) AS mes_fechamento,
    QUARTER(data_fechamento) AS trimestre_fechamento,
    WEEKOFYEAR(data_fechamento) AS semana_do_ano,
    FLOOR((DAY(data_fechamento) - 1) / 7) + 1 AS semana_do_mes_fechamento,
    DAYOFWEEK(data_fechamento) AS dia_da_semana_fechamento,
    DAY(data_fechamento) AS dia_do_mes_fechamento,
    HOUR(data_fechamento) AS hora_fechamento,
    CASE 
        WHEN HOUR(data_fechamento) < 6 THEN 'Madrugada'
        WHEN HOUR(data_fechamento) < 12 THEN 'Manhã'
        WHEN HOUR(data_fechamento) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_fechamento,
    id_wfl_param_os,
    valor_total_comissao,
    valor_total,
    valor_outras_despesas,
    idx,
    id_su_diagnostico,
    gera_comissao,
    id_estrutura,
    id_login,
    valor_unit_comissao,
    data_prazo_limite,
    data_reservada,
    id_ticket,
    origem_endereco,
    justificativa_sla_atrasado,
    origem_endereco_estrutura,
    data_reagendar,
    data_prev_final,
    origem_cadastro,
    ultima_atualizacao,
    last_update,
    DATE_FORMAT(data_abertura, 'yyyyMM') AS month_key
FROM
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}ordem_servico_aberto`
UNION
SELECT 
    mensagem_resposta,
    data_hora_analise,
    data_hora_encaminhado,
    data_hora_assumido,
    data_hora_execucao,
    id_contrato_kit,
    preview,
    data_agenda_final,
    id,
    tipo,
    id_filial,
    id_wfl_tarefa,
    status_sla,
    CAST(data_abertura AS TIMESTAMP) AS data_abertura,
    YEAR(data_abertura) AS ano_abertura,
    DATE_FORMAT(data_abertura, 'yyyy-MM') AS ano_mes_abertura,
    MONTH(data_abertura) AS mes_abertura,
    QUARTER(data_abertura) AS trimestre_abertura,
    WEEKOFYEAR(data_abertura) AS semana_do_ano_abertura,
    FLOOR((DAY(data_abertura) - 1) / 7) + 1 AS semana_do_mes_abertura,
    DAYOFWEEK(data_abertura) AS dia_da_semana_abertura,
    DAY(data_abertura) AS dia_do_mes_abertura,
    HOUR(data_abertura) AS hora_abertura,
    CASE 
        WHEN HOUR(data_abertura) < 6 THEN 'Madrugada'
        WHEN HOUR(data_abertura) < 12 THEN 'Manhã'
        WHEN HOUR(data_abertura) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_abertura,
    melhor_horario_agenda,
    liberado,
    status id_status,
    CASE 
        WHEN status = 'RAG' THEN 'Reagendada'
        WHEN status = 'EN' THEN 'Encaminhada'
        WHEN status = 'AS' THEN 'Assumida'
        WHEN status = 'DS' THEN 'Deslocamento'
        WHEN status = 'AG' THEN 'Agendada'
        WHEN status = 'A' THEN 'Aberta'
        WHEN status = 'EX' THEN 'Em Execução'
        ELSE 'Fechada'
    END AS status,
    id_cliente,
    id_assunto,
    setor as id_setor,
    id_cidade,
    id_tecnico,
    prioridade,
    mensagem,
    protocolo,
    endereco,
    complemento,
    id_condominio,
    bloco,
    apartamento,
    latitude,
    bairro,
    longitude,
    referencia,
    impresso,
    data_inicio,
    data_agenda,
    data_final,
    CAST(data_fechamento AS TIMESTAMP) AS data_fechamento,
    YEAR(data_fechamento) AS ano_fechamento,
    DATE_FORMAT(data_fechamento, 'yyyy-MM') AS ano_mes_fechamento,
    MONTH(data_fechamento) AS mes_fechamento,
    QUARTER(data_fechamento) AS trimestre_fechamento,
    WEEKOFYEAR(data_fechamento) AS semana_do_ano,
    FLOOR((DAY(data_fechamento) - 1) / 7) + 1 AS semana_do_mes_fechamento,
    DAYOFWEEK(data_fechamento) AS dia_da_semana_fechamento,
    DAY(data_fechamento) AS dia_do_mes_fechamento,
    HOUR(data_fechamento) AS hora_fechamento,
    CASE 
        WHEN HOUR(data_fechamento) < 6 THEN 'Madrugada'
        WHEN HOUR(data_fechamento) < 12 THEN 'Manhã'
        WHEN HOUR(data_fechamento) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_fechamento,
    id_wfl_param_os,
    valor_total_comissao,
    valor_total,
    valor_outras_despesas,
    idx,
    id_su_diagnostico,
    gera_comissao,
    id_estrutura,
    id_login,
    valor_unit_comissao,
    data_prazo_limite,
    data_reservada,
    id_ticket,
    origem_endereco,
    justificativa_sla_atrasado,
    origem_endereco_estrutura,
    data_reagendar,
    data_prev_final,
    origem_cadastro,
    ultima_atualizacao,
    last_update,
    DATE_FORMAT(data_fechamento, 'yyyyMM') AS month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}ordem_servico_fechado`
""",
}


# ************************
# Start Gold Tables
# ************************
tables_gold = {
    # Ordem Serviço Aberto
    "performance_ordem_servico": """
WITH BASE_PERFORMANCE AS (
    SELECT
        t1.ano_abertura,
        t1.ano_mes_abertura,
        t1.data_abertura,
        t1.ano_fechamento,
        t1.ano_mes_fechamento,
        t1.data_fechamento,
        t2.id AS id_filial,
        t2.fantasia AS filial,
        t4.id AS id_setor,
        t4.setor AS setor,
        t6.id AS id_relator,
        t6.login AS relator,
        t5.id AS id_tecnico,
        t5.funcionario AS tecnico,
        t3.id AS id_assunto,
        t3.assunto AS assunto,
        t1.id AS ordem_servico_id,
        t1.id_status,
        t1.status
    FROM
        delta.silver.silver_ordem_servico t1
    LEFT JOIN delta.silver.silver_dim_filial t2 ON t2.id = t1.id_filial
    LEFT JOIN delta.silver.silver_dim_assunto t3 ON t3.id = t1.id_assunto
    LEFT JOIN delta.silver.silver_dim_setor t4 ON t4.id = t1.id_setor
    LEFT JOIN delta.silver.silver_dim_colaboradores t5 ON t5.id = t1.id_tecnico
    LEFT JOIN delta.silver.silver_dim_usuarios t6 ON t6.id = t1.id_login
    ORDER BY
        t1.ano_mes_abertura,
        t2.id
),
STATUS_COUNTS AS (
    SELECT
        ano_abertura,
        ano_mes_abertura,
        ano_fechamento,
        ano_mes_fechamento,
        id_filial,
        filial,
        id_setor,
        setor,
        id_assunto,
        assunto,
        id_relator,
	    relator,
	    id_tecnico,
	    tecnico,
        id_status,
        status,
        data_abertura,
        data_fechamento,
        COUNT(ordem_servico_id) AS qtd
    FROM BASE_PERFORMANCE
    GROUP BY
        ano_abertura,
        ano_mes_abertura,
        ano_fechamento,
        ano_mes_fechamento,
        id_filial,
        filial,
        id_setor,
        setor,
        id_assunto,
        assunto,
        id_relator,
	    relator,
	    id_tecnico,
	    tecnico,
        id_status,
        status,
        data_abertura,
        data_fechamento
)
SELECT
    ano_abertura,
    ano_mes_abertura,
    ano_fechamento,
    ano_mes_fechamento,
    id_filial,
    filial,
    id_setor,
    setor,
    id_assunto,
    assunto,
    id_relator,
    relator,
    id_tecnico,
    tecnico,
    SUM(qtd) AS qtd_total,
    SUM(CASE WHEN status = 'Reagendada' THEN qtd ELSE 0 END) AS qtd_reagendada,
    SUM(CASE WHEN status = 'Encaminhada' THEN qtd ELSE 0 END) AS qtd_encaminhada,
    SUM(CASE WHEN status = 'Assumida' THEN qtd ELSE 0 END) AS qtd_assumida,
    SUM(CASE WHEN status = 'Deslocamento' THEN qtd ELSE 0 END) AS qtd_deslocamento,
    SUM(CASE WHEN status = 'Agendada' THEN qtd ELSE 0 END) AS qtd_agendada,
    SUM(CASE WHEN status = 'Aberta' THEN qtd ELSE 0 END) AS qtd_aberta,
    SUM(CASE WHEN status = 'Em Execução' THEN qtd ELSE 0 END) AS qtd_execucao,
    SUM(CASE WHEN status = 'Fechada' THEN qtd ELSE 0 END) AS qtd_fechada,
    AVG(CAST(date_diff('second', data_abertura, data_fechamento) AS DECIMAL)) AS tempo_medio_fechamento_segundos
FROM STATUS_COUNTS
GROUP BY 
    ano_abertura,
    ano_mes_abertura,
    ano_fechamento,
    ano_mes_fechamento,
    id_filial,
    filial,
    id_setor,
    setor,
    id_assunto,
    assunto,
    id_relator,
    relator,
    id_tecnico,
    tecnico;
    """,
}


# ************************
# Start Landing Parquet from API Produtividade
# ************************
tables_landing_produtividade = {
    "1": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/ordem-servico/fechado",
}

# ************************
# Start Bronze Tables Produtividade
# ************************
tables_api_isp_performance_produtividade = {
    "1": "ordem_servico_fechado",
}

# ************************
# Start Silver Tables Produtividade
# ************************
tables_silver_produtividade = {
    # Ordem Serviço Fechado
    "ordem_servico_fechado": f"""
SELECT 
    mensagem_resposta,
    data_hora_analise,
    data_hora_encaminhado,
    data_hora_assumido,
    data_hora_execucao,
    id_contrato_kit,
    preview,
    data_agenda_final,
    id,
    tipo,
    id_filial,
    id_wfl_tarefa,
    status_sla,
    data_abertura,
    YEAR(data_abertura) AS ano_abertura,
    DATE_FORMAT(data_abertura, 'yyyy-MM') AS ano_mes_abertura,
    MONTH(data_abertura) AS mes_abertura,
    QUARTER(data_abertura) AS trimestre_abertura,
    WEEKOFYEAR(data_abertura) AS semana_do_ano_abertura,
    FLOOR((DAY(data_abertura) - 1) / 7) + 1 AS semana_do_mes_abertura,
    DAYOFWEEK(data_abertura) AS dia_da_semana_abertura,
    DAY(data_abertura) AS dia_do_mes_abertura,
    HOUR(data_abertura) AS hora_abertura,
    CASE 
        WHEN HOUR(data_abertura) < 6 THEN 'Madrugada'
        WHEN HOUR(data_abertura) < 12 THEN 'Manhã'
        WHEN HOUR(data_abertura) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_abertura,
    melhor_horario_agenda,
    liberado,
    status id_status,
    CASE 
        WHEN status = 'RAG' THEN 'Reagendada'
        WHEN status = 'EN' THEN 'Encaminhada'
        WHEN status = 'AS' THEN 'Assumida'
        WHEN status = 'DS' THEN 'Deslocamento'
        WHEN status = 'AG' THEN 'Agendada'
        WHEN status = 'A' THEN 'Aberta'
        WHEN status = 'EX' THEN 'Em Execução'
        ELSE 'Fechada'
    END AS status,
    id_cliente,
    id_assunto,
    setor as id_setor,
    id_cidade,
    id_tecnico,
    prioridade,
    mensagem,
    protocolo,
    endereco,
    complemento,
    id_condominio,
    bloco,
    apartamento,
    latitude,
    bairro,
    longitude,
    referencia,
    impresso,
    data_inicio,
    data_agenda,
    data_final,
    data_fechamento,
    YEAR(data_fechamento) AS ano_fechamento,
    DATE_FORMAT(data_fechamento, 'yyyy-MM') AS ano_mes_fechamento,
    MONTH(data_fechamento) AS mes_fechamento,
    QUARTER(data_fechamento) AS trimestre_fechamento,
    WEEKOFYEAR(data_fechamento) AS semana_do_ano,
    FLOOR((DAY(data_fechamento) - 1) / 7) + 1 AS semana_do_mes_fechamento,
    DAYOFWEEK(data_fechamento) AS dia_da_semana_fechamento,
    DAY(data_fechamento) AS dia_do_mes_fechamento,
    HOUR(data_fechamento) AS hora_fechamento,
    CASE 
        WHEN HOUR(data_fechamento) < 6 THEN 'Madrugada'
        WHEN HOUR(data_fechamento) < 12 THEN 'Manhã'
        WHEN HOUR(data_fechamento) < 18 THEN 'Tarde'
        ELSE 'Noite'
    END AS periodo_horario_fechamento,
    id_wfl_param_os,
    valor_total_comissao,
    valor_total,
    valor_outras_despesas,
    idx,
    id_su_diagnostico,
    gera_comissao,
    id_estrutura,
    id_login,
    valor_unit_comissao,
    data_prazo_limite,
    data_reservada,
    id_ticket,
    origem_endereco,
    justificativa_sla_atrasado,
    origem_endereco_estrutura,
    data_reagendar,
    data_prev_final,
    origem_cadastro,
    ultima_atualizacao,
    last_update,
    DATE_FORMAT(data_fechamento, 'yyyyMM') AS month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}ordem_servico_fechado`
    """,
}

# ************************
# Start Gold Tables
# ************************
tables_gold_produtividade = {
    # Resumo de Ordens Fechadas por Situacao
    "ordem_servico_fechado_resumo_situacao": """
SELECT
    t1.ano_abertura,
    t1.ano_mes_abertura,
    t1.mes_abertura,
    t1.trimestre_abertura,
    t1.semana_do_ano_abertura,
    t1.semana_do_mes_abertura,
    t1.dia_da_semana_abertura,
    t1.dia_do_mes_abertura,
    t1.hora_abertura,
    t1.periodo_horario_abertura,
    t1.ano_fechamento,
    t1.ano_mes_fechamento,
    t1.mes_fechamento,
    t1.trimestre_fechamento,
    t1.semana_do_ano,
    t1.semana_do_mes_fechamento,
    t1.dia_da_semana_fechamento,
    t1.dia_do_mes_fechamento,
    t1.hora_fechamento,
    t1.periodo_horario_fechamento,
    t1.id_filial,
    t2.fantasia,
    t1.id_setor,
    t5.setor,
    t1.id_assunto,
    t3.assunto,
    t1.id_tecnico,
    t4.funcionario,
    t1.status,
    COUNT(DISTINCT(t1.id)) AS total_ordem_fechado,
    t1.month_key
FROM
     delta.`s3a://silver/isp_performance/silver_ordem_servico_fechado` t1
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t4 ON (t4.id = t1.id_tecnico)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t5 ON (t5.id = t1.id_setor)
GROUP BY
    t1.ano_abertura,
    t1.ano_mes_abertura,
    t1.mes_abertura,
    t1.trimestre_abertura,
    t1.semana_do_ano_abertura,
    t1.semana_do_mes_abertura,
    t1.dia_da_semana_abertura,
    t1.dia_do_mes_abertura,
    t1.hora_abertura,
    t1.periodo_horario_abertura,
    t1.ano_fechamento,
    t1.ano_mes_fechamento,
    t1.mes_fechamento,
    t1.trimestre_fechamento,
    t1.semana_do_ano,
    t1.semana_do_mes_fechamento,
    t1.dia_da_semana_fechamento,
    t1.dia_do_mes_fechamento,
    t1.hora_fechamento,
    t1.periodo_horario_fechamento,
    t1.id_filial,
    t2.fantasia,
    t1.id_setor,
    t5.setor,
    t1.id_assunto,
    t3.assunto,
    t1.id_tecnico,
    t4.funcionario,
    t1.status,
    t1.month_key
    """,
}