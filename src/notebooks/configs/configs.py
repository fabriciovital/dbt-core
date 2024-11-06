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
    login,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_usuarios`
    """,
        # Ordem Serviço Aberto
    "ordem_servico_aberto": f"""
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
    status,
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
""",
}


# ************************
# Start Gold Tables
# ************************
tables_gold = {
    # Ordem Serviço Aberto
    "ordem_servico_aberto": """
SELECT
    t1.mensagem_resposta,
    t1.data_hora_analise,
    t1.data_hora_encaminhado,
    t1.data_hora_assumido,
    t1.data_hora_execucao,
    t1.id_contrato_kit,
    t1.preview,
    t1.data_agenda_final,
    t1.id,
    t1.tipo,
    t1.id_filial,
    t2.fantasia,
    t1.id_wfl_tarefa,
    t1.status_sla,
    t1.data_abertura,
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
    t1.melhor_horario_agenda,
    t1.liberado,
    t1.status,
    t1.id_cliente,
    t1.id_assunto,
    t3.assunto,
    t1.id_setor,
    t6.setor,
    t1.id_cidade,
    t1.id_tecnico,
    t4.funcionario,
    t1.prioridade,
    t1.mensagem,
    t1.protocolo,
    t1.endereco,
    t1.complemento,
    t1.id_condominio,
    t1.bloco,
    t1.apartamento,
    t1.latitude,
    t1.bairro,
    t1.longitude,
    t1.referencia,
    t1.impresso,
    t1.data_inicio,
    t1.data_agenda,
    t1.data_final,
    t1.data_fechamento,
    t1.id_wfl_param_os,
    t1.valor_total_comissao,
    t1.valor_total,
    t1.valor_outras_despesas,
    t1.idx,
    t1.id_su_diagnostico,
    t1.gera_comissao,
    t1.id_estrutura,
    t1.id_login,
    t5.login,
    t1.valor_unit_comissao,
    t1.data_prazo_limite,
    t1.data_reservada,
    t1.id_ticket,
    t1.origem_endereco,
    t1.justificativa_sla_atrasado,
    t1.origem_endereco_estrutura,
    t1.data_reagendar,
    t1.data_prev_final,
    t1.origem_cadastro,
    t1.ultima_atualizacao,
    t1.last_update,
    t1.month_key
FROM
     delta.`s3a://silver/isp_performance/silver_ordem_servico_aberto` t1
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t4 ON (t4.id = t1.id_tecnico)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t5 ON (t5.id = t1.id_login)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t6 ON (t6.id = t1.id_setor)
    """,
    # Resumo de Ordens Abertas por Situacao
    "ordem_servico_aberto_resumo_situacao": """
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
    t1.id_filial,
    t2.fantasia,
    t1.id_setor,
    t6.setor,
    t1.id_assunto,
    t3.assunto,
    t1.id_tecnico,
    t4.funcionario,
    t1.id_login,
    t5.login,
    t1.status,
    COUNT(DISTINCT(t1.id)) AS total_ordem_aberta,
    t1.month_key
FROM
     delta.`s3a://silver/isp_performance/silver_ordem_servico_aberto` t1
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t4 ON (t4.id = t1.id_tecnico)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t5 ON (t5.id = t1.id_login)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t6 ON (t6.id = t1.id_setor)
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
    t1.id_filial,
    t2.fantasia,
    t1.id_setor,
    t6.setor,
    t1.id_assunto,
    t3.assunto,
    t1.id_tecnico,
    t4.funcionario,
    t1.id_login,
    t5.login,
    t1.status,
    t1.month_key
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
    status,
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
    # Ordem Serviço Fechado
    "ordem_servico_fechado": """
SELECT
    t1.mensagem_resposta,
    t1.data_hora_analise,
    t1.data_hora_encaminhado,
    t1.data_hora_assumido,
    t1.data_hora_execucao,
    t1.id_contrato_kit,
    t1.preview,
    t1.data_agenda_final,
    t1.id,
    t1.tipo,
    t1.id_filial,
    t2.fantasia,
    t1.id_wfl_tarefa,
    t1.status_sla,
    t1.data_abertura,
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
    t1.melhor_horario_agenda,
    t1.liberado,
    t1.status,
    t1.id_cliente,
    t1.id_assunto,
    t3.assunto,
    t1.id_setor,
    t6.setor,
    t1.id_cidade,
    t1.id_tecnico,
    t4.funcionario,
    t1.prioridade,
    t1.mensagem,
    t1.protocolo,
    t1.endereco,
    t1.complemento,
    t1.id_condominio,
    t1.bloco,
    t1.apartamento,
    t1.latitude,
    t1.bairro,
    t1.longitude,
    t1.referencia,
    t1.impresso,
    t1.data_inicio,
    t1.data_agenda,
    t1.data_final,
    t1.data_fechamento,
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
    t1.id_wfl_param_os,
    t1.valor_total_comissao,
    t1.valor_total,
    t1.valor_outras_despesas,
    t1.idx,
    t1.id_su_diagnostico,
    t1.gera_comissao,
    t1.id_estrutura,
    t1.id_login,
    t5.login,
    t1.valor_unit_comissao,
    t1.data_prazo_limite,
    t1.data_reservada,
    t1.id_ticket,
    t1.origem_endereco,
    t1.justificativa_sla_atrasado,
    t1.origem_endereco_estrutura,
    t1.data_reagendar,
    t1.data_prev_final,
    t1.origem_cadastro,
    t1.ultima_atualizacao,
    t1.last_update,
    t1.month_key
FROM
     delta.`s3a://silver/isp_performance/silver_ordem_servico_fechado` t1
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t4 ON (t4.id = t1.id_tecnico)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t5 ON (t5.id = t1.id_login)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t6 ON (t6.id = t1.id_setor)
    """,
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
    t6.setor,
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
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t5 ON (t5.id = t1.id_login)
LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t6 ON (t6.id = t1.id_setor)
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
    t6.setor,
    t1.id_assunto,
    t3.assunto,
    t1.id_tecnico,
    t4.funcionario,
    t1.status,
    t1.month_key
    """,
}
