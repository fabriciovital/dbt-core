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
    "8": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/cliente",
    "9": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/cidade",
    "10": "http://api.nexusitconsulting.com.br:3000/api/v1/ixc/contrato",
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
    "8": "dim_cliente",
    "9": "dim_cidade",
    "10": "dim_contrato",
}

# ************************
# Start Bronze Tables Situacionais
# ************************
tables_manuais_isp_performance = {
    "1": "dim_equipe_tecnica",
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
     upper(funcionario) funcionario,
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
    status_sla AS id_status_sla,
    CASE 
        WHEN status_sla = 'N' THEN 'Fora do Prazo'        
        ELSE 'Dentro do Prazo'
    END AS status_sla,
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
    status AS id_status,
    CASE 
        WHEN status = 'RAG' THEN 'Reagendar'
        WHEN status = 'EN' THEN 'Encaminhada'
        WHEN status = 'AS' THEN 'Assumida'
        WHEN status = 'DS' THEN 'Deslocamento'
        WHEN status = 'AG' THEN 'Agendada'
        WHEN status = 'A' THEN 'Aberta'
        WHEN status = 'EX' THEN 'Em Execução'
        ELSE 'Finalizada'
    END AS status,
    id_cliente,
    id_assunto,
    setor AS id_setor,
    id_cidade,
    id_tecnico,
    prioridade AS id_prioridade,
    CASE
        WHEN prioridade = 'B' THEN 'Baixa'
        WHEN prioridade = 'N' THEN 'Neutra'
        WHEN prioridade = 'A' THEN 'Alta'
        WHEN prioridade = 'C' THEN 'Crítica'
        ELSE 'Sem Prioridade'
    END AS prioridade,
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
    CAST(data_agenda AS TIMESTAMP) AS data_agenda,
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
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}ordem_servico_aberto`
""",
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
    status_sla AS id_status_sla,
    CASE 
        WHEN status_sla = 'N' THEN 'Fora do Prazo'        
        ELSE 'Dentro do Prazo'
    END AS status_sla,
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
    status AS id_status,
    CASE 
        WHEN status = 'RAG' THEN 'Reagendar'
        WHEN status = 'EN' THEN 'Encaminhada'
        WHEN status = 'AS' THEN 'Assumida'
        WHEN status = 'DS' THEN 'Deslocamento'
        WHEN status = 'AG' THEN 'Agendada'
        WHEN status = 'A' THEN 'Aberta'
        WHEN status = 'EX' THEN 'Em Execução'
        ELSE 'Finalizada'
    END AS status,
    id_cliente,
    id_assunto,
    setor AS id_setor,
    id_cidade,
    id_tecnico,
    prioridade AS id_prioridade,
    CASE
        WHEN prioridade = 'B' THEN 'Baixa'
        WHEN prioridade = 'N' THEN 'Neutra'
        WHEN prioridade = 'A' THEN 'Alta'
        WHEN prioridade = 'C' THEN 'Crítica'
        ELSE 'Sem Prioridade'
    END AS prioridade,
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
    CAST(data_agenda AS TIMESTAMP) AS data_agenda,
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
        # Dimensao Cliente
    "dim_cliente": f"""
SELECT 
    id,
    upper(razao) nome_cliente,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_cliente`
    """,
        # Dimensao Cidade
    "dim_cidade": f"""
SELECT 
    id,
    nome as cidade,
    last_update,
    month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_cidade`
    """,
        # Contrato
    "dim_contrato": f"""
SELECT 
 	id_instalador,
	indicacao_contrato_id,
	id_indexador_reajuste,
    TO_TIMESTAMP(data_desistencia, 'yyyy-MM-dd') AS data_desistencia,
    YEAR(TO_TIMESTAMP(data_desistencia, 'yyyy-MM-dd')) AS ano_desistencia,
    DATE_FORMAT(TO_TIMESTAMP(data_desistencia, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_desistencia,
	motivo_desistencia,
	obs_desistencia,
	obs_contrato,
	alerta_contrato,
	ids_contratos_recorrencia,
	id_responsavel_desistencia,
	id_responsavel_cancelamento,
	id_responsavel_negativacao,
	id,
	id_filial,
	status,
	status_internet,
	id_cliente,
    TO_TIMESTAMP(data_assinatura, 'yyyy-MM-dd') AS data_assinatura,
    YEAR(TO_TIMESTAMP(data_assinatura, 'yyyy-MM-dd')) AS ano_assinatura,
    DATE_FORMAT(TO_TIMESTAMP(data_assinatura, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_assinatura,
    TO_TIMESTAMP(data_ativacao, 'yyyy-MM-dd') AS data_ativacao,
    YEAR(TO_TIMESTAMP(data_ativacao, 'yyyy-MM-dd')) AS ano_ativacao,
    DATE_FORMAT(TO_TIMESTAMP(data_ativacao, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_ativacao,
    TO_TIMESTAMP(data, 'yyyy-MM-dd') AS data,
    YEAR(TO_TIMESTAMP(data, 'yyyy-MM-dd')) AS ano_data,
    DATE_FORMAT(TO_TIMESTAMP(data, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_data,
    TO_TIMESTAMP(data_renovacao, 'yyyy-MM-dd') AS data_renovacao,
    YEAR(TO_TIMESTAMP(data_renovacao, 'yyyy-MM-dd')) AS ano_renovacao,
    DATE_FORMAT(TO_TIMESTAMP(data_renovacao, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_renovacao,
	bloqueio_automatico,
	imp_carteira,
    TO_TIMESTAMP(data_expiracao, 'yyyy-MM-dd') AS data_expiracao,
    YEAR(TO_TIMESTAMP(data_expiracao, 'yyyy-MM-dd')) AS ano_expiracao,
    DATE_FORMAT(TO_TIMESTAMP(data_expiracao, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_expiracao,
	isentar_contrato,
    TO_TIMESTAMP(pago_ate_data, 'yyyy-MM-dd') AS pago_ate_data,
    YEAR(TO_TIMESTAMP(pago_ate_data, 'yyyy-MM-dd')) AS ano_pago_ate_data,
    DATE_FORMAT(TO_TIMESTAMP(pago_ate_data, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_pago_ate_data,
	id_vd_contrato,
	contrato,
	endereco,
	numero,
	comissao,
	bairro,
	tipo,
	descricao_aux_plano_venda,
	aviso_atraso,
	id_tipo_contrato,
	id_carteira_cobranca,
	obs,
	id_modelo,
	status_velocidade,
	id_vendedor,
	cc_previsao,
	nao_avisar_ate,
	nao_bloquear_ate,
	id_tipo_documento,
	tipo_doc_opc,
	desconto_fidelidade,
	tipo_doc_opc2,
	tipo_doc_opc3,
	taxa_improdutiva,
	tipo_doc_opc4,
	desbloqueio_confianca,
    TO_TIMESTAMP(data_negativacao, 'yyyy-MM-dd') AS data_negativacao,
    YEAR(TO_TIMESTAMP(data_negativacao, 'yyyy-MM-dd')) AS ano_negativacao,
    DATE_FORMAT(TO_TIMESTAMP(data_negativacao, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_negativacao,
    TO_TIMESTAMP(data_acesso_desativado, 'yyyy-MM-dd') AS data_acesso_desativado,
    YEAR(TO_TIMESTAMP(data_acesso_desativado, 'yyyy-MM-dd')) AS ano_acesso_desativado,
    DATE_FORMAT(TO_TIMESTAMP(data_acesso_desativado, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_acesso_desativado,    
	motivo_cancelamento,
    TO_TIMESTAMP(data_cancelamento, 'yyyy-MM-dd') AS data_cancelamento,
    YEAR(TO_TIMESTAMP(data_cancelamento, 'yyyy-MM-dd')) AS ano_cancelamento,
    DATE_FORMAT(TO_TIMESTAMP(data_cancelamento, 'yyyy-MM-dd'), 'yyyy-MM') AS ano_mes_cancelamento,
	obs_cancelamento,
	id_vendedor_ativ,
	fidelidade,
	tipo_cobranca,
	desbloqueio_confianca_ativo,
	id_responsavel,
	taxa_instalacao,
	protocolo_negativacao,
	dt_ult_bloq_auto,
	dt_ult_bloq_manual,
	dt_ult_des_bloq_conf,
	dt_ult_ativacao,
	avalista_1,
	dt_ult_finan_atraso,
	avalista_2,
	dt_utl_negativacao,
	data_cadastro_sistema,
	ultima_atualizacao,
	complemento,
	cidade,
	imp_importacao,
	renovacao_automatica,
	imp_rede,
	imp_bkp,
	motivo_inclusao,
	imp_treinamento,
	imp_status,
	liberacao_bloqueio_manual,
	imp_obs,
	contrato_suspenso,
	imp_realizado,
	imp_motivo,
	imp_inicial,
	imp_final,
	ativacao_numero_parcelas,
	ativacao_vencimentos,
	ativacao_valor_parcela,
	id_tipo_doc_ativ,
	id_produto_ativ,
	id_cond_pag_ativ,
	endereco_padrao_cliente,
	cep,
	referencia,
	id_condominio,
	nf_info_adicionais,
	assinatura_digital,
	tipo_produtos_plano,
	bloco,
	apartamento,
	latitude,
	longitude,
	num_parcelas_atraso,
	dt_ult_desiste,
	id_contrato_principal,
	gerar_finan_assin_digital_contrato,
	credit_card_recorrente_token,
	credit_card_recorrente_bandeira_cartao,
	id_motivo_negativacao,
	obs_negativacao,
	restricao_auto_desbloqueio,
	motivo_restricao_auto_desbloq,
	nao_susp_parc_ate,
	liberacao_suspensao_parcial,
	utilizando_auto_libera_susp_parc,
	restricao_auto_libera_susp_parcial,
	motivo_restri_auto_libera_parc,
	data_inicial_suspensao,
	data_final_suspensao,
	data_retomada_contrato,
	dt_ult_liberacao_susp_parc,
	base_geracao_tipo_doc,
	integracao_assinatura_digital,
	url_assinatura_digital,
	token_assinatura_digital,
	testemunha_assinatura_digital,
	document_photo,
	selfie_photo,
	tipo_localidade,
	estrato_social_col,
	last_update,
	month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_contrato`
    """,
}

tables_silver_manuais = {
        # Dimensao Equipe Tecnica
    "dim_equipe_tecnica": f"""
SELECT 
    periodo,
    id_filial,
    filial,
    equipe,
    id_tecnico,
    tecnico,
    last_update,
    DATE_FORMAT(periodo, 'yyyyMM') AS month_key
FROM 
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}dim_equipe_tecnica`
    """,
}

# ************************
# Start Gold Tables
# ************************
tables_gold = {
        # Perfomance Ordem Serviço
    "performance_ordem_servico": """
WITH BASE_PERFORMANCE AS (
    SELECT
        t1.ano_abertura,
        t1.ano_mes_abertura,
        t1.data_abertura,
        t1.dia_do_mes_abertura,
        t1.hora_abertura,
        t1.ano_fechamento,
        t1.ano_mes_fechamento,
        t1.data_fechamento,
        t1.dia_do_mes_fechamento,
        t1.hora_fechamento,
        t1.data_agenda,
        t1.data_hora_assumido,
        t1.data_hora_execucao,
        t1.id_filial,
        t2.fantasia AS filial,
        t1.id_setor,
        t4.setor AS setor,
        t6.id AS id_relator,
        t6.login AS relator,
        t1.id_tecnico,
        t5.funcionario AS tecnico,
        t1.id_assunto,
        t3.assunto AS assunto,
        t1.id_su_diagnostico,
        t1.id AS ordem_servico_id,
        t1.id_status,
        t1.status,
        t1.prioridade,
        t1.id_status_sla,
        t1.status_sla,
        t1.id_cliente,
        t7.nome_cliente,
        t1.bairro,
        t1.latitude,
        t1.longitude,
        t1.id_cidade,
        t8.cidade,
        t1.last_update
    FROM
        delta.`s3a://silver/isp_performance/silver_ordem_servico_aberto` t1
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t4 ON (t4.id = t1.id_setor)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t5 ON (t5.id = t1.id_tecnico)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t6 ON (t6.id = t1.id_login)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cliente` t7 ON (t7.id = t1.id_cliente)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cidade` t8 ON (t8.id = t1.id_cidade)
    UNION
    SELECT
        t1.ano_abertura,
        t1.ano_mes_abertura,
        t1.data_abertura,
        t1.dia_do_mes_abertura,
        t1.hora_abertura,
        t1.ano_fechamento,
        t1.ano_mes_fechamento,
        t1.data_fechamento,
        t1.dia_do_mes_fechamento,
        t1.hora_fechamento,
        t1.data_agenda,
        t1.data_hora_assumido,	
        t1.data_hora_execucao,	
        t1.id_filial,
        t2.fantasia AS filial,
        t1.id_setor,
        t4.setor AS setor,
        t6.id AS id_relator,
        t6.login AS relator,
        t1.id_tecnico,
        t5.funcionario AS tecnico,
        t1.id_assunto,
        t3.assunto AS assunto,
        t1.id_su_diagnostico,
        t1.id AS ordem_servico_id,
        t1.id_status,
        t1.status,
        t1.prioridade,
        t1.id_status_sla,
        t1.status_sla,
        t1.id_cliente,
        t7.nome_cliente,
        t1.bairro,
        t1.latitude,
        t1.longitude,
        t1.id_cidade,
        t8.cidade,
        t1.last_update
    FROM
        delta.`s3a://silver/isp_performance/silver_ordem_servico_fechado` t1
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t4 ON (t4.id = t1.id_setor)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t5 ON (t5.id = t1.id_tecnico)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t6 ON (t6.id = t1.id_login)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cliente` t7 ON (t7.id = t1.id_cliente)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cidade` t8 ON (t8.id = t1.id_cidade)
),
STATUS_COUNTS AS (
    SELECT
        ano_abertura,
        ano_mes_abertura,
        data_abertura,
        dia_do_mes_abertura,
        hora_abertura,
        ano_fechamento,
        ano_mes_fechamento,
        data_fechamento,
        dia_do_mes_fechamento,
        hora_fechamento,
        data_agenda,
        data_hora_assumido,	
        data_hora_execucao,	
        id_filial,
        filial,
        id_setor,
        setor,
        id_assunto,
        assunto,
        id_su_diagnostico,
        id_relator,
        relator,
        id_tecnico,
        tecnico,
        id_status,
        status,
        prioridade,
        data_abertura,
        data_fechamento,
        id_status_sla,
        status_sla,
        id_cliente,
        nome_cliente,
        bairro,
        latitude,
        longitude,
        id_cidade,
        cidade,
        COUNT(ordem_servico_id) AS qtd,
        last_update
    FROM BASE_PERFORMANCE
    GROUP BY
        ano_abertura,
        ano_mes_abertura,
        data_abertura,
        dia_do_mes_abertura,
        hora_abertura,
        ano_fechamento,
        ano_mes_fechamento,
        data_fechamento,
        dia_do_mes_fechamento,
        hora_fechamento,
        data_agenda,
        data_hora_assumido,	
        data_hora_execucao,	
        id_filial,
        filial,
        id_setor,
        setor,
        id_assunto,
        assunto,
        id_su_diagnostico,
        id_relator,
        relator,
        id_tecnico,
        tecnico,
        id_status,
        status,
        prioridade,
        data_abertura,
        data_fechamento,
        id_status_sla,
        status_sla,
        id_cliente,
        nome_cliente,
        bairro,
        latitude,
        longitude,
        id_cidade,
        cidade,
        last_update
)
SELECT
    ano_abertura,
    ano_mes_abertura,
    data_abertura,
    dia_do_mes_abertura,
    hora_abertura,
    ano_fechamento,
    ano_mes_fechamento,
    data_fechamento,
    dia_do_mes_fechamento,
    hora_fechamento,
    data_agenda,
    data_hora_assumido,	
    data_hora_execucao,	
    id_filial,
    filial,
    id_setor,
    setor,
    id_assunto,
    assunto,
    id_su_diagnostico,
    id_relator,
    relator,
    id_tecnico,
    tecnico,
    id_status_sla,
    status_sla,
    prioridade,
    id_cliente,
    nome_cliente,
    bairro,
    latitude,
    longitude,
    id_cidade,
    cidade,
    last_update,
    SUM(qtd) AS qtd_total,
    SUM(CASE WHEN status = 'Reagendar' THEN qtd ELSE 0 END) AS qtd_reagendar,
    SUM(CASE WHEN status = 'Encaminhada' THEN qtd ELSE 0 END) AS qtd_encaminhada,
    SUM(CASE WHEN status = 'Assumida' THEN qtd ELSE 0 END) AS qtd_assumida,
    SUM(CASE WHEN status = 'Deslocamento' THEN qtd ELSE 0 END) AS qtd_deslocamento,
    SUM(CASE WHEN status = 'Agendada' THEN qtd ELSE 0 END) AS qtd_agendada,
    SUM(CASE WHEN status = 'Aberta' THEN qtd ELSE 0 END) AS qtd_aberta,
    SUM(CASE WHEN status = 'Em Execução' THEN qtd ELSE 0 END) AS qtd_execucao,
    SUM(CASE WHEN status IN ('Reagendar', 'Encaminhada', 'Aberta') THEN qtd ELSE 0 END) AS qtd_pendente,
    SUM(CASE WHEN status IN ('Agendada') AND (current_date - CAST(data_agenda AS DATE)) >= INTERVAL 1 DAY THEN qtd ELSE 0 END) AS qtd_agendados_atrasados,
    SUM(CASE WHEN status = 'Finalizada' THEN qtd ELSE 0 END) AS qtd_finalizada,
    AVG(CAST(unix_timestamp(data_fechamento) - unix_timestamp(data_abertura) AS DECIMAL)) AS tempo_medio_fechamento_segundos
FROM STATUS_COUNTS
GROUP BY 
    ano_abertura,
    ano_mes_abertura,
    data_abertura,
    dia_do_mes_abertura,
    hora_abertura,
    ano_fechamento,
    ano_mes_fechamento,
    data_fechamento,
    dia_do_mes_fechamento,
    hora_fechamento,
    data_agenda,
    data_hora_assumido,	
    data_hora_execucao,	
    id_filial,
    filial,
    id_setor,
    setor,
    id_assunto,
    assunto,
    id_su_diagnostico,
    id_relator,
    relator,
    id_tecnico,
    tecnico,
    id_status_sla,
    status_sla,
    prioridade,
    id_cliente,
    nome_cliente,
    bairro,
    latitude,
    longitude,
    id_cidade,
    cidade,
    last_update
""",
        # Ordem Serviço Aberto
    "ordem_servico_aberto": """
    SELECT
        t1.ano_abertura,
        t1.ano_mes_abertura,
        t1.data_abertura,
        t1.dia_do_mes_abertura,
        t1.hora_abertura,
        t1.data_agenda,
        t1.data_hora_assumido,	
        t1.data_hora_execucao,	
        t1.id_filial,
        t2.fantasia AS filial,
        t1.id_setor,
        t4.setor AS setor,
        t6.id AS id_relator,
        t6.login AS relator,
        t1.id_tecnico,
        t5.funcionario AS tecnico,
        t1.id_assunto,
        t3.assunto AS assunto,
        t1.id_su_diagnostico,
        t1.id_ticket AS id_atendimento,
        t1.id AS id_ordem_servico,
        t1.id_status,
        t1.status,
        t1.prioridade,
        t1.id_status_sla,
        t1.status_sla,
        t1.id_cliente,
        t7.nome_cliente,
        t1.bairro,
        t1.latitude,
        t1.longitude,
        t1.id_cidade,
        t8.cidade,
        t1.last_update
    FROM
        delta.`s3a://silver/isp_performance/silver_ordem_servico_aberto` t1
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_assunto` t3 ON (t3.id = t1.id_assunto)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_setor` t4 ON (t4.id = t1.id_setor)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_colaboradores` t5 ON (t5.id = t1.id_tecnico)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_usuarios` t6 ON (t6.id = t1.id_login)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cliente` t7 ON (t7.id = t1.id_cliente)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cidade` t8 ON (t8.id = t1.id_cidade)
""",
    # Performance Contrato
    "performance_contrato": """
SELECT
    t1.data_desistencia,
    t1.ano_desistencia,
    t1.ano_mes_desistencia,
    t1.motivo_desistencia,
    t1.id_filial,
    t2.fantasia AS filial,
    t1.status,
    t1.status_internet,
    t1.id_cliente,
    t3.nome_cliente,
    t1.data_assinatura,
    t1.ano_assinatura,
    t1.ano_mes_assinatura,
    t1.data_ativacao,
    t1.ano_ativacao,
    t1.ano_mes_ativacao,
    t1.data,
    t1.ano_data,
    t1.ano_mes_data,
    t1.data_renovacao,
    t1.ano_renovacao,
    t1.ano_mes_renovacao,
    t1.bloqueio_automatico,
    t1.data_expiracao,
    t1.ano_expiracao,
    t1.ano_mes_expiracao,
    t1.pago_ate_data,
    t1.ano_pago_ate_data,
    t1.ano_mes_pago_ate_data,
    t1.contrato,
    t1.status_velocidade,
    t1.id_vendedor,
    t1.data_negativacao,
    t1.ano_negativacao,
    t1.ano_mes_negativacao,    
    t1.data_acesso_desativado,
    t1.ano_acesso_desativado,
    t1.ano_mes_acesso_desativado,
    t1.motivo_cancelamento,
    t1.data_cancelamento,
    t1.ano_cancelamento,
    t1.ano_mes_cancelamento,    
    t1.id_vendedor_ativ,
    t1.fidelidade,
    t1.cidade AS id_cidade,
    t4.cidade,
    t1.contrato_suspenso,
    t1.ativacao_numero_parcelas,
    t1.ativacao_vencimentos,
    t1.ativacao_valor_parcela,
    t1.tipo_produtos_plano,
    t1.num_parcelas_atraso,
    COUNT(t1.contrato) AS qtd_total,
    t1.last_update
FROM
    delta.`s3a://silver/isp_performance/silver_dim_contrato` t1
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_filial` t2 ON (t2.id = t1.id_filial)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cliente` t3 ON (t3.id = t1.id_cliente)
    LEFT JOIN delta.`s3a://silver/isp_performance/silver_dim_cidade` t4 ON (t4.id = t1.cidade)
GROUP BY
    t1.data_desistencia,
    t1.ano_desistencia,
    t1.ano_mes_desistencia,
    t1.motivo_desistencia,
    t1.id_filial,
    t2.fantasia,
    t1.status,
    t1.status_internet,
    t1.id_cliente,
    t3.nome_cliente,
    t1.data_assinatura,
    t1.ano_assinatura,
    t1.ano_mes_assinatura,
    t1.data_ativacao,
    t1.ano_ativacao,
    t1.ano_mes_ativacao,
    t1.data,
    t1.ano_data,
    t1.ano_mes_data,
    t1.data_renovacao,
    t1.ano_renovacao,
    t1.ano_mes_renovacao,
    t1.bloqueio_automatico,
    t1.data_expiracao,
    t1.ano_expiracao,
    t1.ano_mes_expiracao,
    t1.pago_ate_data,
    t1.ano_pago_ate_data,
    t1.ano_mes_pago_ate_data,
    t1.contrato,
    t1.status_velocidade,
    t1.id_vendedor,
    t1.data_negativacao,
    t1.ano_negativacao,
    t1.ano_mes_negativacao,
    t1.data_acesso_desativado,
    t1.ano_acesso_desativado,
    t1.ano_mes_acesso_desativado,
    t1.motivo_cancelamento,
    t1.data_cancelamento,
    t1.ano_cancelamento,
    t1.ano_mes_cancelamento,
    t1.id_vendedor_ativ,
    t1.fidelidade,
    t1.cidade,
    t4.cidade,
    t1.contrato_suspenso,
    t1.ativacao_numero_parcelas,
    t1.ativacao_vencimentos,
    t1.ativacao_valor_parcela,
    t1.tipo_produtos_plano,
    t1.num_parcelas_atraso,
    t1.last_update
""",
}
