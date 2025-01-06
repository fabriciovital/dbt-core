# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import textwrap

from superset import db
from superset.models.core import CssTemplate


def load_css_templates() -> None:
    """Loads 2 css templates to demonstrate the feature"""
    print("Creating default CSS templates")

    obj = db.session.query(CssTemplate).filter_by(template_name="Flat").first()
    if not obj:
        obj = CssTemplate(template_name="Flat")
        db.session.add(obj)
    css = textwrap.dedent(
        """\
    .navbar {
        transition: opacity 0.5s ease;
        opacity: 0.05;
    }
    .navbar:hover {
        opacity: 1;
    }
    .chart-header .header{
        font-weight: @font-weight-normal;
        font-size: 12px;
    }
    /*
    var bnbColors = [
        //rausch    hackb      kazan      babu      lima        beach     tirol
        '#ff5a5f', '#7b0051', '#007A87', '#00d1c1', '#8ce071', '#ffb400', '#b4a76c',
        '#ff8083', '#cc0086', '#00a1b3', '#00ffeb', '#bbedab', '#ffd266', '#cbc29a',
        '#ff3339', '#ff1ab1', '#005c66', '#00b3a5', '#55d12e', '#b37e00', '#988b4e',
     ];
    */
    """
    )
    obj.css = css

    obj = db.session.query(CssTemplate).filter_by(template_name="Courier Black").first()
    if not obj:
        obj = CssTemplate(template_name="Courier Black")
        db.session.add(obj)
    css = textwrap.dedent(
        """\
    h2 {
        color: white;
        font-size: 52px;
    }
    .navbar {
        box-shadow: none;
    }
    .navbar {
        transition: opacity 0.5s ease;
        opacity: 0.05;
    }
    .navbar:hover {
        opacity: 1;
    }
    .chart-header .header{
        font-weight: @font-weight-normal;
        font-size: 12px;
    }
    .nvd3 text {
        font-size: 12px;
        font-family: inherit;
    }
    body{
        background: #000;
        font-family: Courier, Monaco, monospace;;
    }
    /*
    var bnbColors = [
        //rausch    hackb      kazan      babu      lima        beach     tirol
        '#ff5a5f', '#7b0051', '#007A87', '#00d1c1', '#8ce071', '#ffb400', '#b4a76c',
        '#ff8083', '#cc0086', '#00a1b3', '#00ffeb', '#bbedab', '#ffd266', '#cbc29a',
        '#ff3339', '#ff1ab1', '#005c66', '#00b3a5', '#55d12e', '#b37e00', '#988b4e',
     ];
    */
    """
    )
    obj.css = css

import textwrap

obj = db.session.query(CssTemplate).filter_by(template_name="Tema Padrão").first()
if not obj:
    obj = CssTemplate(template_name="Tema Padrão")
    db.session.add(obj)

css = textwrap.dedent("""
    h2 {
        color: white;
        font-size: 52px;
    }
    .navbar {
        box-shadow: none;
    }
    .navbar {
        transition: opacity 0.5s ease;
        opacity: 0.05;
    }
    .navbar:hover {
        opacity: 1;
    }
    .chart-header .header {
        font-weight: normal;
        font-size: 12px;
    }
    .nvd3 text {
        font-size: 12px;
        font-family: inherit;
    }
    body {
        background: #000;
        font-family: Courier, Monaco, monospace;
    }

    /* Father Container from each container on the dashboard */
    .dashboard-component {
        display: flex;
        justify-content: center;
        align-items: center;
        border-radius: 4px;
    }
  
    /* Define o fundo das tabs como transparente */
    .ant-tabs-content-holder {
        background: transparent;
    }
  
    /* Icons */
    .ant-tabs-content-holder * {
        color: black;
    }
  
    /* Deixa o texto das tabs ativas em negrito */
    .ant-tabs-tab-active * {
        font-weight: bold;
        color: #0000FF !important;
    }
  
    /* Fundo azul claro no dashboard */
    .dashboard-content {
        background-image: linear-gradient(5deg, #FFFFFF, #4169E1);
        font-family: 'Inter', sans-serif;
        padding: 5px;
        border-radius: 1px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
    }
  
    /* Remove sombra das tabs */
    .dashboard-component-tabs {
        background: #f7f7f7;
        box-shadow: none;
    }
  
    /* Fundo dos gráficos ajustado para contraste */
    .superset-chart-container .chart-container {
        background-color: #ffffff;
        border: 2px solid #90caf9 !important;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    }
  
    /* Altera a cor das colunas dos gráficos para azul */
    .superset-chart-container .ant-chart-column {
        fill: #42a5f5 !important;
    }
  
    /* Altera a cor do texto (eixos, títulos, legendas) para preto */
    .superset-chart-container .ant-chart-axis-label,
    .superset-chart-container .ant-chart-legend-item {
        fill: #212121 !important;
        font-weight: 500;
    }
  
    /* Linhas de grade com contraste suave */
    .superset-chart-container .ant-chart-grid-line {
        stroke: #e0e0e0 !important;
    }
  
    /* Barra de título das colunas transparente com texto preto */
    .superset-chart-container .ant-chart-axis-title {
        background: transparent !important;
        color: #212121 !important;
        font-size: 14px;
        font-weight: 600;
    }
  
    /* Barra de título das colunas (eixos) sem bordas */
    .superset-chart-container .ant-chart-axis {
        border: none !important;
    }
  
    /* Rótulos dos dados no gráfico ajustados para preto */
    .superset-chart-container .ant-chart-label {
        fill: #424242 !important;
        font-size: 12px;
        font-weight: 400;
    }
  
    /* Tooltip dos gráficos com fundo escuro e texto claro */
    .superset-chart-container .ant-chart-tooltip {
        background-color: rgba(50, 50, 50, 0.9) !important;
        color: #ffffff !important;
        border-radius: 8px;
        padding: 10px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    }
  
    /* Tabelas com fundo branco para legibilidade */
    table {
        color: black;
        background-color: #ffffff !important;
        border-spacing: 0;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    }
  
    /* Títulos das tabelas com fundo mais escuro e bordas */
    table th {
        background-color: #90caf9 !important; /* Azul realçado */
        font-weight: bold !important;
        padding: 12px; /* Espaçamento interno */
        text-align: center;
        font-size: 14px; /* Texto maior e legível */
    }
  
    /* Células das tabelas ajustadas */
    table td {
        border: 2px solid #e0e0e0 !important;
        padding: 10px;
        font-size: 16px;
    }
  
    /* Ajuste adicional nas bordas */
    table th:first-child {
        border-top-left-radius: 10px;
    }
  
    table th:last-child {
        border-top-right-radius: 10px;
    }
""")

obj.css = css