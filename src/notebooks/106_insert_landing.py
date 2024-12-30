import requests
from configs import configs

def fetch_selected_apis(table_indexes):
    responses = {}

    # Filtra as tabelas com base nos índices fornecidos na lista table_indexes
    tables_to_process = list(configs.tables_landing.items())
    
    for i in table_indexes:
        if i < len(tables_to_process):  # Verifica se o índice está dentro do limite
            key, url = tables_to_process[i]
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    responses[key] = response.json()  # Armazena a resposta JSON no dicionário
                else:
                    print(f"Erro: Código de status {response.status_code} para a URL {url}")
                    responses[key] = None  # Armazena None para indicar que houve erro
            except requests.exceptions.RequestException as e:
                print(f"Erro na requisição para {url}: {e}")
                responses[key] = None
        else:
            print(f"Índice {i} fora do alcance das tabelas.")

    return responses

# Exemplo de uso
table_indexes = [0, 1, 2, 3, 4, 5, 7]  # Passar os índices das tabelas que você deseja processar
api_responses = fetch_selected_apis(table_indexes)
print("Respostas das APIs:", api_responses)