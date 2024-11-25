import requests
from configs import configs

def fetch_all_apis():
    responses = {}

    for key, url in configs.tables_landing_produtividade.items():
        try:
            response = requests.get(url)
            if response.status_code == 200:
                responses[key] = response.json()
            else:
                print(f"Erro: Código de status {response.status_code} para a URL {url}")
                responses[key] = None
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição para {url}: {e}")
            responses[key] = None

    return responses

api_responses = fetch_all_apis()
print("Respostas das APIs:", api_responses)