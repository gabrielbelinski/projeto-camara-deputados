from pyspark.core.rdd import RDD
import requests, json, logging, time, url_manipulation

def fetch_data(url: str) -> list:
    """
    Função genérica para extração de dados a partir das APIs.

    Args:
        url (str): Qual é o endpoint desejado
        map_fields: Função lambda para mapear as informações desejadas

    Returns: 
        list: Resposta de requisição, filtrada conforme os campos passados como argumento.

    Raises:
        Exception: Erro durante a requisição.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception:
        raise Exception(f"Houve um erro durante a requisição.\nErro: {response.status_code} - {response.text}")

def make_requests(url : str):
    """
    Realiza requisições as APIs, com detecção de paginação. Método desenvolvido de acordo com as características da API de dados abertos da Cãmara dos Deputados.

    Args:
        url (str): URL a qual a requisição será realizada

    Returns: 
        iter(results): Retorna o resultado da requisição, em forma de objeto iterável.
    Raises:
        Exception: Erro durante a requisição.
    """
    results = []
    try:
        with requests.Session() as s:
            while url:
                response = s.get(url, timeout=10, headers={"accept": "application/json"})
                response.raise_for_status()
                json_response = response.json()

                dados = json_response.get("dados", [])
                if dados:
                    for item in dados:
                        results.append(json.dumps(item))

                links = json_response.get("links", [])
                next_url = None

                for link in links:
                    if link["rel"] == "next":
                        next_url = link["href"]
                        break

                if next_url and next_url != url:
                    url = next_url
                    time.sleep(2) # Espera 2 segundos até a próxima requisição, visando eliminar 429 - Too Many Requests.
                else:
                    url = None
    except Exception as e:
        logging.error(e)
    
    return iter(results)

def fetch_data_v2(partition: RDD, column_name: str, url: str, endpoint : str = None, id_param_name : str = None, **kwargs):
    final_url = url
    for row in partition:
        url_pattern = url_manipulation \
            .detect_url_pattern(
                                    endpoint=endpoint, 
                                    id_param_name=id_param_name
                                )
        logging.info(f"PATTERN DETECTED: {url_pattern}")

        if url_pattern == 1:
            final_url = url_manipulation.build_url_pattern_1(url, row[column_name], endpoint, id_param_name, kwargs)
        elif url_pattern == 2:
            final_url = url_manipulation.build_url_pattern_2(url, row[column_name], endpoint, kwargs)
        else:
            continue

        yield from make_requests(final_url)