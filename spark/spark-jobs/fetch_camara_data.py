from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests, json, logging

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

def fetch_data_based_on_attribute(partition, column_name: str, url: str):
    results = []
    headers = {"accept": "application/json"}
    
    with requests.Session() as s:
        for row in partition:
            value = row[column_name]
            try:
                response = s.get(f"{url}{value}", timeout=10, headers=headers)
                response.raise_for_status()
                json_response = response.json()
                dados = json_response.get("dados", [])
                for item in dados:
                    results.append(json.dumps(item))
            except Exception as e:
                logging.error(e)
    
    return iter(results)

def main():
    spark = SparkSession.builder.appName("Teste").getOrCreate()

    legislaturas = fetch_data("https://dadosabertos.camara.leg.br/api/v2/legislaturas?&ordem=ASC&ordenarPor=id&itens=300")
    rdd_legislaturas = spark.sparkContext.parallelize([json.dumps(item) for item in legislaturas["dados"]])
    df_legislaturas = spark.read.option("multiLine", "true").json(rdd_legislaturas)
    df_legislaturas.show(15)

    rdd_deputados = df_legislaturas.select("id") \
        .repartition(2) \
        .rdd.mapPartitions(lambda i : 
                           fetch_data_based_on_attribute(i, "id", "https://dadosabertos.camara.leg.br/api/v2/deputados?&ordem=ASC&ordenarPor=id&idLegislatura=")
                           )
    df_deputados = spark.read.option("multiLine", "true").json(rdd_deputados)
    df_deputados = df_deputados.dropDuplicates(["id"])
    df_deputados.show(15)

    rdd_partidos = df_legislaturas.select("id") \
        .repartition(2) \
        .rdd.mapPartitions(lambda i : 
                           fetch_data_based_on_attribute(i, "id", "https://dadosabertos.camara.leg.br/api/v2/partidos?ordem=DESC&ordenarPor=id&itens=300&idLegislatura=")
                           )
    df_partidos = spark.read.option("multiLine", "true").json(rdd_partidos)
    df_partidos = df_partidos.dropDuplicates(["id"])
    df_partidos.show(15)

    # deputados = fetch_data("https://dadosabertos.camara.leg.br/api/v2/deputados?&ordem=ASC&ordenarPor=id")
    # rdd_deputados = spark.sparkContext.parallelize([json.dumps(item) for item in deputados["dados"]])
    # df_deputados = spark.read.option("multiLine", "true").json(rdd_deputados)
    # df_deputados.show(5)

    spark.stop()
            
if __name__ == '__main__':
    main()