from pyspark.sql import SparkSession
from pyspark.core.rdd import RDD
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
                    time.sleep(2)
                else:
                    url = None
                # if links:
                #     ind_self = 0
                #     ind_next = 0
                #     ind_first = 0
                #     ind_last = 0

                #     for ind, link in enumerate(links):
                #         if link["rel"] == 'self':
                #             ind_self = ind
                #         if link["rel"] == 'next':
                #             ind_next = ind
                #         if link["rel"] == 'first':
                #             ind_first = ind
                #         if link["rel"] == 'last':
                #             ind_last = ind

                #     if links[ind_first]["href"] == links[ind_last]["href"] or \
                #         links[ind_self]["href"] == links[ind_last]["href"]:
                #         break
                #     else:
                #         url = links[ind_next]["href"]

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

def to_RDD(spark: SparkSession, iterable) -> RDD[any]:
    return spark.sparkContext.parallelize(iterable)

def main():
    spark = SparkSession.builder.appName("Teste").getOrCreate()

    df_legislaturas = spark.read.option("multiLine", "true") \
    .json(
        to_RDD(spark, make_requests("https://dadosabertos.camara.leg.br/api/v2/legislaturas"))
    )

    df_legislaturas = df_legislaturas.withColumn("ingestion_date", current_timestamp())
    df_legislaturas.show(57)


    df_deputados = spark.read.option("multiLine", "true") \
    .json(
            df_legislaturas.select("id") \
            .rdd \
            .repartition(1) \
                .mapPartitions(
                        lambda i : fetch_data_v2(i, "id", "https://dadosabertos.camara.leg.br/api/v2", \
                                                "deputados", "idLegislatura")
                        )
        )
    
    df_deputados = df_deputados.withColumn("ingestion_date", current_timestamp())
    df_ex = df_deputados.select("idLegislatura").distinct().orderBy("idLegislatura", ascending=False)
    df_ex.show(57)



    # rdd_partidos = df_legislaturas.select("id") \
    #     .repartition(2) \
    #     .rdd.mapPartitions(lambda i : 
    #                        fetch_data_based_on_attribute(i, "id", "https://dadosabertos.camara.leg.br/api/v2/partidos?", ordem="ASC", ordenarPor="id", idLegislatura="")
    #                        )
    # df_partidos = spark.read.option("multiLine", "true").json(rdd_partidos)
    # df_partidos = df_partidos.dropDuplicates(["id"])
    # df_partidos.show(15)

    # rdd_despesas = df_deputados.filter(col("idLegislatura") == 57) \
    # .repartition(2) \
    # .rdd.mapPartitions(lambda i : 
    #                        fetch_data_based_on_attribute(i, "id", f"https://dadosabertos.camara.leg.br/api/v2/deputados", endpoint='/despesas?', ordem="ASC", ordenarPor="id", idLegislatura='')
    # )
    # df_despesas = spark.read.option("multiLine", "true").json(rdd_despesas)
    # df_despesas.show(15)
    # spark.stop()
            
if __name__ == '__main__':
    main()