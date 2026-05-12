from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, ShortType, ByteType, TimestampType, FloatType
from pyspark.sql.functions import col, sha2, concat_ws, expr
import fetch_client as f, url_builders as u
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--entity", choices=['legislatures', 'parties', 'deputies', 'expenses'], type=str, required=True)
args = parser.parse_args()

class ChamberData:
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        print("---------------------------------INICIALIZANDO SESSÃO DO PYSPARK---------------------------------")
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/fetch_client.py")
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/retry_session.py")
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/url_builders.py")
        
    def fetch_legislatures_data(self):
        print("Obtendo dados de legislatura...")
        url = u.build_legislatures_url()

        with f.FetchClient() as client:
            data = list(client.paginate(url, params={"itens": 100}))
        
        print("Requisições para a API concluídas.")
        
        schema = StructType([
            StructField("id", ShortType(), True),
            StructField("uri", StringType(), True),
            StructField("dataInicio", StringType(), True),
            StructField("dataFim", StringType(), True)
        ])

        self.df_legislatures_data = self.spark.createDataFrame(data, schema)
        print("Dataframe de legislatura criado. Iniciando operação de armazenamento...")
        self.df_legislatures_data.write.mode("append").parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

    def fetch_parties_data(self):
        def legislatures_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    yield from client.paginate(
                        url, params={"idLegislatura": row["id"], "itens": 100}
                    )
        print("Obtendo dados de partidos...")
        url = u.build_parties_url()
        df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        rdd = df_legislatures.rdd.mapPartitions(legislatures_partition_udf)
        print("Requisições para a API concluídas.")

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("sigla", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("uri", StringType(), True)
        ])

        self.df_parties_data = self.spark.createDataFrame(rdd, schema)
        print("Dataframe de partidos criado. Iniciando operação de armazenamento...")
        self.df_parties_data.write.mode("append").parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

    def fetch_deputies_data(self):
        def legislatures_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    yield from client.paginate(
                        url, params={"idLegislatura": row["id"], "itens": 100}
                    )
        print("Obtendo dados de parlamentares...")
        url = u.build_deputies_url()
        df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        rdd = df_legislatures.rdd.mapPartitions(legislatures_partition_udf)
        print("Requisições para a API concluídas.")

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("uri", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("siglaPartido", StringType(), True),
            StructField("uriPartido", StringType(), True),
            StructField("siglaUf", StringType(), True),
            StructField("idLegislatura", ShortType(), True),
            StructField("urlFoto", StringType(), True),
            StructField("email", StringType(), True)
        ])

        self.df_deputies_data = self.spark.createDataFrame(rdd, schema)
        print("Dataframe de deputados criado. Iniciando operação de armazenamento...")
        self.df_deputies_data.write.mode("append").parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

    def fetch_expenses_data(self):
        def expenses_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    id_deputado = row["id"]
                    print(f"Obtendo despesas parlamentares de ID: {id_deputado}")
                    url = u.build_expenses_url(id_deputado)

                    # for leg in range(54, 58):
                    #     print(f"Obtendo despesas referentes a legislatura {leg}...")
                    #     for item in client.paginate(url, params={"idLegislatura": leg, "itens": 100}):
                    #         yield {
                    #             **item, "id_deputado" : id_deputado, "id_legislatura": leg
                    #         }

                    for item in client.paginate(url, params={"idLegislatura": 57, "itens": 100, "ano": "2025,2026"}):
                        yield {
                            **item, "id_deputado" : id_deputado, "id_legislatura": 57
                        }

        df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        #df_filtered = df_deputies.filter((col("idLegislatura")) >= 54 ).select("id").dropDuplicates(["id"])
        df_filtered = df_deputies.filter((col("idLegislatura") == 57)  & (col("siglaUf") == 'PR')).select("id").dropDuplicates(["id"])
        rdd = df_filtered.rdd.mapPartitions(expenses_partition_udf)
        print("Requisições para a API concluídas.")

        schema = StructType([
            StructField("id_deputado", LongType(), True),
            StructField("id_legislatura", StringType(), True),
            StructField("ano", ShortType(), True),
            StructField("mes", ByteType(), True),
            StructField("tipoDespesa", StringType(), True),
            StructField("codDocumento", StringType(), True),
            StructField("tipoDocumento", StringType(), True),
            StructField("codTipoDocumento", StringType(), True),
            StructField("dataDocumento", StringType(), True),
            StructField("numDocumento", StringType(), True),
            StructField("valorDocumento", FloatType(), True),
            StructField("urlDocumento", StringType(), True),
            StructField("nomeFornecedor", StringType(), True),
            StructField("cnpjCpfFornecedor", StringType(), True),
            StructField("valorLiquido", FloatType(), True),
            StructField("valorGlosa", FloatType(), True),
            StructField("numRessarcimento", StringType(), True),
            StructField("codLote", LongType(), True),
            StructField("parcela", ShortType(), True)
        ])
    
        self.df_expenses_data = self.spark.createDataFrame(rdd, schema)
        print("Dataframe de despesas criado.")
        print("Atribuindo ID individual...")
        self.df_expenses_data = self.df_expenses_data.withColumn("id_despesa", sha2(concat_ws("|", col("id_deputado"), col("id_legislatura"), col("numDocumento"), col("valorDocumento")), 256))
        print("Iniciando operação de armazenamento...")
        self.df_expenses_data.withColumn("id_legislatura1", col("id_legislatura")) \
            .withColumn("ano1", col("ano")) \
            .withColumn("mes1", col("mes")) \
            .write.mode("append") \
            .partitionBy("id_legislatura", "ano", "mes") \
            .parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

if __name__ == '__main__':
    job = ChamberData()
    
    match args.entity:
        case 'legislatures':
            job.fetch_legislatures_data()
        case 'parties':
            job.fetch_parties_data()
        case 'deputies':
            job.fetch_deputies_data()
        case 'expenses':
            job.fetch_expenses_data()
        case _:
            raise NotImplementedError("Opção inválida ou entidade não implementada")
        
    job.spark.stop()