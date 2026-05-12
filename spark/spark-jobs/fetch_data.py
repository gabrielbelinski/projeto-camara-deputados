from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, ShortType, ByteType, TimestampType, FloatType
from pyspark.sql.functions import col, sha2, concat_ws
import fetch_client as f, url_builders as u
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--entity", choices=['legislatures', 'parties', 'deputies', 'expenses'], type=str, required=True)
args = parser.parse_args()

class ChamberData:
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/fetch_client.py")
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/retry_session.py")
        self.spark.sparkContext.addPyFile("/opt/spark/spark-jobs/url_builders.py")
        
    def fetch_legislatures_data(self):
        url = u.build_deputies_url()
        with f.FetchClient() as client:
            data = list(client.paginate(url))
        
        schema = StructType([
            StructField("id", ShortType(), False),
            StructField("uri", StringType(), False),
            StructField("dataInicio", StringType(), False),
            StructField("dataFim", StringType(), False)
        ])

        self.df_legislatures_data = self.spark.createDataFrame(data, schema)
        self.df_legislatures_data.write.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")

    def fetch_parties_data(self):
        url = u.build_parties_url()

        with f.FetchClient() as client:
            data = list(client.paginate(url))
        
        schema = StructType([
            StructField("id", LongType(), False),
            StructField("sigla", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("uri", StringType(), True)
        ])

        self.df_parties_data = self.spark.createDataFrame(data, schema)
        self.df_parties_data.write.parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")

    def fetch_deputies_data(self):
        url = u.build_deputies_url()

        def legislatures_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    yield from client.paginate(
                        url, params={"idLegislatura": row["id"]}
                    )

        rdd = self.df_legislatures_data.rdd.mapPartitions(legislatures_partition_udf)

        schema = StructType([
            StructField("id", LongType(), False),
            StructField("uri", StringType(), True),
            StructField("nome", StringType(), False),
            StructField("siglaPartido", StringType(), True),
            StructField("uriPartido", StringType(), True),
            StructField("siglaUf", StringType(), True),
            StructField("idLegislatura", ShortType(), False),
            StructField("urlFoto", StringType(), True),
            StructField("email", StringType(), True)
        ])

        self.df_deputies_data = self.spark.createDataFrame(rdd, schema)
        self.df_deputies_data.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")

    def fetch_expenses_data(self):
        
        def expenses_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    id_deputado = row["id"]
                    url = u.build_expenses_url(id_deputado)

                    for leg in range(54, 58):
                        for item in client.paginate(url, params={"idLegislatura": leg, "itens": 100}):
                            yield {
                                **item, "id_deputado" : id_deputado, "id_legislatura": leg
                            }

        df_filtered = self.df_deputies_data.filter((col("idLegislatura")) >= 54).select("id").dropDuplicates()

        rdd = df_filtered.rdd.mapPartitions(expenses_partition_udf)

        schema = StructType([
            StructField("id_deputado", LongType(), False),
            StructField("id_legislatura", LongType(), False),
            StructField("ano", ShortType(), False),
            StructField("mes", ByteType(), False),
            StructField("tipoDespesa", StringType(), True),
            StructField("codDocumento", LongType(), True),
            StructField("tipoDocumento", StringType(), True),
            StructField("codTipoDocumento", ByteType(), True),
            StructField("dataDocumento", TimestampType(), True),
            StructField("numDocumento", LongType(), True),
            StructField("valorDocumento", FloatType(), True),
            StructField("urlDocumento", StringType(), True),
            StructField("nomeFornecedor", StringType(), True),
            StructField("cnpjCpfFornecedor", StringType(), True),
            StructField("valorLiquido", FloatType(), True),
            StructField("valorGlosa", FloatType(), True),
            StructField("numRessarcimento", ByteType(), True),
            StructField("codLote", LongType(), True),
            StructField("parcela", ShortType(), True)
        ])
    
        self.df_expenses_data = self.spark.createDataFrame(rdd, schema)
        self.df_expenses_data = self.df_expenses_data.withColumn("id_despesa", sha2(concat_ws("|", col("id_deputado"), col("id_legislatura"), col("numDocumento"), col("valorDocumento")), 256))

        self.df_deputies_data.write.mode("append").partitionBy("id_legislatura", "ano", "mes").parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")

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