from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, ShortType, ByteType, TimestampType, FloatType, IntegerType, ArrayType
from pyspark.sql.functions import col, sha2, concat_ws, expr, array_distinct, collect_list, broadcast
import fetch_client as f, url_builders as u
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--entity", choices=['legislatures', 'parties', 'deputies', 'expenses'], type=str, required=True)
args = parser.parse_args()

def build_partition_udf(url=None, params_builder=None, uri_field=None):
    def partition_udf(rows):
        with f.FetchClient() as client:
            for row in rows:
                if url:
                    params = params_builder(row) if params_builder else {}
                    yield from client.paginate(url, params=params)
                elif uri_field:
                    yield from client.paginate(row[uri_field])

    return partition_udf

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

        df_legislatures_data = self.spark.createDataFrame(data, schema)
        print("Dataframe de legislatura criado. Iniciando operação de armazenamento...")
        
        df_legislatures_data.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

    def fetch_parties_data(self):

        df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        df_legislatures = df_legislatures.filter((col("id")) > 54).select("id")

        ids = [row.id for row in df_legislatures.collect()]

        print("Obtendo dados de partidos...")

        url = u.build_parties_url()
        
        with f.FetchClient() as client:
            data_id_parties = list(client.paginate(url, params={"idLegislatura": ','.join(map(str, ids)),"itens": 100}))

        df_id_parties = self.spark.createDataFrame(data_id_parties, StructType([
            StructField("id", LongType(), True),
            StructField("uri", StringType(), True)
        ]))

        parties_partition_udf = build_partition_udf(uri_field="uri")

        rdd_parties = df_id_parties.rdd.mapPartitions(parties_partition_udf)
        
        schema_parties = StructType([
            StructField("id", LongType(), True),
            StructField("sigla", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("uri", StringType(), True),
            StructField("status", StructType([
            StructField("data", StringType(), True),  # null no exemplo
            StructField("idLegislatura", StringType(), True),
            StructField("situacao", StringType(), True),
            StructField("totalPosse", StringType(), True),
            StructField("totalMembros", StringType(), True),
            StructField("uriMembros", StringType(), True),
            StructField("lider", StructType([
                StructField("uri", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("siglaPartido", StringType(), True),
                StructField("uriPartido", StringType(), True),
                StructField("uf", StringType(), True),
                StructField("idLegislatura", IntegerType(), True),
                StructField("urlFoto", StringType(), True),
                ]), True),
            ]), True),
            StructField("numeroEleitoral", StringType(), True),
            StructField("urlLogo", StringType(), True),
            StructField("urlWebSite", StringType(), True),
            StructField("urlFacebook", StringType(), True),
        ])

        df_parties = self.spark.createDataFrame(rdd_parties, schema_parties)
        print("Dataframe de partidos criado. Iniciando operação de armazenamento...")

        df_parties.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")


    def fetch_deputies_data(self):
        print("Obtendo dados de parlamentares...")
        df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        df_legislatures = df_legislatures.filter((col("id")) > 54).select("id")

        ids = [row.id for row in df_legislatures.collect()]

        url = u.build_deputies_url()
        
        with f.FetchClient() as client:
            data_id_deputies = list(client.paginate(url, params={"idLegislatura": ','.join(map(str, ids)),"itens": 100}))

        print("Requisições para a API concluídas.")

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("uri", StringType(), True),
            StructField("idLegislatura", IntegerType(), True)
        ])

        df_deputies_aggregation = self.spark.createDataFrame(data_id_deputies, schema)
        df_deputies_aggregation = df_deputies_aggregation.groupBy("id", "uri").agg(array_distinct(collect_list("idLegislatura")).alias("legislaturas"))
        df_deputies_aggregation = df_deputies_aggregation.dropDuplicates(["id"])

        deputies_partition_udf = build_partition_udf(uri_field="uri")
        rdd_deputies = df_deputies_aggregation.rdd.mapPartitions(deputies_partition_udf)

        df_deputies_aggregation = df_deputies_aggregation.drop("idLegislatura").drop("uri").withColumnRenamed("id", "id_deputado")

        schema_deputies = StructType([
            StructField("id", LongType(), True),
            StructField("uri", StringType(), True),
            StructField("nomeCivil", StringType(), True),
            StructField("ultimoStatus", StructType([
                StructField("id", LongType(), True),
                StructField("uri", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("siglaPartido", StringType(), True),
                StructField("uriPartido", StringType(), True),
                StructField("siglaUf", StringType(), True),
                StructField("idLegislatura", IntegerType(), True),
                StructField("urlFoto", StringType(), True),
                StructField("email", StringType(), True),
                StructField("data", StringType(), True),
                StructField("nomeEleitoral", StringType(), True),
                StructField("gabinete", StructType([
                        StructField("nome", StringType(), True),
                        StructField("predio", StringType(), True),
                        StructField("sala", StringType(), True),
                        StructField("andar", StringType(), True),
                        StructField("telefone", StringType(), True),
                        StructField("email", StringType(), True),
                    ]), True),
                StructField("situacao", StringType(), True),
                StructField("condicaoEleitoral", StringType(), True),
                StructField("descricaoStatus", StringType(), True),
            ]), True),
            StructField("cpf", StringType(), True),
            StructField("sexo", StringType(), True),
            StructField("urlWebsite", StringType(), True),
            StructField(
                "redeSocial",
                ArrayType(StringType(), True),
                True
            ),

            StructField("dataNascimento", StringType(), True),
            StructField("dataFalecimento", StringType(), True),
            StructField("ufNascimento", StringType(), True),
            StructField("municipioNascimento", StringType(), True),
            StructField("escolaridade", StringType(), True),
        ])

        df_deputies = self.spark.createDataFrame(rdd_deputies, schema_deputies)
        df_deputies = df_deputies.join(broadcast(df_deputies_aggregation), (df_deputies["id"]) == (df_deputies_aggregation["id_deputado"]), "inner")
        print("Dataframe de deputados criado. Iniciando operação de armazenamento...")

        df_deputies.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        print("Dados armazenados com sucesso. Obtenção de dados concluída.")

    def fetch_expenses_data(self):
        def expenses_partition_udf(rows):
            with f.FetchClient() as client:
                for row in rows:
                    id_deputado = row["id"]
                    url = u.build_expenses_url(id_deputado)
                    
                    for item in client.paginate(url, params={"idLegislatura": 57, "itens": 100}):
                        yield {
                            **item, "id_deputado" : id_deputado, "id_legislatura": 57
                        }

        df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        #rdd = df_deputies.select("id").rdd.mapPartitions(expenses_partition_udf)
        rdd = df_deputies.filter((col("ultimoStatus.idLegislatura") == 57)  & (col("ultimoStatus.siglaUf") == 'PR')).select("id").rdd.mapPartitions(expenses_partition_udf)
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
    
        df_expenses = self.spark.createDataFrame(rdd, schema)
        print("Dataframe de despesas criado.")
        print("Atribuindo ID individual...")
        df_expenses = df_expenses.withColumn("id_despesa", sha2(concat_ws("|", col("id_deputado"), col("id_legislatura"), col("numDocumento"), col("valorDocumento")), 256))

        print("Iniciando operação de armazenamento...")
        df_expenses.withColumn("ano1", col("ano")) \
            .withColumn("mes1", col("mes")) \
            .write.mode("append") \
            .partitionBy("ano", "mes") \
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
        case 'all':
            job.fetch_legislatures_data()
            job.fetch_parties_data()
            job.fetch_deputies_data()
            job.fetch_expenses_data()
        case _:
            raise NotImplementedError("Opção inválida ou entidade não implementada")
        
    job.spark.stop()