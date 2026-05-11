from pyspark.sql import SparkSession as ss
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fetch_data as fd

def main():
    session = ss.builder.appName("Fetch Expenses Data").getOrCreate()
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/fetch_data.py")
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/url_manipulation.py")
    df_deputies = session.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")

    # selecionar apenas as despesas referentes a última legislatura e apenas deputados do estado do Paraná, 
    # visando delimitar o escopo da análise e adaptação a capacidade de processamento local

    df_deputies_filtered = df_deputies.filter((col("siglaUf") == 'PR') & (col("idLegislatura") == 57)).dropDuplicates(["id"])
    df_query = df_deputies_filtered.select("id")
    # df_query.withColumn("count", lit(df_query.count())).limit(1).select('count').show()
    

    # df = session.read.option("multiLine", "true") \
    # .json(
    #     df_query \
    #     .rdd \
    #     .repartition(1) \
    #     .mapPartitions(
    #         lambda i : fd.fetch_data_v2(i, "id", "https://dadosabertos.camara.leg.br/api/v2/deputados",
    #                                     endpoint="/despesas/", idLegislatura=57)
    #     )
    # )

    expenses_rdd = df_query.rdd.repartition(2).mapPartitions(lambda i : fd.fetch_partitioned_data(i, "https://dadosabertos.camara.leg.br/api/v2/deputados", "despesas", idLegislatura=57, itens=100, id_deputado=''))
    schema_expenses = StructType([
    # StructField("id_deputado", IntegerType(), True),
    StructField("ano", IntegerType(), True),
    StructField("mes", IntegerType(), True),
    StructField("tipoDespesa", StringType(), True),
    StructField("codDocumento", StringType(), True),
    StructField("tipoDocumento", StringType(), True),
    StructField("codTipoDocumento", IntegerType(), True),
    StructField("dataDocumento", StringType(), True),
    StructField("numDocumento", StringType(), True),
    StructField("valorDocumento", DoubleType(), True),
    StructField("urlDocumento", StringType(), True),
    StructField("nomeFornecedor", StringType(), True),
    StructField("cnpjCpfFornecedor", StringType(), True),
    StructField("valorLiquido", DoubleType(), True),
    StructField("valorGlosa", DoubleType(), True),
    StructField("numRessarcimento", StringType(), True),
    StructField("codLote", IntegerType(), True),
    StructField("parcela", IntegerType(), True)
])
    df = session.read.option("multiLine", "true").json(expenses_rdd, schema=schema_expenses)
    
    df = df.withColumn("ingestion_date", current_timestamp())
    df.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")
    session.stop()

if __name__ == '__main__':
    main()
