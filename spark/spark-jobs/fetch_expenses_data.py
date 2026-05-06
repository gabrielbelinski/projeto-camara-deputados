from pyspark.sql import SparkSession as ss
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fetch_data as fd

def main():
    session = ss.builder.appName("Fetch Expenses Data").getOrCreate()
    df_deputies = ss.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")

    # selecionar apenas as despesas referentes a última legislatura e apenas deputados do estado do Paraná, 
    # visando delimitar o escopo da análise e adaptação a capacidade de processamento local

    df_deputies_filtered = df_deputies.filter(col("siglaUf") == 'PR' & col("idLegislatura") == 57).dropDuplicates()

    df = ss.read.option("multiLine", "true") \
    .json(
        df_deputies_filtered.select("id").dropDuplicates(["id"]) \
        .rdd \
        .repartition(1) \
        .mapPartitions(
            lambda i : fd.fetch_data_v2(i, "id", "https://dadosabertos.camara.leg.br/api/v2/deputados",
                                        "despesas", idLegislatura=57)
        )
    )

    df = df.withColumn("ingestion_date", current_timestamp())
    df.write.mode("append").parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")
    session.stop()

if __name__ == '__main__':
    main()
