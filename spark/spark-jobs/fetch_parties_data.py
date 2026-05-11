from pyspark.sql import SparkSession as ss
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fetch_data as fd

def main():
    session = ss.builder.appName("Fetch Parties Data").getOrCreate()
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/fetch_data.py")
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/url_manipulation.py")
    df_legislatures = session.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
    df = session.read.option("multiLine", "true") \
    .json(
            df_legislatures.select("id") \
            .rdd \
            .repartition(1) \
                .mapPartitions(
                        lambda i : fd.fetch_data_v2(i, "id", "https://dadosabertos.camara.leg.br/api/v2", \
                                                "partidos", "idLegislatura")
                        )
        )
    
    df = df.withColumn("ingestion_date", current_timestamp())
    df.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")
    session.stop()

if __name__ == '__main__':
    main()