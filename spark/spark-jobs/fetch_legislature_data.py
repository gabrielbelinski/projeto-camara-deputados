from pyspark.sql import SparkSession as ss
from pyspark.core.rdd import RDD
from pyspark.sql.functions import *
from pyspark.sql.types import *
import fetch_data as fd

def to_RDD(spark: ss, iterable) -> RDD[any]:
    return spark.sparkContext.parallelize(iterable)

def main():
    
    session = ss.builder.appName("Fetch Legislature Data").getOrCreate()
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/fetch_data.py")
    session.sparkContext.addPyFile("/opt/spark/spark-jobs/url_manipulation.py")

    df = session.read.option("multiLine", "true") \
    .json(
        to_RDD(session, fd.make_requests("https://dadosabertos.camara.leg.br/api/v2/legislaturas"))
    )
    df = df.withColumn("ingestion_date", current_timestamp())
    df.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
    session.stop()

if __name__ == '__main__':
    main()