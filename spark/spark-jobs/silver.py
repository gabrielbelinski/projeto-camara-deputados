from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, collect_list, array_distinct

class SilverLayer():
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        print("---------------------------------INICIALIZANDO SESSÃO DO PYSPARK---------------------------------")
        #self.df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        #self.df_parties = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")
        self.df_deputies_legislature = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_legislature_data/")
        self.df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        #self.df_expenses = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")

    def legislatures_manipulation(self):
        print("Entidade: Legislaturas")
        self.df_legislatures = self.df_legislatures.drop(col("uri"))
        self.df_legislatures = self.df_legislatures.withColumn("dataInicio", expr("try_cast(dataInicio as DATE)"))
        self.df_legislatures = self.df_legislatures.withColumn("dataFim", expr("try_cast(dataFim as DATE)"))

        print("Transformação realizada com sucesso.")
        self.df_legislatures.show(5, False)

        self.df_legislatures.write.mode("append").parquet("s3a://personalprojects/chamber_project/silver_layer/legislature_data/")
        print("Armazenamento concluído.")

    def parties_manipulation(self):
        print("Entidade: Partidos")
        self.df_parties.printSchema()
        self.df_parties.show(105, True)
        # print("Transformação realizada com sucesso.")
        # self.df_parties.write.mode("append").parquet("s3a://personalprojects/chamber_project/silver_layer/parties_data/")
        # print("Armazenamento concluído.")
        self.df_parties = self.df_parties.withColumn("situacao", col("status.situacao")) \
        .withColumn("total_membros", col("status.totalMembros")) \
        .withColumn("total_posse", col("status.totalPosse")) \
        .drop("status", "lider", "numeroEleitoral", "urlWebSite", "urlFacebook", "uri")
        print("Transformação realizada com sucesso.")
        self.df_parties.show(105, False)
        self.df_parties.write.mode("append").parquet("s3a://personalprojects/chamber_project/silver_layer/parties_data/")
        print("Armazenamento concluído.")

    def deputies_manipulation(self):
        print("Entidade: Deputados")
        self.df_deputies_legislature = self.df_deputies_legislature


if __name__ == '__main__':
    silver = SilverLayer()
    # silver.legislatures_manipulation()
    #silver.parties_manipulation()
    silver.deputies_manipulation()