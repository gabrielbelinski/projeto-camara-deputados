from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, upper

class SilverLayer():
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        print("---------------------------------INICIALIZANDO SESSÃO DO PYSPARK---------------------------------")
        self.df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/legislature_data/")
        self.df_parties = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/parties_data/")
        self.df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/deputies_data/")
        self.df_expenses = self.spark.read.parquet("s3a://personalprojects/chamber_project/bronze_layer/expenses_data/")

    def legislatures_manipulation(self):
        print("Entidade: Legislaturas")
        self.df_legislatures = self.df_legislatures.withColumn("dataInicio", expr("try_cast(dataInicio as DATE)"))
        self.df_legislatures = self.df_legislatures.withColumn("dataFim", expr("try_cast(dataFim as DATE)"))

        print("Transformação realizada com sucesso.")

        self.df_legislatures.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/silver_layer/legislature_data/")
        print("Armazenamento concluído.")

    def parties_manipulation(self):
        print("Entidade: Partidos")

        self.df_parties = self.df_parties.withColumn("situacao", col("status.situacao")) \
        .withColumn("total_membros", col("status.totalMembros")) \
        .withColumn("total_posse", col("status.totalPosse")) \
        .drop("status", "lider", "numeroEleitoral", "urlWebSite", "urlFacebook", "uri")
        print("Transformação realizada com sucesso.")

        self.df_parties.write.mode("overwrite").parquet("s3a://personalprojects/chamber_project/silver_layer/parties_data/")
        print("Armazenamento concluído.")

    def deputies_manipulation(self):
        print("Entidade: Deputados")

        self.df_deputies = self.df_deputies.withColumn("nomeCivil", upper(col("nomeCivil"))) \
        .withColumn("nome_eleitoral", upper(col("ultimoStatus.nomeEleitoral"))) \
        .withColumn("nome", upper(col("ultimoStatus.nome"))) \
        .withColumn("id_ultima_legislatura", col("ultimoStatus.idLegislatura")) \
        .withColumn("ultima_filiacao", col("ultimoStatus.siglaPartido")) \
        .withColumn("foto", col("ultimoStatus.urlFoto")) \
        .withColumn("situacao_atual", col("ultimoStatus.situacao")) \
        .withColumn("condicao_eleitoral_atual", col("ultimoStatus.condicaoEleitoral")) \
        .withColumn("dataNascimento", expr("try_cast(dataNascimento as DATE)")) \
        .withColumn("dataFalecimento", expr("try_cast(dataFalecimento as DATE)"))

        print("Transformação realizada com sucesso.")

        self.df_deputies.withColumn("id_ultima_legislatura1", col("id_ultima_legislatura")) \
        .withColumn("ultima_filiacao1", col("ultima_filiacao")).write \
        .partitionBy("id_ultima_legislatura", "ultima_filiacao") \
        .mode("overwrite").parquet("s3a://personalprojects/chamber_project/silver_layer/deputies_data/")

        print("Armazenamento concluído.")

    def expenses_manipulation(self):
        print("Entidade: Despesas")

        self.df_expenses = self.df_expenses.withColumn("dataDocumento", expr("try_cast(dataDocumento as TIMESTAMP)"))

        print("Transformação realizada com sucesso.")

        self.df_expenses.withColumn("id_deputado1", col("id_deputado")) \
        .withColumn("ano1", col("ano")) \
        .withColumn("mes1", col("mes")).write \
        .partitionBy("id_deputado, ano, mes") \
        .mode("overwrite").parquet("s3a://personalprojects/chamber_project/silver_layer/expenses_data/")
        print("Armazenamento concluído.")

if __name__ == '__main__':
    silver = SilverLayer()
    silver.legislatures_manipulation()
    silver.parties_manipulation()
    silver.deputies_manipulation()
    silver.expenses_manipulation()