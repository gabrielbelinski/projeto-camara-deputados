from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class GoldLayer:
    def __init__(self):
        self.spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        print("---------------------------------INICIALIZANDO SESSÃO DO PYSPARK---------------------------------")
        self.df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/legislature_data/")
        self.df_parties = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/parties_data/")
        self.df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/deputies_data/")
        self.df_expenses = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/expenses_data/")

    def legislatures(self):
        return self.df_legislatures.select("id", "dataInicio", "dataFim").withColumn("sk_legislatura", expr("uuid()"))
    def parties(self):
        return self.df_parties.select("sigla", "nome", "situacao", "total_membros", "total_posse", "urlLogo").withColumn("sk_partido", expr("uuid()"))
    def deputies(self):
        return self.df_deputies.select("id", "nomeCivil", "nome_eleitoral", "id_ultima_legislatura", 
                                                        "ultima_filiacao", "foto", "situacao_atual", "condicao_eleitoral_atual",
                                                        "dataNascimento", "dataFalecimento", "ufNascimento", "municipioNascimento", "escolaridade") \
                                                        .withColumn("sk_deputado", expr("uuid()")) 
    def expenses(self):
        return self.df_expenses.select("id_deputado1", "id_legislatura", "ano1", "mes1", 
                                       "dataDocumento", "tipoDespesa", "valorDocumento", 
                                       "urlDocumento", "nomeFornecedor", "cnpjCpfFornecedor", "valorLiquido",
                                       "valorGlosa") \
                                       .withColumn("sk_despesa", expr("uuid()"))
    
    def create_dimensions(self):
        leg = self.legislatures()
        part = self.parties()
        dep = self.deputies()
        exp = self.expenses()

        exp_type = exp.select("tipoDespesa").dropDuplicates()
        exp_type = exp_type.withColumn("sk_tipo_despesa", expr("uuid()"))

        dealer = exp.select("nomeFornecedor", "cnpjCpfFornecedor").dropDuplicates(["cnpjCpfFornecedor"])
        dealer = dealer.withColumn("sk_fornecedor", expr("uuid()"))
