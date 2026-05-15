from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class GoldLayer:
    def __init__(self):
        self.spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        print("---------------------------------INICIALIZANDO SESSÃO DO PYSPARK---------------------------------")
        self.df_legislatures = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/legislature_data/").select("id", "dataInicio", "dataFim").withColumn("sk_legislatura", expr("uuid()"))
        self.df_parties = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/parties_data/").select("sigla", "nome", "situacao", "total_membros", "total_posse", "urlLogo").withColumn("sk_partido", expr("uuid()"))
        self.df_deputies = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/deputies_data/").select("id", "nomeCivil", "nome_eleitoral", "id_ultima_legislatura", 
                                                        "ultima_filiacao", "foto", "situacao_atual", "condicao_eleitoral_atual",
                                                        "dataNascimento", "dataFalecimento", "ufNascimento", "municipioNascimento", "escolaridade") \
                                                        .withColumn("sk_deputado", expr("uuid()")) 
        self.df_expenses = self.spark.read.parquet("s3a://personalprojects/chamber_project/silver_layer/expenses_data/").select("id_deputado1", "id_legislatura", "ano1", "mes1", 
                                       "dataDocumento", "tipoDespesa", "valorDocumento", 
                                       "urlDocumento", "nomeFornecedor", "cnpjCpfFornecedor", "valorLiquido",
                                       "valorGlosa") \
                                       .withColumn("sk_despesa", expr("uuid()"))
    
    def create_relationships_deputies(self):
        legs = self.df_legislatures.select("id", "sk_legislatura").withColumnRenamed("id", "id_legs")
        part = self.df_parties.select("sigla", "sk_partido").withColumnRenamed("sigla", "sigla2")
        new_df = self.df_deputies.join(broadcast(legs), self.df_deputies["id_ultima_legislatura"]==legs["id_legs"], "left") \
        .join(broadcast(part), self.df_deputies["ultima_filiacao"]==part["sigla2"], "left")
        new_df = new_df.drop("id_legs", "sigla2", "id_ultima_legislatura", "ultima_filiacao")

        return new_df

    def create_dimensions(self):
        exp_type = self.df_expenses.select("tipoDespesa").dropDuplicates()
        exp_type = exp_type.withColumn("sk_tipo_despesa", expr("uuid()"))

        providers = self.df_expenses.select("nomeFornecedor", "cnpjCpfFornecedor").dropDuplicates(["nomeFornecedor"])
        providers = providers.withColumn("sk_fornecedor", expr("uuid()"))

        return exp_type, providers

    def build_fact(self):
        self.exp, self.prov = self.create_dimensions()
        self.exp = self.exp.withColumnsRenamed({"tipoDespesa" : "tipoDespesa2"})
        self.prov = self.prov.withColumnsRenamed({"nomeFornecedor" : "nomeFornecedor2", "cnpjCpfFornecedor" : "cnpjCpfFornecedor2"})
        dep = self.df_deputies.withColumnsRenamed({"id" : "id_dep2"})
        leg = self.df_legislatures.withColumnRenamed("id", "id_leg2")
        self.df_expenses = self.df_expenses.join(broadcast(self.exp), self.df_expenses["tipoDespesa"] == self.exp["tipoDespesa2"], "left") \
        .join(broadcast(self.prov), self.df_expenses["nomeFornecedor"] == self.prov["nomeFornecedor2"], "left") \
        .join(broadcast(dep), self.df_expenses["id_deputado1"] == dep["id_dep2"], "left") \
        .join(broadcast(leg), self.df_expenses["id_legislatura1"] == leg["id_leg2"])

        self.df_expenses = self.df_expenses.select("sk_despesa", "sk_deputado", "sk_tipo_despesa", 
                                                   "sk_fornecedor", "ano1", "mes1", "dataDocumento", "valorDocumento",
                                                   "valorLiquido", "valorGlosa").withColumnsRenamed({"dataDocumento" : "data_despesa", "valorDocumento" : "valor_documento",
                                                                                                     "valorLiquido" : "valor_debitado", "valorGlosa" : "valor_glosa"})
        
        self.df_expenses.write.mode("overwrite").format("delta").save("s3a://personalprojects/chamber_project/gold_layer/expenses_data/providers")

if __name__ == '__main__':
    gold = GoldLayer()