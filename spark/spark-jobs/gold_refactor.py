from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col

class GoldLayer:

    SILVER_PATH = "s3a://personalprojects/chamber_project/silver_layer/"
    GOLD_PATH = "s3a://personalprojects/chamber_project/gold_layer/"

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC-3")
            .getOrCreate()
        )

        print("Inicializando Gold Layer")

        self._load_dataframes()

    def _load_dataframes(self):

        self.df_legislatures = (
            self.spark.read.parquet(
                f"{self.SILVER_PATH}/legislature_data/"
            )
            .select(
                "id",
                "dataInicio",
                "dataFim"
            )
            .withColumn(
                "sk_legislatura",
                F.xxhash64("id")
            )
            .cache()
        )

        self.df_parties = (
            self.spark.read.parquet(
                f"{self.SILVER_PATH}/parties_data/"
            )
            .select(
                "sigla",
                "nome",
                "situacao",
                "total_membros",
                "total_posse",
                "urlLogo"
            )
            .withColumn(
                "sk_partido",
                F.xxhash64("sigla")
            )
            .cache()
        )

        self.df_deputies = (
            self.spark.read.parquet(
                f"{self.SILVER_PATH}/deputies_data/"
            )
            .select(
                "id",
                "nomeCivil",
                "nome_eleitoral",
                "id_ultima_legislatura",
                "ultima_filiacao",
                "foto",
                "situacao_atual",
                "condicao_eleitoral_atual",
                "dataNascimento",
                "dataFalecimento",
                "ufNascimento",
                "municipioNascimento",
                "escolaridade"
            )
            .withColumn(
                "sk_deputado",
                F.xxhash64("id")
            )
            .cache()
        )

        self.df_expenses = (
            self.spark.read.parquet(
                f"{self.SILVER_PATH}/expenses_data/"
            )
            .select(
                "id_deputado1",
                "id_legislatura",
                "ano1",
                "mes1",
                "dataDocumento",
                "tipoDespesa",
                "valorDocumento",
                "urlDocumento",
                "nomeFornecedor",
                "cnpjCpfFornecedor",
                "valorLiquido",
                "valorGlosa"
            )
            .withColumn(
                "sk_despesa",
                F.xxhash64(
                    "id_deputado1",
                    "dataDocumento",
                    "valorDocumento"
                )
            )
        )

    def create_relationships_deputies(self):

        self.df_deputies = self.df_deputies.alias("dep") \
            .join(
                broadcast(self.df_legislatures.alias("leg")),
                F.col("dep.id_ultima_legislatura") == F.col("leg.id"),
                "left"
            ) \
            .join(
                broadcast(self.df_parties.alias("par")),
                F.col("dep.ultima_filiacao") == F.col("par.sigla"),
                "left"
            ) \
            .select(
                "dep.*",
                "leg.sk_legislatura",
                "par.sk_partido"
            )

    def create_dimensions(self):

        self.dim_expense_type = (
            self.df_expenses
            .select("tipoDespesa")
            .dropDuplicates()
            .withColumn(
                "sk_tipo_despesa",
                F.xxhash64("tipoDespesa")
            )
        )

        self.dim_provider = (
            self.df_expenses
            .select(
                "nomeFornecedor",
                "cnpjCpfFornecedor"
            )
            .dropDuplicates(
                ["nomeFornecedor", "cnpjCpfFornecedor"]
            )
            .withColumn(
                "sk_fornecedor",
                F.xxhash64(
                    "nomeFornecedor",
                    "cnpjCpfFornecedor"
                )
            )
        )

    def build_fact(self):
        self.fact_expenses = (
            self.df_expenses.alias("exp")

            .join(
                broadcast(self.dim_expense_type.alias("det")),
                F.col("exp.tipoDespesa") == F.col("det.tipoDespesa"),
                "left"
            )

            .join(
                broadcast(self.dim_provider.alias("prov")),
                (
                    F.col("exp.nomeFornecedor") == F.col("prov.nomeFornecedor")
                ) &
                (
                    F.col("exp.cnpjCpfFornecedor") == F.col("prov.cnpjCpfFornecedor")
                ),
                "left"
            )

            .join(
                broadcast(self.df_deputies.alias("dep")),
                F.col("exp.id_deputado1") == F.col("dep.id"),
                "left"
            )

            .join(
                broadcast(self.df_legislatures.alias("leg")),
                F.col("exp.id_legislatura") == F.col("leg.id"),
                "left"
            )

            .select(
                "exp.sk_despesa",
                "dep.sk_deputado",
                "leg.sk_legislatura",
                "det.sk_tipo_despesa",
                "prov.sk_fornecedor",

                F.col("exp.ano1").alias("ano"),
                F.col("exp.mes1").alias("mes"),

                F.col("exp.dataDocumento").alias("data_despesa"),

                F.col("exp.valorDocumento").alias("valor_documento"),

                F.col("exp.valorLiquido").alias("valor_liquido"),

                F.col("exp.valorGlosa").alias("valor_glosa")
            )
        )

    def write_dataframes(self):
        self.df_legislatures.write.mode("overwrite").format("delta").save(f"{self.GOLD_PATH}/legislature_data/")
        self.df_parties.write.mode("overwrite").format("delta").save(f"{self.GOLD_PATH}/parties_data/")
        self.df_deputies.write.mode("overwrite").format("delta").save(f"{self.GOLD_PATH}/deputies_data/")
        self.dim_expense_type.write.mode("overwrite").format("delta").save(f"{self.GOLD_PATH}/expenses_data/expense_type")
        self.dim_provider.write.mode("overwrite").format("delta").save(f"{self.GOLD_PATH}/expenses_data/providers")
        
        self.fact_expenses.withColumns({"ano_despesa" :col("ano"), "mes_despesa" : col("mes")}).write \
        .mode("overwrite") \
        .partitionBy("ano", "mes") \
        .format("delta") \
        .save(f"{self.GOLD_PATH}/fact_expenses/expenses") 

if __name__ == "__main__":

    gold = GoldLayer()
    gold._load_dataframes()
    gold.create_relationships_deputies()
    gold.create_dimensions()
    gold.build_fact()
    gold.write_dataframes()
