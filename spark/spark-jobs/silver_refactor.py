from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

class SilverLayer:

    BRONZE_PATH = "s3a://personalprojects/chamber_project/bronze_layer"
    SILVER_PATH = "s3a://personalprojects/chamber_project/silver_layer"

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC-3")
            .getOrCreate()
        )

        print("Inicializando Silver Layer")

    def read_parquet(self, entity: str) -> DataFrame:

        return self.spark.read.parquet(
            f"{self.BRONZE_PATH}/{entity}/"
        )

    def write_silver(
        self,
        df: DataFrame,
        entity: str,
        partition_cols: list | None = None
    ) -> None:

        writer = (
            df.write
            .mode("overwrite")
            .format("delta")
            .option("compression", "snappy")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(f"{self.SILVER_PATH}/{entity}/")

    def legislatures_manipulation(self):

        print("Entidade: Legislaturas")

        df = (
            self.read_parquet("legislature_data")
            .select(
                "id",
                F.to_date("dataInicio").alias("dataInicio"),
                F.to_date("dataFim").alias("dataFim")
            )
            .dropDuplicates()
        )

        self.write_silver(df, "legislature_data")

        print("Legislaturas processadas")

    def parties_manipulation(self):

        print("Entidade: Partidos")

        df = (
            self.read_parquet("parties_data")
            .select(
                "id",
                "sigla",
                "nome",
                "urlLogo",

                F.col("status.situacao").alias("situacao"),
                F.col("status.totalMembros").alias("total_membros"),
                F.col("status.totalPosse").alias("total_posse")
            )
            .dropDuplicates()
        )

        self.write_silver(df, "parties_data")

        print("Partidos processados")

    def deputies_manipulation(self):

        print("Entidade: Deputados")

        df = (
            self.read_parquet("deputies_data")
            .select(
                "id",

                F.upper("nomeCivil").alias("nome_civil"),

                F.upper(
                    F.col("ultimoStatus.nomeEleitoral")
                ).alias("nome_eleitoral"),

                F.col(
                    "ultimoStatus.idLegislatura"
                ).alias("id_ultima_legislatura"),

                F.col(
                    "ultimoStatus.siglaPartido"
                ).alias("ultima_filiacao"),

                F.col(
                    "ultimoStatus.urlFoto"
                ).alias("foto"),

                F.col(
                    "ultimoStatus.situacao"
                ).alias("situacao_atual"),

                F.col(
                    "ultimoStatus.condicaoEleitoral"
                ).alias("condicao_eleitoral_atual"),

                F.to_date("dataNascimento").alias(
                    "data_nascimento"
                ),

                F.to_date("dataFalecimento").alias(
                    "data_falecimento"
                ),

                "ufNascimento",
                "municipioNascimento",
                "escolaridade"
            )
            .dropDuplicates(["id"])
        )

        self.write_silver(
            df,
            "deputies_data"
        )

        print("Deputados processados")

    def expenses_manipulation(self):

        print("Entidade: Despesas")

        df = (
            self.read_parquet("expenses_data")
            .select(
                F.col("id_deputado").alias("id_deputado"),

                F.col("id_legislatura"),

                F.col("ano").alias("ano"),

                F.col("mes").alias("mes"),

                F.to_timestamp(
                    "dataDocumento"
                ).alias("data_documento"),

                "tipoDespesa",
                "valorDocumento",
                "urlDocumento",
                "nomeFornecedor",
                "cnpjCpfFornecedor",
                "valorLiquido",
                "valorGlosa"
            )
            .dropDuplicates()
        )

        self.write_silver(
            df,
            "expenses_data",
            partition_cols=["ano", "mes"]
        )

        print("Despesas processadas")


if __name__ == "__main__":

    silver = SilverLayer()
    silver.legislatures_manipulation()
    silver.parties_manipulation()
    silver.deputies_manipulation()
    silver.expenses_manipulation()