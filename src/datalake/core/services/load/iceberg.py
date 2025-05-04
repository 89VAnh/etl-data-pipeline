from datalake.core.services.load import Load
from datalake.base.utils.logger import Logger
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col

LOGGER = Logger.get_logger("IcebergLoadImpl")


class IcebergLoadImpl(Load):
    def process(self):
        table = f"glue_catalog.{self.load_meta.target_database}.{self.load_meta.target_schema}"
        path = f"{self.load_meta.target_zone}/{self.load_meta.target_database}/{self.load_meta.target_schema}"

        try:
            existing_df = self.spark.table(table)
            schema_matches = existing_df.schema == self.df.schema

            if not schema_matches:
                self.spark.sql(
                    f"""
                    ALTER TABLE {table} 
                    ADD COLUMNS ({self._get_schema_diff(existing_df, self.df)})
                """
                )

        except AnalysisException:
            self.df.writeTo(table).tableProperty("location", path).partitionedBy(
                "year", "month", "day"
            ).createOrReplace()
            return

        writer = writer.partitionBy(["year", "month", "day"])

        writer.save(path)
