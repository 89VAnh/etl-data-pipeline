from pyspark.sql import DataFrame

from datalake.base.meta.meta_job import ExtractMeta, LoadMeta
from datalake.base.enums.source import SourceType, SourceSubType
from datalake.base.enums.target import TargetType, TargetSubType
from datalake.base.utils.logger import Logger
from datalake.core.services.extract.db import DbExtract
from datalake.core.services.transform import Transform

LOGGER = Logger.get_logger("ETLFactory")


class ETLFactory:

    @staticmethod
    def extract(extract_meta: ExtractMeta, spark, args: dict):
        match extract_meta.source_type:
            case SourceType.S3:
                match extract_meta.source_sub_type:
                    case SourceSubType.CSV:
                        from datalake.core.services.extract.s3_csv import CSVExtract

                        extract = CSVExtract(
                            extract_meta=extract_meta, spark=spark, args=args
                        )
                    case SourceSubType.JSON:
                        from datalake.core.services.extract.s3_json import JSONExtract

                        extract = JSONExtract(
                            extract_meta=extract_meta, spark=spark, args=args
                        )
                    case SourceSubType.PARQUET:
                        from datalake.core.services.extract.s3_parquet import (
                            ParquetExtract,
                        )

                        extract = ParquetExtract(
                            extract_meta=extract_meta, spark=spark, args=args
                        )
                    case _:
                        raise Exception(
                            f"[extract] dont support source_type={extract_meta.source_type} source_sub_type={extract_meta.source_sub_type}"
                        )
            case SourceType.DATABASE:
                extract = DbExtract(extract_meta=extract_meta, spark=spark, args=args)
            case _:
                raise Exception(
                    f"[extract] dont support source_type={extract_meta.source_type}"
                )
        return extract.process()

    @staticmethod
    def load(spark, dataframe, load_meta: LoadMeta):
        match load_meta.target_type:
            case TargetType.S3:
                match load_meta.target_sub_type:
                    case TargetSubType.CSV:
                        from datalake.core.services.load.csv import CSVLoadImpl

                        load = CSVLoadImpl(
                            spark=spark, dataframe=dataframe, load_meta=load_meta
                        )
                    case TargetSubType.PARQUET:
                        from datalake.core.services.load.parquet import ParquetLoadImpl

                        load = ParquetLoadImpl(
                            spark=spark, dataframe=dataframe, load_meta=load_meta
                        )
                    case _:
                        raise Exception(
                            f"[load] dont support target_type={load_meta.target_type} target_sub_type={load_meta.target_sub_type}",
                        )
            case TargetType.ICEBERG:
                from datalake.core.services.load.iceberg import IcebergLoadImpl

                load = IcebergLoadImpl(
                    spark=spark, dataframe=dataframe, load_meta=load_meta
                )
            case _:
                raise Exception(
                    f"[load] dont support target_type={load_meta.target_type}"
                )
        load.process()

    @staticmethod
    def transforms(dataframe: DataFrame, udf_infos: list = None):
        return Transform.transforms(dataframe=dataframe, udf_infos=udf_infos)
