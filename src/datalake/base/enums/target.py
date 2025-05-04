from datalake.base.enums.base_enum import BaseEnum


class TargetType(BaseEnum):
    DATABASE = "DATABASE"
    S3 = "S3"
    ICEBERG = "ICEBERG"


class TargetSubType(BaseEnum):
    CSV = "CSV"
    EXCEL = "EXCEL"
    ORACLE = "ORACLE"
    MSSQL = "MSSQL"
    PARQUET = "PARQUET"
