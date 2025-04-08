from datalake.base.enums.base_enum import BaseEnum
from datalake.base.utils.str import StrUtil


class TargetType(BaseEnum):
    DATABASE = "database"
    S3 = "S3"
    ICEBERG = "ICEBERG"


class TargetSubType(BaseEnum):
    CSV = "CSV"
    EXCEL = "EXCEL"
    ORACLE = "ORACLE"
    MSSQL = "MSSQL"
