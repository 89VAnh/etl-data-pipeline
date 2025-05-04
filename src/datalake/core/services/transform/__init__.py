from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from datalake.base.enums.param import ParamKey

class Transform:
    df: DataFrame

    def __init__(self, df):
        self.df = df

    def add_file_path(self, args: dict = None):
        self.df = self.df.withColumn(args.get(ParamKey.COL_NAME), F.input_file_name())

    def regex_in_col(self, args: dict = None):
        self.df = self.df.withColumn(
            args.get(ParamKey.COL_NAME),
            F.regexp_extract(args.get(ParamKey.IN_COL), args.get(ParamKey.REGEX), 0),
        )

    def str_ptime(self, args: dict = None):
        self.df = self.df.withColumn(
            args.get(ParamKey.COL_NAME),
            F.from_unixtime(
                F.unix_timestamp(args.get(ParamKey.IN_COL), args.get(ParamKey.FORMAT))
            ),
        )

    def drop_col(self, args: dict = None):
        self.df = self.df.drop(F.col(args.get(ParamKey.IN_COL)))

    def add_col(self, args: dict = None):
        self.df = self.df.withColumn(
            args.get(ParamKey.COL_NAME), F.lit(args.get(ParamKey.VALUE))
        )

    @staticmethod
    def transforms(dataframe: DataFrame, udf_infos) -> DataFrame:
        udf = Transform(dataframe)
        for udf_info in udf_infos:
            f_name = udf_info.get(ParamKey.UDF).lower()
            f_args = udf_info.get(ParamKey.ARGS)
            print(f_args)
            func = udf.__getattribute__(f_name)
            func(f_args)
        return udf.df
