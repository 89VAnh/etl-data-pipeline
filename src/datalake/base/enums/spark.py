from datalake.base import config
from datalake.base.enums.base_enum import BaseEnum
from datalake.base.enums.env import Env


class SparkEnv(BaseEnum):
    GLUE = "Glue"
    LOCAL = "Local"
    PROTO = "Databricks Proto"

    @staticmethod
    def get_env(env: Env):
        spark_env = config.SPARK_ENV
        if env == Env.DEV:
            spark_env = SparkEnv.LOCAL
        return spark_env
