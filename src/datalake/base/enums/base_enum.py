from enum import Enum
from datalake.base.utils.str import StringUtils


class BaseEnum(Enum):
    @staticmethod
    def value_of(s: str):
        try:
            return BaseEnum[StringUtils.upper(s)]
        except:
            return None
