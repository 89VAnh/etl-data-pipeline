import json

from datalake.base.enums.load import LoadType
from datalake.base.enums.target import TargetType, TargetSubType
from datalake.base.utils.json import JsonUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)

"""
    @Load Target Meta Data
"""


class LoadMeta:

    type: LoadType
    params: dict
    schema: str
    target_object: str
    target_schema: str
    target_database: str
    target_zone: str
    target_type: TargetType
    target_sub_type: TargetSubType
    target_path: str
    connection_name: str
    connection_url: str
    connection_host: str
    connection_port: str
    connection_database: str
    connection_username: str
    connection_password: str
    connection_driver: str
    partition_col: str

    def __init__(self, load_data: dict = None):
        if load_data is not None:
            extract_flat = JsonUtils.flatten_json(load_data, ["params"])
            for key, value in extract_flat.items():
                self.setattr(key, value)

    def setattr(self, key, value):
        try:
            if key == "params":
                value = json.loads(value)
            elif key == "type":
                value = LoadType.valueOf(value)
            elif key == "target_type":
                value = TargetType.valueOf(value)
            elif key == "target_sub_type":
                value = TargetSubType.valueOf(value)
            self.__setattr__(key, value)
        except Exception as ex:
            LOGGER.debug("[setattr] key=%s value=%s msg=%s", key, value, ex)

    def __str__(self):
        return json.dumps(self.__dict__)
