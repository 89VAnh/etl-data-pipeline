from datalake.base.components.imetadata import IMetadata
from datalake.base.utils.logger import Logger


LOGGER = Logger.get_logger("Usecase")


class Usecase:
    metadata: IMetadata

    def __init__(self, metadata: IMetadata):
        self.metadata = metadata
        self.responses = Hist
