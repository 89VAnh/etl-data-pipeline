import json

from datalake.base.components.imetadata import IMetadata
from datalake.base.model.meta_scenario import MetaScenario
from datalake.base.model.meta_history import HistoryMeta
from datalake.base.utils.date import DateUtil
from datalake.base.utils.data import DataUtil
from datalake.base.utils.logger import Logger
import boto3
import io

LOGGER = Logger.get_logger(__name__)


class S3Metadata(IMetadata):
    metadata_bucket = "ght-config"
    history_object_key = "metadata/history/history.json"
    scenario_object_key = "metadata/scenarios/etl_ingestion_1.json"

    def load_scenarios(self):
        with DataUtil.read_s3_object(
            self.metadata_bucket, self.scenario_object_key
        ) as f:
            data = json.load(f)
        metas = [x for x in data if x["status"] == True]
        for meta in metas:
            self.scenarios.append(MetaScenario(meta))

    def save_history(self, history):
        LOGGER.debug("history response from scenario: %s", history.__dict__)
        with DataUtil.read_s3_object(
            self.metadata_bucket, self.history_object_key
        ) as f:
            histories = json.load(f)
        if history.criteria_value:
            history.criteria_value = DateUtil.strftime(
                history.criteria_value, "%Y-%m-%d %H:%M:%S"
            )
        histories.append(history.__dict__)
        DataUtil.write_s3_object(
            self.metadata_bucket,
            self.history_object_key,
            json.dumps(histories).encode("utf-8"),
        )

    def get_latest_history(self, meta_extract, meta_load):
        with DataUtil.read_s3_object(
            self.metadata_bucket, self.history_object_key
        ) as f:
            data = json.load(f)
        histories: list[HistoryMeta] = [HistoryMeta(hist) for hist in data]
        latest_date = DateUtil.min_date()
        for history in histories:
            if (
                history.source_object == meta_extract.source_object
                and history.status == "SUCCESS"
            ):
                if history.criteria_value:
                    criteria_val = DateUtil.strptime(
                        history.criteria_value, "%Y-%m-%d %H:%M:%S"
                    )
                    if criteria_val > latest_date:
                        latest_date = criteria_val
        return latest_date
