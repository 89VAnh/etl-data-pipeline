from datalake.base.enums.env import Env
from datalake.core.factory.usecase import USFactory

if __name__ == "__main__":
    USFactory.run(env=Env.DEV, job_name="etl_ingestion_1")
    # USFactory.run(env=Env.DEV, job_name="etl_ingestion_2")
