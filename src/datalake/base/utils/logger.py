import logging

from datalake.base import config


class Logger:
    @staticmethod
    def get_logger(name: str = "ROOT") -> logging.Logger:
        logging.basicConfig(format=config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)

        logger = logging.getLogger(name)
        logger.setLevel(config.LOG_LEVEL)

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(config.LOG_LEVEL)
            formatter = logging.Formatter(
                config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger
