import logging

from log.types import LogLevelAdapter
from model.test import LoggingParams


class Logger:
    __logger: logging.Logger

    @classmethod
    def __init__(cls, params: LoggingParams):
        cls.__logger = logging.getLogger(__name__)

        cls.__logger.setLevel(LogLevelAdapter.to_native_log_level(params.level).value)

        formatter = logging.Formatter("%(levelname)s - %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        cls.__logger.addHandler(handler)

    @classmethod
    @property
    def logger(cls):
        if cls.__logger:
            return cls.__logger
        else:
            raise Exception("Logger has not been initialized.")

    @classmethod
    def info(cls, msg: str, *args, **kwargs):
        cls.__logger.info(msg, *args, **kwargs)

    @classmethod
    def debug(cls, msg: str, *args, **kwargs):
        cls.__logger.debug(msg, *args, **kwargs)

    @classmethod
    def warn(cls, msg: str, *args, **kwargs):
        cls.__logger.warning(msg, *args, **kwargs)

    @classmethod
    def error(cls, msg: str, *args, **kwargs):
        cls.__logger.error(msg, *args, **kwargs)
