from enum import Enum
from logging import DEBUG, INFO


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"


class NativeLogLevel(int, Enum):
    debug = DEBUG
    info = INFO


class LogLevelAdapter:
    __LOG_LEVEL_MAP: dict[LogLevel, NativeLogLevel] = {
        LogLevel.debug: NativeLogLevel.debug,
        LogLevel.info: NativeLogLevel.info,
    }

    @classmethod
    def to_native_log_level(cls, level: LogLevel) -> NativeLogLevel:
        return cls.__LOG_LEVEL_MAP[level]
