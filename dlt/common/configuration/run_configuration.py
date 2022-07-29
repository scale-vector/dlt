import randomname
from os.path import isfile
from typing import Any, Optional, Tuple, IO

from dlt.common.typing import StrAny
from dlt.common.utils import encoding_for_mode
from dlt.common.configuration.exceptions import ConfigFileNotFoundException

DEVELOPMENT_CONFIG_FILES_STORAGE_PATH = "_storage/config/%s"
PRODUCTION_CONFIG_FILES_STORAGE_PATH = "/run/config/%s"


class BaseConfiguration:
    @classmethod
    def as_dict(config, lowercase: bool = True) -> StrAny:
        may_lower = lambda k: k.lower() if lowercase else k
        return {may_lower(k):getattr(config, k) for k in dir(config) if not callable(getattr(config, k)) and not k.startswith("__")}

    @classmethod
    def apply_dict(config, values: StrAny, uppercase: bool = True, apply_non_spec: bool = False) -> None:
        if not values:
            return

        for k, v in values.items():
            k = k.upper() if uppercase else k
            # overwrite only declared values
            if not apply_non_spec and hasattr(config, k):
                setattr(config, k, v)


class RunConfiguration(BaseConfiguration):
    PIPELINE_NAME: Optional[str] = None  # the name of the component
    SENTRY_DSN: Optional[str] = None  # keep None to disable Sentry
    PROMETHEUS_PORT: Optional[int] = None  # keep None to disable Prometheus
    LOG_FORMAT: str = '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}'
    LOG_LEVEL: str = "DEBUG"
    IS_DEVELOPMENT_CONFIG: bool = True
    REQUEST_TIMEOUT: Tuple[int, int] = (15, 300)  # default request timeout for all http clients
    CONFIG_FILES_STORAGE_PATH: str = DEVELOPMENT_CONFIG_FILES_STORAGE_PATH

    @classmethod
    def check_integrity(cls) -> None:
        # generate random name if missing
        if not cls.PIPELINE_NAME:
            cls.PIPELINE_NAME = "dlt_" + randomname.get_name().replace("-", "_")
        # if CONFIG_FILES_STORAGE_PATH not overwritten and we are in production mode
        if cls.CONFIG_FILES_STORAGE_PATH == DEVELOPMENT_CONFIG_FILES_STORAGE_PATH and not cls.IS_DEVELOPMENT_CONFIG:
            # set to mount where config files will be present
            cls.CONFIG_FILES_STORAGE_PATH = PRODUCTION_CONFIG_FILES_STORAGE_PATH

    @classmethod
    def has_configuration_file(cls, name: str) -> bool:
        return isfile(cls.get_configuration_file_path(name))

    @classmethod
    def open_configuration_file(cls, name: str, mode: str) -> IO[Any]:
        path = cls.get_configuration_file_path(name)
        if not cls.has_configuration_file(name):
            raise ConfigFileNotFoundException(path)
        return open(path, mode, encoding=encoding_for_mode(mode))

    @classmethod
    def get_configuration_file_path(cls, name: str) -> str:
        return cls.CONFIG_FILES_STORAGE_PATH % name