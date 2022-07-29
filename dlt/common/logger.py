import logging
import json_logging
import traceback
import sentry_sdk
from sentry_sdk.transport import HttpTransport
from logging import LogRecord, Logger
from typing import Any, Callable, Dict, Type

from dlt.common.json import json
from dlt.common.typing import DictStrAny, DictStrStr, StrStr
from dlt.common.configuration import RunConfiguration
from dlt.common.utils import filter_env_vars
from dlt._version import common_version as __version__

DLT_LOGGER_NAME = "sv-dlt"
LOGGER: Logger = None

def _add_logging_level(level_name: str, level: int, method_name:str = None) -> None:
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
       raise AttributeError('{} already defined in logging module'.format(level_name))
    if hasattr(logging, method_name):
       raise AttributeError('{} already defined in logging module'.format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
       raise AttributeError('{} already defined in logger class'.format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(level):
            self._log(level, message, args, **kwargs)
    def logToRoot(message: str, *args: Any, **kwargs: Any) -> None:
        logging.root._log(level, message, args, **kwargs)

    logging.addLevelName(level, level_name)
    setattr(logging, level_name, level)
    setattr(logging.getLoggerClass(), method_name, logForLevel)
    setattr(logging, method_name, logToRoot)


class _MetricsFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:  # noqa: A003
        s = super(_MetricsFormatter, self).format(record)
        if record.exc_text:
            s = s + '|'
        # dump metrics dictionary nicely
        if "metrics" in record.__dict__:
            s = s + ": " + json.dumps(record.__dict__["metrics"])
        return s


class _CustomJsonFormatter(json_logging.JSONLogFormatter):

    version: StrStr = None

    def _format_log_object(self, record: LogRecord, request_util: Any) -> Any:
        json_log_object = super(_CustomJsonFormatter, self)._format_log_object(record, request_util)
        if self.version:
            json_log_object.update({"version": self.version})
        return json_log_object


def _init_logging(logger_name: str, level: str, fmt: str, component: str, version: StrStr) -> Logger:
    if logger_name == "root":
        logging.basicConfig(level=level)
        handler = logging.getLogger().handlers[0]
        # handler.setFormatter(_MetricsFormatter(fmt=format, style='{'))
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(DLT_LOGGER_NAME)
        logger.propagate = False
        logger.setLevel(level)
        # get or create logging handler
        handler = next(iter(logger.handlers), logging.StreamHandler())
        logger.addHandler(handler)

    # set right formatter
    if is_json_logging(fmt):
        json_logging.COMPONENT_NAME = component
        json_logging.JSON_SERIALIZER = json.dumps
        json_logging.RECORD_ATTR_SKIP_LIST.remove("process")
        # set version as class variable as we cannot pass custom constructor parameters
        _CustomJsonFormatter.version = version
        # the only thing method above effectively does is to replace the formatter
        json_logging.init_non_web(enable_json=True, custom_formatter=_CustomJsonFormatter)
        if logger_name == "root":
            json_logging.config_root_logger()
    else:
        handler.setFormatter(_MetricsFormatter(fmt=fmt, style='{'))

    return logger


def __getattr__(name: str) -> Callable[..., Any]:
    # a catch all function for a module that forwards calls to unknown methods to LOGGER
    def wrapper(msg: str, *args: Any, **kwargs: Any) -> None:
        if LOGGER:
            getattr(LOGGER, name)(msg, *args, **kwargs, stacklevel=2)
    return wrapper


def _extract_version_info(config: Type[RunConfiguration]) -> StrStr:
    version_info = {"version": __version__, "component_name": config.PIPELINE_NAME}
    version = getattr(config, "_VERSION", None)
    if version:
        version_info["component_version"] = version
    # extract envs with build info
    version_info.update(filter_env_vars(["COMMIT_SHA", "IMAGE_VERSION"]))
    return version_info


def _extract_pod_info() -> StrStr:
    return filter_env_vars(["KUBE_NODE_NAME", "KUBE_POD_NAME", "KUBE_POD_NAMESPACE"])

class _SentryHttpTransport(HttpTransport):

    timeout: int = 0

    def _get_pool_options(self, *a: Any, **kw: Any) -> DictStrAny:
        rv = HttpTransport._get_pool_options(self, *a, **kw)
        rv['timeout'] = self.timeout
        return rv


def _init_sentry(config: Type[RunConfiguration], version: StrStr) -> None:
    if config.SENTRY_DSN:
        sys_ver = version["version"]
        release = sys_ver + "_" + version.get("commit_sha", "")
        _SentryHttpTransport.timeout = config.REQUEST_TIMEOUT[0]
        # TODO: setup automatic sending of log messages by log level (ie. we send a lot dbt trash logs)
        # https://docs.sentry.io/platforms/python/guides/logging/
        sentry_sdk.init(config.SENTRY_DSN, release=release, transport=_SentryHttpTransport)
        # add version tags
        for k, v in version.items():
            sentry_sdk.set_tag(k, v)
        # add kubernetes tags
        pod_tags = _extract_pod_info()
        for k, v in pod_tags.items():
            sentry_sdk.set_tag(k, v)


def init_telemetry(config: Type[RunConfiguration]) -> None:
    if config.PROMETHEUS_PORT:
        from prometheus_client import start_http_server, Info

        logging.info(f"Starting prometheus server port {config.PROMETHEUS_PORT}")
        start_http_server(config.PROMETHEUS_PORT)
        # collect info
        Info("runs_component_name", "Name of the executing component").info(_extract_version_info(config))


def init_logging_from_config(C: Type[RunConfiguration]) -> None:
    global LOGGER

    # add HEALTH and METRICS log levels
    if not hasattr(logging, "health"):
        _add_logging_level("HEALTH", logging.WARNING - 1, "health")
        _add_logging_level("METRICS", logging.WARNING - 2, "metrics")

    version = _extract_version_info(C)
    LOGGER = _init_logging(
        DLT_LOGGER_NAME,
        C.LOG_LEVEL,
        C.LOG_FORMAT,
        C.PIPELINE_NAME,
        version)
    _init_sentry(C, version)


def is_json_logging(log_format: str) -> bool:
    return log_format == "JSON"


def process_internal_exception(msg: str, exc_info: Any = True) -> None:
    # Passing default True value will cause implementation to use data provided by sys.exc_info
    if LOGGER:
        LOGGER.error(msg, exc_info=exc_info, stacklevel=2)
    report_exception()


def report_exception() -> None:
    if sentry_sdk.Hub.current:
        sentry_sdk.capture_exception()


def pretty_format_exception() -> str:
    return traceback.format_exc()
