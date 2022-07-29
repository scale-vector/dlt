import pytest
import json_logging
from os import environ
from typing import Any

from dlt import __version__ as auto_version
from dlt.common import logger, sleep
from dlt.common.typing import StrStr
from dlt.common.configuration import RunConfiguration

from tests.utils import preserve_environ


class PureBasicConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "logger"


class PureBasicConfigurationProc(PureBasicConfiguration):
    _VERSION: str = "1.6.6"


class JsonLoggerConfiguration(PureBasicConfigurationProc):
    LOG_FORMAT: str = "JSON"


class SentryLoggerConfiguration(JsonLoggerConfiguration):
    SENTRY_DSN: str = "http://user:pass@localhost/818782"


@pytest.fixture(scope="function")
def environment() -> StrStr:
    environ.clear()

    return environ


def test_version_extract(environment: StrStr) -> None:
    version = logger._extract_version_info(PureBasicConfiguration)
    # if component ver not avail use system version
    assert version == {'version': auto_version, 'component_name': 'logger'}
    version = logger._extract_version_info(PureBasicConfigurationProc)
    assert version["component_version"] == PureBasicConfigurationProc._VERSION
    # mock image info available in container
    _mock_image_env(environment)
    version = logger._extract_version_info(PureBasicConfigurationProc)
    assert version == {'version': auto_version, 'commit_sha': '192891', 'component_name': 'logger', 'component_version': '1.6.6', 'image_version': 'scale/v:112'}


def test_pod_info_extract(environment: StrStr) -> None:
    pod_info = logger._extract_pod_info()
    assert pod_info == {}
    _mock_pod_env(environment)
    pod_info = logger._extract_pod_info()
    assert pod_info == {'kube_node_name': 'node_name', 'kube_pod_name': 'pod_name', 'kube_pod_namespace': 'namespace'}


@pytest.mark.forked
def test_text_logger_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(PureBasicConfigurationProc)
    logger.health("HEALTH data", extra={"metrics": "props"})
    logger.metrics("METRICS data", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.process_internal_exception("DIV")


@pytest.mark.forked
def test_json_logger_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(JsonLoggerConfiguration)
    # correct component was set
    json_logging.COMPONENT_NAME = "logger"
    logger.health("HEALTH data", extra={"metrics": "props"})
    logger.metrics("METRICS data", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.process_internal_exception("DIV")


@pytest.mark.forked
def test_sentry_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(SentryLoggerConfiguration)
    try:
        1 / 0
    except ZeroDivisionError:
        logger.process_internal_exception("DIV")
    sleep(1)


def test_cleanup(environment: StrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass


def _mock_image_env(environment: StrStr) -> None:
    environment["COMMIT_SHA"] = "192891"
    environment["IMAGE_VERSION"] = "scale/v:112"


def _mock_pod_env(environment: StrStr) -> None:
    environment["KUBE_NODE_NAME"] = "node_name"
    environment["KUBE_POD_NAME"] = "pod_name"
    environment["KUBE_POD_NAMESPACE"] = "namespace"
