import requests
from typing import Type
import pytest
import logging
from os import environ

from dlt.common.configuration.utils import _get_config_attrs_with_hints, make_configuration
from dlt.common.configuration import RunConfiguration
from dlt.common.logger import init_logging_from_config
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Schema
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import StrAny


TEST_STORAGE = "_storage"


class MockHttpResponse():
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 300:
            raise requests.HTTPError(response=self)


def write_version(storage: FileStorage, version: str) -> None:
    storage.save(VersionedStorage.VERSION_FILE, str(version))


def delete_storage() -> None:
    storage = FileStorage(TEST_STORAGE)
    if storage.has_folder(""):
        storage.delete_folder("", recursively=True)


@pytest.fixture()
def root_storage() -> FileStorage:
    return clean_storage()


@pytest.fixture(autouse=True)
def autouse_root_storage() -> FileStorage:
    return clean_storage()


@pytest.fixture(scope="module", autouse=True)
def preserve_environ() -> None:
    saved_environ = environ.copy()
    yield
    environ.clear()
    environ.update(saved_environ)


def init_logger(C: Type[RunConfiguration] = None) -> None:
    if not hasattr(logging, "health"):
        if not C:
            C = make_configuration(RunConfiguration, RunConfiguration)
        init_logging_from_config(C)


def clean_storage(init_normalize: bool = False, init_loader: bool = False) -> FileStorage:
    storage = FileStorage(TEST_STORAGE, "t", makedirs=True)
    storage.delete_folder("", recursively=True)
    storage.create_folder(".")
    if init_normalize:
        from dlt.common.storages.normalize_storage import NormalizeStorage
        from dlt.common.configuration import NormalizeVolumeConfiguration
        NormalizeStorage(True, NormalizeVolumeConfiguration)
    if init_loader:
        from dlt.common.storages.loader_storage import LoaderStorage
        from dlt.common.configuration import LoadingVolumeConfiguration
        LoaderStorage(True, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
    return storage


def add_config_to_env(config: Type[RunConfiguration]) ->  None:
    # write back default values in configuration back into environment
    possible_attrs = _get_config_attrs_with_hints(config).keys()
    for attr in possible_attrs:
        if attr not in environ:
            v = getattr(config, attr)
            if v is not None:
                # print(f"setting {attr} to {v}")
                environ[attr] = str(v)


def create_schema_with_name(schema_name) -> Schema:
    schema = Schema(schema_name)
    return schema


def assert_no_dict_key_starts_with(d: StrAny, key_prefix: str) -> None:
    assert all(not key.startswith(key_prefix) for key in d.keys())
