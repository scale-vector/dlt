import pytest
from typing import Sequence, Tuple

from dlt.common.file_storage import FileStorage
from dlt.common.storages.loader_storage import LoaderStorage
from dlt.common.configuration import LoadingVolumeConfiguration, make_configuration
from dlt.common.storages.exceptions import NoMigrationPathException
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from tests.utils import write_version, autouse_root_storage


@pytest.fixture
def storage() -> LoaderStorage:
    C = make_configuration(LoadingVolumeConfiguration, LoadingVolumeConfiguration)
    s = LoaderStorage(True, C, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
    s.initialize_storage()
    return s


def test_archive_completed(storage: LoaderStorage) -> None:
    # should delete archive in full
    storage.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_load_path(load_id))
    storage.complete_job(load_id, file_name)
    storage.archive_load(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_load_path(load_id))
    # deleted from archive
    assert not storage.storage.has_folder(storage.get_archived_path(load_id))

    # do not delete completed jobs
    storage.delete_completed_jobs = False
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    storage.complete_job(load_id, file_name)
    storage.archive_load(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_load_path(load_id))
    # has load archived
    assert storage.storage.has_folder(storage.get_archived_path(load_id))


def test_archive_failed(storage: LoaderStorage) -> None:
    # loads with failed jobs are always archived
    storage.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_load_path(load_id))
    storage.fail_job(load_id, file_name, "EXCEPTION")
    storage.archive_load(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_load_path(load_id))
    # present in archive
    assert storage.storage.has_folder(storage.get_archived_path(load_id))


def test_full_migration_path() -> None:
    # create directory structure
    s = LoaderStorage(True, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = LoaderStorage(False, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
    assert s.version == LoaderStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = LoaderStorage(True, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        LoaderStorage(False, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)


def start_loading_file(s: LoaderStorage, content: Sequence[StrAny]) -> Tuple[str, str]:
    load_id = uniq_id()
    s.create_temp_load_folder(load_id)
    file_name = s.write_temp_loading_file(load_id, "mock_table", None, uniq_id(), content)
    s.commit_temp_load_folder(load_id)
    s.start_job(load_id, file_name)
    return load_id, file_name
