import pytest

from dlt.common.storages.exceptions import NoMigrationPathException
from dlt.common.storages.unpacker_storage import UnpackerStorage
from dlt.common.configuration import UnpackingVolumeConfiguration
from dlt.common.utils import uniq_id

from tests.utils import write_version, autouse_root_storage

@pytest.mark.skip()
def test_load_events_and_group_by_sender() -> None:
    # TODO: create fixture with two sender ids and 3 files and check the result
    pass


@pytest.mark.skip()
def test_chunk_by_events() -> None:
    # TODO: should distribute ~ N events evenly among m cores with fallback for small amounts of events
    pass


def test_build_unpack_file_name() -> None:
    load_id = uniq_id()
    name = UnpackerStorage.build_unpack_file_name("event", "table", 121, load_id)
    assert UnpackerStorage.get_schema_name(name) == "event"
    assert UnpackerStorage.get_events_count(name) == 121
    assert UnpackerStorage._parse_unpack_file_name(name) == (121, load_id, "event")

    # empty schema should be supported
    name = UnpackerStorage.build_unpack_file_name("", "table", 121, load_id)
    assert UnpackerStorage._parse_unpack_file_name(name) == (121, load_id, "")


def test_full_migration_path() -> None:
    # create directory structure
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    assert s.version == UnpackerStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        UnpackerStorage(False, UnpackingVolumeConfiguration)
