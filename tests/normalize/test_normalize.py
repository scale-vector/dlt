from typing import Dict, List, Sequence
import os
import pytest
import shutil
from fnmatch import fnmatch
from prometheus_client import CollectorRegistry
from multiprocessing.dummy import Pool as ThreadPool

from dlt.common import json
from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny
from dlt.common.normalizers.json.relational import normalize_data_item
from dlt.common.file_storage import FileStorage
from dlt.common.schema import TDataType
from dlt.common.storages.loader_storage import LoaderStorage
from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.storages import SchemaStorage
from dlt.extractors.extractor_storage import ExtractorStorageBase

from dlt.normalize import normalize, __version__
from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES

from tests.normalize.utils import json_case_path
from tests.utils import TEST_STORAGE, assert_no_dict_key_starts_with, write_version, clean_storage, init_logger


@pytest.fixture()
def raw_normalize() -> FileStorage:
    # does not install default schemas, so no type hints and row filters
    # uses default json normalizer that does not yield additional rasa tables
    return init_normalize()


@pytest.fixture
def rasa_normalize() -> FileStorage:
    # install default schemas, includes type hints and row filters
    # uses rasa json normalizer that yields event table and separate tables for event types
    return init_normalize("tests/normalize/cases/schemas", ["event", "ethereum"])


def init_normalize(default_schemas_path: str = None, schema_names: List[str] = None) -> FileStorage:
    storage = clean_storage()
    normalize.configure(normalize.configuration(), CollectorRegistry(), default_schemas_path, schema_names)
    # set jsonl as default writer
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "jsonl"
    return storage


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_logger()


def test_intialize(rasa_normalize: FileStorage) -> None:
    # create storages in fixture
    pass


def test_empty_schema_name(raw_normalize: FileStorage) -> None:
    schema = normalize.load_or_create_schema("")
    assert schema.schema_name == ""


def test_normalize_single_user_event_jsonl(raw_normalize: FileStorage) -> None:
    expected_tables, load_files = normalize_event_user("event_user_load_1", EXPECTED_USER_TABLES)
    # load, parse and verify jsonl
    for expected_table in expected_tables:
        expect_lines_file(load_files[expected_table])
    # return first line from event_user file
    event_text, lines = expect_lines_file(load_files["event"], 0)
    assert lines == 1
    event_json = json.loads(event_text)
    assert event_json["event"] == "user"
    assert event_json["parse_data__intent__name"] == "greet"
    assert event_json["text"] == "hello"
    event_text, lines = expect_lines_file(load_files["event__parse_data__response_selector__default__ranking"], 9)
    assert lines == 10
    event_json = json.loads(event_text)
    assert "id" in event_json
    assert "confidence" in event_json
    assert "intent_response_key" in event_json


def test_normalize_single_user_event_insert(raw_normalize: FileStorage) -> None:
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "insert_values"
    expected_tables, load_files = normalize_event_user("event_user_load_1", EXPECTED_USER_TABLES)
    # verify values line
    for expected_table in expected_tables:
        expect_lines_file(load_files[expected_table])
    # return first values line from event_user file
    event_text, lines = expect_lines_file(load_files["event"], 2)
    assert lines == 3
    assert "'user'" in  event_text
    assert "'greet'" in event_text
    assert "'hello'" in event_text
    event_text, lines = expect_lines_file(load_files["event__parse_data__response_selector__default__ranking"], 11)
    assert lines == 12
    assert "(7005479104644416710," in event_text


def test_normalize_filter_user_event(rasa_normalize: FileStorage) -> None:
    load_id = normalize_cases(["event_user_load_v228_1"])
    load_files = expect_load_package(load_id, ["event", "event_user", "event_user__metadata__user_nicknames",
                                               "event_user__parse_data__entities", "event_user__parse_data__entities__processors", "event_user__parse_data__intent_ranking"])
    event_text, lines = expect_lines_file(load_files["event_user"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "parse_data__intent__name" in filtered_row
    # response selectors are removed
    assert_no_dict_key_starts_with(filtered_row, "parse_data__response_selector")


def test_normalize_filter_bot_event(rasa_normalize: FileStorage) -> None:
    load_id = normalize_cases(["event_bot_load_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_bot"])
    event_text, lines = expect_lines_file(load_files["event_bot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "metadata__utter_action" in filtered_row
    assert "metadata__account_balance" not in filtered_row


def test_preserve_slot_complex_value_json_l(rasa_normalize: FileStorage) -> None:
    load_id = normalize_cases(["event_slot_session_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_slot"])
    event_text, lines = expect_lines_file(load_files["event_slot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert type(filtered_row["value"]) is str
    assert filtered_row["value"] == json.dumps({
            "user_id": "world",
            "mitter_id": "hello"
        })


def test_preserve_slot_complex_value_insert(rasa_normalize: FileStorage) -> None:
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "insert_values"
    load_id = normalize_cases(["event_slot_session_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_slot"])
    event_text, lines = expect_lines_file(load_files["event_slot"], 2)
    assert lines == 3
    c_val = json.dumps({
            "user_id": "world",
            "mitter_id": "hello"
        })
    assert c_val in event_text


def test_normalize_raw_no_type_hints(raw_normalize: FileStorage) -> None:
    normalize_event_user("event_user_load_1", EXPECTED_USER_TABLES)
    assert_timestamp_data_type("double")


def test_normalize_raw_type_hints(rasa_normalize: FileStorage) -> None:
    normalize_cases(["event_user_load_1"])
    assert_timestamp_data_type("timestamp")


def test_normalize_many_events_insert(rasa_normalize: FileStorage) -> None:
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "insert_values"
    load_id = normalize_cases(["event_many_load_2", "event_user_load_1"])
    expected_tables = EXPECTED_USER_TABLES_RASA_NORMALIZER + ["event_bot", "event_action"]
    load_files = expect_load_package(load_id, expected_tables)
    # return first values line from event_user file
    event_text, lines = expect_lines_file(load_files["event"], 4)
    assert lines == 5
    assert f"'{load_id}'" in event_text


def test_normalize_many_schemas(rasa_normalize: FileStorage) -> None:
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "insert_values"
    copy_cases(["event_many_load_2", "event_user_load_1", "ethereum_blocks_9c1d9b504ea240a482b007788d5cd61c_2"])
    normalize.normalize(ThreadPool(processes=1))
    # must have two loading groups with model and event schemas
    loads = normalize.load_storage.list_loads()
    assert len(loads) == 2
    schema_storage = SchemaStorage(normalize.load_storage.storage.storage_path)
    schemas = []
    # load all schemas
    for load_id in loads:
        schema = schema_storage.load_folder_schema(normalize.load_storage.get_load_path(load_id))
        schemas.append(schema.schema_name)
        # expect event tables
        if schema.schema_name == "event":
            expected_tables = EXPECTED_USER_TABLES_RASA_NORMALIZER + ["event_bot", "event_action"]
            expect_load_package(load_id, expected_tables)
        if schema.schema_name == "ethereum":
            expect_load_package(load_id, EXPECTED_ETH_TABLES)
    assert set(schemas) == set(["ethereum", "event"])


def test_normalize_typed_json(raw_normalize: FileStorage) -> None:
    normalize.load_storage.preferred_file_format = normalize.CONFIG.LOADER_FILE_FORMAT = "jsonl"
    extract_items([JSON_TYPED_DICT], "special")
    normalize.normalize(ThreadPool(processes=1))
    loads = normalize.load_storage.list_loads()
    assert len(loads) == 1
    schema_storage = SchemaStorage(normalize.load_storage.storage.storage_path)
    # load all schemas
    schema = schema_storage.load_folder_schema(normalize.load_storage.get_load_path(loads[0]))
    assert schema.schema_name == "special"
    # named as schema - default fallback
    table = schema.get_table_columns("special")
    # assert inferred types
    for k, v in JSON_TYPED_DICT_TYPES.items():
        assert table[k]["data_type"] == v


EXPECTED_ETH_TABLES = ["blocks", "blocks__transactions", "blocks__transactions__logs", "blocks__transactions__logs__topics",
                       "blocks__uncles", "blocks__transactions__access_list", "blocks__transactions__access_list__storage_keys"]

EXPECTED_USER_TABLES_RASA_NORMALIZER = ["event", "event_user", "event_user__parse_data__intent_ranking"]


EXPECTED_USER_TABLES = ["event", "event__parse_data__intent_ranking", "event__parse_data__response_selector__all_retrieval_intents",
         "event__parse_data__response_selector__default__ranking", "event__parse_data__response_selector__default__response__response_templates",
         "event__parse_data__response_selector__default__response__responses"]


def extract_items(items: Sequence[StrAny], schema_name: str) -> None:
    extractor = ExtractorStorageBase("1.0.0", True, FileStorage(os.path.join(TEST_STORAGE, "extractor"), makedirs=True), normalize.normalize_storage)
    load_id = uniq_id()
    extractor.save_json(f"{load_id}.json", items)
    extractor.commit_events(
        schema_name,
        extractor.storage._make_path(f"{load_id}.json"),
        "items",
        len(items),
        load_id
    )

def normalize_event_user(case: str, expected_user_tables: List[str] = None) -> None:
    expected_user_tables = expected_user_tables or EXPECTED_USER_TABLES_RASA_NORMALIZER
    load_id = normalize_cases([case])
    return expected_user_tables, expect_load_package(load_id, expected_user_tables)


def normalize_cases(cases: Sequence[str]) -> str:
    copy_cases(cases)
    load_id = uniq_id()
    normalize.load_storage.create_temp_load_folder(load_id)
    # pool not required for map_single
    dest_cases = [f"{NormalizeStorage.EXTRACTED_FOLDER}/{c}.extracted.json" for c in cases]
    # create schema if it does not exist
    normalize.load_or_create_schema("event")
    normalize.spool_files(None, "event", load_id, normalize.map_single, dest_cases)
    return load_id


def copy_cases(cases: Sequence[str]) -> None:
    for case in cases:
        event_user_path = json_case_path(f"{case}.extracted")
        shutil.copy(event_user_path, normalize.normalize_storage.storage._make_path(NormalizeStorage.EXTRACTED_FOLDER))


def expect_load_package(load_id: str, expected_tables: Sequence[str]) -> Dict[str, str]:
    files = normalize.load_storage.list_new_jobs(load_id)
    assert len(files) == len(expected_tables)
    ofl: Dict[str, str] = {}
    for expected_table in expected_tables:
        file_mask = normalize.load_storage.build_loading_file_name(load_id, expected_table, "*")
        candidates = [f for f in files if fnmatch(f, f"{LoaderStorage.LOADING_FOLDER}/{file_mask}")]
        assert len(candidates) == 1
        ofl[expected_table] = candidates[0]
    return ofl


def expect_lines_file(load_file: str, line: int = 0) -> str:
    with normalize.load_storage.storage.open_file(load_file) as f:
        lines = f.readlines()
    return lines[line], len(lines)


def assert_timestamp_data_type(data_type: TDataType) -> None:
    # load generated schema
    schema_storage = SchemaStorage(normalize.load_storage.storage.storage_path)
    loads = normalize.load_storage.list_loads()
    event_schema = schema_storage.load_folder_schema(normalize.load_storage.get_load_path(loads[0]))
    # in raw normalize timestamp column must not be coerced to timestamp
    assert event_schema.get_table_columns("event")["timestamp"]["data_type"] == data_type


def test_version() -> None:
    assert normalize.configuration()._VERSION == __version__
