from typing import Any, Iterable, Iterator, List, Sequence, cast, IO

from dlt.common import json, Decimal
from dlt.common.dataset_writers import write_insert_values, write_jsonl
from dlt.common.file_storage import FileStorage
from dlt.common.schema import TColumn, TTableColumns
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.schema.utils import new_table
from dlt.common.time import sleep
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.loaders.loader import import_client, get_load_table
from dlt.loaders.configuration import LoaderConfiguration, configuration
from dlt.loaders.client_base import JobClientBase, LoadJob, SqlJobClientBase

TABLE_UPDATE: List[TColumn] = [
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    },
    {
        "name": "col2",
        "data_type": "double",
        "nullable": False
    },
    {
        "name": "col3",
        "data_type": "bool",
        "nullable": False
    },
    {
        "name": "col4",
        "data_type": "timestamp",
        "nullable": False
    },
    {
        "name": "col5",
        "data_type": "text",
        "nullable": True
    },
    {
        "name": "col6",
        "data_type": "decimal",
        "nullable": False
    },
    {
        "name": "col7",
        "data_type": "binary",
        "nullable": True
    },
    {
        "name": "col8",
        "data_type": "wei",
        "nullable": True
    },
    {
        "name": "col9",
        "data_type": "complex",
        "nullable": False
    },
]

TABLE_ROW = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45+00:00",
    "col5": "string data",
    "col6": Decimal("2323.34"),
    "col7": b'binary data',
    "col8": 2**56 + 92093890840,
    "col9": "{complex: [1,2,3]}"
}

def load_table(name: str) -> TTableColumns:
    with open(f"./tests/loaders/cases/{name}.json", "tr", encoding="utf-8") as f:
        return cast(TTableColumns, json.load(f))


def expect_load_file(client: JobClientBase, file_storage: FileStorage, query: str, table_name: str, status = "completed") -> LoadJob:
    file_name = uniq_id()
    file_storage.save(file_name, query.encode("utf-8"))
    table = get_load_table(client.schema, table_name, file_name)
    job = client.start_file_load(table, file_storage._make_path(file_name))
    while job.status() == "running":
        sleep(0.5)
    assert job.file_name() == file_name
    assert job.status() ==  status
    return job


def prepare_event_user_table(client: JobClientBase) -> None:
    client.update_storage_schema()
    user_table = load_table("event_user")["event_user"]
    user_table_name = "event_user_" + uniq_id()
    client.schema.update_schema(new_table(user_table_name, columns=user_table.values()))
    client.update_storage_schema()
    return user_table_name


def yield_client_with_storage(client_type: str) -> Iterator[SqlJobClientBase]:
    # create dataset with random name
    schema_prefix = "test_" + uniq_id()
    CLIENT_CONFIG: LoaderConfiguration = configuration({"CLIENT_TYPE": client_type})
    if client_type == "gcp":
        CLIENT_CONFIG.DATASET = schema_prefix
    else:
        CLIENT_CONFIG.PG_SCHEMA_PREFIX = schema_prefix
    # get event default schema
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    # create client and dataset
    client: SqlJobClientBase = None
    with import_client(client_type).make_client(schema, CLIENT_CONFIG) as client:
        client.initialize_storage()
        yield client
        client.sql_client.drop_schema()


def write_dataset(client: JobClientBase, f: IO[Any], rows: Sequence[StrAny], headers: Iterable[str]) -> None:
    if client.capabilities["writer_type"] == "jsonl":
        write_jsonl(f, rows)
    elif client.capabilities["writer_type"] == "insert_values":
        write_insert_values(f, rows, headers)
    else:
        raise ValueError(client.capabilities["writer_type"])
