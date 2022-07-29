from copy import deepcopy
import io
import pytest
from typing import Iterator

from dlt.common import json, pendulum
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.file_storage import FileStorage
from dlt.common.schema import TTableSchemaColumns
from dlt.common.utils import uniq_id

from dlt.loaders.client_base import DBCursor, SqlJobClientBase

from tests.utils import TEST_STORAGE, delete_storage
from tests.common.utils import load_json_case
from tests.loaders.utils import TABLE_UPDATE, TABLE_ROW, expect_load_file, yield_client_with_storage, write_dataset, prepare_event_user_table


ALL_CLIENTS = ['redshift_client', 'bigquery_client']


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


@pytest.fixture(scope="module")
def redshift_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("redshift")


@pytest.fixture(scope="module")
def bigquery_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("gcp")


@pytest.fixture(scope="module")
def client(request) -> SqlJobClientBase:
    yield request.getfixturevalue(request.param)



@pytest.mark.order(1)
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_initialize_storage(client: SqlJobClientBase) -> None:
    pass

@pytest.mark.order(2)
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_get_version_on_empty(client: SqlJobClientBase) -> None:
    version = client._get_schema_version_from_storage()
    assert version == 0


@pytest.mark.order(3)
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_get_update_basic_schema(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    version = client._get_schema_version_from_storage()
    assert version == 1
    # modify version
    client._update_schema_version(2)
    version = client._get_schema_version_from_storage()
    assert version == 2
    client._update_schema_version(1)
    version = client._get_schema_version_from_storage()
    assert version == 1
    # check if tables are present and identical
    exists, version_storage_table = client._get_storage_table(Schema.VERSION_TABLE_NAME)
    version_schema_table = client.schema.get_table_columns(Schema.VERSION_TABLE_NAME)
    assert exists is True
    # schema version must be contained in storage version (schema may not have all hints)
    assert version_schema_table.keys() == version_storage_table.keys()
    assert all(set(v) <= set(version_storage_table[k]) for k, v in version_schema_table.items())


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_complete_load(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    load_table = client.sql_client.make_qualified_table_name(Schema.LOADS_TABLE_NAME)
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 1
    assert load_rows[0][0] == load_id
    assert load_rows[0][1] == 0
    import datetime  # noqa: I251
    assert type(load_rows[0][2]) is datetime.datetime
    client.complete_load("load2")
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 2


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_query_iterator(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    curr: DBCursor
    # get data from unqualified name
    with client.sql_client.execute_query(f"SELECT * FROM {Schema.LOADS_TABLE_NAME} ORDER BY inserted_at") as curr:
        columns = [c[0] for c in curr.description]
        data = curr.fetchall()

    # get data from qualified name
    load_table = client.sql_client.make_qualified_table_name(Schema.LOADS_TABLE_NAME)
    with client.sql_client.execute_query(f"SELECT * FROM {load_table} ORDER BY inserted_at") as curr:
        assert [c[0] for c in curr.description] == columns
        assert curr.fetchall() == data


@pytest.mark.parametrize('client', ["redshift_client"], indirect=True)
def test_schema_update_create_table_redshift(client: SqlJobClientBase) -> None:
    # infer typical rasa event schema
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    # this will be sort
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    assert timestamp["sort"] is True
    # this will be destkey
    sender_id = schema._infer_column("sender_id", "982398490809324")
    assert  sender_id["cluster"] is True
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    assert record_hash["unique"] is True
    schema.update_schema(new_table(table_name, columns=[timestamp, sender_id, record_hash]))
    client.update_storage_schema()
    exists, _ = client._get_storage_table(table_name)
    assert exists is True


@pytest.mark.parametrize('client', ["bigquery_client"], indirect=True)
def test_schema_update_create_table_bigquery(client: SqlJobClientBase) -> None:
    # infer typical rasa event schema
    schema = client.schema
    # this will be partition
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    # this will be cluster
    sender_id = schema._infer_column("sender_id", "982398490809324")
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    schema.update_schema(new_table("event_test_table", columns=[timestamp, sender_id, record_hash]))
    client.update_storage_schema()
    exists, storage_table = client._get_storage_table("event_test_table")
    assert exists is True
    assert storage_table["timestamp"]["partition"] is True
    assert storage_table["sender_id"]["cluster"] is True
    exists, storage_table = client._get_storage_table("_dlt_version")
    assert exists is True
    assert storage_table["version"]["partition"] is False
    assert storage_table["version"]["cluster"] is False


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_schema_update_alter_table(client: SqlJobClientBase) -> None:
    schema = client.schema
    col1 = schema._infer_column("col1", "string")
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=[col1]))
    client.update_storage_schema()
    # with single alter table
    col2 = schema._infer_column("col2", 1)
    schema.update_schema(new_table(table_name, columns=[col2]))
    client.update_storage_schema()
    # with 2 alter tables
    col3 = schema._infer_column("col3", 1.2)
    col4 = schema._infer_column("col4", 182879721.182912)
    col4["data_type"] = "timestamp"
    schema.update_schema(new_table(table_name, columns=[col3, col4]))
    client.update_storage_schema()
    _, storage_table = client._get_storage_table(table_name)
    # 4 columns
    assert len(storage_table) == 4
    assert storage_table["col4"]["data_type"] == "timestamp"


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_get_storage_table_with_all_types(client: SqlJobClientBase) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=TABLE_UPDATE))
    client.update_storage_schema()
    exists, storage_table = client._get_storage_table(table_name)
    assert exists is True
    # column order must match TABLE_UPDATE
    storage_columns = list(storage_table.values())
    for c, s_c in zip(TABLE_UPDATE, storage_columns):
        assert c["name"] == s_c["name"]
        if c["data_type"] == "complex":
            assert s_c["data_type"] == "text"
        else:
            assert c["data_type"] == s_c["data_type"]


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_data_writer_load(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write only first row
    with io.StringIO() as f:
        write_dataset(client, f, [rows[0]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0]
    # content must equal
    assert list(db_row) == list(rows[0].values())
    # write second row that contains two nulls
    with io.StringIO() as f:
        write_dataset(client, f, [rows[1]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name} WHERE f_int = {rows[1]['f_int']}")[0]
    assert db_row[3] is None
    assert db_row[5] is None


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_data_writer_string_escape(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    row = rows[0]
    # this will really drop table without escape
    inj_str = f", NULL'); DROP TABLE {canonical_name} --"
    row["f_str"] = inj_str
    with io.StringIO() as f:
        write_dataset(client, f, [rows[0]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0]
    assert list(db_row) == list(row.values())


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_data_writer_string_escape(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "weird_rows")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    with io.StringIO() as f:
        write_dataset(client, f, rows, rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    for i in range(1,4):
        db_row = client.sql_client.execute_sql(f"SELECT str FROM {canonical_name} WHERE idx = {i}")
        assert db_row[0][0] == rows[i-1]["str"]


@pytest.mark.parametrize('write_disposition', ["append", "replace"])
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_load_with_all_types(client: SqlJobClientBase, write_disposition: str, file_storage: FileStorage) -> None:
    table_name = "event_test_table" + uniq_id()
    # we should have identical content with all disposition types
    client.schema.update_schema(new_table(table_name, write_disposition=write_disposition, columns=TABLE_UPDATE))
    client.update_storage_schema()
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write row
    with io.StringIO() as f:
        write_dataset(client, f, [TABLE_ROW], TABLE_ROW.keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = list(client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0])
    # content must equal
    db_row[3] = str(pendulum.instance(db_row[3]))  # serialize date
    if isinstance(db_row[6], str):
        db_row[6] = bytes.fromhex(db_row[6])  # redshift returns binary as hex string

    assert db_row == list(TABLE_ROW.values())


@pytest.mark.parametrize('write_disposition', ["append", "replace"])
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_write_dispositions(client: SqlJobClientBase, write_disposition: str, file_storage: FileStorage) -> None:
    table_name = "event_test_table" + uniq_id()
    client.schema.update_schema(
        new_table(table_name, write_disposition=write_disposition, columns=TABLE_UPDATE)
        )
    child_table = client.schema.normalize_make_path(table_name, "child")
    # add child table without write disposition so it will be inferred from the parent
    client.schema.update_schema(
        new_table(child_table, columns=TABLE_UPDATE, parent_name=table_name)
        )
    client.update_storage_schema()
    for idx in range(2):
        for t in [table_name, child_table]:
            # write row, use col1 (INT) as row number
            table_row = deepcopy(TABLE_ROW)
            table_row["col1"] = idx
            with io.StringIO() as f:
                write_dataset(client, f, [table_row], TABLE_ROW.keys())
                query = f.getvalue()
            expect_load_file(client, file_storage, query, t)
            db_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {t} ORDER BY col1 ASC"))
            if write_disposition == "append":
                # we append 1 row to tables in each iteration
                assert len(db_rows) == idx + 1
            else:
                # we overwrite with the same row
                assert len(db_rows) == 1
            # last row must have our last idx - make sure we append and overwrite
            assert db_rows[-1][0] == idx


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_retrieve_job(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    user_table_name = prepare_event_user_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id":'90238094809sajlkjxoiewjhduuiuehd',
        "timestamp": str(pendulum.now())
    }
    with io.StringIO() as f:
        write_dataset(client, f, [load_json], load_json.keys())
        dataset = f.getvalue()
    job = expect_load_file(client, file_storage, dataset, user_table_name)
    # now try to retrieve the job
    # TODO: we should re-create client instance as this call is intended to be run after some disruption ie. stopped loader process
    r_job = client.restore_file_load(file_storage._make_path(job.file_name()))
    assert r_job.status() == "completed"
    # use just file name to restore
    r_job = client.restore_file_load(job.file_name())
    assert r_job.status() == "completed"


def prepare_schema(client: SqlJobClientBase, case: str) -> None:
    client.update_storage_schema()
    rows = load_json_case(case)
    # use first row to infer table
    table: TTableSchemaColumns = {k: client.schema._infer_column(k, v) for k, v in rows[0].items()}
    table_name = f"event_{case}_{uniq_id()}"
    client.schema.update_schema(new_table(table_name, columns=table.values()))
    client.update_storage_schema()
    return rows, table_name
