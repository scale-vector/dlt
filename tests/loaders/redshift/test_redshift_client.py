from typing import Iterator
import psycopg2
import pytest

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.file_storage import FileStorage
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id

from dlt.loaders.exceptions import LoadClientTerminalInnerException
from dlt.loaders.loader import import_client_cls
from dlt.loaders.redshift.client import RedshiftClient

from tests.utils import TEST_STORAGE, delete_storage
from tests.loaders.utils import expect_load_file, prepare_event_user_table, yield_client_with_storage


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


@pytest.fixture(scope="module")
def client() -> Iterator[RedshiftClient]:
    yield from yield_client_with_storage("redshift")


def test_empty_schema_name_init_storage(client: RedshiftClient) -> None:
    e_client: RedshiftClient = None
    # will reuse same configuration
    with import_client_cls("redshift", initial_values={"DEFAULT_DATASET": client.CONFIG.DEFAULT_DATASET})(Schema("")) as e_client:
        e_client.initialize_storage()
        try:
            # schema was created with the name of just schema prefix
            assert e_client.sql_client.default_dataset_name == client.CONFIG.DEFAULT_DATASET
            # update schema
            e_client.update_storage_schema()
            assert e_client._get_schema_version_from_storage() == 1
        finally:
            e_client.sql_client.drop_dataset()


def test_recover_tx_rollback(client: RedshiftClient) -> None:
    client.update_storage_schema()
    version_table = client.sql_client.make_qualified_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(psycopg2.errors.SyntaxError):
        client.sql_client.execute_sql(sql)
    # still can execute tx and selects
    client._get_schema_version_from_storage()
    client._update_schema_version(3)
    # syntax error within tx
    sql = f"BEGIN TRANSACTION;INVERT INTO {version_table} VALUES(1);COMMIT TRANSACTION;"
    with pytest.raises(psycopg2.errors.SyntaxError):
        client.sql_client.execute_sql(sql)
    client._get_schema_version_from_storage()
    client._update_schema_version(4)
    # wrong value inserted
    sql = f"BEGIN TRANSACTION;INSERT INTO {version_table}(version) VALUES(1);COMMIT TRANSACTION;"
    # cannot insert NULL value
    with pytest.raises(psycopg2.errors.InternalError_):
        client.sql_client.execute_sql(sql)
    client._get_schema_version_from_storage()
    # lower the schema version in storage so subsequent tests can run
    client._update_schema_version(1)


def test_simple_load(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_event_user_table(client)
    canonical_name = client.sql_client.make_qualified_table_name(user_table_name)
    # create insert
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}')"
    expect_load_file(client, file_storage, insert_sql+insert_values+";", user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 1
    # insert 100 more rows
    query = insert_sql + (insert_values + ",\n") * 99 + insert_values + ";"
    expect_load_file(client, file_storage, query, user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 101
    # insert null value
    insert_sql_nc = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, text) VALUES\n"
    insert_values_nc = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    expect_load_file(client, file_storage, insert_sql_nc+insert_values_nc, user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 102


def test_loading_errors(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_event_user_table(client)
    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.UndefinedColumn
    # insert null value
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
    # insert wrong type
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', TRUE);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.DatatypeMismatch
    # numeric overflow on bigint
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id) VALUES\n"
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.NumericValueOutOfRange
    # numeric overflow on NUMERIC
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__intent__id) VALUES\n"
    # default decimal is (38, 9) (128 bit), use local context to generate decimals with 38 precision
    with numeric_default_context():
        below_limit = Decimal(10**29) - Decimal('0.001')
        above_limit = Decimal(10**29)
    # this will pass
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {below_limit});"
    expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    # this will raise
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {above_limit});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
    # max redshift decimal is (38, 0) (128 bit) = 10**38 - 1
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**38});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
