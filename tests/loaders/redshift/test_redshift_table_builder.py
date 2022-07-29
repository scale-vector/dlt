import pytest
from copy import deepcopy

from dlt.common.utils import uniq_id, custom_environ
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.configuration import PostgresConfiguration, make_configuration

from dlt.loaders.exceptions import LoadClientSchemaWillNotUpdate
from dlt.loaders.redshift.client import RedshiftClient

from tests.loaders.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def client(schema: Schema) -> RedshiftClient:
    # return client without opening connection
    RedshiftClient.configure(initial_values={"DEFAULT_DATASET": "TEST" + uniq_id()})
    return RedshiftClient(schema)


def test_configuration() -> None:
    # check names normalized
    with custom_environ({"PG_DATABASE_NAME": "UPPER_CASE_DATABASE", "PG_PASSWORD": " pass\n"}):
        C = make_configuration(PostgresConfiguration, PostgresConfiguration)
        assert C.PG_DATABASE_NAME == "upper_case_database"
        assert C.PG_PASSWORD == "pass"


def test_create_table(client: RedshiftClient) -> None:
    client.schema.update_schema(new_table("event_test_table", columns=TABLE_UPDATE))
    sql = client._get_table_update_sql("event_test_table", {}, False)
    assert sql.startswith("BEGIN TRANSACTION;\n")
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar(max)' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" varbinary' in sql
    assert '"col8" numeric(38,0)' in sql
    assert '"col9" varchar(max)  NOT NULL' in sql
    assert sql.endswith('\nCOMMIT TRANSACTION;')


def test_alter_table(client: RedshiftClient) -> None:
    client.schema.update_schema(new_table("event_test_table", columns=TABLE_UPDATE))
    # table has no columns
    sql = client._get_table_update_sql("event_test_table", {}, True)
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.startswith("BEGIN TRANSACTION;\n")
    # must have several ALTER TABLE statements
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD COLUMN") == len(TABLE_UPDATE)
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar(max)' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" varbinary' in sql
    assert '"col8" numeric(38,0)' in sql
    assert '"col9" varchar(max)  NOT NULL' in sql
    assert sql.endswith("\nCOMMIT TRANSACTION;")


def test_create_table_with_hints(client: RedshiftClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["cluster"] = True
    mod_update[4]["cluster"] = True
    client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    sql = client._get_table_update_sql("event_test_table", {}, False)
    # PRIMARY KEY will not be present https://heap.io/blog/redshift-pitfalls-avoid
    assert '"col1" bigint SORTKEY NOT NULL' in sql
    assert '"col2" double precision DISTKEY NOT NULL' in sql
    assert '"col5" varchar(max) DISTKEY' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql


def test_hint_alter_table_exception(client: RedshiftClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["sort"] = True
    client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    with pytest.raises(LoadClientSchemaWillNotUpdate) as excc:
        client._get_table_update_sql("event_test_table", {}, True)
    assert excc.value.columns == ["col4"]
