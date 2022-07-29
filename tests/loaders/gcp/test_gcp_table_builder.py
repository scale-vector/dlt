import os
import pytest
from copy import deepcopy
from typing import List

from dlt.common.utils import custom_environ, uniq_id
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.configuration import make_configuration, GcpClientConfiguration

from dlt.loaders.gcp.client import BigQueryClient
from dlt.loaders.exceptions import LoadClientSchemaWillNotUpdate

from tests.loaders.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


def test_configuration() -> None:
    # check names normalized
    with custom_environ({"BQ_CRED_PRIVATE_KEY": "---NO NEWLINE---\n"}):
        C = make_configuration(GcpClientConfiguration, GcpClientConfiguration)
        assert C.BQ_CRED_PRIVATE_KEY == "---NO NEWLINE---\n"

    with custom_environ({"BQ_CRED_PRIVATE_KEY": "---WITH NEWLINE---\n"}):
        C = make_configuration(GcpClientConfiguration, GcpClientConfiguration)
        assert C.BQ_CRED_PRIVATE_KEY == "---WITH NEWLINE---\n"


@pytest.fixture
def gcp_client(schema: Schema) -> BigQueryClient:
    # return client without opening connection
    BigQueryClient.configure(initial_values={"DEFAULT_DATASET": uniq_id()})
    return BigQueryClient(schema)


def test_create_table(gcp_client: BigQueryClient) -> None:
    gcp_client.schema.update_schema(new_table("event_test_table", columns=TABLE_UPDATE))
    sql = gcp_client._get_table_update_sql("event_test_table", {}, False)
    assert sql.startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` INTEGER NOT NULL" in sql
    assert "`col2` FLOAT64 NOT NULL" in sql
    assert "`col3` BOOLEAN NOT NULL" in sql
    assert "`col4` TIMESTAMP NOT NULL" in sql
    assert "`col5` STRING " in sql
    assert "`col6` NUMERIC(38,9) NOT NULL" in sql
    assert "`col7` BYTES" in sql
    assert "`col8` BIGNUMERIC" in sql
    assert "`col9` STRING NOT NULL)" in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITION BY" not in sql


def test_alter_table(gcp_client: BigQueryClient) -> None:
    gcp_client.schema.update_schema(new_table("event_test_table", columns=TABLE_UPDATE))
    # table has no columns
    sql = gcp_client._get_table_update_sql("event_test_table", {}, True)
    assert sql.startswith("ALTER TABLE")
    assert "event_test_table" in sql
    assert "ADD COLUMN `col1` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql
    assert "ADD COLUMN `col3` BOOLEAN NOT NULL" in sql
    assert "ADD COLUMN `col4` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5` STRING" in sql
    assert "ADD COLUMN `col6` NUMERIC(38,9) NOT NULL" in sql
    assert "ADD COLUMN `col7` BYTES" in sql
    assert "ADD COLUMN `col8` BIGNUMERIC" in sql
    assert "ADD COLUMN `col9` STRING NOT NULL" in sql
    # table has col1 already in storage
    sql = gcp_client._get_table_update_sql("event_test_table", {"col1": {}}, True)
    assert "ADD COLUMN `col1` INTEGER NOT NULL" not in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql


def test_create_table_with_partition_and_cluster(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    gcp_client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    sql = gcp_client._get_table_update_sql("event_test_table", {}, False)
    # clustering must be the last
    assert sql.endswith("CLUSTER BY `col2`,`col5`")
    assert "PARTITION BY DATE(`col4`)" in sql


def test_double_partition_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["partition"] = True
    # double partition
    gcp_client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    with pytest.raises(LoadClientSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", {}, False)
    assert excc.value.columns == ["`col4`", "`col5`"]


def test_partition_alter_table_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    # double partition
    gcp_client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    with pytest.raises(LoadClientSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", {}, True)
    assert excc.value.columns == ["`col4`"]


def test_cluster_alter_table_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["cluster"] = True
    # double partition
    gcp_client.schema.update_schema(new_table("event_test_table", columns=mod_update))
    with pytest.raises(LoadClientSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", {}, True)
    assert excc.value.columns == ["`col4`"]
