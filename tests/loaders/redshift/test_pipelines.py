import os
import pytest
from os import environ

from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.pipeline import Pipeline, PostgresPipelineCredentials
from dlt.pipeline.exceptions import InvalidPipelineContextException

from tests.utils import autouse_root_storage, TEST_STORAGE


FAKE_CREDENTIALS = PostgresPipelineCredentials("redshift", None, None, None, None)


def test_empty_default_schema_name() -> None:
    p = Pipeline("test_empty_default_schema_name")
    FAKE_CREDENTIALS.DEFAULT_DATASET = environ["DEFAULT_DATASET"] = "test_empty_default_schema_name" + uniq_id()
    p.create_pipeline(FAKE_CREDENTIALS, os.path.join(TEST_STORAGE, FAKE_CREDENTIALS.DEFAULT_DATASET), Schema(""))
    p.extract(iter(["a", "b", "c"]), table_name="test")
    p.normalize()
    p.load()

    # delete data
    with p.sql_client() as c:
        c.drop_dataset()

    # try to restore pipeline
    r_p = Pipeline("test_empty_default_schema_name")
    r_p.restore_pipeline(FAKE_CREDENTIALS, p.working_dir)
    schema = r_p.get_default_schema()
    assert schema.schema_name == ""


def test_create_wipes_working_dir() -> None:
    p = Pipeline("test_create_wipes_working_dir")
    FAKE_CREDENTIALS.DEFAULT_DATASET = environ["DEFAULT_DATASET"] = "test_create_wipes_working_dir" + uniq_id()
    p.create_pipeline(FAKE_CREDENTIALS, os.path.join(TEST_STORAGE, FAKE_CREDENTIALS.DEFAULT_DATASET), Schema("table"))
    p.extract(iter(["a", "b", "c"]), table_name="test")
    p.normalize()
    assert len(p.list_normalized_loads()) > 0

    # try to restore pipeline
    r_p = Pipeline("test_create_wipes_working_dir")
    r_p.restore_pipeline(FAKE_CREDENTIALS, p.working_dir)
    assert len(r_p.list_normalized_loads()) > 0
    schema = r_p.get_default_schema()
    assert schema.schema_name == "table"

    # create pipeline in the same dir
    p = Pipeline("overwrite_old")
    FAKE_CREDENTIALS.DEFAULT_DATASET = "new"
    p.create_pipeline(FAKE_CREDENTIALS, os.path.join(TEST_STORAGE, FAKE_CREDENTIALS.DEFAULT_DATASET), Schema("matrix"))

    # old pipeline contextes are destroyed
    with pytest.raises(InvalidPipelineContextException):
        assert len(r_p.list_normalized_loads()) == 0

    # so recreate it
    r_p = Pipeline("overwrite_old")
    r_p.restore_pipeline(FAKE_CREDENTIALS, p.working_dir)
    assert len(r_p.list_normalized_loads()) == 0
    schema = r_p.get_default_schema()
    assert schema.schema_name == "matrix"
