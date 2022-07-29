from os import environ
import pytest

from dlt.common import logger
from dlt.common.configuration import GcpClientConfiguration
from dlt.common.telemetry import TRunMetrics, get_metrics_from_prometheus
from dlt.common.typing import StrStr
from dlt.common.utils import uniq_id, with_custom_environ

from dlt.dbt_runner.utils import DBTProcessingError
from dlt.dbt_runner import runner
from dlt.loaders.gcp.client import BigQuerySqlClient

from tests.utils import add_config_to_env, init_logger, preserve_environ
from tests.dbt_runner.utils import setup_runner

DEST_SCHEMA_PREFIX = "test_" + uniq_id()


@pytest.fixture(scope="module", autouse=True)
def module_autouse() -> None:
    # disable Redshift in environ
    # del environ["PG_SCHEMA_PREFIX"]
    # set the test case for the unit tests
    environ["DEFAULT_DATASET"] = "test_fixture_carbon_bot_session_cases"
    add_config_to_env(GcpClientConfiguration)

    setup_runner(DEST_SCHEMA_PREFIX)
    init_logger(runner.CONFIG)

    # create client and dataset
    with BigQuerySqlClient("event", runner.CONFIG) as client:
        yield
        # delete temp datasets
        dataset_name = f"{DEST_SCHEMA_PREFIX}_staging"
        try:
            with client.with_alternative_dataset_name(dataset_name):
                client.drop_dataset()
        except Exception as ex1:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex1)}")

        dataset_name = f"{DEST_SCHEMA_PREFIX}_views"
        try:
            with client.with_alternative_dataset_name(dataset_name):
                client.drop_dataset()
        except Exception as ex2:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex2)}")
        dataset_name = f"{DEST_SCHEMA_PREFIX}_event"
        try:
            with client.with_alternative_dataset_name(dataset_name):
                client.drop_dataset()
        except Exception as ex2:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex2)}")


def test_create_folders() -> None:
    setup_runner("eks_dev_dest", override_values={
        "SOURCE_SCHEMA_PREFIX": "carbon_bot_3",
        "PACKAGE_ADDITIONAL_VARS": {"add_var_name": "add_var_value"},
        "LOG_FORMAT": "JSON",
        "LOG_LEVEL": "INFO"
    })

    assert runner.repo_path.endswith(runner.CLONED_PACKAGE_NAME)
    assert runner.profile_name == "rasa_semantic_schema_bigquery"
    assert runner.global_args == ["--log-format", "json"]
    assert runner.dbt_package_vars == {"source_schema_prefix": "carbon_bot_3", "dest_schema_prefix": "eks_dev_dest", "add_var_name": "add_var_value"}


def test_dbt_test_no_raw_schema() -> None:
    # force non existing schema
    setup_runner(DEST_SCHEMA_PREFIX, override_values={"SOURCE_SCHEMA_PREFIX": "jm_dev_2" + uniq_id()})
    # source test should not pass
    run_result = runner.run(None)
    # those are metrics returned when source schema test fail
    assert run_result == TRunMetrics(False, True, 1)


def test_dbt_run_full_refresh() -> None:
    setup_runner(DEST_SCHEMA_PREFIX, override_values={
        "PACKAGE_ADDITIONAL_VARS": {"user_id": "metadata__user_id"}
    })
    run_result = runner.run(None)
    assert run_result == TRunMetrics(False, False, 0)
    # enumerate gauges
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # all models must be CREATE TABLE as we do full refresh
    assert all(v.startswith("CREATE TABLE") for k,v in metrics.items() if k != "_loads")
    # _loads are MERGE - always go to the raw schema
    assert metrics["_loads"].startswith("CREATE TABLE")
    # all tests should pass
    runner.run_dbt("test")


def test_dbt_run_error_via_additional_vars() -> None:
    # generate with setting external user and session to non existing fields (metadata__sess_id not exists in JM schema)
    setup_runner(DEST_SCHEMA_PREFIX, override_values={
        "PACKAGE_ADDITIONAL_VARS": {"user_id": "metadata__user_id", "external_session_id": "metadata__sess_id"}
    })
    with pytest.raises(DBTProcessingError):
        runner.run(None)
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    assert "stg_interactions" in metrics
    assert "metadata__sess_id" in metrics["stg_interactions"]


def test_dbt_incremental_schema_out_of_sync_error() -> None:

    setup_runner(DEST_SCHEMA_PREFIX, override_values={
        # run stg_interactions and all parents
        "PACKAGE_RUN_PARAMS": ["--fail-fast", "--model", "+interactions"],
        # remove all counter metrics
        "PACKAGE_ADDITIONAL_VARS": {"count_metrics": []}
    })
    run_result = runner.run(None)
    assert run_result == TRunMetrics(False, False, 0)
    setup_runner(DEST_SCHEMA_PREFIX, override_values={
        # run stg_interactions and all parents
        "PACKAGE_RUN_PARAMS": ["--fail-fast", "--model", "+interactions"],
        # allow count metrics to generate schema error
        "PACKAGE_ADDITIONAL_VARS": {}
    })
    run_result = runner.run(None)
    assert run_result == TRunMetrics(False, False, 0)
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # full refresh on interactions
    assert metrics["interactions"].startswith("CREATE TABLE")
    # now incremental load should happen
    run_result = runner.run(None)
    assert run_result == TRunMetrics(False, False, 0)
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # full refresh on interactions
    assert metrics["interactions"].startswith("MERGE")

