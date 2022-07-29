from os import environ
from git import GitCommandError
import pytest
from prometheus_client import CollectorRegistry

from dlt.common import logger
from dlt.common.configuration import PostgresConfiguration
from dlt.common.configuration.utils import make_configuration
from dlt.common.file_storage import FileStorage
from dlt.common.telemetry import TRunMetrics, get_metrics_from_prometheus
from dlt.common.typing import StrStr
from dlt.common.utils import uniq_id, with_custom_environ

from dlt.dbt_runner.utils import DBTProcessingError
from dlt.dbt_runner.configuration import DBTRunnerConfiguration
from dlt.dbt_runner import runner
from dlt.loaders.redshift.client import RedshiftSqlClient

from tests.utils import add_config_to_env, clean_storage, init_logger, preserve_environ
from tests.dbt_runner.utils import modify_and_commit_file, load_secret, setup_runner

DEST_SCHEMA_PREFIX = "test_" + uniq_id()


@pytest.fixture(scope="module", autouse=True)
def module_autouse() -> None:
    # disable GCP in environ
    del environ["PROJECT_ID"]
    # set the test case for the unit tests
    environ["DEFAULT_DATASET"] = "test_fixture_carbon_bot_session_cases"
    add_config_to_env(PostgresConfiguration)

    setup_runner(DEST_SCHEMA_PREFIX)
    init_logger(runner.CONFIG)

    # create client and dataset
    with RedshiftSqlClient("event", runner.CONFIG) as client:
        yield
        # delete temp schemas
        dataset_name = f"{DEST_SCHEMA_PREFIX}_views"
        try:
            with client.with_alternative_dataset_name(dataset_name):
                client.drop_dataset()
        except Exception as ex1:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex1)}")

        dataset_name = f"{DEST_SCHEMA_PREFIX}_staging"
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


def test_configuration() -> None:
    # check names normalized
    C = make_configuration(
        DBTRunnerConfiguration,
        DBTRunnerConfiguration,
        initial_values={"PACKAGE_REPOSITORY_SSH_KEY": "---NO NEWLINE---", "SOURCE_SCHEMA_PREFIX": "schema"}
    )
    assert C.PACKAGE_REPOSITORY_SSH_KEY == "---NO NEWLINE---\n"

    C = make_configuration(
        DBTRunnerConfiguration,
        DBTRunnerConfiguration,
        initial_values={"PACKAGE_REPOSITORY_SSH_KEY": "---WITH NEWLINE---\n", "SOURCE_SCHEMA_PREFIX": "schema"}
    )
    assert C.PACKAGE_REPOSITORY_SSH_KEY == "---WITH NEWLINE---\n"


def test_create_folders() -> None:
    setup_runner("eks_dev_dest", override_values={
        "SOURCE_SCHEMA_PREFIX": "carbon_bot_3",
        "PACKAGE_ADDITIONAL_VARS": {"add_var_name": "add_var_value"},
        "LOG_FORMAT": "JSON",
        "LOG_LEVEL": "INFO"
    })
    assert runner.repo_path.endswith(runner.CLONED_PACKAGE_NAME)
    assert runner.profile_name == "rasa_semantic_schema_redshift"
    assert runner.global_args == ["--log-format", "json"]
    assert runner.dbt_package_vars == {"source_schema_prefix": "carbon_bot_3", "dest_schema_prefix": "eks_dev_dest", "add_var_name": "add_var_value"}


def test_initialize_package_wrong_key() -> None:
    setup_runner(DEST_SCHEMA_PREFIX, override_values={
        # private repo
        "PACKAGE_REPOSITORY_URL": "git@github.com:scale-vector/rasa_bot_experiments.git"
    })
    runner.CONFIG.PACKAGE_REPOSITORY_SSH_KEY = load_secret("DEPLOY_KEY")

    with pytest.raises(GitCommandError):
        runner.run(None)


def test_reinitialize_package() -> None:
    setup_runner(DEST_SCHEMA_PREFIX)
    runner.ensure_newest_package()
    # mod the package
    readme_path = modify_and_commit_file(runner.repo_path, "README.md", content=runner.CONFIG.DEST_SCHEMA_PREFIX)
    assert runner.storage.has_file(readme_path)
    # this will wipe out old package and clone again
    runner.ensure_newest_package()
    # we have old file back
    assert runner.storage.load(f"{runner.CLONED_PACKAGE_NAME}/README.md") != runner.CONFIG.DEST_SCHEMA_PREFIX


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
    # all models must be SELECT as we do full refresh
    assert set(v for k,v in metrics.items() if k != "_loads") == set(["SELECT"])
    # _loads are INSERT - always go to the raw schema
    assert metrics["_loads"].startswith("SELECT")
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
    assert metrics["interactions"].startswith("SELECT")
    # now incremental load should happen
    run_result = runner.run(None)
    assert run_result == TRunMetrics(False, False, 0)
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # full refresh on interactions
    assert metrics["interactions"].startswith("INSERT")


def get_runner() -> FileStorage:
    clean_storage()
    runner.storage, runner.dbt_package_vars, runner.global_args, runner.repo_path, runner.profile_name  = runner.create_folders()
    runner.model_elapsed_gauge, runner.model_exec_info = runner.create_gauges(CollectorRegistry(auto_describe=True))
    return runner.storage
