from typing import Optional, Sequence, Tuple, Type
from git import GitError
from prometheus_client import REGISTRY, Gauge, CollectorRegistry, Info
from prometheus_client.metrics import MetricWrapperBase
from dlt.common.configuration import GcpClientConfiguration

from dlt.common import logger
from dlt.common.typing import DictStrAny, DictStrStr, StrAny
from dlt.common.logger import process_internal_exception, is_json_logging
from dlt.common.telemetry import get_logging_extras
from dlt.common.file_storage import FileStorage
from dlt.common.runners import TRunArgs, initialize_runner, run_pool
from dlt.common.telemetry import TRunMetrics

from dlt.dbt_runner.configuration import DBTRunnerConfiguration, gen_configuration_variant
from dlt.dbt_runner.utils import DBTProcessingError, clone_repo, dbt_results, ensure_remote_head, git_custom_key_command, initialize_dbt_logging, is_incremental_schema_out_of_sync_error, run_dbt_command
from dlt.dbt_runner.exceptions import PrerequisitesException


CLONED_PACKAGE_NAME = "dbt_package"

CONFIG: Type[DBTRunnerConfiguration] = None
storage: FileStorage = None
dbt_package_vars: StrAny = None
global_args: Sequence[str] = None
repo_path: str = None
profile_name: str = None

model_elapsed_gauge: Gauge = None
model_exec_info: Info = None


def create_folders() -> Tuple[FileStorage, StrAny, Sequence[str], str, str]:
    storage = FileStorage(CONFIG.PACKAGE_VOLUME_PATH, makedirs=True)
    dbt_package_vars: DictStrAny = {
        "source_schema_prefix": CONFIG.SOURCE_SCHEMA_PREFIX
    }
    if CONFIG.DEST_SCHEMA_PREFIX:
        dbt_package_vars["dest_schema_prefix"] = CONFIG.DEST_SCHEMA_PREFIX
    if CONFIG.PACKAGE_ADDITIONAL_VARS:
        dbt_package_vars.update(CONFIG.PACKAGE_ADDITIONAL_VARS)

    # initialize dbt logging, returns global parameters to dbt command
    global_args = initialize_dbt_logging(CONFIG.LOG_LEVEL, is_json_logging(CONFIG.LOG_FORMAT))

    # generate path for the dbt package repo
    repo_path = storage._make_path(CLONED_PACKAGE_NAME)

    # generate profile name
    profile_name: str = None
    if CONFIG.PACKAGE_PROFILE_PREFIX:
        if issubclass(CONFIG, GcpClientConfiguration):
            profile_name = "%s_bigquery" % (CONFIG.PACKAGE_PROFILE_PREFIX)
        else:
            profile_name = "%s_redshift" % (CONFIG.PACKAGE_PROFILE_PREFIX)

    return storage, dbt_package_vars, global_args, repo_path, profile_name


def create_gauges(registry: CollectorRegistry) -> Tuple[MetricWrapperBase, MetricWrapperBase]:
    return (
        Gauge("dbtrunner_model_elapsed_seconds", "Last model processing time", ["model"], registry=registry),
        Info("dbtrunner_model_status", "Last execution status of the model", registry=registry)
    )


def run_dbt(command: str, command_args: Sequence[str] = None) -> Sequence[dbt_results. BaseResult]:
    logger.info(f"Exec dbt command: {global_args} {command} {command_args} {dbt_package_vars} on profile {profile_name or '<project_default>'}")
    return run_dbt_command(
        repo_path, command,
        CONFIG.PACKAGE_PROFILES_DIR,
        profile_name=profile_name,
        command_args=command_args,
        global_args=global_args,
        dbt_vars=dbt_package_vars
    )


def log_dbt_run_results(results: dbt_results.RunExecutionResult) -> None:
    # run may return RunResult of something different depending on error
    if issubclass(type(results), dbt_results.BaseResult):
        results = [results]  # make it iterable
    elif issubclass(type(results), dbt_results.ExecutionResult):
        pass
    else:
        logger.warning(f"{type(results)} is unknown and cannot be logged")
        return

    info: DictStrStr = {}
    for res in results:
        name = res.node.name
        message = res.message
        time = res.execution_time
        if res.status == dbt_results.RunStatus.Error:
            logger.error(f"Model {name} error! Error: {message}")
        else:
            logger.info(f"Model {name} {res.status} in {time} seconds with {message}")
        model_elapsed_gauge.labels(name).set(time)
        info[name] = message

    # log execution
    model_exec_info.info(info)
    logger.metrics("Executed models", extra=get_logging_extras([model_elapsed_gauge, model_exec_info]))


def initialize_package(with_git_command: Optional[str]) -> None:
    try:
        # cleanup package folder
        if storage.has_folder(CLONED_PACKAGE_NAME):
            storage.delete_folder(CLONED_PACKAGE_NAME, recursively=True)
        logger.info(f"Will clone {CONFIG.PACKAGE_REPOSITORY_URL} head {CONFIG.PACKAGE_REPOSITORY_BRANCH} into {repo_path}")
        clone_repo(CONFIG.PACKAGE_REPOSITORY_URL, repo_path, branch=CONFIG.PACKAGE_REPOSITORY_BRANCH, with_git_command=with_git_command)
        run_dbt("deps")
    except Exception:
        # delete folder so we start clean next time
        if storage.has_folder(CLONED_PACKAGE_NAME):
            storage.delete_folder(CLONED_PACKAGE_NAME, recursively=True)
        raise


def ensure_newest_package() -> None:
    with git_custom_key_command(CONFIG.PACKAGE_REPOSITORY_SSH_KEY) as ssh_command:
        try:
            ensure_remote_head(repo_path, with_git_command=ssh_command)
        except GitError as err:
            # cleanup package folder
            logger.info(f"Package will be cloned due to {type(err).__name__}:{str(err)}")
            initialize_package(with_git_command=ssh_command)


def run_db_steps() -> Sequence[dbt_results.BaseResult]:
    # make sure we use package from the remote head
    ensure_newest_package()
    # check if raw schema exists
    try:
        if CONFIG.PACKAGE_SOURCE_TESTS_SELECTOR:
            run_dbt("test", ["-s", CONFIG.PACKAGE_SOURCE_TESTS_SELECTOR])
    except DBTProcessingError as err:
        raise PrerequisitesException() from err

    # always run seeds
    run_dbt("seed")
    # throws DBTProcessingError
    try:
        return run_dbt("run", CONFIG.PACKAGE_RUN_PARAMS)
    except DBTProcessingError as e:
        # detect incremental model out of sync
        if is_incremental_schema_out_of_sync_error(e.results) and CONFIG.AUTO_FULL_REFRESH_WHEN_OUT_OF_SYNC:
            logger.warning(f"Attempting full refresh due to incremental model out of sync on {e.results.message}")
            return run_dbt("run", CONFIG.PACKAGE_RUN_PARAMS + ["--full-refresh"])
        else:
            raise


def run(_: None) -> TRunMetrics:
    try:
        # there were many issues with running the method below with pool.apply
        # 1 - some exceptions are not serialized well on process boundary and queue hangs
        # 2 - random hangs event if there's no exception, probably issues with DBT spawning its own workers
        # instead the runner host was configured to recycle each run
        results = run_db_steps()
        log_dbt_run_results(results)
        return TRunMetrics(False, False, 0)
    except PrerequisitesException:
        logger.warning("Raw schema test failed, it may yet not be created")
        # run failed and loads possibly still pending
        return TRunMetrics(False, True, 1)
    except DBTProcessingError as runerr:
        log_dbt_run_results(runerr.results)
        # pass exception to the runner
        raise


def configure(C: Type[DBTRunnerConfiguration], collector: CollectorRegistry) -> None:
    global CONFIG
    global storage, dbt_package_vars, global_args, repo_path, profile_name
    global model_elapsed_gauge, model_exec_info

    CONFIG = C
    storage, dbt_package_vars, global_args, repo_path, profile_name = create_folders()
    try:
        model_elapsed_gauge, model_exec_info = create_gauges(REGISTRY)
    except ValueError as v:
        # ignore re-creation of gauges
        if "Duplicated timeseries" not in str(v):
            raise


def main(args: TRunArgs) -> int:
    C = gen_configuration_variant()
    # we should force single run
    initialize_runner(C, args)
    try:
        configure(C, REGISTRY)
    except Exception:
        process_internal_exception("init module")
        return -1

    return run_pool(C, run)


def run_main(args: TRunArgs) -> None:
    exit(main(args))
