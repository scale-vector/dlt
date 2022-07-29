from typing import List, Optional, Type

from dlt.common.typing import StrAny
from dlt.common.configuration.utils import TSecretValue, make_configuration, _get_key_value
from dlt.common.configuration import PoolRunnerConfiguration, TPoolType, PostgresConfiguration, PostgresProductionConfiguration, GcpClientConfiguration, GcpClientProductionConfiguration

from . import __version__


class DBTRunnerConfiguration(PoolRunnerConfiguration):
    POOL_TYPE: TPoolType = "none"
    STOP_AFTER_RUNS: int = 1
    PACKAGE_VOLUME_PATH: str = "_storage/dbt_runner"
    PACKAGE_REPOSITORY_URL: str = "https://github.com/scale-vector/rasa_semantic_schema_customization.git"
    PACKAGE_REPOSITORY_BRANCH: Optional[str] = None
    PACKAGE_REPOSITORY_SSH_KEY: TSecretValue = TSecretValue("")  # the default is empty value which will disable custom SSH KEY
    PACKAGE_PROFILES_DIR: str = "."
    PACKAGE_PROFILE_PREFIX: str = "rasa_semantic_schema"
    PACKAGE_SOURCE_TESTS_SELECTOR: str = "tag:prerequisites"
    PACKAGE_ADDITIONAL_VARS: Optional[StrAny] = None
    PACKAGE_RUN_PARAMS: List[str] = ["--fail-fast"]
    AUTO_FULL_REFRESH_WHEN_OUT_OF_SYNC: bool = True

    SOURCE_SCHEMA_PREFIX: str = None
    DEST_SCHEMA_PREFIX: Optional[str] = None

    @classmethod
    def check_integrity(cls) -> None:
        if cls.PACKAGE_REPOSITORY_SSH_KEY and cls.PACKAGE_REPOSITORY_SSH_KEY[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            cls.PACKAGE_REPOSITORY_SSH_KEY = TSecretValue(cls.PACKAGE_REPOSITORY_SSH_KEY + "\n")
        if cls.STOP_AFTER_RUNS != 1:
            # always stop after one run
            cls.STOP_AFTER_RUNS = 1


class DBTRunnerProductionConfiguration(DBTRunnerConfiguration):
    PACKAGE_VOLUME_PATH: str = "/var/local/app"  # this is actually not exposed as volume
    PACKAGE_REPOSITORY_URL: str = None


def gen_configuration_variant(initial_values: StrAny = None) -> Type[DBTRunnerConfiguration]:
    # derive concrete config depending on env vars present
    DBTRunnerConfigurationImpl: Type[DBTRunnerConfiguration]
    DBTRunnerProductionConfigurationImpl: Type[DBTRunnerProductionConfiguration]

    source_schema_prefix = _get_key_value("DEFAULT_DATASET", type(str))

    if _get_key_value("PROJECT_ID", type(str)):
        class DBTRunnerConfigurationPostgres(PostgresConfiguration, DBTRunnerConfiguration):
            SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerConfigurationImpl = DBTRunnerConfigurationPostgres

        class DBTRunnerProductionConfigurationPostgres(DBTRunnerProductionConfiguration, PostgresProductionConfiguration, DBTRunnerConfigurationPostgres):
            pass
            # SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerProductionConfigurationImpl = DBTRunnerProductionConfigurationPostgres

    else:
        class DBTRunnerConfigurationGcp(GcpClientConfiguration, DBTRunnerConfiguration):
            SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerConfigurationImpl = DBTRunnerConfigurationGcp

        class DBTRunnerProductionConfigurationGcp(DBTRunnerProductionConfiguration, GcpClientProductionConfiguration, DBTRunnerConfigurationGcp):
            pass
            # SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerProductionConfigurationImpl = DBTRunnerProductionConfigurationGcp

    return make_configuration(DBTRunnerConfigurationImpl, DBTRunnerProductionConfigurationImpl, initial_values=initial_values)
