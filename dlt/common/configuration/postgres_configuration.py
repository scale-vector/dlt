from dlt.common.configuration import BaseConfiguration
from dlt.common.configuration.utils import TSecretValue


class PostgresConfiguration(BaseConfiguration):
    DEFAULT_DATASET: str = None

    PG_DATABASE_NAME: str = None
    PG_PASSWORD: TSecretValue = None
    PG_USER: str = None
    PG_HOST: str = None
    PG_PORT: int = 5439
    PG_CONNECTION_TIMEOUT: int = 15

    @classmethod
    def check_integrity(cls) -> None:
        cls.PG_DATABASE_NAME = cls.PG_DATABASE_NAME.lower()
        cls.DEFAULT_DATASET = cls.DEFAULT_DATASET.lower()
        cls.PG_PASSWORD = TSecretValue(cls.PG_PASSWORD.strip())


class PostgresProductionConfiguration(PostgresConfiguration):
    pass
