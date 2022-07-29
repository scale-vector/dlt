
from dataclasses import dataclass
from typing import Literal
from dlt.common import json

from dlt.common.typing import StrAny
from dlt.common.configuration.utils import TSecretValue

TLoaderType = Literal["gcp", "redshift"]
TPipelineStage = Literal["extract", "normalize", "load"]

# extractor generator yields functions that returns list of items of the type (table) when called
# this allows generator to implement retry logic
# TExtractorItem = Callable[[], Iterator[StrAny]]
# # extractor generator yields tuples: (type of the item (table name), function defined above)
# TExtractorItemWithTable = Tuple[str, TExtractorItem]
# TExtractorGenerator = Callable[[DictStrAny], Iterator[TExtractorItemWithTable]]


@dataclass
class PipelineCredentials:
    CLIENT_TYPE: TLoaderType

    @property
    def default_dataset(self) -> str:
        pass

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        pass

@dataclass
class GCPPipelineCredentials(PipelineCredentials):
    PROJECT_ID: str
    DEFAULT_DATASET: str
    BQ_CRED_CLIENT_EMAIL: str
    BQ_CRED_PRIVATE_KEY: TSecretValue = None
    TIMEOUT: float = 30.0

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value

    @classmethod
    def from_services_dict(cls, services: StrAny, dataset_prefix: str) -> "GCPPipelineCredentials":
        assert dataset_prefix is not None

        return cls("gcp", services["project_id"], dataset_prefix, services["client_email"], services["private_key"])

    @classmethod
    def from_services_file(cls, services_path: str, dataset_prefix: str) -> "GCPPipelineCredentials":
        with open(services_path, "r", encoding="utf-8") as f:
            services = json.load(f)
        return GCPPipelineCredentials.from_services_dict(services, dataset_prefix)


@dataclass
class PostgresPipelineCredentials(PipelineCredentials):
    PG_DATABASE_NAME: str
    DEFAULT_DATASET: str
    PG_USER: str
    PG_HOST: str
    PG_PASSWORD: TSecretValue = None
    PG_PORT: int = 5439
    PG_CONNECTION_TIMEOUT: int = 15

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value
