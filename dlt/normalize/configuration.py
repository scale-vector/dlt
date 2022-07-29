from typing import Type

from dlt.common.typing import StrAny
from dlt.common.dataset_writers import TLoaderFileFormat
from dlt.common.configuration import (PoolRunnerConfiguration, NormalizeVolumeConfiguration,
                                              LoadingVolumeConfiguration, SchemaVolumeConfiguration,
                                              ProductionLoadingVolumeConfiguration, ProductionNormalizeVolumeConfiguration,
                                              ProductionSchemaVolumeConfiguration,
                                              TPoolType, make_configuration)

from . import __version__


class NormalizeConfiguration(PoolRunnerConfiguration, NormalizeVolumeConfiguration, LoadingVolumeConfiguration, SchemaVolumeConfiguration):
    MAX_EVENTS_IN_CHUNK: int = 40000  # maximum events to be processed in single chunk
    LOADER_FILE_FORMAT: TLoaderFileFormat = "jsonl"  # jsonp or insert commands will be generated
    POOL_TYPE: TPoolType = "process"


class ProductionNormalizeConfiguration(ProductionNormalizeVolumeConfiguration, ProductionLoadingVolumeConfiguration,
                                      ProductionSchemaVolumeConfiguration, NormalizeConfiguration):
    pass


def configuration(initial_values: StrAny = None) -> Type[NormalizeConfiguration]:
    return make_configuration(NormalizeConfiguration, ProductionNormalizeConfiguration, initial_values=initial_values)
