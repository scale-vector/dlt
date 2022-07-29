from typing import Any, Type

from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny
from dlt.common.configuration import (PoolRunnerConfiguration,
                                              LoadingVolumeConfiguration,
                                              ProductionLoadingVolumeConfiguration,
                                              PostgresConfiguration, PostgresProductionConfiguration,
                                              GcpClientConfiguration, GcpClientProductionConfiguration,
                                              TPoolType, make_configuration)

from dlt.loaders.dummy.configuration import DummyClientConfiguration

from . import __version__

class LoaderConfiguration(PoolRunnerConfiguration, LoadingVolumeConfiguration):
    CLIENT_TYPE: str = "dummy"  # which destination to load data to
    # MAX_PARALLEL_LOADS: int = 20  # how many parallel loads can be executed
    # MAX_PARALLELISM: int = 20  # in 20 separate threads
    POOL_TYPE: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool


class ProductionLoaderConfiguration(ProductionLoadingVolumeConfiguration, LoaderConfiguration):
    pass


def configuration(initial_values: StrAny = None) -> Type[LoaderConfiguration]:
    return make_configuration(LoaderConfiguration, ProductionLoaderConfiguration, initial_values=initial_values)
