from dlt.common.configuration.run_configuration import BaseConfiguration


class LoadingVolumeConfiguration(BaseConfiguration):
    LOADING_VOLUME_PATH: str = "_storage/loading"  # path to volume where files to be loaded to analytical storage are stored
    DELETE_COMPLETED_JOBS: bool = False  # if set to true the folder with completed jobs will be deleted

class ProductionLoadingVolumeConfiguration(LoadingVolumeConfiguration):
    LOADING_VOLUME_PATH: str = None
