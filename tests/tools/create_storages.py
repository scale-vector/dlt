from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.configuration import NormalizeVolumeConfiguration
from dlt.common.storages.loader_storage import LoaderStorage
from dlt.common.configuration import LoadingVolumeConfiguration
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.configuration import SchemaVolumeConfiguration


NormalizeStorage(True, NormalizeVolumeConfiguration)
LoaderStorage(True, LoadingVolumeConfiguration, "jsonl", LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)
SchemaStorage(SchemaVolumeConfiguration.SCHEMA_VOLUME_PATH, makedirs=True)
