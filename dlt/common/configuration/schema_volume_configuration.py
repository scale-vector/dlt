from typing import Optional


class SchemaVolumeConfiguration:
    SCHEMA_VOLUME_PATH: str = "_storage/schemas"  # path to volume with default schemas
    SCHEMA_SYNC_PATH: Optional[str] = None


class ProductionSchemaVolumeConfiguration:
    SCHEMA_VOLUME_PATH: str = None
