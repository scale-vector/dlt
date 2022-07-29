from typing import Any, Callable, Dict, List, Literal, Optional, Set, Type, TypedDict, NewType, get_args

from dlt.common.typing import StrAny


TDataType = Literal["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"]
THintType = Literal["not_null", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TColumnProp = Literal["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TWriteDisposition = Literal["skip", "append", "replace", "merge", "upsert"]
TTypeDetections = Literal["timestamp", "iso_timestamp"]
TTypeDetectionFunc = Callable[[Type[Any], Any], Optional[TDataType]]

DATA_TYPES: Set[TDataType] = set(get_args(TDataType))
COLUMN_PROPS: Set[TColumnProp] = set(get_args(TColumnProp))
COLUMN_HINTS: Set[THintType] = set(["partition", "cluster", "primary_key", "foreign_key", "sort", "unique"])
WRITE_DISPOSITIONS: Set[TWriteDisposition] = set(get_args(TWriteDisposition))


class TColumnSchemaBase(TypedDict, total=True):
    name: Optional[str]
    data_type: TDataType
    nullable: bool


class TColumnSchema(TColumnSchemaBase, total=False):
    description: Optional[str]
    partition: Optional[bool]
    cluster: Optional[bool]
    unique: Optional[bool]
    sort: Optional[bool]
    primary_key: Optional[bool]
    foreign_key: Optional[bool]


TTableSchemaColumns = Dict[str, TColumnSchema]
TSimpleRegex = NewType("TSimpleRegex", str)
TColumnName = NewType("TColumnName", str)
SIMPLE_REGEX_PREFIX = "re:"


class TRowFilters(TypedDict, total=True):
    excludes: Optional[List[TSimpleRegex]]
    includes: Optional[List[TSimpleRegex]]


class TTableSchema(TypedDict, total=False):
    name: Optional[str]
    description: Optional[str]
    write_disposition: Optional[TWriteDisposition]
    table_sealed: Optional[bool]
    parent: Optional[str]
    filters: Optional[TRowFilters]
    columns: TTableSchemaColumns


class TPartialTableSchema(TTableSchema):
    pass


TSchemaTables = Dict[str, TTableSchema]
TSchemaUpdate = Dict[str, List[TPartialTableSchema]]


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is consumed by `module`


class TNormalizersConfig(TypedDict, total=True):
    names: str
    detections: Optional[List[TTypeDetections]]
    json: TJSONNormalizer


class TSchemaSettings(TypedDict, total=False):
    schema_sealed: Optional[bool]
    default_hints: Optional[Dict[THintType, List[TSimpleRegex]]]
    preferred_types: Optional[Dict[TSimpleRegex, TDataType]]


class TStoredSchema(TypedDict, total=True):
    version: int
    engine_version: int
    name: str
    settings: Optional[TSchemaSettings]
    tables: TSchemaTables
    normalizers: TNormalizersConfig
