from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.normalizers.names.snake_case import normalize_make_path, normalize_break_path, normalize_make_dataset_name  # import path functions from snake case
from dlt.common.schema.schema import Schema
from dlt.common.typing import TDataItem


def normalize_table_name(name: str) -> str:
    return name.capitalize()


def normalize_column_name(name: str) -> str:
    return "column_" + name.lower()


def extend_schema(schema: Schema) -> None:
    json_config = schema._normalizers_config["json"]["config"]
    d_h = schema._settings.setdefault("default_hints", {})
    d_h["not_null"] = json_config["not_null"]


def normalize_data_item(schema: Schema, source_event: TDataItem, load_id: str) -> TNormalizedRowIterator:
    yield ("table", None), source_event
