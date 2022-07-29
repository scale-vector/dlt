from typing import Dict, Mapping, Optional, Sequence, Tuple, cast, TypedDict, Any

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TColumnName, TSimpleRegex
from dlt.common.schema.utils import column_name_validator
from dlt.common.utils import uniq_id, digest128
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem, StrAny
from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.sources import DLT_METADATA_FIELD, TEventDLTMeta, get_table_name
from dlt.common.validation import validate_dict


class TEventRow(TypedDict, total=False):
    _dlt_id: str  # unique id of current row


class TEventRowRoot(TEventRow, total=False):
    _dlt_load_id: str  # load id to identify records loaded together that ie. need to be processed
    _dlt_meta: TEventDLTMeta  # stores metadata, should never be sent to the normalizer


class TEventRowChild(TEventRow, total=False):
    _dlt_root_id: str  # unique id of top level parent
    _dlt_parent_id: str  # unique id of parent row
    _dlt_list_idx: int  # position in the list of rows
    value: Any  # for lists of simple types


class JSONNormalizerConfigPropagation(TypedDict, total=True):
    root: Optional[Mapping[str, TColumnName]]
    tables: Optional[Mapping[str, Mapping[str, TColumnName]]]


class JSONNormalizerConfig(TypedDict, total=True):
    generate_dlt_id: Optional[bool]
    max_nesting: Optional[int]
    propagation: Optional[JSONNormalizerConfigPropagation]


# for those paths the complex nested objects should be left in place
def _is_complex_type(schema: Schema, table_name: str, field_name: str, _r_lvl: int) -> bool:
    # turn everything at the recursion level into complex type
    max_nesting  = schema._normalizers_config["json"].get("config", {}).get("max_nesting", 1000)
    assert _r_lvl <= max_nesting
    if _r_lvl == max_nesting:
        return True
    # or use definition in the schema
    column: TColumnSchema = None
    table = schema._schema_tables.get(table_name)
    if table:
        column = table["columns"].get(field_name, None)
    if column is None:
        data_type = schema.get_preferred_type(field_name)
    else:
        data_type = column["data_type"]
    return data_type == "complex"


def _flatten(schema: Schema, table: str, dict_row: TEventRow, _r_lvl: int) -> Tuple[TEventRow, Dict[str, Sequence[Any]]]:
    out_rec_row: DictStrAny = {}
    out_rec_list: Dict[str, Sequence[Any]] = {}

    def norm_row_dicts(dict_row: StrAny, __r_lvl: int, parent_name: Optional[str]) -> None:
        for k, v in dict_row.items():
            corrected_k = schema.normalize_column_name(k)
            child_name = corrected_k if not parent_name else schema.normalize_make_path(parent_name, corrected_k)
            # for lists and dicts we must check if type is possibly complex
            if isinstance(v, dict) or isinstance(v, list):
                if not _is_complex_type(schema, table, child_name, __r_lvl):
                    if isinstance(v, dict):
                        # flatten the dict more
                        norm_row_dicts(v, __r_lvl + 1, parent_name=child_name)
                    else:
                        # pass the list to out_rec_list
                        out_rec_list[child_name] = v
                    continue
                else:
                    # pass the complex value to out_rec_row
                    pass

            out_rec_row[child_name] = v

    norm_row_dicts(dict_row, _r_lvl, None)
    return cast(TEventRow, out_rec_row), out_rec_list


def _get_child_row_hash(parent_row_id: str, child_table: str, list_idx: int) -> str:
    # create deterministic unique id of the child row taking into account that all lists are ordered
    # and all child tables must be lists
    return digest128(f"{parent_row_id}_{child_table}_{list_idx}")


def _add_linking(row: TEventRowChild, extend: DictStrAny, parent_row_id: str, list_idx: int) -> TEventRowChild:
        row["_dlt_parent_id"] = parent_row_id
        row["_dlt_list_idx"] = list_idx
        row.update(extend)  # type: ignore

        return row


def _get_content_hash(schema: Schema, table: str, row: StrAny) -> str:
    return digest128(uniq_id())


def _get_propagated_values(schema: Schema, table: str, row: TEventRow, is_top_level: bool) -> StrAny:
    config: JSONNormalizerConfigPropagation = schema._normalizers_config["json"].get("config", {}).get("propagation", None)
    extend: DictStrAny = {}
    if config:
        # mapping(k:v): propagate property with name "k" as property with name "v" in child table
        mappings: DictStrStr = {}
        if is_top_level:
            mappings.update(config.get("root", {}))
        if table in config.get("tables", {}):
            mappings.update(config["tables"][table])
        # look for keys and create propagation as values
        for prop_from, prop_as in mappings.items():
            if prop_from in row:
                extend[prop_as] = row[prop_from]  # type: ignore

    return extend


# generate child tables only for lists
def _normalize_list(
    schema: Schema,
    seq: Sequence[Any],
    extend: DictStrAny,
    table: str,
    parent_table: str,
    parent_row_id: Optional[str] = None,
    _r_lvl: int = 0
) -> TNormalizedRowIterator:

    v: TEventRowChild = None
    for idx, v in enumerate(seq):
        # yield child table row
        if isinstance(v, dict):
            yield from _normalize_row(schema, v, extend, table, parent_table, parent_row_id, idx, _r_lvl)
        elif isinstance(v, list):
            # normalize lists of lists, we assume all lists in the list have the same type so they should go to the same table
            list_table_name = schema.normalize_make_path(table, "list")
            yield from _normalize_list(schema, v, extend, list_table_name, parent_table, parent_row_id, _r_lvl + 1)
        else:
            # list of simple types
            child_row_hash = _get_child_row_hash(parent_row_id, table, idx)
            e = _add_linking({"value": v, "_dlt_id": child_row_hash}, extend, parent_row_id, idx)
            yield (table, parent_table), e


def _normalize_row(
    schema: Schema,
    dict_row: TEventRow,
    extend: DictStrAny,
    table: str,
    parent_table: Optional[str] = None,
    parent_row_id: Optional[str] = None,
    pos: Optional[int] = None,
    _r_lvl: int = 0
) -> TNormalizedRowIterator:

    is_top_level = parent_table is None
    # flatten current row and extract all lists to recur into
    flattened_row, lists = _flatten(schema, table, dict_row, _r_lvl)
    # infer record hash or leave existing primary key if present
    row_id = flattened_row.get("_dlt_id", None)
    if not row_id:
        # check if we have primary key: if so use it
        primary_key = schema.filter_row_with_hint(table, "primary_key", flattened_row)
        if primary_key:
            # create row id from primary key
            row_id = digest128("_".join(map(lambda v: str(v), primary_key.values())))
        elif not is_top_level:
            # child table row deterministic hash
            row_id = _get_child_row_hash(parent_row_id, table, pos)
            # link to parent table
            _add_linking(cast(TEventRowChild, flattened_row), extend, parent_row_id, pos)
        else:
            # create hash based on the content of the row
            row_id = _get_content_hash(schema, table, flattened_row)
        flattened_row["_dlt_id"] = row_id

    # find fields to propagate to child tables in config
    extend.update(_get_propagated_values(schema, table, flattened_row, is_top_level))

    # yield parent table first
    yield (table, parent_table), flattened_row

    # normalize and yield lists
    for k, list_content in lists.items():
        yield from _normalize_list(schema, list_content, extend, schema.normalize_make_path(table, k), table, row_id, _r_lvl + 1)


def extend_schema(schema: Schema) -> None:
    # validate config
    config = schema._normalizers_config["json"].get("config", {})
    validate_dict(JSONNormalizerConfig, config, "./normalizers/json/config", validator_f=column_name_validator(schema.normalize_column_name))

    # quick check to see if hints are applied
    default_hints = schema.schema_settings.get("default_hints", {})
    if "not_null" in default_hints and "^_dlt_id$" in default_hints["not_null"]:
        return
    # add hints
    schema.merge_hints(
        {
            "not_null": [
                TSimpleRegex("_dlt_id"), TSimpleRegex("_dlt_root_id"), TSimpleRegex("_dlt_parent_id"),
                TSimpleRegex("_dlt_list_idx"), TSimpleRegex("_dlt_load_id")
                ],
            "foreign_key": [TSimpleRegex("_dlt_parent_id")],
            "unique": [TSimpleRegex("_dlt_id")]
        }
    )


def normalize_data_item(schema: Schema, source_event: TDataItem, load_id: str) -> TNormalizedRowIterator:
    # we will extend event with all the fields necessary to load it as root row
    event = cast(TEventRowRoot, source_event)
    # identify load id if loaded data must be processed after loading incrementally
    event["_dlt_load_id"] = load_id
    # find table name
    table_name = schema.normalize_table_name(get_table_name(event) or schema.schema_name)
    # drop dlt metadata before normalizing
    event.pop(DLT_METADATA_FIELD, None)  # type: ignore
    # use event type or schema name as table name, request _dlt_root_id propagation
    yield from _normalize_row(schema, cast(TEventRowChild, event), {}, table_name)
