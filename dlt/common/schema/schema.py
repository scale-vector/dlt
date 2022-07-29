from importlib import import_module
import yaml
from copy import copy
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, Any, cast

from dlt.common.typing import DictStrAny, StrAny, REPattern
from dlt.common.normalizers.names import TNormalizeBreakPath, TNormalizeMakePath, TNormalizeNameFunc
from dlt.common.normalizers.json import TNormalizeJSONFunc
from dlt.common.schema.typing import TNormalizersConfig, TPartialTableSchema, TSchemaSettings, TSimpleRegex, TStoredSchema, TSchemaTables, TTableSchema, TTableSchemaColumns, TColumnSchema, TColumnProp, TDataType, THintType, TWriteDisposition
from dlt.common.schema import utils
from dlt.common.schema.exceptions import (CannotCoerceColumnException, CannotCoerceNullException, InvalidSchemaName,
                                          ParentTableNotFoundException, SchemaCorruptedException, TablePropertiesClashException)
from dlt.common.validation import validate_dict


class Schema:

    VERSION_TABLE_NAME = "_dlt_version"
    VERSION_COLUMN_NAME = "version"
    LOADS_TABLE_NAME = "_dlt_loads"
    ENGINE_VERSION = 3

    def __init__(self, name: str, normalizers: TNormalizersConfig = None) -> None:
        # verify schema name
        if name != utils.normalize_schema_name(name):
            raise InvalidSchemaName(name, utils.normalize_schema_name(name))
        self._schema_tables: TSchemaTables = {}
        self._schema_name: str = name
        self._version = 1
        # schema settings to hold default hints, preferred types and other settings
        self._settings: TSchemaSettings = {}

        # list of preferred types: map regex on columns into types
        self._compiled_preferred_types: List[Tuple[REPattern, TDataType]] = []
        # compiled default hints
        self._compiled_hints: Dict[THintType, Sequence[REPattern]] = {}
        # compiled exclude filters per table
        self._compiled_excludes: Dict[str, Sequence[REPattern]] = {}
        # compiled include filters per table
        self._compiled_includes: Dict[str, Sequence[REPattern]] = {}

        # normalizers config
        self._normalizers_config: TNormalizersConfig = normalizers
        # name normalization functions
        self.normalize_table_name: TNormalizeNameFunc = None
        self.normalize_column_name: TNormalizeNameFunc = None
        self.normalize_schema_name: TNormalizeNameFunc = None
        self.normalize_make_dataset_name: TNormalizeMakePath = None
        self.normalize_make_path: TNormalizeMakePath = None
        self.normalize_break_path: TNormalizeBreakPath = None
        # json normalization function
        self.normalize_data_item: TNormalizeJSONFunc = None

        # add version tables
        self._add_standard_tables()
        # add standard hints
        self._add_standard_hints()
        # configure normalizers, including custom config if present
        self._configure_normalizers()
        # compile all known regexes
        self._compile_regexes()

    @classmethod
    def from_dict(cls, d: DictStrAny) -> "Schema":
        # upgrade engine if needed
        stored_schema = utils.upgrade_engine_version(d, d["engine_version"], cls.ENGINE_VERSION)
        # verify schema
        utils.validate_stored_schema(stored_schema)
        # add defaults
        utils.apply_defaults(stored_schema)

        # create new instance from dict
        self: Schema = cls(stored_schema["name"], normalizers=stored_schema.get("normalizers", None))
        self._schema_tables = stored_schema.get("tables", {})
        # TODO: generate difference if STANDARD SCHEMAS are different than those and increase schema version
        if Schema.VERSION_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {Schema.VERSION_TABLE_NAME}")
        if Schema.LOADS_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {Schema.LOADS_TABLE_NAME}")
        self._version = stored_schema["version"]
        self._settings = stored_schema.get("settings", {})
        # compile regexes
        self._compile_regexes()

        return self

    def filter_row(self, table_name: str, row: StrAny) -> StrAny:
        # exclude row elements according to the rules in `filter` elements of the table
        # include rules have precedence and are used to make exceptions to exclude rules
        # the procedure will apply rules from the table_name and it's all parent tables up until root
        # parent tables are computed by `normalize_break_path` function so they do not need to exist in the schema
        # note: the above is not very clean. the `parent` element of each table should be used but as the rules
        #  are typically used to prevent not only table fields but whole tables from being created it is not possible

        def _exclude(path: str, excludes: Sequence[REPattern], includes: Sequence[REPattern]) -> bool:
            is_included = False
            is_excluded = any(exclude.search(path) for exclude in excludes)
            if is_excluded:
                # we may have exception if explicitly included
                is_included = any(include.search(path) for include in includes)
            return is_excluded and not is_included

        # break table name in components
        branch = self.normalize_break_path(table_name)

        # check if any of the rows is excluded by rules in any of the tables
        for i in range(len(branch), 0, -1):  # stop is exclusive in `range`
            # start at the top level table
            c_t = self.normalize_make_path(*branch[:i])
            excludes = self._compiled_excludes.get(c_t)
            # only if there's possibility to exclude, continue
            if excludes:
                includes = self._compiled_includes.get(c_t, [])
                for field_name in list(row.keys()):
                    path = self.normalize_make_path(*branch[i:], field_name)
                    if _exclude(path, excludes, includes):
                        # TODO: copy to new instance
                        del row[field_name]  # type: ignore
            # if row is empty, do not process further
            if not row:
                break
        return row

    def coerce_row(self, table_name: str, parent_table: str, row: StrAny) -> Tuple[StrAny, TPartialTableSchema]:
        # get existing or create a new table
        table = self._schema_tables.get(table_name, utils.new_table(table_name, parent_table))
        table_columns = table["columns"]

        partial_table: TPartialTableSchema = None
        new_row: DictStrAny = {}
        for col_name, v in row.items():
            # skip None values, we should infer the types later
            if v is None:
                # just check if column is nullable if exists
                self._coerce_null_value(table_columns, table_name, col_name)
            else:
                new_col_name, new_col_def, new_v = self._coerce_non_null_value(table_columns, table_name, col_name, v)
                new_row[new_col_name] = new_v
                if new_col_def:
                    if not partial_table:
                        partial_table = copy(table)
                        partial_table["columns"] = {}
                    partial_table["columns"][new_col_name] = new_col_def

        return new_row, partial_table

    def update_schema(self, partial_table: TPartialTableSchema) -> None:
        table_name = partial_table["name"]
        parent_table_name = partial_table.get("parent")
        # check if parent table present
        if parent_table_name is not None:
            if self._schema_tables.get(parent_table_name) is None:
                raise ParentTableNotFoundException(
                    table_name, parent_table_name,
                    f" This may be due to misconfigured excludes filter that fully deletes content of the {parent_table_name}. Add includes that will preserve the parent table."
                    )
        table = self._schema_tables.get(table_name)
        if table is None:
            # add the whole new table to SchemaTables
            self._schema_tables[table_name] = partial_table
        else:
            # check if table properties can be merged
            if table.get("parent") != partial_table.get("parent"):
                raise TablePropertiesClashException(table_name, "parent", table.get("parent"), partial_table.get("parent"))
            if table.get("write_disposition") != partial_table.get("write_disposition"):
                raise TablePropertiesClashException(table_name, "write_disposition", table.get("write_disposition"), partial_table.get("write_disposition"))
            # add several columns to existing table
            table_columns = table["columns"]
            for column in partial_table["columns"].values():
                column_name = column["name"]
                if column_name in table_columns:
                    # we do not support changing existing columns
                    if not utils.compare_columns(table_columns[column_name], column):
                        # attempt to update to incompatible columns
                        raise CannotCoerceColumnException(table_name, column_name, table_columns[column_name]["data_type"], column["data_type"], None)
                else:
                    table_columns[column_name] = column
        # bump schema version
        self._version += 1

    def filter_row_with_hint(self, table_name: str, hint_type: THintType, row: StrAny) -> StrAny:
        rv_row: DictStrAny = {}
        column_prop: TColumnProp = utils.hint_to_column_prop(hint_type)
        try:
            table = self.get_table_columns(table_name)
            for column_name in table:
                if column_name in row:
                    hint_value = table[column_name][column_prop]
                    if (hint_value and column_prop != "nullable") or (column_prop == "nullable" and not hint_value):
                        rv_row[column_name] = row[column_name]
        except KeyError:
            for k, v in row.items():
                if self._infer_hint(hint_type, v, k):
                    rv_row[k] = v

        # dicts are ordered and we will return the rows with hints in the same order as they appear in the columns
        return rv_row

    def merge_hints(self, new_hints: Mapping[THintType, Sequence[TSimpleRegex]]) -> None:
        # validate regexes
        validate_dict(TSchemaSettings, {"default_hints": new_hints}, ".", validator_f=utils.simple_regex_validator)
        # prepare hints to be added
        default_hints = self._settings.setdefault("default_hints", {})
        # add `new_hints` to existing hints
        for h, l in new_hints.items():
            if h in default_hints:
                # merge if hint of this type exist
                default_hints[h] = list(set(default_hints[h] + list(l)))
            else:
                # set new hint type
                default_hints[h] = l  # type: ignore
        self._compile_regexes()

    def get_schema_update_for(self, table_name: str, t: TTableSchemaColumns) -> List[TColumnSchema]:
        # gets new columns to be added to "t" to bring up to date with stored schema
        diff_c: List[TColumnSchema] = []
        s_t = self.get_table_columns(table_name)
        for c in s_t.values():
            if c["name"] not in t:
                diff_c.append(c)
        return diff_c

    def get_table(self, table_name: str) -> TTableSchema:
        return self._schema_tables[table_name]

    def get_table_columns(self, table_name: str) -> TTableSchemaColumns:
        return self._schema_tables[table_name]["columns"]

    def all_tables(self, with_dlt_tables: bool = False) -> List[TTableSchema]:
        return [t for t in self._schema_tables.values() if not t["name"].startswith("_dlt") or with_dlt_tables]

    def get_write_disposition(self, table_name: str) -> TWriteDisposition:
        table = self.get_table(table_name)
        w_d = table.get("write_disposition")
        if w_d:
            return w_d
        return self.get_write_disposition(table["parent"])

    def get_preferred_type(self, col_name: str) -> Optional[TDataType]:
        return next((m[1] for m in self._compiled_preferred_types if m[0].search(col_name)), None)

    def to_dict(self, remove_defaults: bool = False) -> TStoredSchema:
        stored_schema: TStoredSchema = {
            "version": self._version,
            "engine_version": Schema.ENGINE_VERSION,
            "name": self._schema_name,
            "tables": self._schema_tables,
            "settings": self._settings,
            "normalizers": self._normalizers_config
        }
        if remove_defaults:
            utils.remove_defaults(stored_schema)
        return stored_schema

    @property
    def schema_version(self) -> int:
        return self._version

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def schema_tables(self) -> TSchemaTables:
        return self._schema_tables

    @property
    def schema_settings(self) -> TSchemaSettings:
        return self._settings

    def as_yaml(self, remove_defaults: bool = False) -> str:
        d = self.to_dict(remove_defaults=remove_defaults)
        return cast(str, yaml.dump(d, allow_unicode=True, default_flow_style=False, sort_keys=False))

    def _infer_column(self, k: str, v: Any) -> TColumnSchema:
        return TColumnSchema(
            name=k,
            data_type=self._map_value_to_column_type(v, k),
            nullable=not self._infer_hint("not_null", v, k),
            partition=self._infer_hint("partition", v, k),
            cluster=self._infer_hint("cluster", v, k),
            sort=self._infer_hint("sort", v, k),
            unique=self._infer_hint("unique", v, k),
            primary_key=self._infer_hint("primary_key", v, k),
            foreign_key=self._infer_hint("foreign_key", v, k)
        )

    def _coerce_null_value(self, table_columns: TTableSchemaColumns, table_name: str, col_name: str) -> None:
        if col_name in table_columns:
            existing_column = table_columns[col_name]
            if not existing_column["nullable"]:
                raise CannotCoerceNullException(table_name, col_name)

    def _coerce_non_null_value(self, table_columns: TTableSchemaColumns, table_name: str, col_name: str, v: Any) -> Tuple[str, TColumnSchema, Any]:
        new_column: TColumnSchema = None
        variant_col_name = col_name

        if col_name in table_columns:
            existing_column = table_columns[col_name]
            # existing columns cannot be changed so we must update row
            py_data_type = utils.py_type_to_sc_type(type(v))
            # first try to coerce existing value into destination type
            try:
                rv = utils.coerce_type(existing_column["data_type"], py_data_type, v)
            except (ValueError, SyntaxError):
                # if that does not work we must create variant extension to the table
                variant_col_name = f"{col_name}_v_{py_data_type}"
                # if variant exists check type, coercions are not required
                if variant_col_name in table_columns:
                    if table_columns[variant_col_name]["data_type"] != py_data_type:
                        raise CannotCoerceColumnException(table_name, variant_col_name, table_columns[variant_col_name]["data_type"], py_data_type, v)
                else:
                    # add new column
                    new_column = self._infer_column(variant_col_name, v)
                    # must have variant type, not preferred or coerced type
                    new_column["data_type"] = py_data_type
                # coerce even if types are the same (complex case)
                rv = utils.coerce_type(py_data_type, py_data_type, v)
        else:
            # infer new column
            new_column = self._infer_column(col_name, v)
            # and coerce type if inference changed the python type
            py_type = utils.py_type_to_sc_type(type(v))
            rv = utils.coerce_type(new_column["data_type"], py_type, v)

        return variant_col_name, new_column, rv

    def _map_value_to_column_type(self, v: Any, k: str) -> TDataType:
        tv = type(v)
        # try to autodetect data type
        mapped_type = utils.autodetect_sc_type(self._normalizers_config.get("detections"), tv, v)
        # if not try standard type mapping
        if mapped_type is None:
            mapped_type = utils.py_type_to_sc_type(tv)
        # get preferred type based on column name
        preferred_type = self.get_preferred_type(k)
        # try to match python type to preferred
        if preferred_type:
            # try to coerce to destination type
            try:
                utils.coerce_type(preferred_type, mapped_type, v)
                # coercion possible so preferred type may be used
                mapped_type = preferred_type
            except ValueError:
                # coercion not possible
                pass
        return mapped_type

    def _infer_hint(self, hint_type: THintType, _: Any, k: str) -> bool:
        if hint_type in self._compiled_hints:
            return any(h.search(k) for h in self._compiled_hints[hint_type])
        else:
            return False

    def _add_standard_tables(self) -> None:
        self._schema_tables[Schema.VERSION_TABLE_NAME] = utils.version_table()
        self._schema_tables[Schema.LOADS_TABLE_NAME] = utils.load_table()

    def _add_standard_hints(self) -> None:
        default_hints = utils.standard_hints()
        if default_hints:
            self._settings["default_hints"] = default_hints

    def _configure_normalizers(self) -> None:
        if not self._normalizers_config:
            # create default normalizer config
            self._normalizers_config = utils.default_normalizers()
        # import desired modules
        naming_module = import_module(self._normalizers_config["names"])
        json_module = import_module(self._normalizers_config["json"]["module"])
        # name normalization functions
        self.normalize_table_name = naming_module.normalize_table_name
        self.normalize_column_name = naming_module.normalize_column_name
        self.normalize_schema_name = utils.normalize_schema_name
        self.normalize_make_dataset_name = naming_module.normalize_make_dataset_name
        self.normalize_make_path = naming_module.normalize_make_path
        self.normalize_break_path = naming_module.normalize_break_path
        # data item normalization function
        self.normalize_data_item = json_module.normalize_data_item
        json_module.extend_schema(self)

    def _compile_regexes(self) -> None:
        if self._settings:
            for pattern, dt in self._settings.get("preferred_types", {}).items():
                # add tuples to be searched in coercions
                self._compiled_preferred_types.append((utils.compile_simple_regex(pattern), dt))
            for hint_name, hint_list in self._settings.get("default_hints", {}).items():
                # compile hints which are column matching regexes
                self._compiled_hints[hint_name] = list(map(lambda hint: utils.compile_simple_regex(hint), hint_list))
        if self._schema_tables:
            for table in self._schema_tables.values():
                if "filters" in table:
                    if "excludes" in table["filters"]:
                        self._compiled_excludes[table["name"]] = list(map(lambda exclude: utils.compile_simple_regex(exclude), table["filters"]["excludes"]))
                    if "includes" in table["filters"]:
                        self._compiled_includes[table["name"]] = list(map(lambda exclude: utils.compile_simple_regex(exclude), table["filters"]["includes"]))

    def __repr__(self) -> str:
        return f"Schema {self.schema_name} at {id(self)}"
