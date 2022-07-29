from contextlib import contextmanager
import os
import psycopg2
from psycopg2.sql import SQL, Identifier, Composed, Literal as SQLLiteral
from typing import Any, AnyStr, Dict, Iterator, List, Literal, Optional, Sequence, Tuple, Type

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration import PostgresConfiguration, PostgresProductionConfiguration, make_configuration
from dlt.common.dataset_writers import escape_redshift_identifier
from dlt.common.schema import COLUMN_HINTS, TColumnSchema, TColumnSchemaBase, TDataType, THintType, Schema, TTableSchemaColumns, add_missing_hints
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.typing import StrAny

from dlt.loaders.exceptions import (LoadClientSchemaWillNotUpdate, LoadClientTerminalInnerException,
                                            LoadClientTransientInnerException, LoadFileTooBig)
from dlt.loaders.typing import LoadJobStatus, DBCursor, TLoaderCapabilities, TNativeConn
from dlt.loaders.client_base import JobClientBase, SqlClientBase, SqlJobClientBase, LoadJob


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "varchar(max)",
    "text": "varchar(max)",
    "double": "double precision",
    "bool": "boolean",
    "timestamp": "timestamp with time zone",
    "bigint": "bigint",
    "binary": "varbinary",
    "decimal": f"numeric({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "varchar(max)": "text",
    "double precision": "double",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
    "bigint": "bigint",
    "binary varying": "binary",
    "numeric": "decimal"
}

HINT_TO_REDSHIFT_ATTR: Dict[THintType, str] = {
    "cluster": "DISTKEY",
    # it is better to not enforce constraints in redshift
    # "primary_key": "PRIMARY KEY",
    "sort": "SORTKEY"
}


class RedshiftSqlClient(SqlClientBase["psycopg2.connection"]):

    MAX_STATEMENT_SIZE = 16 * 1024 * 1204

    def __init__(self, default_dataset_name: str, CREDENTIALS: Type[PostgresConfiguration]) -> None:
        super().__init__(default_dataset_name)
        self._conn: psycopg2.connection = None
        self.C = CREDENTIALS

    def open_connection(self) -> None:
        self._conn = psycopg2.connect(dbname=self.C.PG_DATABASE_NAME,
                             user=self.C.PG_USER,
                             host=self.C.PG_HOST,
                             port=self.C.PG_PORT,
                             password=self.C.PG_PASSWORD,
                             connect_timeout=self.C.PG_CONNECTION_TIMEOUT,
                             options=f"-c search_path={self.fully_qualified_dataset_name()},public"
                             )
        # we'll provide explicit transactions
        self._conn.set_session(autocommit=True)

    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def native_connection(self) -> "psycopg2.connection":
        return self._conn

    def has_dataset(self) -> bool:
        query = """
                SELECT 1
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE schema_name = {};
                """
        rows = self.execute_sql(SQL(query).format(SQLLiteral(self.fully_qualified_dataset_name())))
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql(
            SQL("CREATE SCHEMA {};").format(Identifier(self.fully_qualified_dataset_name()))
            )

    def drop_dataset(self) -> None:
        self.execute_sql(
            SQL("DROP SCHEMA {} CASCADE;").format(Identifier(self.fully_qualified_dataset_name()))
            )

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        curr: DBCursor = None
        with self._conn.cursor() as curr:
            try:
                curr.execute(sql, *args, **kwargs)
                if curr.description is None:
                    return None
                else:
                    f = curr.fetchall()
                    return f
            except psycopg2.Error as outer:
                try:
                    self._conn.rollback()
                    self._conn.reset()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    @contextmanager
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBCursor]:  # type: ignore
        curr: DBCursor = None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, *args, **kwargs)
                yield curr
            except psycopg2.Error as outer:
                try:
                    self._conn.rollback()
                    self._conn.reset()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    def fully_qualified_dataset_name(self) -> str:
        return self.default_dataset_name


class RedshiftInsertLoadJob(LoadJob):
    def __init__(self, table_name: str, write_disposition: TWriteDisposition, file_path: str, sql_client: SqlClientBase["psycopg2.connection"]) -> None:
        super().__init__(JobClientBase.get_file_name_from_file_path(file_path))
        self._sql_client = sql_client
        # insert file content immediately
        self._insert(sql_client.make_qualified_table_name(table_name), write_disposition, file_path)

    def status(self) -> LoadJobStatus:
        # this job is always done
        return "completed"

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _insert(self, qualified_table_name: str, write_disposition: TWriteDisposition, file_path: str) -> None:
        # TODO: implement tracking of jobs in storage, both completed and failed
        # WARNING: maximum redshift statement is 16MB https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
        # in case of postgres: 2GiB
        if os.stat(file_path).st_size >= RedshiftSqlClient.MAX_STATEMENT_SIZE:
            # terminal exception
            raise LoadFileTooBig(file_path, RedshiftSqlClient.MAX_STATEMENT_SIZE)
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            content = f.read()
        insert_sql = [SQL("BEGIN TRANSACTION;")]
        if write_disposition == "replace":
            insert_sql.append(SQL("DELETE FROM {};").format(SQL(qualified_table_name)))
        insert_sql.extend(
            [SQL(header).format(SQL(qualified_table_name)),
            SQL(content),
            SQL("COMMIT TRANSACTION;")]
        )
        self._sql_client.execute_sql(Composed(insert_sql))

class RedshiftClient(SqlJobClientBase):

    CONFIG: Type[PostgresConfiguration] = None

    def __init__(self, schema: Schema) -> None:
        sql_client = RedshiftSqlClient(schema.normalize_make_dataset_name(self.CONFIG.DEFAULT_DATASET, schema.schema_name), self.CONFIG)
        super().__init__(schema, sql_client)
        self.sql_client = sql_client

    def initialize_storage(self) -> None:
        if not self.sql_client.has_dataset():
            self.sql_client.create_dataset()

    def restore_file_load(self, file_path: str) -> LoadJob:
        # always returns completed jobs as RedshiftInsertLoadJob is executed
        # atomically in start_file_load so any jobs that should be recreated are already completed
        # in case of bugs in loader (asking for jobs that were never created) we are not able to detect that
        return JobClientBase.make_job_with_status(file_path, "completed")

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        try:
            return RedshiftInsertLoadJob(table["name"], table["write_disposition"], file_path, self.sql_client)
        except (psycopg2.OperationalError, psycopg2.InternalError) as tr_ex:
            if tr_ex.pgerror is not None:
                if "Cannot insert a NULL value into column" in tr_ex.pgerror:
                    # NULL violations is internal error, probably a redshift thing
                    raise LoadClientTerminalInnerException("Terminal error, file will not load", tr_ex)
                if "Numeric data overflow" in tr_ex.pgerror:
                    raise LoadClientTerminalInnerException("Terminal error, file will not load", tr_ex)
                if "Precision exceeds maximum":
                    raise LoadClientTerminalInnerException("Terminal error, file will not load", tr_ex)
            raise LoadClientTransientInnerException("Error may go away, will retry", tr_ex)
        except (psycopg2.DataError, psycopg2.ProgrammingError, psycopg2.IntegrityError) as ter_ex:
            raise LoadClientTerminalInnerException("Terminal error, file will not load", ter_ex)

    def _get_schema_version_from_storage(self) -> int:
        try:
            return super()._get_schema_version_from_storage()
        except psycopg2.ProgrammingError:
            # there's no table so there's no schema
            return 0

    def _build_schema_update_sql(self) -> List[str]:
        sql_updates = []
        for table_name in self.schema.schema_tables:
            exists, storage_table = self._get_storage_table(table_name)
            sql = self._get_table_update_sql(table_name, storage_table, exists)
            if sql:
                sql_updates.append(sql)
        return sql_updates

    def _get_table_update_sql(self, table_name: str, storage_table: TTableSchemaColumns, exists: bool) -> str:
        new_columns = self._create_table_update(table_name, storage_table)
        if len(new_columns) == 0:
            # no changes
            return None
        # build sql
        canonical_name = self.sql_client.make_qualified_table_name(table_name)
        sql = "BEGIN TRANSACTION;\n"
        if not exists:
            # build CREATE
            sql += f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ");"
        else:
            # build ALTER as separate statement for each column (redshift limitation)
            sql += "\n".join([f"ALTER TABLE {canonical_name}\nADD COLUMN {self._get_column_def_sql(c)};" for c in new_columns])
        # scan columns to get hints
        if exists:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [c["name"] for c in new_columns if c.get(hint, False)]
                    raise LoadClientSchemaWillNotUpdate(canonical_name, hint_columns, f"{hint} requested after table was created")
        # TODO: add FK relations
        sql += "\nCOMMIT TRANSACTION;"
        return sql

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(HINT_TO_REDSHIFT_ATTR.get(h, "") for h in HINT_TO_REDSHIFT_ATTR.keys() if c.get(h, False) is True)
        column_name = escape_redshift_identifier(c["name"])
        return f"{column_name} {self._sc_t_to_pq_t(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    def _get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        schema_table: TTableSchemaColumns = {}
        query = f"""
                SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
                    FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{self.sql_client.fully_qualified_dataset_name()}' AND table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
        rows = self.sql_client.execute_sql(query)
        # if no rows we assume that table does not exist
        if len(rows) == 0:
            # TODO: additionally check if table exists
            return False, schema_table
        # TODO: pull more data to infer DISTKEY, PK and SORTKEY attributes/constraints
        for c in rows:
            schema_c: TColumnSchemaBase = {
                "name": c[0],
                "nullable": self._null_to_bool(c[2]),
                "data_type": self._pq_t_to_sc_t(c[1], c[3], c[4]),
            }
            schema_table[c[0]] = add_missing_hints(schema_c)
        return True, schema_table

    @staticmethod
    def _null_to_bool(v: str) -> bool:
        if v == "NO":
            return False
        elif v == "YES":
            return True
        raise ValueError(v)

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    @staticmethod
    def _sc_t_to_pq_t(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return f"numeric({DEFAULT_NUMERIC_PRECISION},0)"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _pq_t_to_sc_t(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if precision == DEFAULT_NUMERIC_PRECISION and scale == 0:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")

    @classmethod
    def capabilities(cls) -> TLoaderCapabilities:
        return {
            "preferred_loader_file_format": "insert_values",
            "supported_loader_file_formats": ["insert_values"]
        }

    @classmethod
    def configure(cls, initial_values: StrAny = None) -> Type[PostgresConfiguration]:
        cls.CONFIG = make_configuration(PostgresConfiguration, PostgresProductionConfiguration, initial_values=initial_values)
        return cls.CONFIG


CLIENT = RedshiftClient
