from pathlib import Path
from contextlib import contextmanager
from typing import Any, AnyStr, Dict, Iterator, List, Optional, Sequence, Tuple, Type
import google.cloud.bigquery as bigquery  # noqa: I250
from google.cloud.bigquery.dbapi import Connection as DbApiConnection
from google.cloud import exceptions as gcp_exceptions
from google.oauth2 import service_account
from google.api_core import exceptions as api_core_exceptions


from dlt.common import json, logger
from dlt.common.typing import StrAny
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration import GcpClientConfiguration, GcpClientProductionConfiguration, make_configuration
from dlt.common.dataset_writers import TLoaderFileFormat, escape_bigquery_identifier
from dlt.common.schema import TColumnSchema, TDataType, Schema, TTableSchemaColumns

from dlt.loaders.typing import LoadJobStatus, DBCursor, TLoaderCapabilities
from dlt.loaders.client_base import JobClientBase, SqlClientBase, SqlJobClientBase, LoadJob
from dlt.loaders.exceptions import LoadClientSchemaWillNotUpdate, LoadJobNotExistsException, LoadJobServerTerminalException, LoadUnknownTableException

SCT_TO_BQT: Dict[TDataType, str] = {
    "complex": "STRING",
    "text": "STRING",
    "double": "FLOAT64",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "bigint": "INTEGER",
    "binary": "BYTES",
    "decimal": f"NUMERIC({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})",
    "wei": "BIGNUMERIC"  # non parametrized should hold wei values
}

BQT_TO_SCT: Dict[str, TDataType] = {
    "STRING": "text",
    "FLOAT": "double",
    "BOOLEAN": "bool",
    "TIMESTAMP": "timestamp",
    "INTEGER": "bigint",
    "BYTES": "binary",
    "NUMERIC": "decimal",
    "BIGNUMERIC": "decimal"
}


class BigQuerySqlClient(SqlClientBase[bigquery.Client]):
    def __init__(self, default_dataset_name: str, CREDENTIALS: Type[GcpClientConfiguration]) -> None:
        self._client: bigquery.Client = None
        self.C = CREDENTIALS
        super().__init__(default_dataset_name)
        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(CREDENTIALS.TIMEOUT)
        self.default_query = bigquery.QueryJobConfig(default_dataset=self.fully_qualified_dataset_name())

    def open_connection(self) -> None:
        credentials = service_account.Credentials.from_service_account_info(self.C.to_service_credentials())
        self._client = bigquery.Client(self.C.PROJECT_ID, credentials=credentials)

    def close_connection(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @property
    def native_connection(self) -> bigquery.Client:
        return self._client

    def has_dataset(self) -> bool:
        try:
            self._client.get_dataset(self.fully_qualified_dataset_name(), retry=self.default_retry, timeout=self.C.TIMEOUT)
            return True
        except gcp_exceptions.NotFound:
            return False

    def create_dataset(self) -> None:
        self._client.create_dataset(
            self.fully_qualified_dataset_name(),
            exists_ok=False,
            retry=self.default_retry,
            timeout=self.C.TIMEOUT
        )

    def drop_dataset(self) -> None:
        self._client.delete_dataset(
            self.fully_qualified_dataset_name(),
            not_found_ok=True,
            delete_contents=True,
            retry=self.default_retry,
            timeout=self.C.TIMEOUT
        )

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        logger.debug(f"Will execute query {sql}")  # type: ignore
        def_kwargs = {
            "job_config": self.default_query,
            "job_retry": self.default_retry,
            "timeout": self.C.TIMEOUT
            }
        kwargs = {**def_kwargs, **(kwargs or {})}
        results = self._client.query(sql, *args, **kwargs).result()
        if results:
            # consume and return all results
            return [list(r) for r in results]
        else:
            # no results were returned
            return None

    @contextmanager
    def execute_query(self, query: AnyStr,  *args: Any, **kwargs: Any) -> Iterator[DBCursor]:  # type: ignore
        conn: DbApiConnection = None
        def_kwargs = {
            "job_config": self.default_query
            }
        kwargs = {**def_kwargs, **(kwargs or {})}
        try:
            conn = DbApiConnection(client=self._client)
            curr = conn.cursor()
            curr.execute(query, *args, **kwargs)
            yield curr
        finally:
            if conn:
                # will also close all cursors
                conn.close()

    def fully_qualified_dataset_name(self) -> str:
        return f"{self.C.PROJECT_ID}.{self.default_dataset_name}"


class BigQueryLoadJob(LoadJob):
    def __init__(self, file_name: str, bq_load_job: bigquery.LoadJob, CONFIG: Type[GcpClientConfiguration]) -> None:
        self.bq_load_job = bq_load_job
        self.C = CONFIG
        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(CONFIG.TIMEOUT)
        super().__init__(file_name)

    def status(self) -> LoadJobStatus:
        # check server if done
        done = self.bq_load_job.done(retry=self.default_retry, timeout=self.C.TIMEOUT)
        if done:
            # rows processed
            if self.bq_load_job.output_rows is not None and self.bq_load_job.error_result is None:
                return "completed"
            else:
                return "failed"
        else:
            return "running"

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        exception: str = json.dumps({
            "error_result": self.bq_load_job.error_result,
            "errors": self.bq_load_job.errors,
            "job_start": self.bq_load_job.started,
            "job_end": self.bq_load_job.ended,
            "job_id": self.bq_load_job.job_id
        })
        return exception


class BigQueryClient(SqlJobClientBase):

    CONFIG: Type[GcpClientConfiguration] = None

    def __init__(self, schema: Schema) -> None:
        sql_client = BigQuerySqlClient(schema.normalize_make_dataset_name(self.CONFIG.DEFAULT_DATASET, schema.schema_name), self.CONFIG)
        super().__init__(schema, sql_client)
        self.sql_client: BigQuerySqlClient = sql_client

    def initialize_storage(self) -> None:
        if not self.sql_client.has_dataset():
            self.sql_client.create_dataset()

    def restore_file_load(self, file_path: str) -> LoadJob:
        try:
            return BigQueryLoadJob(
                JobClientBase.get_file_name_from_file_path(file_path),
                self._retrieve_load_job(file_path),
                self.CONFIG
            )
        except api_core_exceptions.NotFound:
            raise LoadJobNotExistsException(file_path)
        except (api_core_exceptions.BadRequest, api_core_exceptions.NotFound):
            raise LoadJobServerTerminalException(file_path)

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        try:
            return BigQueryLoadJob(
                JobClientBase.get_file_name_from_file_path(file_path),
                self._create_load_job(table["name"], table["write_disposition"], file_path),
                self.CONFIG
            )
        except api_core_exceptions.NotFound:
            # google.api_core.exceptions.NotFound: 404 - table not found
            raise LoadUnknownTableException(table["name"], file_path)
        except (api_core_exceptions.BadRequest, api_core_exceptions.NotFound):
            # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
            raise LoadJobServerTerminalException(file_path)
        except api_core_exceptions.Conflict:
            # google.api_core.exceptions.Conflict: 409 PUT - already exists
            return self.restore_file_load(file_path)

    def _get_schema_version_from_storage(self) -> int:
        try:
            return super()._get_schema_version_from_storage()
        except api_core_exceptions.NotFound:
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
        if not exists:
            # build CREATE
            sql = f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ")"
        else:
            # build ALTER
            sql = f"ALTER TABLE {canonical_name}\n"
            sql += ",\n".join(["ADD COLUMN " + self._get_column_def_sql(c) for c in new_columns])
        # scan columns to get hints
        cluster_list = [escape_bigquery_identifier(c["name"]) for c in new_columns if c.get("cluster", False)]
        partition_list = [escape_bigquery_identifier(c["name"]) for c in new_columns if c.get("partition", False)]
        # partition by must be added first
        if len(partition_list) > 0:
            if exists:
                raise LoadClientSchemaWillNotUpdate(canonical_name, partition_list, "Partition requested after table was created")
            elif len(partition_list) > 1:
                raise LoadClientSchemaWillNotUpdate(canonical_name, partition_list, "Partition requested for more than one column")
            else:
                sql += f"\nPARTITION BY DATE({partition_list[0]})"
        if len(cluster_list) > 0:
            if exists:
                raise LoadClientSchemaWillNotUpdate(canonical_name, cluster_list, "Clustering requested after table was created")
            else:
                sql += "\nCLUSTER BY " + ",".join(cluster_list)

        return sql

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        name = escape_bigquery_identifier(c["name"])
        return f"{name} {self._sc_t_to_bq_t(c['data_type'])} {self._gen_not_null(c['nullable'])}"

    def _get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        schema_table: TTableSchemaColumns = {}
        try:
            table = self.sql_client.native_connection.get_table(
                self.sql_client.make_qualified_table_name(table_name), retry=self.sql_client.default_retry, timeout=self.CONFIG.TIMEOUT
            )
            partition_field = table.time_partitioning.field if table.time_partitioning else None
            for c in table.schema:
                schema_c: TColumnSchema = {
                    "name": c.name,
                    "nullable": c.is_nullable,
                    "data_type": self._bq_t_to_sc_t(c.field_type, c.precision, c.scale),
                    "unique": False,
                    "sort": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "cluster": c.name in (table.clustering_fields or []),
                    "partition": c.name == partition_field
                }
                schema_table[c.name] = schema_c
            return True, schema_table
        except gcp_exceptions.NotFound:
            return False, schema_table

    def _create_load_job(self, table_name: str, write_disposition: TWriteDisposition, file_path: str) -> bigquery.LoadJob:
        bq_wd = bigquery.WriteDisposition.WRITE_APPEND if write_disposition == "append" else bigquery.WriteDisposition.WRITE_TRUNCATE
        job_id = BigQueryClient._get_job_id_from_file_path(file_path)
        job_config = bigquery.LoadJobConfig(
            autodetect=False,
            write_disposition=bq_wd,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=False,
            max_bad_records=0,

            )
        with open(file_path, "rb") as f:
            return self.sql_client.native_connection.load_table_from_file(f,
                                                     self.sql_client.make_qualified_table_name(table_name),
                                                     job_id=job_id,
                                                     job_config=job_config,
                                                     timeout=self.CONFIG.TIMEOUT
                                                    )

    def _retrieve_load_job(self, file_path: str) -> bigquery.LoadJob:
        job_id = BigQueryClient._get_job_id_from_file_path(file_path)
        return self.sql_client.native_connection.get_job(job_id)

    @staticmethod
    def _get_job_id_from_file_path(file_path: str) -> str:
        return Path(file_path).name.replace(".", "_")

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    @staticmethod
    def _sc_t_to_bq_t(sc_t: TDataType) -> str:
        return SCT_TO_BQT[sc_t]

    @staticmethod
    def _bq_t_to_sc_t(bq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if bq_t == "BIGNUMERIC":
            if precision is None:  # biggest numeric possible
                return "wei"
        return BQT_TO_SCT.get(bq_t, "text")

    @classmethod
    def capabilities(cls) -> TLoaderCapabilities:
        return {
            "preferred_loader_file_format": "jsonl",
            "supported_loader_file_formats": ["jsonl"]
        }

    @classmethod
    def configure(cls, initial_values: StrAny = None) -> Type[GcpClientConfiguration]:
        cls.CONFIG = make_configuration(GcpClientConfiguration, GcpClientProductionConfiguration, initial_values=initial_values)
        return cls.CONFIG


CLIENT = BigQueryClient
