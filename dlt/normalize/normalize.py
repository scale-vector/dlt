from typing import Any, Callable, Type, List, Dict, Optional, Sequence, Tuple
from multiprocessing.pool import Pool as ProcessPool
from itertools import chain
from prometheus_client import Counter, CollectorRegistry, REGISTRY, Gauge
from prometheus_client.metrics import MetricWrapperBase

from dlt.common import pendulum, signals, json, logger
from dlt.common.json import custom_pua_decode
from dlt.common.runners import TRunArgs, TRunMetrics, run_pool, initialize_runner
from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.telemetry import get_logging_extras
from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItem
from dlt.common.logger import process_internal_exception
from dlt.common.exceptions import PoolException
from dlt.common.storages import SchemaStorage
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.storages.loader_storage import LoaderStorage

from dlt.normalize.configuration import configuration, NormalizeConfiguration

CONFIG: Type[NormalizeConfiguration] = None
normalize_storage: NormalizeStorage = None
load_storage: LoaderStorage = None
schema_storage: SchemaStorage = None
load_schema_storage: SchemaStorage = None
event_counter: Counter = None
event_gauge: Gauge = None
schema_version_gauge: Gauge = None
load_package_counter: Counter = None


def create_gauges(registry: CollectorRegistry) -> Tuple[MetricWrapperBase, MetricWrapperBase, MetricWrapperBase, MetricWrapperBase]:
    return (
        Counter("normalize_event_count", "Events processed in normalize", ["schema"], registry=registry),
        Gauge("normalize_last_events", "Number of events processed in last run", ["schema"], registry=registry),
        Gauge("normalize_schema_version", "Current schema version", ["schema"], registry=registry),
        Gauge("normalize_load_packages_created_count", "Count of load package created", ["schema"], registry=registry)
    )


def create_folders() -> Tuple[NormalizeStorage, LoaderStorage, SchemaStorage, SchemaStorage]:
    normalize_storage = NormalizeStorage(True, CONFIG)
    schema_storage = SchemaStorage(CONFIG.SCHEMA_VOLUME_PATH, makedirs=True)
    load_schema_storage = SchemaStorage(CONFIG.LOADING_VOLUME_PATH, makedirs=False)
    # normalize saves in preferred format but can read all supported formats
    load_storage = LoaderStorage(True, CONFIG, CONFIG.LOADER_FILE_FORMAT, LoaderStorage.ALL_SUPPORTED_FILE_FORMATS)

    normalize_storage.initialize_storage()
    load_storage.initialize_storage()

    return normalize_storage, load_storage, schema_storage, load_schema_storage


def install_schemas(default_schemas_path: str, schema_names: List[str]) -> None:
    # copy default schemas if not present
    default_schemas = SchemaStorage(default_schemas_path)
    logger.info(f"Checking default schemas in {schema_storage.storage.storage_path}")
    for name in schema_names:
        if not schema_storage.has_store_schema(name):
            logger.info(f"Schema, {name} not present in {schema_storage.storage.storage_path}, installing...")
            schema = default_schemas.load_store_schema(name)
            schema_storage.save_store_schema(schema)


def load_or_create_schema(schema_name: str) -> Schema:
    try:
        schema = schema_storage.load_store_schema(schema_name)
        logger.info(f"Loaded schema with name {schema_name} with version {schema.schema_version}")
    except FileNotFoundError:
        schema = Schema(schema_name)
        logger.info(f"Created new schema with name {schema_name}")
    return schema


# this is a worker process
def w_normalize_files(schema_name: str, load_id: str, events_files: Sequence[str]) -> TSchemaUpdate:
    normalized_data: Dict[str, List[Any]] = {}

    schema_update: TSchemaUpdate = {}
    schema = load_or_create_schema(schema_name)
    file_id = uniq_id()

    # process all event files and store rows in memory
    for events_file in events_files:
        try:
            logger.debug(f"Processing events file {events_file} in load_id {load_id} with file_id {file_id}")
            with normalize_storage.storage.open_file(events_file) as f:
                events: Sequence[TDataItem] = json.load(f)
            for event in events:
                for (table_name, parent_table), row in schema.normalize_data_item(schema, event, load_id):
                    # filter row, may eliminate some or all fields
                    row = schema.filter_row(table_name, row)
                    # do not process empty rows
                    if row:
                        # decode pua types
                        for k, v in row.items():
                            row[k] = custom_pua_decode(v)  # type: ignore
                        # check if schema can be updated
                        row, partial_table = schema.coerce_row(table_name, parent_table, row)
                        if partial_table:
                            # update schema and save the change
                            schema.update_schema(partial_table)
                            table_updates = schema_update.setdefault(table_name, [])
                            table_updates.append(partial_table)
                        # store row
                        rows = normalized_data.setdefault(table_name, [])
                        rows.append(row)
        except Exception:
            process_internal_exception(f"Exception when processing file {events_file}")
            raise PoolException("normalize_files", events_file)

    # save rows and return schema changes to be gathered in parent process
    for table_name, rows in normalized_data.items():
        # save into new jobs to processed as load
        table = schema.get_table_columns(table_name)
        load_storage.write_temp_loading_file(load_id, table_name, table, file_id, rows)

    return schema_update


TMapFuncRV = Tuple[List[TSchemaUpdate], List[Sequence[str]]]
TMapFuncType = Callable[[ProcessPool, str, str, Sequence[str]], TMapFuncRV]

def map_parallel(pool: ProcessPool, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
    # we chunk files in a way to not exceed MAX_EVENTS_IN_CHUNK and split them equally
    # between processors
    configured_processes = pool._processes  # type: ignore
    chunk_files = NormalizeStorage.chunk_by_events(files, CONFIG.MAX_EVENTS_IN_CHUNK, configured_processes)
    logger.info(f"Obtained {len(chunk_files)} processing chunks")
    param_chunk = [(schema_name, load_id, files) for files in chunk_files]
    return pool.starmap(w_normalize_files, param_chunk), chunk_files


def map_single(_: ProcessPool, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
    chunk_files = NormalizeStorage.chunk_by_events(files, CONFIG.MAX_EVENTS_IN_CHUNK, 1)
    # get in one chunk
    assert len(chunk_files) == 1
    logger.info(f"Obtained {len(chunk_files)} processing chunks")
    return [w_normalize_files(schema_name, load_id, chunk_files[0])], chunk_files


def update_schema(schema_name: str, schema_updates: List[TSchemaUpdate]) -> Schema:
    schema = load_or_create_schema(schema_name)
    # gather schema from all manifests, validate consistency and combine
    for schema_update in schema_updates:
        for table_name, table_updates in schema_update.items():
            logger.debug(f"Updating schema for table {table_name} with {len(table_updates)} deltas")
            for partial_table in table_updates:
                schema.update_schema(partial_table)
    return schema


def spool_files(pool: ProcessPool, schema_name: str, load_id: str, map_f: TMapFuncType, files: Sequence[str]) -> None:
    # process files in parallel or in single thread, depending on map_f
    schema_updates, chunk_files = map_f(pool, schema_name, load_id, files)

    schema = update_schema(schema_name, schema_updates)
    schema_version_gauge.labels(schema_name).set(schema._version)
    logger.metrics("Normalize metrics", extra=get_logging_extras([schema_version_gauge.labels(schema_name)]))
    logger.info(f"Saving schema {schema_name} with version {schema._version}, writing manifest files")
    # schema is updated, save it to schema volume
    schema_storage.save_store_schema(schema)
    # save schema and schema updates to temp load folder
    load_schema_storage.save_folder_schema(schema, load_id)
    load_storage.save_schema_updates(load_id, schema_updates)
    # files must be renamed and deleted together so do not attempt that when process is about to be terminated
    signals.raise_if_signalled()
    logger.info("Committing storage, do not kill this process")
    # rename temp folder to processing
    load_storage.commit_temp_load_folder(load_id)
    # delete event files and count events to provide metrics
    total_events = 0
    for event_file in chain.from_iterable(chunk_files):  # flatten chunks
        normalize_storage.storage.delete(event_file)
        total_events += NormalizeStorage.get_events_count(event_file)
    # log and update metrics
    logger.info(f"Chunk {load_id} processed")
    load_package_counter.labels(schema_name).inc()
    event_counter.labels(schema_name).inc(total_events)
    event_gauge.labels(schema_name).set(total_events)
    logger.metrics("Normalize metrics", extra=get_logging_extras(
        [load_package_counter.labels(schema_name), event_counter.labels(schema_name), event_gauge.labels(schema_name)]))


def spool_schema_files(pool: ProcessPool, schema_name: str, files: Sequence[str]) -> str:
    # normalized files will go here before being atomically renamed
    load_id = str(pendulum.now().timestamp())
    load_storage.create_temp_load_folder(load_id)
    logger.info(f"Created temp load folder {load_id} on loading volume")

    try:
        # process parallel
        spool_files(pool, schema_name, load_id, map_parallel, files)
    except CannotCoerceColumnException as exc:
        # schema conflicts resulting from parallel executing
        logger.warning(f"Parallel schema update conflict, switching to single thread ({str(exc)}")
        # start from scratch
        load_storage.create_temp_load_folder(load_id)
        spool_files(pool, schema_name, load_id, map_single, files)

    return load_id


def normalize(pool: ProcessPool) -> TRunMetrics:
    logger.info("Running file normalizing")
    # list files and group by schema name, list must be sorted for group by to actually work
    files = normalize_storage.list_files_to_normalize_sorted()
    logger.info(f"Found {len(files)} files, will process in chunks of {CONFIG.MAX_EVENTS_IN_CHUNK} of events")
    if len(files) == 0:
        return TRunMetrics(True, False, 0)
    # group files by schema
    for schema_name, files_in_schema in normalize_storage.get_grouped_iterator(files):
        logger.info(f"Found files in schema {schema_name}")
        spool_schema_files(pool, schema_name, list(files_in_schema))
    # return info on still pending files (if extractor saved something in the meantime)
    return TRunMetrics(False, False, len(normalize_storage.list_files_to_normalize_sorted()))


def configure(C: Type[NormalizeConfiguration], collector: CollectorRegistry, default_schemas_path: str = None, schema_names: List[str] = None) -> None:
    global CONFIG
    global normalize_storage, load_storage, schema_storage, load_schema_storage
    global event_counter, event_gauge, schema_version_gauge, load_package_counter

    CONFIG = C
    # setup singletons
    normalize_storage, load_storage, schema_storage, load_schema_storage = create_folders()
    try:
        event_counter, event_gauge, schema_version_gauge, load_package_counter = create_gauges(collector)
    except ValueError as v:
        # ignore re-creation of gauges
        if "Duplicated timeseries" not in str(v):
            raise
    if default_schemas_path and schema_names:
        install_schemas(default_schemas_path, schema_names)


def main(args: TRunArgs, default_schemas_path: str = None, schema_names: List[str] = None) -> int:
    # initialize runner
    C = configuration()
    initialize_runner(C, args)
    # create objects and gauges
    try:
        configure(C, REGISTRY, default_schemas_path, schema_names)
    except Exception:
        process_internal_exception("init module")
        return -1
    return run_pool(C, normalize)


def run_main(args: TRunArgs) -> None:
    exit(main(args))
