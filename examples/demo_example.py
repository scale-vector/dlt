# ─────── DLT ───────────────
# ───▄▄█████████▄────────────
# ──███████▄██▀▀─────────────
# ─▐████████── JSON ── JSON──
# ──█████████▄▄──────────────
# ───▀██████████▀────────────

from typing import Sequence

from dlt.common.typing import DictStrAny
from dlt.common import json
from dlt.common.schema import Schema
from dlt.pipeline import Pipeline, GCPPipelineCredentials

# the load schema will be named {pipeline_name}_{source_name}
# this allows you to easily consume multiple environments/instances of the same source
schema_prefix = 'library_documents'
schema_source_suffix = 'prod'

# you authenticate by passing a credential, such as RDBMS host/user/port/pass or gcp service account json.
gcp_credential_json_file_path = "/Users/adrian/PycharmProjects/sv/dlt/temp/scalevector-1235ac340b0b.json"

# get some data and give a name to the parent table.
# If you have a stream with different document types, pass the doc type into the table name to separate the stream.
# In our example we have a list of 2 books

data_file_path = "/Users/adrian/PycharmProjects/sv/dlt/examples/data/demo_example.json"
parent_table = 'books'

data_schema = None
data_schema_file_path = "/Users/adrian/PycharmProjects/sv/dlt/examples/schemas/inferred_demo_schema.yml"

# or pass a saved data schema file
# f = open(data_schema_file_path, "r", encoding="utf-8")
# data_schema = f.read()
#
#

"""
to get your data in a flat table, you need to join the sub-json back to the parent
example sql:

SELECT
    b.* EXCEPT(_dlt_load_id, _dlt_id, _dlt_root_id),
    bc.* EXCEPT(_dlt_parent_id, _dlt_id, _dlt_root_id),
FROM `scalevector.library_documents_prod.books` as b
left join `scalevector.library_documents_prod.books__category` as bc
on b._dlt_id = bc._dlt_parent_id

SQL result rows:
{  "isbn": "123-456-222",  "author__lastname": "Panda",  "author__firstname": "Jane",  "editor__lastname": "Smite",  "editor__firstname": "Jane",  "title": "The Ultimate Database Study Guide",  "value": "Non-Fiction",  "_dlt_list_idx": "0"}
{  "isbn": "123-456-222",  "author__lastname": "Panda",  "author__firstname": "Jane",  "editor__lastname": "Smite",  "editor__firstname": "Jane",  "title": "The Ultimate Database Study Guide",  "value": "Technology",  "_dlt_list_idx": "1"}
{  "isbn": "123-456-789",  "author__lastname": "Jayson",  "author__firstname": "Joe",  "editor__lastname": "Smite",  "editor__firstname": "Jane",  "title": "Json for big data",  "value": "SF",  "_dlt_list_idx": "0"}
{  "isbn": "123-456-789",  "author__lastname": "Jayson",  "author__firstname": "Joe",  "editor__lastname": "Smite",  "editor__firstname": "Jane",  "title": "Json for big data",  "value": "Horror",  "_dlt_list_idx": "1"}
{  "isbn": "123-456-789",  "author__lastname": "Jayson",  "author__firstname": "Joe",  "editor__lastname": "Smite",  "editor__firstname": "Jane",  "title": "Json for big data",  "value": "Dystopia",  "_dlt_list_idx": "2"}
"""  # noqa: E501


if __name__ == "__main__":

    # loading and error handling below:

    def get_json_file_data(path: str) -> Sequence[DictStrAny]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data  # type: ignore

    data = get_json_file_data(data_file_path)

    # this is example of extracting from iterator that may be a
    # - jsonl file you read line by line
    # - a list of dicts
    # - iterator over panda frame https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
    # - any iterator with map function
    # - any iterator whatsoever ie getting data from REST API

    # extracting from iterator always happens in single worker thread and iterator is directly returning data so there's no retry mechanism if anything fails

    # working BQ creds
    # credentials = Pipeline.load_gcp_credentials("_secrets/project1234_service.json", "gamma_guild")

    # working redshift creds, you can pass password as last parameter or via PG_PASSWORD env variable ie.
    # LOG_LEVEL=INFO PG_PASSWORD=.... python examples/discord_iterator.py
    credentials = GCPPipelineCredentials.from_services_file(gcp_credential_json_file_path, schema_prefix)

    pipeline = Pipeline(schema_source_suffix)

    schema: Schema = None
    # uncomment to use already modified schema, but the auto-inferred schema will also work nicely
    # TODO: schema below needs is not yet finished, more exploration needed
    # schema = Pipeline.load_schema_from_file("examples/schemas/discord_schema.yml")
    pipeline.create_pipeline(credentials, schema=schema)
    # the pipeline created a working directory. you can always attach the pipeline back by just providing the dir to
    # Pipeline::restore_pipeline
    print(pipeline.root_path)

    # and extract it
    pipeline.extract(iter(data), table_name=parent_table)

    # now create loading packages and infer the schema
    pipeline.normalize()

    schema = pipeline.get_default_schema()
    schema_yaml = schema.as_yaml()
    f = open(data_schema_file_path, "a", encoding="utf-8")
    f.write(schema_yaml)
    f.close()
    # pipeline.save_schema_to_file(data_schema_file_path, schema)

    # show loads, each load contains a copy of the schema that corresponds to the data inside
    # and a set of directories for job states (new -> in progress -> failed|completed)
    new_loads = pipeline.list_normalized_loads()
    print(new_loads)

    # load packages
    pipeline.load()

    # should be empty
    new_loads = pipeline.list_normalized_loads()
    print(new_loads)

    # now enumerate all complete loads if we have any failed packages
    # complete but failed job will not raise any exceptions
    completed_loads = pipeline.list_completed_loads()
    # print(completed_loads)
    for load_id in completed_loads:
        print(f"Checking failed jobs in {load_id}")
        for job, failed_message in pipeline.list_failed_jobs(load_id):
            print(f"JOB: {job}\nMSG: {failed_message}")
