
from dlt.common import json
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny

from dlt.pipeline import Pipeline, PostgresPipelineCredentials

# this is example of extracting from iterator that may be a
# - jsonl file you read line by line
# - a list of dicts
# - iterator over panda frame https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
# - any iterator with map function
# - any iterator whatsoever ie getting data from REST API

# extracting from iterator always happens in single worker thread and iterator is directly returning data so there's no retry mechanism if anything fails

# working BQ creds
# credentials = Pipeline.load_gcp_credentials("_secrets/project1234_service.json", "gamma_guild")

if __name__ == '__main__':
    # working redshift creds, you can pass password as last parameter or via PG_PASSWORD env variable ie.
    # LOG_LEVEL=INFO PG_PASSWORD=.... python examples/discord_iterator.py
    credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "gamma_guild_8", "loader", "3.73.90.3")

    pipeline = Pipeline("discord")
    schema: Schema = None
    # uncomment to use already modified schema, but the auto-inferred schema will also work nicely
    # TODO: schema below needs is not yet finished, more exploration needed
    # schema = Pipeline.load_schema_from_file("examples/schemas/discord_schema.yml")
    pipeline.create_pipeline(credentials, schema=schema)
    # the pipeline created a working directory. you can always attach the pipeline back by just providing the dir to
    # Pipeline::restore_pipeline
    print(pipeline.root_path)

    # load our sample data - list of discord channels
    with open("examples/data/channels.json", "r", encoding="utf-8") as f:
        channels = json.load(f)
    # and extract it
    pipeline.extract(channels, table_name="channels")
    # load list of messages
    with open("examples/data/messages.json", "r", encoding="utf-8") as f:
        messages = json.load(f)

    # but we actually want to process messages before processing so pass mapping function instead
    def processor(m: DictStrAny) -> DictStrAny:
        if "referenced_message" in m and m["referenced_message"] is not None:
            # we do not want full entity for referenced message, just the id
            m["referenced_message"] = m["referenced_message"]["id"]

        return m

    # pass mapping iterator over messages and extract
    pipeline.extract(map(processor, messages), table_name="messages")

    # from now on each pipeline does more or less the same thing: normalize and load data

    # now create loading packages and infer the schema
    pipeline.normalize()

    # show loads, each load contains a copy of the schema that corresponds to the data inside
    # and a set of directories for job states (new -> in progress -> failed|completed)
    new_loads = pipeline.list_normalized_loads()

    # get inferred schema
    # schema = pipeline.get_current_schema()
    # print(schema.as_yaml(remove_default=True))

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
