import base64
from dlt.pipeline import Pipeline
from dlt.pipeline.typing import GCPPipelineCredentials
from examples.sources.metabase_api import MetabaseStatsApi

metabase_creds = dict(url='https://metabase-rasa-analytics.scalevector.ai/',
                 user='adrian+1@scalevector.ai',
                 password='***')

m = MetabaseStatsApi(**metabase_creds)

#gather events into separate tables

tables = {}
for tablename, row in m.get_rss_data():
    if tablename not in tables:
        tables[tablename]=[]
    tables[tablename].append(row)

for tablename, row in m.get_stateful_data():
    if tablename not in tables:
        tables[tablename]=[]
    tables[tablename].append(row)


#next let's try writing the data to bigquery
# let's create bigquery credentials
gcp_credentials_json = {
    "type": "service_account",
    "project_id": "zinc-mantra-353207",
    "private_key": "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",  # noqa
    "client_email": "data-load-tool-public-demo@zinc-mantra-353207.iam.gserviceaccount.com",
}
# we do not want to have this key verbatim in repo so we decode it here
gcp_credentials_json["private_key"] = bytes([_a ^ _b for _a, _b in zip(base64.b64decode(gcp_credentials_json["private_key"]), b"quickstart-sv"*150)]).decode("utf-8")

prefix = "test"
cred = GCPPipelineCredentials.from_services_dict(gcp_credentials_json, dataset_prefix=prefix)
# now create pipeline
p = Pipeline("metabase")
schema_path = 'examples/schemas/metabase_schema.yml'

schema = Pipeline.load_schema_from_file(schema_path)
#schema: Schema = None

p.create_pipeline(cred, schema=schema)

# we have a list of tables, we can iterate over it to put each table's data to the desired tablename
for tablename, rows in tables.items():
    p.extract(rows, table_name=tablename)
    p.unpack()

# uncomment to see nice final schema
#schema_yaml = p.get_default_schema().as_yaml(remove_defaults=True)
#f = open('examples/schemas/metabase_schema_edit.yml', "w")
#f.write(schema_yaml)
#f.close()

p.load()
