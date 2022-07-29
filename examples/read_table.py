import os

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.typing import PostgresPipelineCredentials

from examples.sources.sql_query import get_source

credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "gamma_guild_7", "loader", "chat-analytics.czwteevq7bpe.eu-central-1.redshift.amazonaws.com")

# create sql alchemy connector
conn_str = f"redshift+redshift_connector://{credentials.PG_USER}:{os.environ['PG_PASSWORD']}@{credentials.PG_HOST}:{credentials.PG_PORT}/{credentials.PG_DATABASE_NAME}"

# items = get_source("select *  from test_fixture_carbon_bot_session_cases_views.users limit 1", conn_str)


# we preserve method signature from pandas
items = get_source("select *  from mainnet_2_ethereum.blocks__transactions limit 10", conn_str, coerce_float=False)

# for i in items:
#     for k, v in i.items():
#         print(f"{k}:{v} ({type(v)}:{Schema._py_type_to_sc_type(type(v))})")

# normalize and display schema
p = Pipeline("mydb")
p.create_pipeline(credentials)
p.extract(items, table_name="blocks__transactions")
p.normalize()
print(p.get_default_schema().as_yaml(remove_defaults=True))
